package engine

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// ──────────────────────────────────────────────────────────────
//  Configuration
// ──────────────────────────────────────────────────────────────

const (
	// MemTable is flushed to L0 when it reaches this size.
	DefaultMemTableLimit int64 = 4 << 20 // 4 MiB

	// When L0 has this many SSTables, a compaction into L1 is triggered.
	DefaultL0CompactionThreshold = 4
)

// Option configures an LSMTree at open time.
type Option func(*LSMTree)

// WithMemTableLimit sets the MemTable flush threshold in bytes.
func WithMemTableLimit(n int64) Option {
	return func(t *LSMTree) { t.memTableLimit = n }
}

// WithL0CompactionThreshold sets how many L0 SSTables trigger compaction.
func WithL0CompactionThreshold(n int) Option {
	return func(t *LSMTree) { t.l0CompactionThreshold = n }
}

// ──────────────────────────────────────────────────────────────
//  LSMTree
// ──────────────────────────────────────────────────────────────

// LSMTree is the top-level storage engine. It manages an active MemTable
// backed by a WAL, an optional immutable MemTable being flushed, and a
// set of on-disk SSTables organised into levels.
//
// All public methods are goroutine-safe.
type LSMTree struct {
	dir     string // directory for SSTable and WAL files
	walPath string

	mu       sync.RWMutex      // protects memTable, imm, levels
	memTable *SkipList         // active (mutable) MemTable
	imm      *SkipList         // immutable MemTable being flushed (nil when idle)
	wal      *WAL              // WAL for the active MemTable
	levels   [][]*SSTableReader // levels[0] = L0, levels[1] = L1, …

	nextSST   atomic.Int64   // monotonic counter for SSTable filenames
	flushCh   chan struct{}   // signals the background flusher
	compactCh chan struct{}   // signals the background compactor
	closeCh   chan struct{}   // closed to signal shutdown
	wg        sync.WaitGroup // tracks background goroutines

	memTableLimit         int64
	l0CompactionThreshold int

	observer Observer // nil by default
}

// SetObserver registers an observer that will receive events. Pass nil to
// disable observation. Not goroutine-safe — call before starting operations.
func (t *LSMTree) SetObserver(o Observer) {
	t.observer = o
}

// emit sends an event to the observer if one is registered.
func (t *LSMTree) emit(e Event) {
	if t.observer != nil {
		t.observer.OnEvent(e)
	}
}

// statsLocked returns a Stats snapshot. Caller must hold t.mu (read or write).
func (t *LSMTree) statsLocked() Stats {
	s := Stats{
		MemTableSize:     t.memTable.SizeBytes(),
		MemTableCapacity: t.memTableLimit,
		MemTableEntries:  t.memTable.Len(),
		L0Threshold:      t.l0CompactionThreshold,
		WALEntries:       t.wal.EntryCount(),
	}
	if t.imm != nil {
		s.ImmEntries = t.imm.Len()
	}
	if len(t.levels) > 0 {
		s.L0Count = len(t.levels[0])
	}
	if len(t.levels) > 1 {
		s.L1Count = len(t.levels[1])
	}
	return s
}

// ForceFlush triggers an immediate flush of the active MemTable regardless
// of its size. Blocks until the flush completes.
func (t *LSMTree) ForceFlush() error {
	t.mu.Lock()
	if t.memTable.Len() == 0 {
		t.mu.Unlock()
		return nil
	}
	// Rotate: current MemTable becomes immutable; create a fresh one.
	t.imm = t.memTable
	t.memTable = NewSkipList()

	t.emit(Event{Type: EventFlushBegin, Stats: t.statsLocked()})

	if err := t.wal.Reset(); err != nil {
		t.memTable = t.imm
		t.imm = nil
		t.mu.Unlock()
		return fmt.Errorf("lsm: force flush wal reset: %w", err)
	}

	t.emit(Event{Type: EventWALReset, Stats: t.statsLocked()})
	t.mu.Unlock()

	return t.flushMemTable()
}

// OpenLSMTree opens (or creates) an LSM tree rooted at dir. It recovers
// any entries from the WAL into the MemTable, and re-opens existing SSTables.
func OpenLSMTree(dir string, opts ...Option) (*LSMTree, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("lsm: mkdir %q: %w", dir, err)
	}

	walPath := filepath.Join(dir, "wal.log")
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("lsm: open wal: %w", err)
	}

	tree := &LSMTree{
		dir:                   dir,
		walPath:               walPath,
		memTable:              NewSkipList(),
		wal:                   wal,
		levels:                make([][]*SSTableReader, 2), // L0 and L1
		flushCh:               make(chan struct{}, 1),
		compactCh:             make(chan struct{}, 1),
		closeCh:               make(chan struct{}),
		memTableLimit:         DefaultMemTableLimit,
		l0CompactionThreshold: DefaultL0CompactionThreshold,
	}

	for _, opt := range opts {
		opt(tree)
	}

	// Recover WAL entries into the MemTable.
	if err := tree.recoverWAL(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("lsm: recover wal: %w", err)
	}

	// Re-open any existing SSTables on disk.
	if err := tree.loadSSTables(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("lsm: load sstables: %w", err)
	}

	// Start background goroutines.
	tree.wg.Add(2)
	go tree.flushLoop()
	go tree.compactLoop()

	return tree, nil
}

// ──────────────────────────────────────────────────────────────
//  Public API
// ──────────────────────────────────────────────────────────────

// Put inserts or updates key with value.
func (t *LSMTree) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.wal.Write(OpPut, key, value); err != nil {
		return fmt.Errorf("lsm put: wal write: %w", err)
	}
	t.emit(Event{Type: EventWALWrite, Key: key, Value: value, Stats: t.statsLocked()})

	t.memTable.Put(key, value)
	t.emit(Event{Type: EventMemTableInsert, Key: key, Value: value, Stats: t.statsLocked()})

	t.maybeScheduleFlush()
	return nil
}

// Delete writes a tombstone for key.
func (t *LSMTree) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.wal.Write(OpDelete, key, nil); err != nil {
		return fmt.Errorf("lsm delete: wal write: %w", err)
	}
	t.emit(Event{Type: EventWALWrite, Key: key, Deleted: true, Stats: t.statsLocked()})

	t.memTable.Delete(key)
	t.emit(Event{Type: EventMemTableDelete, Key: key, Deleted: true, Stats: t.statsLocked()})

	t.maybeScheduleFlush()
	return nil
}

// Get returns the value for key. Returns (nil, false) if the key does not
// exist or has been deleted.
func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	t.emit(Event{Type: EventGetBegin, Key: key, Stats: t.statsLocked()})

	// 1. Active MemTable.
	if val, found := t.memTable.Get(key); found {
		t.emit(Event{Type: EventGetMemTable, Key: key, Value: val, Found: true, Stats: t.statsLocked()})
		if val == nil {
			t.emit(Event{Type: EventGetResult, Key: key, Found: false, Deleted: true, Stats: t.statsLocked()})
			return nil, false, nil // tombstone
		}
		t.emit(Event{Type: EventGetResult, Key: key, Value: val, Found: true, Stats: t.statsLocked()})
		return val, true, nil
	}
	t.emit(Event{Type: EventGetMemTable, Key: key, Found: false, Stats: t.statsLocked()})

	// 2. Immutable MemTable (being flushed).
	if t.imm != nil {
		if val, found := t.imm.Get(key); found {
			t.emit(Event{Type: EventGetImm, Key: key, Value: val, Found: true, Stats: t.statsLocked()})
			if val == nil {
				t.emit(Event{Type: EventGetResult, Key: key, Found: false, Deleted: true, Stats: t.statsLocked()})
				return nil, false, nil
			}
			t.emit(Event{Type: EventGetResult, Key: key, Value: val, Found: true, Stats: t.statsLocked()})
			return val, true, nil
		}
		t.emit(Event{Type: EventGetImm, Key: key, Found: false, Stats: t.statsLocked()})
	}

	// 3. L0 SSTables — newest first (end of slice = newest).
	for i := len(t.levels[0]) - 1; i >= 0; i-- {
		val, found, deleted, err := t.levels[0][i].Get(key)
		if err != nil {
			return nil, false, fmt.Errorf("lsm get L0[%d]: %w", i, err)
		}
		t.emit(Event{Type: EventGetL0, Key: key, Found: found, SSTPath: t.levels[0][i].Path(), Level: 0, Stats: t.statsLocked()})
		if found {
			if deleted {
				t.emit(Event{Type: EventGetResult, Key: key, Found: false, Deleted: true, Stats: t.statsLocked()})
				return nil, false, nil
			}
			t.emit(Event{Type: EventGetResult, Key: key, Value: val, Found: true, Stats: t.statsLocked()})
			return val, true, nil
		}
	}

	// 4. L1+ SSTables — sorted by key range, non-overlapping within a level.
	for lvl := 1; lvl < len(t.levels); lvl++ {
		for i := len(t.levels[lvl]) - 1; i >= 0; i-- {
			val, found, deleted, err := t.levels[lvl][i].Get(key)
			if err != nil {
				return nil, false, fmt.Errorf("lsm get L%d[%d]: %w", lvl, i, err)
			}
			t.emit(Event{Type: EventGetL1, Key: key, Found: found, SSTPath: t.levels[lvl][i].Path(), Level: lvl, Stats: t.statsLocked()})
			if found {
				if deleted {
					t.emit(Event{Type: EventGetResult, Key: key, Found: false, Deleted: true, Stats: t.statsLocked()})
					return nil, false, nil
				}
				t.emit(Event{Type: EventGetResult, Key: key, Value: val, Found: true, Stats: t.statsLocked()})
				return val, true, nil
			}
		}
	}

	t.emit(Event{Type: EventGetResult, Key: key, Found: false, Stats: t.statsLocked()})
	return nil, false, nil
}

// Close shuts down background goroutines, flushes the active MemTable if
// non-empty, and closes all open file handles.
func (t *LSMTree) Close() error {
	// Signal background goroutines to stop.
	close(t.closeCh)
	t.wg.Wait()

	// Final flush of active MemTable.
	if t.memTable.Len() > 0 {
		if err := t.flushMemTable(); err != nil {
			return fmt.Errorf("lsm close: final flush: %w", err)
		}
	}

	// Close WAL.
	if err := t.wal.Close(); err != nil {
		return fmt.Errorf("lsm close: wal: %w", err)
	}

	// Close all open SSTable readers.
	for _, level := range t.levels {
		for _, r := range level {
			r.Close()
		}
	}
	return nil
}

// ──────────────────────────────────────────────────────────────
//  Recovery
// ──────────────────────────────────────────────────────────────

func (t *LSMTree) recoverWAL() error {
	entries, err := t.wal.ReadAll()
	if err != nil {
		return err
	}
	for _, e := range entries {
		switch e.Op {
		case OpPut:
			t.memTable.Put(e.Key, e.Value)
		case OpDelete:
			t.memTable.Delete(e.Key)
		}
	}
	return nil
}

// loadSSTables scans the data directory for existing .sst files and opens
// them. Files are assigned to levels based on their name prefix:
//
//	L0_<seq>.sst  →  level 0
//	L1_<seq>.sst  →  level 1
func (t *LSMTree) loadSSTables() error {
	entries, err := os.ReadDir(t.dir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".sst" {
			continue
		}
		path := filepath.Join(t.dir, e.Name())
		r, err := OpenSSTable(path)
		if err != nil {
			return fmt.Errorf("lsm: open %q: %w", path, err)
		}

		var level int
		var seq int64
		if _, scanErr := fmt.Sscanf(e.Name(), "L%d_%d.sst", &level, &seq); scanErr != nil {
			r.Close()
			continue // skip files that don't match
		}

		// Track highest sequence number seen.
		if seq >= t.nextSST.Load() {
			t.nextSST.Store(seq + 1)
		}

		// Grow levels slice if needed.
		for len(t.levels) <= level {
			t.levels = append(t.levels, nil)
		}
		t.levels[level] = append(t.levels[level], r)
	}

	// Sort each level by sequence number (encoded in filename / path).
	for _, lvl := range t.levels {
		sort.Slice(lvl, func(i, j int) bool {
			return lvl[i].Path() < lvl[j].Path()
		})
	}
	return nil
}

// ──────────────────────────────────────────────────────────────
//  SSTable naming
// ──────────────────────────────────────────────────────────────

func (t *LSMTree) nextSSTPath(level int) string {
	seq := t.nextSST.Add(1) - 1
	name := fmt.Sprintf("L%d_%06d.sst", level, seq)
	return filepath.Join(t.dir, name)
}

// ──────────────────────────────────────────────────────────────
//  Flush (MemTable → L0 SSTable)
// ──────────────────────────────────────────────────────────────

// maybeScheduleFlush sends a non-blocking signal to the flush goroutine
// if the active MemTable has crossed the size threshold.
// Caller must hold t.mu.
func (t *LSMTree) maybeScheduleFlush() {
	if t.memTable.SizeBytes() >= t.memTableLimit {
		select {
		case t.flushCh <- struct{}{}:
		default:
		}
	}
}

func (t *LSMTree) flushLoop() {
	defer t.wg.Done()
	for {
		select {
		case <-t.closeCh:
			return
		case <-t.flushCh:
			t.mu.Lock()
			if t.memTable.Len() == 0 {
				t.mu.Unlock()
				continue
			}
			// Rotate: current MemTable becomes immutable; create a fresh one.
			t.imm = t.memTable
			t.memTable = NewSkipList()

			t.emit(Event{Type: EventFlushBegin, Stats: t.statsLocked()})

			// Reset WAL — the immutable MemTable's data is about to be persisted
			// to an SSTable so the WAL entries for it are no longer needed.
			// New writes go to the fresh MemTable and fresh WAL.
			if err := t.wal.Reset(); err != nil {
				// WAL reset failed — we can't safely discard it.
				// Roll back rotation so nothing is lost.
				t.memTable = t.imm
				t.imm = nil
				t.mu.Unlock()
				fmt.Fprintf(os.Stderr, "lsm: wal reset failed: %v\n", err)
				continue
			}

			t.emit(Event{Type: EventWALReset, Stats: t.statsLocked()})
			t.mu.Unlock()

			if err := t.flushMemTable(); err != nil {
				fmt.Fprintf(os.Stderr, "lsm: flush failed: %v\n", err)
			}
		}
	}
}

// flushMemTable writes the immutable MemTable to a new L0 SSTable, then
// clears the immutable MemTable reference.
func (t *LSMTree) flushMemTable() error {
	t.mu.RLock()
	imm := t.imm
	if imm == nil {
		// Called from Close() path — flush the active MemTable directly.
		imm = t.memTable
	}
	t.mu.RUnlock()

	entries := imm.Entries()
	if len(entries) == 0 {
		t.mu.Lock()
		t.imm = nil
		t.mu.Unlock()
		return nil
	}

	path := t.nextSSTPath(0)
	w, err := NewSSTableWriter(path)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.Write(e); err != nil {
			return err
		}
	}
	if err := w.Finalize(); err != nil {
		return err
	}

	// Open the freshly written SSTable for reads.
	reader, err := OpenSSTable(path)
	if err != nil {
		return err
	}

	t.mu.Lock()
	// Grow levels slice if needed.
	for len(t.levels) < 1 {
		t.levels = append(t.levels, nil)
	}
	t.levels[0] = append(t.levels[0], reader)
	t.imm = nil
	needsCompaction := len(t.levels[0]) >= t.l0CompactionThreshold

	t.emit(Event{Type: EventFlushComplete, SSTPath: path, Level: 0, Stats: t.statsLocked()})
	t.mu.Unlock()

	if needsCompaction {
		select {
		case t.compactCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// ──────────────────────────────────────────────────────────────
//  Compaction (L0 → L1 merge)
// ──────────────────────────────────────────────────────────────

func (t *LSMTree) compactLoop() {
	defer t.wg.Done()
	for {
		select {
		case <-t.closeCh:
			return
		case <-t.compactCh:
			if err := t.compactL0(); err != nil {
				fmt.Fprintf(os.Stderr, "lsm: compaction failed: %v\n", err)
			}
		}
	}
}

// compactL0 merges all L0 SSTables and any overlapping L1 SSTables into
// new L1 SSTables, then removes the old files.
func (t *LSMTree) compactL0() error {
	t.mu.Lock()
	if len(t.levels[0]) < t.l0CompactionThreshold {
		t.mu.Unlock()
		return nil
	}

	t.emit(Event{Type: EventCompactBegin, Stats: t.statsLocked()})

	// Snapshot the readers we'll compact.
	l0Readers := make([]*SSTableReader, len(t.levels[0]))
	copy(l0Readers, t.levels[0])

	// Grow levels if needed.
	for len(t.levels) < 2 {
		t.levels = append(t.levels, nil)
	}
	l1Readers := make([]*SSTableReader, len(t.levels[1]))
	copy(l1Readers, t.levels[1])
	t.mu.Unlock()

	// Collect all iterators for the k-way merge.
	iters := make([]*SSTableIterator, 0, len(l0Readers)+len(l1Readers))
	for _, r := range l0Readers {
		iters = append(iters, r.Iterator())
	}
	for _, r := range l1Readers {
		iters = append(iters, r.Iterator())
	}

	// Merge all entries. L0 readers appear first (newest = higher index takes
	// priority for duplicate keys).
	merged := kWayMerge(iters, len(l0Readers))

	// Write merged entries to a new L1 SSTable.
	newPath := t.nextSSTPath(1)
	w, err := NewSSTableWriter(newPath)
	if err != nil {
		return err
	}
	for _, e := range merged {
		if err := w.Write(e); err != nil {
			return err
		}
	}
	if err := w.Finalize(); err != nil {
		return err
	}

	newReader, err := OpenSSTable(newPath)
	if err != nil {
		return err
	}

	// Swap old readers for new ones under the lock.
	t.mu.Lock()
	// Remove compacted L0 readers.
	t.levels[0] = removeReaders(t.levels[0], l0Readers)
	// Remove compacted L1 readers.
	t.levels[1] = removeReaders(t.levels[1], l1Readers)
	// Add the new L1 SSTable.
	t.levels[1] = append(t.levels[1], newReader)

	t.emit(Event{Type: EventCompactComplete, SSTPath: newPath, Level: 1, Stats: t.statsLocked()})
	t.mu.Unlock()

	// Close and delete old SSTable files.
	for _, r := range l0Readers {
		path := r.Path()
		r.Close()
		os.Remove(path)
	}
	for _, r := range l1Readers {
		path := r.Path()
		r.Close()
		os.Remove(path)
	}

	return nil
}

// removeReaders returns a new slice with all readers in toRemove excluded,
// matched by file path.
func removeReaders(all, toRemove []*SSTableReader) []*SSTableReader {
	remove := make(map[string]bool, len(toRemove))
	for _, r := range toRemove {
		remove[r.Path()] = true
	}
	kept := make([]*SSTableReader, 0, len(all))
	for _, r := range all {
		if !remove[r.Path()] {
			kept = append(kept, r)
		}
	}
	return kept
}

// ──────────────────────────────────────────────────────────────
//  k-way merge
// ──────────────────────────────────────────────────────────────

// kWayMerge merges entries from multiple SSTable iterators into a single
// sorted slice. When duplicate keys appear, the entry from the iterator
// with the higher index wins (newer data). The first numL0 iterators are
// from L0 and are ordered oldest→newest; remaining iterators are L1.
//
// For duplicate keys across L0 and L1, the latest L0 entry wins.
func kWayMerge(iters []*SSTableIterator, numL0 int) []SSTableEntry {
	// Collect all entries tagged with their source iterator index.
	type tagged struct {
		entry SSTableEntry
		src   int // iterator index; higher = newer for L0
	}

	var all []tagged
	for i, it := range iters {
		for ; it.Valid(); it.Next() {
			e := it.Entry()
			// Copy key/value so we don't alias the iterator's buffer.
			entry := SSTableEntry{
				Key:     append([]byte(nil), e.Key...),
				Deleted: e.Deleted,
			}
			if !e.Deleted {
				entry.Value = append([]byte(nil), e.Value...)
			}
			all = append(all, tagged{entry: entry, src: i})
		}
	}

	// Sort by key ascending, then by source descending (so newer entries for
	// the same key come first).
	sort.SliceStable(all, func(i, j int) bool {
		cmp := bytes.Compare(all[i].entry.Key, all[j].entry.Key)
		if cmp != 0 {
			return cmp < 0
		}
		return all[i].src > all[j].src // newer source wins
	})

	// Deduplicate: keep only the first entry per key (the newest).
	result := make([]SSTableEntry, 0, len(all))
	for i := 0; i < len(all); i++ {
		result = append(result, all[i].entry)
		// Skip remaining entries with the same key.
		for i+1 < len(all) && bytes.Equal(all[i].entry.Key, all[i+1].entry.Key) {
			i++
		}
	}
	return result
}
