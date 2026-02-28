package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

const (
	sstIndexSparseness = 32       // record one index entry every N data entries
	sstFooterSize      = 16       // [index_offset:8][num_index_entries:8]
	sstWriteBufSize    = 64 << 10 // 64 KiB write buffer
	sstReadBufSize     = 64 << 10 // 64 KiB read buffer
)

// SSTableEntry is one record stored in an SSTable.
// Deleted == true signals a tombstone (the key was deleted).
type SSTableEntry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

// SSTIndexEntry is one entry in the sparse in-memory index.
type SSTIndexEntry struct {
	Key    []byte
	Offset int64 // byte offset of this entry in the data section
}

// ──────────────────────────────────────────────────────────────
//  Wire encoding helpers
// ──────────────────────────────────────────────────────────────

// sstEncodeEntry serialises e into w.
//
// Wire layout: [op:1][key_len:4][val_len:4][key bytes][value bytes]
//
// Tombstones always encode val_len=0 and write no value bytes.
// Returns the total number of bytes written.
func sstEncodeEntry(w io.Writer, e SSTableEntry) (int, error) {
	op := byte(OpPut)
	valLen := int32(len(e.Value))
	if e.Deleted {
		op = byte(OpDelete)
		valLen = 0
	}

	var hdr [9]byte
	hdr[0] = op
	binary.LittleEndian.PutUint32(hdr[1:5], uint32(len(e.Key)))
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(valLen))

	total, n := 0, 0
	var err error

	n, err = w.Write(hdr[:])
	total += n
	if err != nil {
		return total, err
	}
	n, err = w.Write(e.Key)
	total += n
	if err != nil {
		return total, err
	}
	if valLen > 0 {
		n, err = w.Write(e.Value)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// sstDecodeEntry deserialises one entry from r.
// Returns io.EOF when there are no more entries.
func sstDecodeEntry(r io.Reader) (SSTableEntry, error) {
	var hdr [9]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return SSTableEntry{}, io.EOF // partial read at section boundary = clean EOF
		}
		return SSTableEntry{}, err
	}

	op := hdr[0]
	keyLen := binary.LittleEndian.Uint32(hdr[1:5])
	valLen := binary.LittleEndian.Uint32(hdr[5:9])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return SSTableEntry{}, fmt.Errorf("sstable decode key: %w", err)
	}

	var value []byte
	if valLen > 0 {
		value = make([]byte, valLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return SSTableEntry{}, fmt.Errorf("sstable decode value: %w", err)
		}
	}

	return SSTableEntry{Key: key, Value: value, Deleted: op == OpDelete}, nil
}

// ──────────────────────────────────────────────────────────────
//  Writer
// ──────────────────────────────────────────────────────────────

// SSTableWriter writes sorted entries to a new .sst file.
// Entries MUST arrive in strictly ascending key order.
// Call Finalize once all entries have been written.
type SSTableWriter struct {
	path         string
	file         *os.File
	buf          *bufio.Writer
	index        []SSTIndexEntry
	entryCount   int
	bytesWritten int64
}

// NewSSTableWriter creates a new SSTable file at path.
func NewSSTableWriter(path string) (*SSTableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: create %q: %w", path, err)
	}
	return &SSTableWriter{
		path: path,
		file: f,
		buf:  bufio.NewWriterSize(f, sstWriteBufSize),
	}, nil
}

// Write appends one entry to the SSTable.
// Entries must be submitted in ascending key order.
func (w *SSTableWriter) Write(e SSTableEntry) error {
	// Record a sparse index entry at every sstIndexSparseness-th entry.
	if w.entryCount%sstIndexSparseness == 0 {
		w.index = append(w.index, SSTIndexEntry{
			Key:    append([]byte(nil), e.Key...),
			Offset: w.bytesWritten,
		})
	}

	n, err := sstEncodeEntry(w.buf, e)
	if err != nil {
		return fmt.Errorf("sstable: encode entry: %w", err)
	}
	w.bytesWritten += int64(n)
	w.entryCount++
	return nil
}

// Finalize flushes the data section, writes the sparse index block and the
// 16-byte footer, then closes the file.
//
// File layout after Finalize:
//
//	[data section  ] – variable length, one encoded entry after another
//	[index block   ] – sstIndexSparseness-spaced entries: [key_len:4][offset:8][key]
//	[footer 16 B   ] – [index_offset:8][num_index_entries:8]
//
// Do not use the writer after calling Finalize.
func (w *SSTableWriter) Finalize() error {
	// 1. Flush buffered data to the underlying file.
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("sstable: flush data section: %w", err)
	}

	indexOffset := w.bytesWritten

	// 2. Write the index block (data buf already flushed; write directly).
	idxBuf := bufio.NewWriter(w.file)
	for _, ie := range w.index {
		if err := binary.Write(idxBuf, binary.LittleEndian, int32(len(ie.Key))); err != nil {
			return fmt.Errorf("sstable: write index key_len: %w", err)
		}
		if err := binary.Write(idxBuf, binary.LittleEndian, ie.Offset); err != nil {
			return fmt.Errorf("sstable: write index offset: %w", err)
		}
		if _, err := idxBuf.Write(ie.Key); err != nil {
			return fmt.Errorf("sstable: write index key: %w", err)
		}
	}
	if err := idxBuf.Flush(); err != nil {
		return fmt.Errorf("sstable: flush index block: %w", err)
	}

	// 3. Write the 16-byte footer.
	var footer [sstFooterSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(w.index)))
	if _, err := w.file.Write(footer[:]); err != nil {
		return fmt.Errorf("sstable: write footer: %w", err)
	}

	return w.file.Close()
}

// ──────────────────────────────────────────────────────────────
//  Reader
// ──────────────────────────────────────────────────────────────

// SSTableReader provides point lookups and sequential iteration over a .sst
// file. Get is goroutine-safe (uses ReadAt internally); iterators are not.
type SSTableReader struct {
	path     string
	file     *os.File
	index    []SSTIndexEntry
	dataSize int64 // byte offset where the index block begins
}

// OpenSSTable opens an existing SSTable for reading.
func OpenSSTable(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %q: %w", path, err)
	}
	r := &SSTableReader{path: path, file: f}
	if err := r.loadIndex(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

// Path returns the path of the underlying .sst file.
func (r *SSTableReader) Path() string { return r.path }

// loadIndex reads the footer then loads the full sparse index into memory.
func (r *SSTableReader) loadIndex() error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size < int64(sstFooterSize) {
		return fmt.Errorf("sstable %q: file too small (%d bytes)", r.path, size)
	}

	// Read footer via ReadAt (no seek needed, concurrent-safe).
	var footer [sstFooterSize]byte
	if _, err := r.file.ReadAt(footer[:], size-int64(sstFooterSize)); err != nil {
		return fmt.Errorf("sstable: read footer: %w", err)
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	numEntries := int64(binary.LittleEndian.Uint64(footer[8:16]))
	r.dataSize = indexOffset

	indexBlockEnd := size - int64(sstFooterSize)
	indexBlockSize := indexBlockEnd - indexOffset
	if indexBlockSize < 0 || indexOffset < 0 {
		return fmt.Errorf("sstable %q: corrupt footer (index_offset=%d)", r.path, indexOffset)
	}

	// Read the entire index block in one ReadAt call.
	indexBuf := make([]byte, indexBlockSize)
	if _, err := r.file.ReadAt(indexBuf, indexOffset); err != nil {
		return fmt.Errorf("sstable: read index block: %w", err)
	}

	rd := bytes.NewReader(indexBuf)
	r.index = make([]SSTIndexEntry, 0, numEntries)
	for i := int64(0); i < numEntries; i++ {
		var keyLen int32
		if err := binary.Read(rd, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("sstable: index entry %d key_len: %w", i, err)
		}
		var off int64
		if err := binary.Read(rd, binary.LittleEndian, &off); err != nil {
			return fmt.Errorf("sstable: index entry %d offset: %w", i, err)
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(rd, key); err != nil {
			return fmt.Errorf("sstable: index entry %d key: %w", i, err)
		}
		r.index = append(r.index, SSTIndexEntry{Key: key, Offset: off})
	}
	return nil
}

// Get looks up key and returns (value, found, deleted, error).
//
// deleted==true means the most recent record for this key is a tombstone
// (the key was deleted). The caller should treat that as "not found" for
// user-facing reads, but the tombstone itself signals that no lower LSM
// level should be consulted.
func (r *SSTableReader) Get(key []byte) (value []byte, found bool, deleted bool, err error) {
	if len(r.index) == 0 {
		return nil, false, false, nil
	}

	// Binary search: last index entry whose key <= target.
	pos := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].Key, key) > 0
	}) - 1

	if pos < 0 {
		// key is smaller than the first indexed key.
		return nil, false, false, nil
	}

	startOff := r.index[pos].Offset
	// SectionReader.ReadAt is concurrent-safe; no seek required.
	sr := io.NewSectionReader(r.file, startOff, r.dataSize-startOff)
	rd := bufio.NewReaderSize(sr, sstReadBufSize)

	for {
		e, rerr := sstDecodeEntry(rd)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, false, false, fmt.Errorf("sstable get: %w", rerr)
		}
		cmp := bytes.Compare(e.Key, key)
		if cmp == 0 {
			return e.Value, true, e.Deleted, nil
		}
		if cmp > 0 {
			break // overshot; key is absent
		}
	}
	return nil, false, false, nil
}

// Iterator returns a new SSTableIterator positioned at the first entry.
// Iterators are not goroutine-safe; create one per goroutine.
func (r *SSTableReader) Iterator() *SSTableIterator {
	// SectionReader limits reads to the data section, preventing the iterator
	// from accidentally consuming bytes from the index block.
	sr := io.NewSectionReader(r.file, 0, r.dataSize)
	it := &SSTableIterator{rd: bufio.NewReaderSize(sr, sstReadBufSize)}
	it.Next() // prime: load the first entry
	return it
}

// Close releases the underlying file descriptor.
func (r *SSTableReader) Close() error { return r.file.Close() }

// ──────────────────────────────────────────────────────────────
//  Visualization accessors
// ──────────────────────────────────────────────────────────────

// KeyRange returns the smallest and largest key in this SSTable.
// Returns (nil, nil) if the SSTable is empty.
func (r *SSTableReader) KeyRange() (minKey, maxKey []byte) {
	if len(r.index) == 0 {
		return nil, nil
	}
	// The first index entry has the smallest key.
	minKey = r.index[0].Key

	// To get the true max key we scan from the last index entry to EOF.
	lastIdx := r.index[len(r.index)-1]
	sr := io.NewSectionReader(r.file, lastIdx.Offset, r.dataSize-lastIdx.Offset)
	rd := bufio.NewReaderSize(sr, sstReadBufSize)
	for {
		e, err := sstDecodeEntry(rd)
		if err != nil {
			break
		}
		maxKey = e.Key
	}
	return minKey, maxKey
}

// IndexEntries returns the sparse index for visualization.
func (r *SSTableReader) IndexEntries() []SSTIndexEntry { return r.index }

// DataSize returns the byte offset where the data section ends (index begins).
func (r *SSTableReader) DataSize() int64 { return r.dataSize }

// AllEntries reads and returns every entry in the SSTable.
func (r *SSTableReader) AllEntries() ([]SSTableEntry, error) {
	it := r.Iterator()
	var out []SSTableEntry
	for it.Valid() {
		out = append(out, it.Entry())
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// ──────────────────────────────────────────────────────────────
//  Iterator
// ──────────────────────────────────────────────────────────────

// SSTableIterator scans every entry in an SSTable in ascending key order.
// Obtain one via SSTableReader.Iterator(). Not goroutine-safe.
type SSTableIterator struct {
	rd      *bufio.Reader
	current SSTableEntry
	valid   bool
	err     error
}

// Valid reports whether the iterator points at a live entry.
func (it *SSTableIterator) Valid() bool { return it.valid }

// Entry returns the current entry. Only meaningful when Valid() is true.
func (it *SSTableIterator) Entry() SSTableEntry { return it.current }

// Error returns the first non-EOF error encountered during iteration.
func (it *SSTableIterator) Error() error { return it.err }

// Next advances to the next entry.
func (it *SSTableIterator) Next() {
	e, err := sstDecodeEntry(it.rd)
	if err == io.EOF {
		it.valid = false
		it.current = SSTableEntry{}
		return
	}
	if err != nil {
		it.err = err
		it.valid = false
		return
	}
	it.current = e
	it.valid = true
}
