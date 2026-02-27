package engine

import (
	"path/filepath"
)

// SnapshotEntry is a single key-value pair in a snapshot.
type SnapshotEntry struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted,omitempty"`
}

// SkipListNode describes a node in the skip list for visualization.
type SkipListNode struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted,omitempty"`
	Level   int    `json:"level"`
}

// MemTableSnapshot captures the state of a MemTable.
type MemTableSnapshot struct {
	Entries []SnapshotEntry `json:"entries"`
	Nodes   []SkipListNode  `json:"nodes"`
	Level   int             `json:"level"`
}

// SSTIndexInfo is a sparse index entry for JSON serialization.
type SSTIndexInfo struct {
	Key    string `json:"key"`
	Offset int64  `json:"offset"`
}

// SSTInfo describes an SSTable on disk.
type SSTInfo struct {
	Path         string         `json:"path"`
	Filename     string         `json:"filename"`
	Level        int            `json:"level"`
	DataSize     int64          `json:"dataSize"`
	IndexEntries []SSTIndexInfo `json:"indexEntries"`
}

// Snapshot captures the full state of the LSM tree for visualization.
type Snapshot struct {
	MemTable MemTableSnapshot  `json:"memTable"`
	Imm      *MemTableSnapshot `json:"imm"`
	Levels   [][]SSTInfo       `json:"levels"`
	Stats    Stats             `json:"stats"`
}

// Snapshot returns a deep copy of the current LSM tree state.
func (t *LSMTree) Snapshot() Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()

	snap := Snapshot{
		MemTable: snapshotSkipList(t.memTable),
		Stats:    t.statsLocked(),
	}

	if t.imm != nil {
		immSnap := snapshotSkipList(t.imm)
		snap.Imm = &immSnap
	}

	snap.Levels = make([][]SSTInfo, len(t.levels))
	for lvl, readers := range t.levels {
		snap.Levels[lvl] = make([]SSTInfo, len(readers))
		for i, r := range readers {
			info := SSTInfo{
				Path:     r.Path(),
				Filename: filepath.Base(r.Path()),
				Level:    lvl,
				DataSize: r.DataSize(),
			}
			for _, ie := range r.IndexEntries() {
				info.IndexEntries = append(info.IndexEntries, SSTIndexInfo{
					Key:    string(ie.Key),
					Offset: ie.Offset,
				})
			}
			snap.Levels[lvl][i] = info
		}
	}

	return snap
}

func snapshotSkipList(sl *SkipList) MemTableSnapshot {
	entries := sl.Entries()
	snap := MemTableSnapshot{
		Entries: make([]SnapshotEntry, len(entries)),
		Nodes:   sl.NodesWithLevels(),
		Level:   sl.CurrentLevel(),
	}
	for i, e := range entries {
		snap.Entries[i] = SnapshotEntry{
			Key:     string(e.Key),
			Value:   string(e.Value),
			Deleted: e.Deleted,
		}
	}
	return snap
}
