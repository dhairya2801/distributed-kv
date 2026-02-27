package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// LogEntry is a single entry in the Raft log (in-memory representation).
type LogEntry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Data  []byte `json:"data"` // encoded Command
}

// RaftLog is a persistent, append-only log with 1-based indexing.
// entries[0] is a sentinel (index=0, term=0) so that prevLogIndex=0
// works without special cases.
type RaftLog struct {
	mu      sync.Mutex
	entries []LogEntry
	path    string // JSON file for persistence
}

// NewRaftLog creates or loads a Raft log from dataDir/raft_log.json.
func NewRaftLog(dataDir string) (*RaftLog, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("raftlog: mkdir: %w", err)
	}

	rl := &RaftLog{
		path:    filepath.Join(dataDir, "raft_log.json"),
		entries: []LogEntry{{Term: 0, Index: 0}}, // sentinel
	}

	if err := rl.load(); err != nil {
		return nil, err
	}
	return rl, nil
}

// Append adds an entry to the log and persists it.
func (rl *RaftLog) Append(entry LogEntry) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	entry.Index = uint64(len(rl.entries))
	rl.entries = append(rl.entries, entry)
	return rl.persist()
}

// AppendEntries appends multiple entries starting at the given index,
// truncating any conflicting entries first. Used by AppendEntries RPC.
func (rl *RaftLog) AppendEntries(prevIndex uint64, entries []LogEntry) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for _, e := range entries {
		idx := e.Index
		if idx < uint64(len(rl.entries)) {
			// Existing entry — check for conflict.
			if rl.entries[idx].Term != e.Term {
				// Conflict: truncate from here and append.
				rl.entries = rl.entries[:idx]
				rl.entries = append(rl.entries, e)
			}
			// Otherwise, entry matches — skip.
		} else {
			rl.entries = append(rl.entries, e)
		}
	}
	return rl.persist()
}

// Get returns the entry at the given index. Returns false if out of range.
func (rl *RaftLog) Get(index uint64) (LogEntry, bool) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if index >= uint64(len(rl.entries)) {
		return LogEntry{}, false
	}
	return rl.entries[index], true
}

// LastIndex returns the index of the last log entry.
func (rl *RaftLog) LastIndex() uint64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return uint64(len(rl.entries) - 1)
}

// LastTerm returns the term of the last log entry.
func (rl *RaftLog) LastTerm() uint64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.entries[len(rl.entries)-1].Term
}

// EntriesFrom returns all entries from startIndex onwards (inclusive).
func (rl *RaftLog) EntriesFrom(startIndex uint64) []LogEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if startIndex >= uint64(len(rl.entries)) {
		return nil
	}
	// Return a copy to avoid data races.
	result := make([]LogEntry, uint64(len(rl.entries))-startIndex)
	copy(result, rl.entries[startIndex:])
	return result
}

// Len returns the total number of entries including the sentinel.
func (rl *RaftLog) Len() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return len(rl.entries)
}

// persist writes the log to disk (caller must hold rl.mu).
func (rl *RaftLog) persist() error {
	data, err := json.Marshal(rl.entries[1:]) // skip sentinel
	if err != nil {
		return fmt.Errorf("raftlog: marshal: %w", err)
	}

	tmpPath := rl.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("raftlog: write tmp: %w", err)
	}
	if err := os.Rename(tmpPath, rl.path); err != nil {
		return fmt.Errorf("raftlog: rename: %w", err)
	}
	return nil
}

// load reads the log from disk if it exists (caller: constructor only).
func (rl *RaftLog) load() error {
	data, err := os.ReadFile(rl.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // fresh log
		}
		return fmt.Errorf("raftlog: read: %w", err)
	}
	if len(data) == 0 {
		return nil
	}

	var entries []LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("raftlog: unmarshal: %w", err)
	}
	rl.entries = append(rl.entries, entries...) // sentinel + loaded entries
	return nil
}
