package engine

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
)

const (
	MaxLevel    = 16
	Probability = 0.5
)

// tombstoneValue is the sentinel that marks a deleted key inside the MemTable.
// A nil value slice means "this key was deleted". We use a dedicated variable
// so that callers can distinguish "empty value" ([]byte{}) from "tombstone" (nil).
var tombstoneValue []byte = nil

// Node is a single element in the skip list.
type Node struct {
	key   []byte
	value []byte // nil == tombstone
	next  []*Node
}

// SkipList is a concurrent, sorted in-memory table (MemTable).
// It is a pure data structure — it does NOT own a WAL. The LSMTree
// that wraps it is responsible for WAL writes before mutating the MemTable.
type SkipList struct {
	head      *Node
	level     int
	mu        sync.RWMutex
	sizeBytes int64 // approximate memory footprint of stored keys+values
}

// NewSkipList creates an empty skip list.
func NewSkipList() *SkipList {
	return &SkipList{
		head:  &Node{next: make([]*Node, MaxLevel)},
		level: 1,
	}
}

func randomLevel() int {
	level := 1
	for rand.Float64() < Probability && level < MaxLevel {
		level++
	}
	return level
}

// SizeBytes returns the approximate memory footprint of stored data.
func (s *SkipList) SizeBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes
}

// ──────────────────────────────────────────────────────────────
//  Internal (caller must hold the lock)
// ──────────────────────────────────────────────────────────────

func (s *SkipList) insertInMemory(key, value []byte) {
	update := make([]*Node, MaxLevel)
	current := s.head

	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	// Update existing key.
	if current.next[0] != nil && bytes.Compare(current.next[0].key, key) == 0 {
		existing := current.next[0]
		s.sizeBytes -= int64(len(existing.value))
		existing.value = value
		s.sizeBytes += int64(len(value))
		return
	}

	// New node.
	newLevel := randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		s.level = newLevel
	}

	newNode := &Node{
		key:   key,
		value: value,
		next:  make([]*Node, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
	s.sizeBytes += int64(len(key) + len(value))
}

func (s *SkipList) deleteInMemory(key []byte) error {
	update := make([]*Node, s.level)
	current := s.head
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}
	target := current.next[0]
	if target == nil || bytes.Compare(target.key, key) != 0 {
		return fmt.Errorf("key not found")
	}

	for i := 0; i < s.level; i++ {
		if update[i].next[i] != target {
			break
		}
		if i >= len(target.next) {
			break
		}
		update[i].next[i] = target.next[i]
	}
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}
	s.sizeBytes -= int64(len(target.key) + len(target.value))
	return nil
}

// ──────────────────────────────────────────────────────────────
//  Public API
// ──────────────────────────────────────────────────────────────

// Put inserts or updates a key. The caller (LSMTree) must have already
// written to the WAL before calling this.
func (s *SkipList) Put(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertInMemory(key, value)
}

// Get returns the value for key. If the key holds a tombstone it returns
// (nil, true) — the caller should treat this as "deleted, stop searching
// lower levels".
func (s *SkipList) Get(key []byte) (value []byte, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	current := s.head
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
	}
	current = current.next[0]
	if current != nil && bytes.Compare(current.key, key) == 0 {
		return current.value, true
	}
	return nil, false
}

// Delete writes a tombstone for key. The actual node is kept (with value=nil)
// so that when the MemTable is flushed to an SSTable the delete propagates to
// lower levels.
func (s *SkipList) Delete(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A tombstone is simply a Put with a nil value.
	s.insertInMemory(key, tombstoneValue)
}

// RangeScan returns all nodes with startKey <= key < endKey.
func (s *SkipList) RangeScan(startKey, endKey []byte) []SSTableEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []SSTableEntry
	current := s.head
	for i := s.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, startKey) < 0 {
			current = current.next[i]
		}
	}
	current = current.next[0]
	for current != nil && bytes.Compare(current.key, endKey) < 0 {
		result = append(result, SSTableEntry{
			Key:     current.key,
			Value:   current.value,
			Deleted: current.value == nil,
		})
		current = current.next[0]
	}
	return result
}

// ──────────────────────────────────────────────────────────────
//  Flush support
// ──────────────────────────────────────────────────────────────

// Entries returns all entries in sorted key order, suitable for flushing
// to an SSTable. The caller must ensure no concurrent writes (typically
// this is called on the immutable MemTable during flush).
func (s *SkipList) Entries() []SSTableEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []SSTableEntry
	current := s.head.next[0]
	for current != nil {
		out = append(out, SSTableEntry{
			Key:     current.key,
			Value:   current.value,
			Deleted: current.value == nil,
		})
		current = current.next[0]
	}
	return out
}

// Len returns the number of entries (including tombstones).
func (s *SkipList) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	current := s.head.next[0]
	for current != nil {
		count++
		current = current.next[0]
	}
	return count
}

// ──────────────────────────────────────────────────────────────
//  Visualization accessors
// ──────────────────────────────────────────────────────────────

// NodesWithLevels returns metadata about each node for visualization.
func (s *SkipList) NodesWithLevels() []SkipListNode {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var nodes []SkipListNode
	current := s.head.next[0]
	for current != nil {
		nodes = append(nodes, SkipListNode{
			Key:     string(current.key),
			Value:   string(current.value),
			Deleted: current.value == nil,
			Level:   len(current.next),
		})
		current = current.next[0]
	}
	return nodes
}

// CurrentLevel returns the current maximum level of the skip list.
func (s *SkipList) CurrentLevel() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.level
}
