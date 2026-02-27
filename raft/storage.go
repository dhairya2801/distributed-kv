package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// persistedState holds the Raft state that must survive restarts.
type persistedState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	LastApplied uint64 `json:"last_applied"`
}

// Storage handles atomic persistence of Raft hard state to disk.
type Storage struct {
	path string
}

// NewStorage creates a Storage that writes to dataDir/raft_state.json.
func NewStorage(dataDir string) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("raft storage: mkdir: %w", err)
	}
	return &Storage{
		path: filepath.Join(dataDir, "raft_state.json"),
	}, nil
}

// Load reads the persisted state from disk. Returns zero values if the
// file does not exist yet (fresh node).
func (s *Storage) Load() (persistedState, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return persistedState{}, nil
		}
		return persistedState{}, fmt.Errorf("raft storage: read: %w", err)
	}
	if len(data) == 0 {
		return persistedState{}, nil
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return persistedState{}, fmt.Errorf("raft storage: unmarshal: %w", err)
	}
	return state, nil
}

// Save atomically persists the state to disk using write-rename.
func (s *Storage) Save(state persistedState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("raft storage: marshal: %w", err)
	}
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("raft storage: write tmp: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("raft storage: rename: %w", err)
	}
	return nil
}
