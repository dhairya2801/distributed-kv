package raft

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"distributed-kv/pb"

	"google.golang.org/grpc"
)

// ── Helpers ──────────────────────────────────────────────────────

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "raft-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// ── Command encode/decode ────────────────────────────────────────

func TestCommandEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		cmd  Command
	}{
		{"put", Command{Op: CmdPut, Key: []byte("hello"), Value: []byte("world")}},
		{"delete", Command{Op: CmdDelete, Key: []byte("gone"), Value: nil}},
		{"empty value", Command{Op: CmdPut, Key: []byte("k"), Value: []byte{}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeCommand(tt.cmd)
			got, err := DecodeCommand(data)
			if err != nil {
				t.Fatalf("DecodeCommand: %v", err)
			}
			if got.Op != tt.cmd.Op {
				t.Errorf("Op = %d; want %d", got.Op, tt.cmd.Op)
			}
			if string(got.Key) != string(tt.cmd.Key) {
				t.Errorf("Key = %q; want %q", got.Key, tt.cmd.Key)
			}
			if string(got.Value) != string(tt.cmd.Value) {
				t.Errorf("Value = %q; want %q", got.Value, tt.cmd.Value)
			}
		})
	}
}

func TestDecodeCommandErrors(t *testing.T) {
	// Too short.
	if _, err := DecodeCommand([]byte{1, 2}); err == nil {
		t.Fatal("expected error for short data")
	}
	// Unknown op.
	bad := EncodeCommand(Command{Op: CmdPut, Key: []byte("k"), Value: []byte("v")})
	bad[0] = 99
	if _, err := DecodeCommand(bad); err == nil {
		t.Fatal("expected error for unknown op")
	}
}

// ── RaftLog ──────────────────────────────────────────────────────

func TestRaftLogAppendAndGet(t *testing.T) {
	dir := tempDir(t)
	rl, err := NewRaftLog(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Sentinel at index 0.
	if rl.LastIndex() != 0 {
		t.Fatalf("LastIndex = %d; want 0", rl.LastIndex())
	}

	// Append entries.
	if err := rl.Append(LogEntry{Term: 1, Data: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := rl.Append(LogEntry{Term: 1, Data: []byte("b")}); err != nil {
		t.Fatal(err)
	}

	if rl.LastIndex() != 2 {
		t.Fatalf("LastIndex = %d; want 2", rl.LastIndex())
	}

	e, ok := rl.Get(1)
	if !ok || e.Term != 1 || string(e.Data) != "a" {
		t.Fatalf("Get(1) = %+v, %v; want term=1 data=a", e, ok)
	}

	e, ok = rl.Get(2)
	if !ok || string(e.Data) != "b" {
		t.Fatalf("Get(2) = %+v, %v", e, ok)
	}

	// Out of range.
	_, ok = rl.Get(99)
	if ok {
		t.Fatal("expected Get(99) to return false")
	}
}

func TestRaftLogPersistence(t *testing.T) {
	dir := tempDir(t)
	rl, err := NewRaftLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := rl.Append(LogEntry{Term: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	// Reload from disk.
	rl2, err := NewRaftLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	if rl2.LastIndex() != 1 {
		t.Fatalf("after reload: LastIndex = %d; want 1", rl2.LastIndex())
	}
	e, ok := rl2.Get(1)
	if !ok || string(e.Data) != "x" {
		t.Fatalf("after reload: Get(1) = %+v, %v", e, ok)
	}
}

func TestRaftLogTruncateOnConflict(t *testing.T) {
	dir := tempDir(t)
	rl, err := NewRaftLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Index 1, term 1.
	if err := rl.Append(LogEntry{Term: 1, Data: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	// Index 2, term 1.
	if err := rl.Append(LogEntry{Term: 1, Data: []byte("b")}); err != nil {
		t.Fatal(err)
	}

	// Overwrite index 2 with term 2 (conflict).
	err = rl.AppendEntries(1, []LogEntry{
		{Term: 2, Index: 2, Data: []byte("c")},
		{Term: 2, Index: 3, Data: []byte("d")},
	})
	if err != nil {
		t.Fatal(err)
	}

	if rl.LastIndex() != 3 {
		t.Fatalf("LastIndex = %d; want 3", rl.LastIndex())
	}
	e, _ := rl.Get(2)
	if e.Term != 2 || string(e.Data) != "c" {
		t.Fatalf("Get(2) = %+v; want term=2, data=c", e)
	}
}

// ── Storage ──────────────────────────────────────────────────────

func TestStorageSaveLoad(t *testing.T) {
	dir := tempDir(t)
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Fresh load returns zeros.
	state, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.CurrentTerm != 0 || state.VotedFor != "" {
		t.Fatalf("fresh state = %+v; want zeros", state)
	}

	// Save and reload.
	err = s.Save(persistedState{CurrentTerm: 5, VotedFor: "node1", LastApplied: 3})
	if err != nil {
		t.Fatal(err)
	}

	state, err = s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if state.CurrentTerm != 5 || state.VotedFor != "node1" || state.LastApplied != 3 {
		t.Fatalf("state = %+v; want term=5, votedFor=node1, lastApplied=3", state)
	}
}

// ── Integration: 3-node cluster ──────────────────────────────────

type testCluster struct {
	nodes   []*RaftNode
	servers []*grpc.Server
	dirs    []string

	mu      sync.Mutex
	applied map[string]string // key → value from apply callbacks
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	tc := &testCluster{
		applied: make(map[string]string),
	}

	// Allocate listeners first so we know addresses.
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		addrs[i] = lis.Addr().String()
	}

	for i := 0; i < n; i++ {
		dir := tempDir(t)
		tc.dirs = append(tc.dirs, dir)

		nodeID := fmt.Sprintf("node%d", i)
		var peers []PeerConfig
		for j := 0; j < n; j++ {
			if j == i {
				continue
			}
			peers = append(peers, PeerConfig{
				ID:   fmt.Sprintf("node%d", j),
				Addr: addrs[j],
			})
		}

		applyFn := func(cmd Command) error {
			tc.mu.Lock()
			defer tc.mu.Unlock()
			switch cmd.Op {
			case CmdPut:
				tc.applied[string(cmd.Key)] = string(cmd.Value)
			case CmdDelete:
				delete(tc.applied, string(cmd.Key))
			}
			return nil
		}

		cfg := Config{
			ID:                 nodeID,
			DataDir:            dir,
			Peers:              peers,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		}

		rn, err := NewRaftNode(cfg, applyFn)
		if err != nil {
			t.Fatal(err)
		}

		srv := grpc.NewServer()
		pb.RegisterRaftServiceServer(srv, rn)
		go srv.Serve(listeners[i])

		tc.nodes = append(tc.nodes, rn)
		tc.servers = append(tc.servers, srv)
	}

	// Start all nodes.
	for _, rn := range tc.nodes {
		rn.Start()
	}

	t.Cleanup(func() {
		for _, rn := range tc.nodes {
			rn.Stop()
		}
		for _, srv := range tc.servers {
			srv.GracefulStop()
		}
	})

	return tc
}

// waitForLeader polls until a leader is elected or timeout.
func (tc *testCluster) waitForLeader(t *testing.T, timeout time.Duration) *RaftNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, rn := range tc.nodes {
			rn.mu.Lock()
			isLeader := rn.role == Leader
			rn.mu.Unlock()
			if isLeader {
				return rn
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func TestRaftElection(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.waitForLeader(t, 5*time.Second)

	leader.mu.Lock()
	term := leader.currentTerm
	id := leader.id
	leader.mu.Unlock()

	if term == 0 {
		t.Fatal("leader term should be > 0")
	}
	t.Logf("leader: %s, term: %d", id, term)

	// Verify exactly one leader.
	leaders := 0
	for _, rn := range tc.nodes {
		rn.mu.Lock()
		if rn.role == Leader {
			leaders++
		}
		rn.mu.Unlock()
	}
	if leaders != 1 {
		t.Fatalf("expected 1 leader, got %d", leaders)
	}
}

func TestRaftReplication(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.waitForLeader(t, 5*time.Second)

	// Propose a Put.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := leader.Propose(ctx, Command{Op: CmdPut, Key: []byte("foo"), Value: []byte("bar")})
	if err != nil {
		t.Fatalf("Propose put: %v", err)
	}

	// Wait for all nodes to apply.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tc.mu.Lock()
		val, ok := tc.applied["foo"]
		tc.mu.Unlock()
		if ok && val == "bar" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	tc.mu.Lock()
	val, ok := tc.applied["foo"]
	tc.mu.Unlock()
	if !ok || val != "bar" {
		t.Fatalf("applied[foo] = %q, %v; want bar, true", val, ok)
	}

	// Propose a Delete.
	err = leader.Propose(ctx, Command{Op: CmdDelete, Key: []byte("foo")})
	if err != nil {
		t.Fatalf("Propose delete: %v", err)
	}

	// Wait for delete to apply.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		tc.mu.Lock()
		_, exists := tc.applied["foo"]
		tc.mu.Unlock()
		if !exists {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	tc.mu.Lock()
	_, exists := tc.applied["foo"]
	tc.mu.Unlock()
	if exists {
		t.Fatal("expected foo to be deleted")
	}
}

func TestProposeToFollower(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.waitForLeader(t, 5*time.Second)

	// Find a follower.
	var follower *RaftNode
	for _, rn := range tc.nodes {
		rn.mu.Lock()
		if rn.role == Follower {
			follower = rn
			rn.mu.Unlock()
			break
		}
		rn.mu.Unlock()
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := follower.Propose(ctx, Command{Op: CmdPut, Key: []byte("x"), Value: []byte("y")})
	if err == nil {
		t.Fatal("expected NotLeaderError from follower")
	}
	if _, ok := err.(*NotLeaderError); !ok {
		t.Fatalf("expected *NotLeaderError, got %T: %v", err, err)
	}
}
