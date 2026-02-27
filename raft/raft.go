package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"distributed-kv/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Role represents the Raft node's current role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ApplyFunc is called when a committed log entry should be applied to
// the state machine. The main package provides tree.Put/Delete here.
type ApplyFunc func(Command) error

// NotLeaderError is returned when a write is proposed to a non-leader node.
type NotLeaderError struct {
	LeaderID string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID != "" {
		return fmt.Sprintf("not leader; leader is %s", e.LeaderID)
	}
	return "not leader; leader unknown"
}

// PeerConfig describes a peer node.
type PeerConfig struct {
	ID   string
	Addr string
}

// Config holds the configuration for a RaftNode.
type Config struct {
	ID      string
	DataDir string
	Peers   []PeerConfig

	// Timing — sensible defaults are applied if zero.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

func (c *Config) applyDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 500 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
}

// peerState tracks per-peer replication state (leader only).
type peerState struct {
	nextIndex  uint64
	matchIndex uint64
	client     pb.RaftServiceClient
	conn       *grpc.ClientConn
}

// pending tracks a client proposal waiting for commit.
type pending struct {
	ch  chan error
	cmd Command
}

// RaftNode implements the Raft consensus algorithm.
type RaftNode struct {
	pb.UnimplementedRaftServiceServer
	mu sync.Mutex

	// Identity & config
	id     string
	config Config

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         *RaftLog
	storage     *Storage
	lastApplied uint64

	// Volatile state
	role        Role
	leaderID    string
	commitIndex uint64

	// Leader-only state
	peers map[string]*peerState

	// Propose → commit tracking
	pendingMu sync.Mutex
	pendings  map[uint64]*pending // log index → pending

	// Channels
	commitNotify chan struct{}
	stopCh       chan struct{}
	wg           sync.WaitGroup

	// Callback
	applyFunc ApplyFunc

	// Timer
	resetElectionCh chan struct{}
}

// NewRaftNode creates a new Raft node. Call Start() to begin operation.
func NewRaftNode(cfg Config, applyFunc ApplyFunc) (*RaftNode, error) {
	cfg.applyDefaults()

	raftLog, err := NewRaftLog(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("raft: open log: %w", err)
	}

	store, err := NewStorage(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("raft: open storage: %w", err)
	}

	state, err := store.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}

	rn := &RaftNode{
		id:              cfg.ID,
		config:          cfg,
		currentTerm:     state.CurrentTerm,
		votedFor:        state.VotedFor,
		log:             raftLog,
		storage:         store,
		lastApplied:     state.LastApplied,
		role:            Follower,
		commitIndex:     state.LastApplied, // safe lower bound
		peers:           make(map[string]*peerState),
		pendings:        make(map[uint64]*pending),
		commitNotify:    make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
		resetElectionCh: make(chan struct{}, 1),
		applyFunc:       applyFunc,
	}

	// Connect to peers.
	for _, p := range cfg.Peers {
		conn, err := grpc.NewClient(p.Addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("raft: connect to peer %s: %w", p.ID, err)
		}
		rn.peers[p.ID] = &peerState{
			nextIndex:  raftLog.LastIndex() + 1,
			matchIndex: 0,
			client:     pb.NewRaftServiceClient(conn),
			conn:       conn,
		}
	}

	return rn, nil
}

// Start begins the Raft event loop and apply loop.
func (rn *RaftNode) Start() {
	rn.wg.Add(2)
	go rn.run()
	go rn.applyLoop()
}

// Stop gracefully shuts down the Raft node.
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
	rn.wg.Wait()

	// Close peer connections.
	rn.mu.Lock()
	for _, ps := range rn.peers {
		if ps.conn != nil {
			ps.conn.Close()
		}
	}
	rn.mu.Unlock()
}

// Propose submits a command to the Raft cluster. Blocks until the command
// is committed and applied, or the context is cancelled.
// Returns NotLeaderError if this node is not the leader.
func (rn *RaftNode) Propose(ctx context.Context, cmd Command) error {
	rn.mu.Lock()
	if rn.role != Leader {
		leaderID := rn.leaderID
		rn.mu.Unlock()
		return &NotLeaderError{LeaderID: leaderID}
	}

	// Append to log.
	data := EncodeCommand(cmd)
	entry := LogEntry{Term: rn.currentTerm, Data: data}
	if err := rn.log.Append(entry); err != nil {
		rn.mu.Unlock()
		return fmt.Errorf("raft propose: %w", err)
	}

	index := rn.log.LastIndex()

	// Register pending channel.
	p := &pending{
		ch:  make(chan error, 1),
		cmd: cmd,
	}
	rn.pendingMu.Lock()
	rn.pendings[index] = p
	rn.pendingMu.Unlock()

	rn.mu.Unlock()

	// Trigger replication.
	go rn.broadcastAppendEntries()

	// Wait for commit or cancellation.
	select {
	case err := <-p.ch:
		return err
	case <-ctx.Done():
		// Clean up pending.
		rn.pendingMu.Lock()
		delete(rn.pendings, index)
		rn.pendingMu.Unlock()
		return ctx.Err()
	case <-rn.stopCh:
		return errors.New("raft: node stopped")
	}
}

// run is the main event loop handling election timeouts and heartbeats.
func (rn *RaftNode) run() {
	defer rn.wg.Done()

	electionTimeout := rn.randomElectionTimeout()
	electionTimer := time.NewTimer(electionTimeout)
	defer electionTimer.Stop()

	heartbeatTicker := time.NewTicker(rn.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-rn.stopCh:
			return

		case <-rn.resetElectionCh:
			if !electionTimer.Stop() {
				select {
				case <-electionTimer.C:
				default:
				}
			}
			electionTimer.Reset(rn.randomElectionTimeout())

		case <-electionTimer.C:
			rn.mu.Lock()
			if rn.role != Leader {
				rn.mu.Unlock()
				rn.startElection()
			} else {
				rn.mu.Unlock()
			}
			electionTimer.Reset(rn.randomElectionTimeout())

		case <-heartbeatTicker.C:
			rn.mu.Lock()
			isLeader := rn.role == Leader
			rn.mu.Unlock()
			if isLeader {
				go rn.broadcastAppendEntries()
			}
		}
	}
}

// applyLoop watches for commits and applies entries to the state machine.
func (rn *RaftNode) applyLoop() {
	defer rn.wg.Done()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-rn.commitNotify:
			rn.applyCommitted()
		}
	}
}

// applyCommitted applies all committed but not-yet-applied entries.
func (rn *RaftNode) applyCommitted() {
	rn.mu.Lock()
	commitIndex := rn.commitIndex
	lastApplied := rn.lastApplied
	rn.mu.Unlock()

	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry, ok := rn.log.Get(i)
		if !ok {
			break
		}

		var applyErr error
		if len(entry.Data) > 0 {
			cmd, err := DecodeCommand(entry.Data)
			if err != nil {
				applyErr = fmt.Errorf("raft apply: decode: %w", err)
			} else if rn.applyFunc != nil {
				applyErr = rn.applyFunc(cmd)
			}
		}

		rn.mu.Lock()
		rn.lastApplied = i
		// Persist lastApplied.
		_ = rn.storage.Save(persistedState{
			CurrentTerm: rn.currentTerm,
			VotedFor:    rn.votedFor,
			LastApplied: rn.lastApplied,
		})
		rn.mu.Unlock()

		// Signal any pending proposal.
		rn.pendingMu.Lock()
		if p, ok := rn.pendings[i]; ok {
			p.ch <- applyErr
			delete(rn.pendings, i)
		}
		rn.pendingMu.Unlock()
	}
}

// notifyCommit sends a non-blocking signal to the commit notification channel.
func (rn *RaftNode) notifyCommit() {
	select {
	case rn.commitNotify <- struct{}{}:
	default:
	}
}

// resetElectionTimer sends a non-blocking signal to reset the election timer.
func (rn *RaftNode) resetElectionTimer() {
	select {
	case rn.resetElectionCh <- struct{}{}:
	default:
	}
}

// randomElectionTimeout returns a random duration between
// ElectionTimeoutMin and ElectionTimeoutMax.
func (rn *RaftNode) randomElectionTimeout() time.Duration {
	min := rn.config.ElectionTimeoutMin
	max := rn.config.ElectionTimeoutMax
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

// persistState saves currentTerm, votedFor, and lastApplied to disk.
// Caller must hold rn.mu.
func (rn *RaftNode) persistState() error {
	return rn.storage.Save(persistedState{
		CurrentTerm: rn.currentTerm,
		VotedFor:    rn.votedFor,
		LastApplied: rn.lastApplied,
	})
}
