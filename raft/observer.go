package raft

// RaftEventType identifies the kind of Raft event being observed.
type RaftEventType int

const (
	EventRoleChange        RaftEventType = iota // node changed role
	EventTermChange                             // term incremented
	EventVoteGranted                            // this node granted a vote
	EventVoteReceived                           // this node received a vote (as candidate)
	EventLogAppend                              // leader appended to its log
	EventCommitAdvance                          // commitIndex moved forward
	EventLogApplied                             // entry applied to state machine
	EventHeartbeatSent                          // leader sent AppendEntries to a peer
	EventHeartbeatReceived                      // follower received AppendEntries
	EventReplicationUpdate                      // matchIndex updated for a peer
	EventElectionStarted                        // node started an election
)

// PeerProgress tracks replication state for a single peer.
type PeerProgress struct {
	PeerID     string `json:"peerId"`
	NextIndex  uint64 `json:"nextIndex"`
	MatchIndex uint64 `json:"matchIndex"`
}

// RaftEvent carries details about a single Raft event for visualization.
type RaftEvent struct {
	Type         RaftEventType `json:"type"`
	NodeID       string        `json:"nodeId"`
	Role         string        `json:"role"`
	Term         uint64        `json:"term"`
	CommitIndex  uint64        `json:"commitIndex"`
	LastApplied  uint64        `json:"lastApplied"`
	LastLogIndex uint64        `json:"lastLogIndex"`
	LeaderID     string        `json:"leaderId"`
	PeerID       string        `json:"peerId,omitempty"`
	VotedFor     string        `json:"votedFor,omitempty"`
	Peers        []PeerProgress `json:"peers,omitempty"`
}

// RaftObserver receives events from a Raft node. Implementations must be
// non-blocking — OnRaftEvent is called while holding internal locks.
type RaftObserver interface {
	OnRaftEvent(RaftEvent)
}

// RaftSnapshot captures the full state of a Raft node for initial page load.
type RaftSnapshot struct {
	NodeID      string         `json:"nodeId"`
	Role        string         `json:"role"`
	Term        uint64         `json:"term"`
	CommitIndex uint64         `json:"commitIndex"`
	LastApplied uint64         `json:"lastApplied"`
	LeaderID    string         `json:"leaderId"`
	VotedFor    string         `json:"votedFor"`
	Peers       []PeerProgress `json:"peers,omitempty"`
	LogEntries  []LogEntry     `json:"logEntries"`
}
