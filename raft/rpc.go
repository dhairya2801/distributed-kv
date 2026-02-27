package raft

import (
	"context"

	"distributed-kv/pb"
)

// Ensure RaftNode implements pb.RaftServiceServer.
var _ pb.RaftServiceServer = (*RaftNode)(nil)

// RequestVote handles an incoming RequestVote RPC.
func (rn *RaftNode) RequestVote(_ context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &pb.VoteResponse{Term: rn.currentTerm, VoteGranted: false}

	// If the candidate's term is behind ours, reject.
	if req.Term < rn.currentTerm {
		return resp, nil
	}

	// If we see a higher term, step down.
	if req.Term > rn.currentTerm {
		rn.becomeFollower(req.Term, "")
		resp.Term = rn.currentTerm
	}

	// Grant vote if we haven't voted for someone else in this term,
	// and the candidate's log is at least as up-to-date as ours.
	alreadyVoted := rn.votedFor != "" && rn.votedFor != req.CandidateId
	if alreadyVoted {
		return resp, nil
	}

	// Log up-to-date check (§5.4.1):
	// The candidate's log is at least as up-to-date if:
	//   1. Its last log term is greater, OR
	//   2. Same last log term but last log index >= ours.
	ourLastTerm := rn.log.LastTerm()
	ourLastIndex := rn.log.LastIndex()
	logOK := req.LastLogTerm > ourLastTerm ||
		(req.LastLogTerm == ourLastTerm && req.LastLogIndex >= ourLastIndex)

	if !logOK {
		return resp, nil
	}

	// Grant vote.
	rn.votedFor = req.CandidateId
	_ = rn.persistState()
	rn.resetElectionTimer()
	resp.VoteGranted = true
	return resp, nil
}

// AppendEntries handles an incoming AppendEntries RPC.
func (rn *RaftNode) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &pb.AppendEntriesResponse{Term: rn.currentTerm, Success: false}

	// Reject if leader's term is behind ours.
	if req.Term < rn.currentTerm {
		return resp, nil
	}

	// If we see a higher (or equal) term from a leader, become follower.
	if req.Term > rn.currentTerm {
		rn.becomeFollower(req.Term, req.LeaderId)
	} else {
		// Same term — acknowledge this leader.
		rn.role = Follower
		rn.leaderID = req.LeaderId
	}
	resp.Term = rn.currentTerm

	// Reset election timer on valid AppendEntries from leader.
	rn.resetElectionTimer()

	// Check log consistency: do we have an entry at prevLogIndex with prevLogTerm?
	if req.PrevLogIndex > 0 {
		entry, ok := rn.log.Get(req.PrevLogIndex)
		if !ok || entry.Term != req.PrevLogTerm {
			return resp, nil
		}
	}

	// Append new entries (handling conflicts via truncation).
	if len(req.Entries) > 0 {
		entries := make([]LogEntry, len(req.Entries))
		for i, e := range req.Entries {
			entries[i] = LogEntry{
				Term:  e.Term,
				Index: e.Index,
				Data:  e.Data,
			}
		}
		if err := rn.log.AppendEntries(req.PrevLogIndex, entries); err != nil {
			return resp, nil
		}
	}

	// Advance commit index.
	if req.LeaderCommit > rn.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			rn.commitIndex = req.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
		rn.notifyCommit()
	}

	resp.Success = true
	return resp, nil
}
