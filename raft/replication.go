package raft

import (
	"context"
	"fmt"
	"sort"
	"time"

	"distributed-kv/pb"
)

// broadcastAppendEntries sends AppendEntries RPCs to all peers in parallel.
func (rn *RaftNode) broadcastAppendEntries() {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return
	}
	term := rn.currentTerm
	leaderID := rn.id
	commitIndex := rn.commitIndex

	type peerSnapshot struct {
		id        string
		nextIndex uint64
		client    pb.RaftServiceClient
	}

	peers := make([]peerSnapshot, 0, len(rn.peers))
	for id, ps := range rn.peers {
		peers = append(peers, peerSnapshot{
			id:        id,
			nextIndex: ps.nextIndex,
			client:    ps.client,
		})
	}
	rn.mu.Unlock()

	for _, p := range peers {
		go rn.sendAppendEntries(p.id, p.client, p.nextIndex, term, leaderID, commitIndex)
	}
}

// sendAppendEntries sends an AppendEntries RPC to a single peer and
// handles the response (updating nextIndex/matchIndex or stepping down).
func (rn *RaftNode) sendAppendEntries(
	peerID string,
	client pb.RaftServiceClient,
	nextIndex uint64,
	term uint64,
	leaderID string,
	leaderCommit uint64,
) {
	// Build the prevLog fields.
	prevLogIndex := nextIndex - 1
	var prevLogTerm uint64
	if entry, ok := rn.log.Get(prevLogIndex); ok {
		prevLogTerm = entry.Term
	}

	// Collect entries to send.
	logEntries := rn.log.EntriesFrom(nextIndex)
	pbEntries := make([]*pb.LogEntry, len(logEntries))
	for i, e := range logEntries {
		pbEntries[i] = &pb.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Data:  e.Data,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	resp, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return
	}

	// If the peer has a higher term, step down.
	if resp.Term > term {
		rn.stepDownIfHigherTerm(resp.Term)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Ignore stale responses.
	if rn.role != Leader || rn.currentTerm != term {
		return
	}

	ps, ok := rn.peers[peerID]
	if !ok {
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex.
		newMatchIndex := prevLogIndex + uint64(len(logEntries))
		if newMatchIndex > ps.matchIndex {
			ps.matchIndex = newMatchIndex
		}
		ps.nextIndex = ps.matchIndex + 1

		// Try to advance commitIndex.
		rn.maybeAdvanceCommitIndex()
	} else {
		// Decrement nextIndex and retry (log inconsistency).
		if ps.nextIndex > 1 {
			ps.nextIndex--
		}
		// Retry with the decremented nextIndex.
		go rn.sendAppendEntries(peerID, client, ps.nextIndex, term, leaderID, rn.commitIndex)
	}
}

// maybeAdvanceCommitIndex checks if there's a new N such that a majority of
// matchIndex[i] >= N, N > commitIndex, and log[N].term == currentTerm.
// Caller must hold rn.mu.
func (rn *RaftNode) maybeAdvanceCommitIndex() {
	// Collect all matchIndex values (including leader's own last index).
	matches := make([]uint64, 0, len(rn.peers)+1)
	matches = append(matches, rn.log.LastIndex()) // leader's own
	for _, ps := range rn.peers {
		matches = append(matches, ps.matchIndex)
	}

	// Sort ascending and pick the median (majority threshold).
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
	majority := len(matches) / 2 // index of majority element
	newCommit := matches[majority]

	if newCommit > rn.commitIndex {
		// Only commit entries from the current term (Raft safety property).
		if entry, ok := rn.log.Get(newCommit); ok && entry.Term == rn.currentTerm {
			rn.commitIndex = newCommit
			rn.notifyCommit()
			fmt.Printf("[%s] commitIndex advanced to %d\n", rn.id, newCommit)
		}
	}
}
