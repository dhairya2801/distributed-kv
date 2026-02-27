package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"distributed-kv/pb"
)

// startElection transitions to Candidate and requests votes from all peers.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.leaderID = ""
	term := rn.currentTerm
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()

	if err := rn.persistState(); err != nil {
		fmt.Printf("[%s] election: persist state: %v\n", rn.id, err)
	}

	// Collect peer clients under lock.
	type peerInfo struct {
		id     string
		client pb.RaftServiceClient
	}
	peers := make([]peerInfo, 0, len(rn.peers))
	for id, ps := range rn.peers {
		peers = append(peers, peerInfo{id: id, client: ps.client})
	}
	rn.mu.Unlock()

	rn.resetElectionTimer()

	// Start with our own vote.
	var (
		mu       sync.Mutex
		votes    = 1 // self-vote
		finished bool
	)
	majority := (len(peers)+1)/2 + 1

	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(peerID string, client pb.RaftServiceClient) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			resp, err := client.RequestVote(ctx, &pb.VoteRequest{
				Term:         term,
				CandidateId:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				return
			}

			// If the peer has a higher term, step down.
			if resp.Term > term {
				rn.stepDownIfHigherTerm(resp.Term)
				return
			}

			if resp.VoteGranted {
				mu.Lock()
				votes++
				won := votes >= majority && !finished
				if won {
					finished = true
				}
				mu.Unlock()

				if won {
					rn.becomeLeader()
				}
			}
		}(p.id, p.client)
	}

	// If single-node cluster, we already have majority.
	if majority <= 1 {
		rn.becomeLeader()
	}

	wg.Wait()
}

// becomeLeader transitions this node to Leader and initialises peer state.
func (rn *RaftNode) becomeLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Guard: only a candidate in the expected term becomes leader.
	if rn.role != Candidate {
		return
	}

	rn.role = Leader
	rn.leaderID = rn.id
	fmt.Printf("[%s] became leader for term %d\n", rn.id, rn.currentTerm)

	// Reinitialise nextIndex and matchIndex for all peers.
	lastIndex := rn.log.LastIndex()
	for _, ps := range rn.peers {
		ps.nextIndex = lastIndex + 1
		ps.matchIndex = 0
	}

	// Send an immediate heartbeat (empty AppendEntries) to assert leadership.
	go rn.broadcastAppendEntries()
}

// becomeFollower transitions this node to Follower for the given term.
// Caller must hold rn.mu.
func (rn *RaftNode) becomeFollower(term uint64, leaderID string) {
	rn.role = Follower
	rn.currentTerm = term
	rn.votedFor = ""
	rn.leaderID = leaderID
	_ = rn.persistState()
}

// stepDownIfHigherTerm checks if the given term is higher than ours and
// steps down to follower if so. Returns true if we stepped down.
func (rn *RaftNode) stepDownIfHigherTerm(term uint64) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if term > rn.currentTerm {
		rn.becomeFollower(term, "")
		rn.resetElectionTimer()
		return true
	}
	return false
}
