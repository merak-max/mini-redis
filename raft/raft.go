package raft

import (
	"math/rand/v2"
	"sync"
	"time"
)

// role is what a node is doing right now. One field instead of
// isLeader/isCandidate booleans, so "leader and candidate at once" can't
// be represented.
type role int

const (
	follower role = iota
	candidate
	leader
)

// Node is one Raft server. All fields are guarded by mu.
type Node struct {
	mu   sync.Mutex
	id   int
	size int // number of nodes in the cluster, including this one
	net  Transport

	role        role
	currentTerm int
	votedFor    int        // who got our vote in currentTerm; -1 means nobody yet
	log         []LogEntry // log[i-1] is entry i: Raft indexes from 1

	lastHeard time.Time     // last heartbeat from a leader, or last vote we granted
	timeout   time.Duration // how long we wait before starting an election
}

func NewNode(id, size int, net Transport) *Node {
	return &Node{
		id:        id,
		size:      size,
		net:       net,
		votedFor:  -1,
		lastHeard: time.Now(),
		timeout:   randomTimeout(),
	}
}

// randomTimeout picks an election timeout in [150ms, 300ms). Each node
// picks differently, so usually one times out first and wins before the
// others even start.
func randomTimeout() time.Duration {
	return 150*time.Millisecond + rand.N(150*time.Millisecond)
}

// lastLog returns the index and term of the last log entry, or 0, 0 for
// an empty log.
func (r *Node) lastLog() (index, term int) {
	if len(r.log) == 0 {
		return 0, 0
	}
	return len(r.log), r.log[len(r.log)-1].Term
}

// stepDown moves to a newer term as a follower. Our vote was for the old
// term, so it is cleared. Caller holds mu.
func (r *Node) stepDown(term int) {
	r.currentTerm = term
	r.role = follower
	r.votedFor = -1
}

// HandleRequestVote decides whether to vote for a candidate (Figure 2).
func (r *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. A candidate from an old term is out of date: refuse, and send our
	//    term back so it steps down.
	if args.Term < r.currentTerm {
		return RequestVoteReply{Term: r.currentTerm}
	}
	// 2. A newer term means a new election: join it as a follower with a
	//    fresh vote. We do this even if we refuse the vote below.
	if args.Term > r.currentTerm {
		r.stepDown(args.Term)
	}

	// 3. One vote per term. Voting again for the same candidate is fine:
	//    our first "yes" may have been lost on the way back.
	canVote := r.votedFor == -1 || r.votedFor == args.CandidateID

	// 4. Only vote for a candidate whose log is at least as up to date as
	//    ours: a later last term wins; with equal terms, the longer log wins.
	lastIndex, lastTerm := r.lastLog()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if !canVote || !upToDate {
		return RequestVoteReply{Term: r.currentTerm}
	}
	r.votedFor = args.CandidateID
	r.lastHeard = time.Now() // someone is running an election; don't start a rival one
	return RequestVoteReply{Term: r.currentTerm, VoteGranted: true}
}

// HandleAppendEntries handles heartbeats. Log replication is milestone 3.
func (r *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return AppendEntriesReply{Term: r.currentTerm}
	}
	if args.Term > r.currentTerm {
		r.stepDown(args.Term)
	}
	// A valid leader exists for this term. A candidate in the same term
	// lost the race, so it goes back to being a follower.
	r.role = follower
	r.lastHeard = time.Now()
	return AppendEntriesReply{Term: r.currentTerm, Success: true}
}
