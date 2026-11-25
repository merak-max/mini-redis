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

	done chan struct{} // closed by Stop
}

const (
	tickInterval      = 10 * time.Millisecond // how often we check the election timer
	heartbeatInterval = 50 * time.Millisecond // well below the 150ms minimum timeout
)

func NewNode(id, size int, net Transport) *Node {
	return &Node{
		id:        id,
		size:      size,
		net:       net,
		votedFor:  -1,
		lastHeard: time.Now(),
		timeout:   randomTimeout(),
		done:      make(chan struct{}),
	}
}

// Start runs the node in the background until Stop.
func (r *Node) Start() { go r.run() }

// Stop ends the node's background loops.
func (r *Node) Stop() { close(r.done) }

// State reports the node's term and whether it thinks it is the leader.
func (r *Node) State() (term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == leader
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

// --- election ---

// run checks the election timer every tick. A follower or candidate that
// hasn't heard from a leader for its whole timeout starts an election.
// A leader has no timer: it is the one sending heartbeats.
func (r *Node) run() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
		}
		r.mu.Lock()
		if r.role != leader && time.Since(r.lastHeard) >= r.timeout {
			r.startElection()
		}
		r.mu.Unlock()
	}
}

// startElection becomes a candidate in a new term and asks every peer for
// a vote. Caller holds mu. The RPCs run in their own goroutines, which
// take mu only after the reply arrives: never hold the lock during an RPC.
func (r *Node) startElection() {
	r.currentTerm++
	r.role = candidate
	r.votedFor = r.id
	r.lastHeard = time.Now()
	r.timeout = randomTimeout() // if this vote splits, retry after a new random wait

	term := r.currentTerm
	lastIndex, lastTerm := r.lastLog()
	args := RequestVoteArgs{Term: term, CandidateID: r.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	votes := 1 // our own; only touched while holding mu

	for peer := 0; peer < r.size; peer++ {
		if peer == r.id {
			continue
		}
		go func() {
			reply, ok := r.net.RequestVote(peer, args)
			if !ok {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.currentTerm {
				r.stepDown(reply.Term)
				return
			}
			// The reply may arrive late. If we have since won, lost, or
			// moved to a newer term, this vote belongs to an old election.
			if r.role != candidate || r.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > r.size/2 { // a majority: 2 of 3, 3 of 5
				r.role = leader
				go r.sendHeartbeats(term)
			}
		}()
	}
}

// sendHeartbeats sends an empty AppendEntries to every peer every
// heartbeatInterval, for as long as we are leader of term.
func (r *Node) sendHeartbeats(term int) {
	for {
		r.mu.Lock()
		if r.role != leader || r.currentTerm != term {
			r.mu.Unlock()
			return
		}
		// We are hearing from the leader: ourselves. If we step down later,
		// our election timer then starts fresh instead of having long run
		// out, so we give the new leader a chance before running again.
		r.lastHeard = time.Now()
		args := AppendEntriesArgs{Term: term, LeaderID: r.id}
		r.mu.Unlock()

		for peer := 0; peer < r.size; peer++ {
			if peer == r.id {
				continue
			}
			go func() {
				reply, ok := r.net.AppendEntries(peer, args)
				if !ok {
					return
				}
				r.mu.Lock()
				defer r.mu.Unlock()
				if reply.Term > r.currentTerm { // a newer leader exists
					r.stepDown(reply.Term)
				}
			}()
		}

		select {
		case <-r.done:
			return
		case <-time.After(heartbeatInterval):
		}
	}
}
