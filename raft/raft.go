package raft

import (
	"math/rand/v2"
	"slices"
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

	// Leader only, reset each time we win an election. Indexed by peer id.
	nextIndex  []int // next log index to send to that peer: our guess
	matchIndex []int // highest index known to be stored on that peer: a fact

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

// Run starts the node in the background until Stop.
func (r *Node) Run() { go r.electionLoop() }

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

// termAt returns the term of entry index, or 0 for index 0 ("before the
// first entry"). Caller holds mu.
func (r *Node) termAt(index int) int {
	if index == 0 {
		return 0
	}
	return r.log[index-1].Term
}

// lastLog returns the index and term of the last log entry, or 0, 0 for
// an empty log.
func (r *Node) lastLog() (index, term int) {
	return len(r.log), r.termAt(len(r.log))
}

// Start asks the leader to add command to the log. It returns the index
// the command will have if it is committed. It returns isLeader false on
// any other node: the caller must find the leader and try there. Start
// doesn't wait: the replication loop sends the entry within one heartbeat.
func (r *Node) Start(command []byte) (index, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != leader {
		return 0, r.currentTerm, false
	}
	r.log = append(r.log, LogEntry{Term: r.currentTerm, Command: command})
	return len(r.log), r.currentTerm, true
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

// HandleAppendEntries accepts entries from the leader if our log matches
// its log up to PrevLogIndex (Figure 2). With no entries it is just a
// heartbeat, but the same check still runs.
func (r *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. A leader from an old term is out of date: refuse, and send our
	//    term back so it steps down.
	if args.Term < r.currentTerm {
		return AppendEntriesReply{Term: r.currentTerm}
	}
	if args.Term > r.currentTerm {
		r.stepDown(args.Term)
	}
	// This is the real leader for our term, even if the check below fails.
	// A candidate in the same term lost the race, so it becomes a follower.
	r.role = follower
	r.lastHeard = time.Now()

	// 2. We must have the entry just before the new ones, with the same
	//    term. Otherwise our logs differ somewhere before it, and the leader
	//    will retry from one entry earlier.
	if args.PrevLogIndex > len(r.log) || r.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		return AppendEntriesReply{Term: r.currentTerm}
	}

	// 3-4. Walk the new entries. Skip the ones we already have. At the
	//      first one we don't have, or one that conflicts (same index,
	//      different term), cut our log there and append the rest.
	for i, e := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= len(r.log) {
			if r.log[index-1].Term == e.Term {
				continue
			}
			r.log = r.log[:index-1] // drop the conflicting entry and everything after it
		}
		r.log = append(r.log, args.Entries[i:]...)
		break
	}
	return AppendEntriesReply{Term: r.currentTerm, Success: true}
}

// --- election ---

// electionLoop checks the election timer every tick. A follower or candidate that
// hasn't heard from a leader for its whole timeout starts an election.
// A leader has no timer: it is the one sending heartbeats.
func (r *Node) electionLoop() {
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
				r.becomeLeader()
			}
		}()
	}
}

// becomeLeader starts leading currentTerm. We don't know what followers
// have, so we guess they match us (nextIndex = after our last entry) and
// let failed checks walk the guess back. We know nothing is stored
// anywhere yet, so matchIndex starts at 0. Caller holds mu.
func (r *Node) becomeLeader() {
	r.becomeLeaderState()
	go r.replicate(r.currentTerm)
}

// becomeLeaderState is becomeLeader without starting the loop, so tests
// can drive sendEntries by hand. Caller holds mu.
func (r *Node) becomeLeaderState() {
	r.role = leader
	r.nextIndex = make([]int, r.size)
	r.matchIndex = make([]int, r.size)
	for peer := range r.nextIndex {
		r.nextIndex[peer] = len(r.log) + 1
	}
}

// replicate sends every peer the entries it is missing, every
// heartbeatInterval, for as long as we are leader of term. A peer that is
// up to date gets no entries: that is the heartbeat.
func (r *Node) replicate(term int) {
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
		for peer := 0; peer < r.size; peer++ {
			if peer != r.id {
				r.sendEntries(peer, term)
			}
		}
		r.mu.Unlock()

		select {
		case <-r.done:
			return
		case <-time.After(heartbeatInterval):
		}
	}
}

// sendEntries sends peer everything from nextIndex[peer] on, and updates
// what we know about peer from the reply. Caller holds mu; the RPC runs
// in its own goroutine without it.
func (r *Node) sendEntries(peer, term int) {
	prev := r.nextIndex[peer] - 1
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.id,
		PrevLogIndex: prev,
		PrevLogTerm:  r.termAt(prev),
		// A copy taken under the lock. The RPC runs after we unlock, and
		// if we step down, a new leader may overwrite this part of our log.
		Entries: slices.Clone(r.log[prev:]),
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
			return
		}
		if r.role != leader || r.currentTerm != term {
			return // a reply to an old term's request
		}
		if reply.Success {
			// The peer now matches us up to the last entry we sent. Replies
			// can arrive out of order, so never move backwards.
			if match := prev + len(args.Entries); match > r.matchIndex[peer] {
				r.matchIndex[peer] = match
				r.nextIndex[peer] = match + 1
			}
		} else if r.nextIndex[peer] == prev+1 {
			// The peer doesn't have entry prev with our term: try one
			// earlier. Only if this reply is about our current guess; a
			// late refusal of a guess we've already moved past means nothing.
			r.nextIndex[peer] = prev
		}
	}()
}
