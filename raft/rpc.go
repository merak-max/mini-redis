// Package raft implements the Raft consensus algorithm as described in
// Figure 2 of the paper "In Search of an Understandable Consensus
// Algorithm" (Ongaro and Ousterhout, 2014). Snapshots and membership
// changes are out of scope.
//
// Nodes are numbered 0..n-1. A log entry's command is opaque bytes: Raft
// orders commands, it never looks inside them.
package raft

// LogEntry is one command in the replicated log, tagged with the term of
// the leader that created it.
type LogEntry struct {
	Term    int
	Command []byte
}

// RequestVoteArgs is sent by a candidate to ask for a vote (Figure 2).
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int
	LastLogIndex int // index of the candidate's last log entry
	LastLogTerm  int // term of the candidate's last log entry
}

type RequestVoteReply struct {
	Term        int // receiver's current term, so a stale candidate steps down
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate entries. With no
// entries it is a heartbeat (Figure 2).
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderID     int
	PrevLogIndex int // index of the entry just before Entries
	PrevLogTerm  int // term of that entry
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int // receiver's current term, so a stale leader steps down
	Success bool
}

// Transport sends RPCs from one node to its peers. ok is false when no
// reply came back: the request was lost, the reply was lost, or the peer
// is down. The caller can't tell which, just like over a real network.
//
// Callers must not hold their node's lock while calling: an RPC can take
// a long time, and two nodes calling each other while holding their own
// locks would deadlock.
type Transport interface {
	RequestVote(to int, args RequestVoteArgs) (reply RequestVoteReply, ok bool)
	AppendEntries(to int, args AppendEntriesArgs) (reply AppendEntriesReply, ok bool)
}

// Handler is the receiving side of the RPCs: what a node does when a
// request arrives.
type Handler interface {
	HandleRequestVote(args RequestVoteArgs) RequestVoteReply
	HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply
}
