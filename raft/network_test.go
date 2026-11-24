package raft

import (
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// network is an in-memory fake network for tests. It delivers RPCs by
// calling the receiver's Handler directly, and can simulate the failures
// Raft must survive:
//
//   - disconnect: a node sends and receives nothing (crashed, or its
//     cable is pulled), while its goroutines keep running.
//   - partition: nodes can only talk to nodes in the same group.
//   - unreliable: random delays, and random loss of requests and replies.
//
// A message can be lost before the receiver runs (it never happens) or
// after (it happened but the sender never hears). Raft must handle both.
type network struct {
	mu         sync.Mutex
	handlers   map[int]Handler
	connected  map[int]bool
	group      map[int]int // partition group; all 0 means no partition
	unreliable bool
	rpcs       int // RPCs attempted, for tests that check traffic
}

func newNetwork() *network {
	return &network{
		handlers:  map[int]Handler{},
		connected: map[int]bool{},
		group:     map[int]int{},
	}
}

// add registers node id, connected and in group 0.
func (n *network) add(id int, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.connected[id] = true
	n.group[id] = 0
}

// endpoint returns the Transport that node id uses to send.
func (n *network) endpoint(id int) Transport {
	return endpoint{net: n, from: id}
}

func (n *network) disconnect(id int) { n.setConnected(id, false) }
func (n *network) connect(id int)    { n.setConnected(id, true) }

func (n *network) setConnected(id int, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = up
}

// partition splits the nodes into groups that can't reach each other.
// partition([]int{0, 1}, []int{2}) cuts node 2 off from 0 and 1. Nodes not
// listed go into group 0 with the first list.
func (n *network) partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.group {
		n.group[id] = 0
	}
	for g, ids := range groups {
		for _, id := range ids {
			n.group[id] = g
		}
	}
}

// heal removes any partition. Disconnected nodes stay disconnected.
func (n *network) heal() { n.partition() }

func (n *network) setUnreliable(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.unreliable = on
}

func (n *network) rpcCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rpcs
}

// canTalk reports whether a message can go from one node to another.
// Caller must hold n.mu.
func (n *network) canTalk(from, to int) bool {
	return n.connected[from] && n.connected[to] && n.group[from] == n.group[to]
}

// deliver runs rpc against the receiver's handler if the network lets the
// request through, and reports whether the reply made it back.
// n.mu is not held while the handler runs: the handler takes its own
// node's lock, and may be slow.
func (n *network) deliver(from, to int, rpc func(h Handler)) bool {
	n.mu.Lock()
	n.rpcs++
	h, exists := n.handlers[to]
	reachable := exists && n.canTalk(from, to)
	unreliable := n.unreliable
	n.mu.Unlock()

	if unreliable {
		time.Sleep(time.Duration(rand.IntN(27)) * time.Millisecond)
		if rand.IntN(10) == 0 { // 10%: request lost
			return false
		}
	}
	if !reachable {
		// No reply ever comes. A real RPC would wait for a timeout, so
		// make the sender wait a little instead of failing instantly.
		time.Sleep(time.Duration(rand.IntN(50)) * time.Millisecond)
		return false
	}

	rpc(h)

	// The reply has to cross the network too: the link may have gone down
	// while the handler ran, or an unreliable network may drop it.
	n.mu.Lock()
	reachable = n.canTalk(from, to)
	n.mu.Unlock()
	if unreliable && rand.IntN(10) == 0 { // 10%: reply lost
		return false
	}
	return reachable
}

// endpoint is one node's view of the network: a Transport whose sender
// is fixed.
type endpoint struct {
	net  *network
	from int
}

func (e endpoint) RequestVote(to int, args RequestVoteArgs) (RequestVoteReply, bool) {
	var reply RequestVoteReply
	ok := e.net.deliver(e.from, to, func(h Handler) {
		reply = h.HandleRequestVote(args)
	})
	if !ok {
		return RequestVoteReply{}, false
	}
	return reply, true
}

func (e endpoint) AppendEntries(to int, args AppendEntriesArgs) (AppendEntriesReply, bool) {
	// A real network sends a copy of the bytes. Without this the receiver
	// would share the sender's slice, and a later change to the leader's
	// log would silently change the follower's too.
	args.Entries = slices.Clone(args.Entries)

	var reply AppendEntriesReply
	ok := e.net.deliver(e.from, to, func(h Handler) {
		reply = h.HandleAppendEntries(args)
	})
	if !ok {
		return AppendEntriesReply{}, false
	}
	return reply, true
}
