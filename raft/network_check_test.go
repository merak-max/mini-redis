package raft

import (
	"sync"
	"testing"
)

// countingHandler records the calls it receives and replies with its term.
type countingHandler struct {
	mu      sync.Mutex
	term    int
	votes   int
	appends int
	entries []LogEntry // Entries from the last AppendEntries
}

func (c *countingHandler) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.votes++
	return RequestVoteReply{Term: c.term, VoteGranted: true}
}

func (c *countingHandler) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appends++
	c.entries = args.Entries
	return AppendEntriesReply{Term: c.term, Success: true}
}

func (c *countingHandler) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.votes + c.appends
}

// threeNodes builds a network with nodes 0, 1, 2, each with term 10+id.
func threeNodes() (*network, []*countingHandler) {
	n := newNetwork()
	hs := make([]*countingHandler, 3)
	for i := range hs {
		hs[i] = &countingHandler{term: 10 + i}
		n.add(i, hs[i])
	}
	return n, hs
}

func TestNetworkDelivers(t *testing.T) {
	n, hs := threeNodes()

	reply, ok := n.endpoint(0).RequestVote(1, RequestVoteArgs{Term: 1})
	if !ok || !reply.VoteGranted || reply.Term != 11 {
		t.Fatalf("got %+v ok=%v, want node 1's reply (term 11, granted)", reply, ok)
	}
	if hs[1].calls() != 1 {
		t.Fatalf("node 1 got %d calls, want 1", hs[1].calls())
	}
}

func TestNetworkDisconnect(t *testing.T) {
	n, hs := threeNodes()
	n.disconnect(1)

	// Nothing reaches a disconnected node, and it can't send either.
	if _, ok := n.endpoint(0).AppendEntries(1, AppendEntriesArgs{}); ok {
		t.Fatal("message to a disconnected node got a reply")
	}
	if _, ok := n.endpoint(1).AppendEntries(0, AppendEntriesArgs{}); ok {
		t.Fatal("message from a disconnected node got a reply")
	}
	if hs[0].calls() != 0 || hs[1].calls() != 0 {
		t.Fatal("a handler ran for a message that should have been dropped")
	}

	n.connect(1)
	if _, ok := n.endpoint(0).AppendEntries(1, AppendEntriesArgs{}); !ok {
		t.Fatal("no reply after reconnecting")
	}
}

// The receiver must get its own copy of Entries, as over a real network.
func TestNetworkCopiesEntries(t *testing.T) {
	n, hs := threeNodes()
	entries := []LogEntry{{Term: 1, Command: []byte("SET x 1")}}

	n.endpoint(0).AppendEntries(1, AppendEntriesArgs{Entries: entries})
	entries[0].Term = 99 // the leader changes its log afterwards

	hs[1].mu.Lock()
	defer hs[1].mu.Unlock()
	if hs[1].entries[0].Term != 1 {
		t.Fatal("receiver's entries changed when the sender's slice did")
	}
}

// An unreliable network must lose some messages but not all.
func TestNetworkUnreliable(t *testing.T) {
	n, _ := threeNodes()
	n.setUnreliable(true)

	const total = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	replies := 0
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := n.endpoint(0).RequestVote(1, RequestVoteArgs{}); ok {
				mu.Lock()
				replies++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// About 81% should arrive (90% request x 90% reply). The bounds are
	// wide so the test doesn't fail by bad luck.
	if replies < total/2 || replies == total {
		t.Fatalf("%d of %d replies arrived, want some lost but most delivered", replies, total)
	}
}
