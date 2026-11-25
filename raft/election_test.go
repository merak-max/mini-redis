package raft

import (
	"testing"
	"time"
)

// newCluster starts size nodes on a fresh fake network and stops them
// when the test ends.
func newCluster(t *testing.T, size int) (*network, []*Node) {
	net := newNetwork()
	nodes := make([]*Node, size)
	for i := range nodes {
		nodes[i] = NewNode(i, size, net.endpoint(i))
		net.add(i, nodes[i])
	}
	for _, r := range nodes {
		r.Start()
	}
	t.Cleanup(func() {
		for _, r := range nodes {
			r.Stop()
		}
	})
	return net, nodes
}

// leaders returns, for each term, the nodes that think they lead it. Two
// leaders in one term is the bug election exists to prevent, so it fails
// the test on the spot.
func leaders(t *testing.T, nodes []*Node) map[int][]int {
	t.Helper()
	byTerm := map[int][]int{}
	for id, r := range nodes {
		if term, isLeader := r.State(); isLeader {
			byTerm[term] = append(byTerm[term], id)
			if len(byTerm[term]) > 1 {
				t.Fatalf("two leaders in term %d: %v", term, byTerm[term])
			}
		}
	}
	return byTerm
}

// waitForLeader waits up to 2s for a connected node to be leader, and
// returns it. A disconnected old leader may still think it leads an old
// term; that is allowed, it can't do any harm without a majority.
func waitForLeader(t *testing.T, net *network, nodes []*Node) int {
	t.Helper()
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		for _, ids := range leaders(t, nodes) {
			if net.isConnected(ids[0]) {
				return ids[0]
			}
		}
	}
	t.Fatal("no leader elected within 2s")
	return -1
}

// checkNoLeader fails if any connected node is leader.
func checkNoLeader(t *testing.T, net *network, nodes []*Node) {
	t.Helper()
	for _, ids := range leaders(t, nodes) {
		if net.isConnected(ids[0]) {
			t.Fatalf("node %d is leader, want none", ids[0])
		}
	}
}

func TestInitialElection(t *testing.T) {
	net, nodes := newCluster(t, 3)
	first := waitForLeader(t, net, nodes)
	term, _ := nodes[first].State()

	// With a healthy network, heartbeats keep everyone a follower: no new
	// elections, so the term doesn't move.
	time.Sleep(500 * time.Millisecond)
	if now := waitForLeader(t, net, nodes); now != first {
		t.Fatalf("leader changed from %d to %d on a healthy network", first, now)
	}
	if now, _ := nodes[first].State(); now != term {
		t.Fatalf("term moved from %d to %d on a healthy network", term, now)
	}
}

func TestReElection(t *testing.T) {
	net, nodes := newCluster(t, 3)
	old := waitForLeader(t, net, nodes)
	oldTerm, _ := nodes[old].State()

	// The leader dies: the other two elect a new one in a higher term.
	net.disconnect(old)
	next := waitForLeader(t, net, nodes)
	if next == old {
		t.Fatal("disconnected node is still the only leader")
	}
	if term, _ := nodes[next].State(); term <= oldTerm {
		t.Fatalf("new leader's term %d, want > %d", term, oldTerm)
	}

	// The old leader comes back, sees the higher term, and steps down. It
	// must not then start an election of its own and disrupt the cluster:
	// the new leader keeps its job and the term doesn't move.
	term, _ := nodes[next].State()
	net.connect(old)
	time.Sleep(500 * time.Millisecond)
	if now := waitForLeader(t, net, nodes); now != next {
		t.Fatalf("leader changed from %d to %d when the old leader rejoined", next, now)
	}
	if now, _ := nodes[next].State(); now != term {
		t.Fatalf("term moved from %d to %d when the old leader rejoined", term, now)
	}
}

// With 3 of 5 nodes gone, the 2 left can't form a majority (3 votes), so
// neither may become leader. Once a third returns, they can.
func TestNoLeaderWithoutMajority(t *testing.T) {
	net, nodes := newCluster(t, 5)
	leader := waitForLeader(t, net, nodes)
	for i := 0; i < 3; i++ {
		net.disconnect((leader + i) % 5)
	}
	time.Sleep(time.Second)
	checkNoLeader(t, net, nodes)

	net.connect(leader)
	waitForLeader(t, net, nodes)
}

// lateVotes holds every vote request until release is closed, then grants
// only the ones from term 1.
type lateVotes struct{ release chan struct{} }

func (l lateVotes) RequestVote(to int, args RequestVoteArgs) (RequestVoteReply, bool) {
	<-l.release
	return RequestVoteReply{Term: args.Term, VoteGranted: args.Term == 1}, true
}

func (l lateVotes) AppendEntries(int, AppendEntriesArgs) (AppendEntriesReply, bool) {
	return AppendEntriesReply{}, false
}

// Votes from an old election must not count towards a newer one. Here the
// term-1 election times out and the node runs again in term 2; then the
// term-1 "yes" votes finally arrive. They must be ignored.
func TestLateVotesIgnored(t *testing.T) {
	tr := lateVotes{release: make(chan struct{})}
	r := NewNode(0, 3, tr)

	r.mu.Lock()
	r.startElection() // term 1
	r.startElection() // term 1 timed out: term 2
	r.mu.Unlock()
	close(tr.release)
	time.Sleep(50 * time.Millisecond)

	if term, isLeader := r.State(); isLeader {
		t.Fatalf("became leader of term %d using votes from term 1", term)
	}
}

// Lost and delayed messages slow an election down but must not stop it,
// or break the one-leader-per-term rule.
func TestElectionUnreliable(t *testing.T) {
	net, nodes := newCluster(t, 5)
	net.setUnreliable(true)
	for i := 0; i < 3; i++ {
		leader := waitForLeader(t, net, nodes)
		net.disconnect(leader)
		waitForLeader(t, net, nodes)
		net.connect(leader)
	}
}
