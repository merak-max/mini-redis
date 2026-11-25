package raft

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// logOf returns a copy of node r's log.
func logOf(r *Node) []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.log)
}

func sameLog(a, b []LogEntry) bool {
	return slices.EqualFunc(a, b, func(x, y LogEntry) bool {
		return x.Term == y.Term && string(x.Command) == string(y.Command)
	})
}

func commands(log []LogEntry) []string {
	out := []string{}
	for _, e := range log {
		out = append(out, string(e.Command))
	}
	return out
}

// waitForSameLogs waits up to 3s for the given nodes to hold identical
// logs of length n, and fails the test with their logs if they don't.
func waitForSameLogs(t *testing.T, nodes []*Node, ids []int, n int) {
	t.Helper()
	for i := 0; i < 30; i++ {
		first := logOf(nodes[ids[0]])
		same := len(first) == n
		for _, id := range ids[1:] {
			same = same && sameLog(first, logOf(nodes[id]))
		}
		if same {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, id := range ids {
		t.Logf("node %d: %v", id, commands(logOf(nodes[id])))
	}
	t.Fatalf("nodes %v don't hold the same %d entries", ids, n)
}

// submit starts each command on the leader, which must accept it.
func submit(t *testing.T, leader *Node, cmds ...string) {
	t.Helper()
	for _, c := range cmds {
		if _, _, isLeader := leader.Start([]byte(c)); !isLeader {
			t.Fatalf("leader refused %q", c)
		}
	}
}

func others(leader, size int) []int {
	out := []int{}
	for id := 0; id < size; id++ {
		if id != leader {
			out = append(out, id)
		}
	}
	return out
}

func TestReplicate(t *testing.T) {
	net, nodes := newCluster(t, 3)
	leader := waitForLeader(t, net, nodes)

	// Only the leader takes commands.
	if _, _, isLeader := nodes[(leader+1)%3].Start([]byte("x")); isLeader {
		t.Fatal("a follower accepted a command")
	}
	index, _, _ := nodes[leader].Start([]byte("SET a 1"))
	if index != 1 {
		t.Fatalf("first command got index %d, want 1", index)
	}
	submit(t, nodes[leader], "SET b 2", "SET c 3")
	waitForSameLogs(t, nodes, []int{0, 1, 2}, 3)
}

// A follower that missed entries while disconnected gets them later.
func TestFollowerCatchesUp(t *testing.T) {
	net, nodes := newCluster(t, 3)
	leader := waitForLeader(t, net, nodes)
	behind := (leader + 1) % 3

	net.disconnect(behind)
	submit(t, nodes[leader], "a", "b", "c")
	waitForSameLogs(t, nodes, others(behind, 3), 3)

	net.connect(behind)
	waitForSameLogs(t, nodes, []int{0, 1, 2}, 3)
}

// A new leader must walk nextIndex back to repair a follower that is many
// entries behind. Also checks the vote rule end to end: the node with the
// shorter log can't become leader.
func TestNewLeaderRepairsFollower(t *testing.T) {
	net, nodes := newCluster(t, 3)
	old := waitForLeader(t, net, nodes)
	behind, ahead := (old+1)%3, (old+2)%3

	submit(t, nodes[old], "a", "b")
	waitForSameLogs(t, nodes, []int{0, 1, 2}, 2)

	net.disconnect(behind)
	submit(t, nodes[old], "c", "d", "e")
	waitForSameLogs(t, nodes, []int{old, ahead}, 5)

	// The old leader dies; behind comes back. Only ahead has all 5
	// entries, so only ahead can win, and it must fill in behind.
	net.disconnect(old)
	net.connect(behind)
	if now := waitForLeader(t, net, nodes); now != ahead {
		t.Fatalf("node %d won with log %v; only node %d has every entry",
			now, commands(logOf(nodes[now])), ahead)
	}
	waitForSameLogs(t, nodes, []int{behind, ahead}, 5)
}

// Entries a cut-off leader accepted, but never replicated, are replaced by
// the new leader's entries once it reconnects.
func TestStaleEntriesOverwritten(t *testing.T) {
	net, nodes := newCluster(t, 3)
	old := waitForLeader(t, net, nodes)
	submit(t, nodes[old], "a")
	waitForSameLogs(t, nodes, []int{0, 1, 2}, 1)

	net.disconnect(old)
	submit(t, nodes[old], "lost1", "lost2") // no majority: these never commit

	next := waitForLeader(t, net, nodes)
	submit(t, nodes[next], "b")
	net.connect(old)
	waitForSameLogs(t, nodes, []int{0, 1, 2}, 2)
	if got := commands(logOf(nodes[old])); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("old leader's log %v, want [a b]", got)
	}
}

// Under lost, delayed and reordered messages, and leader changes, the logs
// must still end up identical once the network recovers.
func TestReplicateUnreliable(t *testing.T) {
	net, nodes := newCluster(t, 5)
	net.setUnreliable(true)
	for i := 0; i < 20; i++ {
		for _, r := range nodes {
			r.Start([]byte(fmt.Sprint("cmd", i))) // only the leader accepts
		}
		time.Sleep(20 * time.Millisecond)
	}
	net.setUnreliable(false)

	leader := waitForLeader(t, net, nodes)
	submit(t, nodes[leader], "last") // the leader's log is now the final one
	n := len(logOf(nodes[leader]))
	waitForSameLogs(t, nodes, []int{0, 1, 2, 3, 4}, n)
}

// lateAppends holds every AppendEntries until release is closed, then
// reports success.
type lateAppends struct{ release chan struct{} }

func (l lateAppends) RequestVote(int, RequestVoteArgs) (RequestVoteReply, bool) {
	return RequestVoteReply{}, false
}

func (l lateAppends) AppendEntries(to int, args AppendEntriesArgs) (AppendEntriesReply, bool) {
	<-l.release
	return AppendEntriesReply{Term: args.Term, Success: true}, true
}

// A success reply to a request from an old term says nothing about the
// follower's log now: another leader may have overwritten it since. It
// must not count as a match in the leader's new term.
func TestLateAppendReplyIgnored(t *testing.T) {
	tr := lateAppends{release: make(chan struct{})}
	r := NewNode(0, 3, tr)

	r.mu.Lock()
	r.currentTerm = 1
	r.log = entries(1, 1)
	r.becomeLeaderState()
	r.sendEntries(1, 1) // term 1: send both entries to node 1

	r.currentTerm = 3 // we lost leadership, then won term 3
	r.log = append(r.log, LogEntry{Term: 3})
	r.becomeLeaderState()
	r.mu.Unlock()

	close(tr.release) // the term-1 success finally arrives
	time.Sleep(50 * time.Millisecond)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.matchIndex[1] != 0 {
		t.Fatalf("matchIndex[1] = %d from a term-1 reply, want 0", r.matchIndex[1])
	}
}
