package raft

import "testing"

// voter builds a node in term 5 that has voted for votedFor (-1: nobody)
// and whose last log entry is index 2, term 3.
func voter(votedFor int) *Node {
	r := NewNode(0, 3, nil)
	r.currentTerm = 5
	r.votedFor = votedFor
	r.log = []LogEntry{{Term: 1}, {Term: 3}}
	return r
}

func TestHandleRequestVote(t *testing.T) {
	tests := []struct {
		name      string
		votedFor  int
		args      RequestVoteArgs
		granted   bool
		wantTerm  int // our term after the call, also sent in the reply
		wantVoted int
	}{
		{"stale term refused", -1,
			RequestVoteArgs{Term: 4, CandidateID: 1, LastLogIndex: 9, LastLogTerm: 9}, false, 5, -1},
		{"no vote yet, log equal", -1,
			RequestVoteArgs{Term: 5, CandidateID: 1, LastLogIndex: 2, LastLogTerm: 3}, true, 5, 1},
		{"already voted for another", 2,
			RequestVoteArgs{Term: 5, CandidateID: 1, LastLogIndex: 2, LastLogTerm: 3}, false, 5, 2},
		{"same candidate asks again", 1,
			RequestVoteArgs{Term: 5, CandidateID: 1, LastLogIndex: 2, LastLogTerm: 3}, true, 5, 1},
		{"newer term clears old vote", 2,
			RequestVoteArgs{Term: 6, CandidateID: 1, LastLogIndex: 2, LastLogTerm: 3}, true, 6, 1},
		{"candidate's last term older", -1,
			RequestVoteArgs{Term: 6, CandidateID: 1, LastLogIndex: 9, LastLogTerm: 2}, false, 6, -1},
		{"same last term, shorter log", -1,
			RequestVoteArgs{Term: 6, CandidateID: 1, LastLogIndex: 1, LastLogTerm: 3}, false, 6, -1},
		{"later last term beats longer log", -1,
			RequestVoteArgs{Term: 6, CandidateID: 1, LastLogIndex: 1, LastLogTerm: 4}, true, 6, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := voter(tc.votedFor)
			reply := r.HandleRequestVote(tc.args)
			if reply.VoteGranted != tc.granted || reply.Term != tc.wantTerm {
				t.Fatalf("reply %+v, want granted=%v term=%d", reply, tc.granted, tc.wantTerm)
			}
			if r.currentTerm != tc.wantTerm || r.votedFor != tc.wantVoted {
				t.Fatalf("node term=%d votedFor=%d, want term=%d votedFor=%d",
					r.currentTerm, r.votedFor, tc.wantTerm, tc.wantVoted)
			}
		})
	}
}

// A leader that sees a newer term must stop being leader, even when it
// refuses the vote.
func TestRequestVoteNewerTermDemotesLeader(t *testing.T) {
	r := voter(0)
	r.role = leader
	r.HandleRequestVote(RequestVoteArgs{Term: 6, CandidateID: 1}) // log behind: refused
	if r.role != follower || r.currentTerm != 6 {
		t.Fatalf("role=%v term=%d, want follower in term 6", r.role, r.currentTerm)
	}
}

func TestHeartbeat(t *testing.T) {
	r := voter(-1)
	r.role = candidate

	if reply := r.HandleAppendEntries(AppendEntriesArgs{Term: 4}); reply.Success || reply.Term != 5 {
		t.Fatalf("stale leader: reply %+v, want refused with term 5", reply)
	}
	if r.role != candidate {
		t.Fatal("a stale heartbeat changed our role")
	}

	// A leader in our own term: the candidate lost and becomes a follower.
	if reply := r.HandleAppendEntries(AppendEntriesArgs{Term: 5}); !reply.Success {
		t.Fatalf("current leader: reply %+v, want success", reply)
	}
	if r.role != follower {
		t.Fatal("candidate didn't step down for the current term's leader")
	}
}
