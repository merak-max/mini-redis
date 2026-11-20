package main

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

type pair struct {
	score  float64
	member string
}

// checkSkipList compares sl against want (already sorted) and verifies the
// structure itself: level-0 order, every span, rank and nodeAt.
func checkSkipList(t *testing.T, sl *skipList, want []pair) {
	t.Helper()
	if sl.length != len(want) {
		t.Fatalf("length = %d, want %d", sl.length, len(want))
	}

	// Level 0 must list every pair in order. Record each node's 1-based index.
	pos := map[*skipNode]int{sl.head: 0}
	i := 0
	for n := sl.head.levels[0].next; n != nil; n = n.levels[0].next {
		if i >= len(want) || n.score != want[i].score || n.member != want[i].member {
			t.Fatalf("level 0 position %d: got (%v, %q)", i, n.score, n.member)
		}
		i++
		pos[n] = i
	}
	if i != len(want) {
		t.Fatalf("level 0 has %d nodes, want %d", i, len(want))
	}

	// On every level, a link's span must equal the index distance it jumps.
	for lvl := 0; lvl < sl.level; lvl++ {
		for x := sl.head; x.levels[lvl].next != nil; x = x.levels[lvl].next {
			next := x.levels[lvl].next
			if got, want := x.levels[lvl].span, pos[next]-pos[x]; got != want {
				t.Fatalf("level %d span from index %d = %d, want %d", lvl, pos[x], got, want)
			}
		}
	}

	for idx, p := range want {
		if r := sl.rank(p.score, p.member); r != idx {
			t.Fatalf("rank(%v, %q) = %d, want %d", p.score, p.member, r, idx)
		}
		if n := sl.nodeAt(idx); n == nil || n.member != p.member {
			t.Fatalf("nodeAt(%d) wrong", idx)
		}
	}
	if sl.nodeAt(len(want)) != nil {
		t.Fatal("nodeAt past the end should be nil")
	}
}

func sortPairs(ps []pair) {
	sort.Slice(ps, func(i, j int) bool { return before(ps[i].score, ps[i].member, ps[j].score, ps[j].member) })
}

func TestSkipListOrderAndTies(t *testing.T) {
	sl := newSkipList()
	in := []pair{{3, "c"}, {1, "z"}, {2, "b"}, {1, "a"}, {2, "a"}, {-5, "neg"}}
	for _, p := range in {
		sl.insert(p.score, p.member)
	}
	want := append([]pair(nil), in...)
	sortPairs(want) // -5 neg, 1 a, 1 z, 2 a, 2 b, 3 c: equal scores ordered by member
	checkSkipList(t, sl, want)

	if sl.rank(9, "missing") != -1 {
		t.Fatal("rank of a missing pair should be -1")
	}
	if sl.delete(1, "nope") || sl.delete(7, "a") {
		t.Fatal("deleting a missing pair should return false")
	}
}

// Thousands of random inserts and deletes, checked against a sorted slice.
// This catches span bugs that small hand-written cases miss.
func TestSkipListRandomAgainstSlice(t *testing.T) {
	sl := newSkipList()
	present := map[string]float64{}
	for step := 0; step < 3000; step++ {
		member := fmt.Sprintf("m%d", rand.IntN(300))
		if score, ok := present[member]; ok && rand.IntN(2) == 0 {
			if !sl.delete(score, member) {
				t.Fatalf("step %d: delete(%v, %q) returned false", step, score, member)
			}
			delete(present, member)
		} else if !ok {
			score := float64(rand.IntN(50)) // few distinct scores, so many ties
			sl.insert(score, member)
			present[member] = score
		}

		if step%100 == 0 {
			want := make([]pair, 0, len(present))
			for m, s := range present {
				want = append(want, pair{s, m})
			}
			sortPairs(want)
			checkSkipList(t, sl, want)
		}
	}
}

func TestNormalizeRange(t *testing.T) {
	tests := []struct {
		start, stop, n int
		s, e           int
		ok             bool
	}{
		{0, -1, 5, 0, 4, true},
		{1, 3, 5, 1, 3, true},
		{-2, -1, 5, 3, 4, true},
		{0, 100, 5, 0, 4, true},
		{-100, 1, 5, 0, 1, true},
		{5, 10, 5, 0, 0, false},
		{3, 1, 5, 0, 0, false},
		{0, -1, 0, 0, 0, false},
	}
	for _, tt := range tests {
		s, e, ok := normalizeRange(tt.start, tt.stop, tt.n)
		if s != tt.s || e != tt.e || ok != tt.ok {
			t.Errorf("normalizeRange(%d, %d, %d) = %d, %d, %v; want %d, %d, %v",
				tt.start, tt.stop, tt.n, s, e, ok, tt.s, tt.e, tt.ok)
		}
	}
}
