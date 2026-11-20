package main

import "math/rand/v2"

// A skip list keeps (score, member) pairs sorted, with O(log n) expected
// insert, delete and rank. It is the same design Redis uses for sorted sets:
// every link also stores a span, the number of nodes it jumps over, so we
// can find a node's rank (its index) or the node at a rank in O(log n).
//
// Ordering is by score, then by member for equal scores, as in Redis.
// The skip list is not safe for concurrent use; Store's mutex protects it.

const (
	skipMaxLevel = 32   // enough for 4^32 elements
	skipP        = 0.25 // chance a node gets one more level (Redis uses 1/4)
)

type skipLevel struct {
	next *skipNode
	span int // how many level-0 steps this link covers
}

type skipNode struct {
	member string
	score  float64
	levels []skipLevel // levels[0] is the full sorted linked list
}

type skipList struct {
	head   *skipNode // sentinel; holds no member
	level  int       // number of levels currently in use
	length int
}

func newSkipList() *skipList {
	return &skipList{
		head:  &skipNode{levels: make([]skipLevel, skipMaxLevel)},
		level: 1,
	}
}

// randomLevel flips a biased coin: level 1 always, level 2 with p=1/4,
// level 3 with p=1/16, and so on. This gives the O(log n) expected height.
func randomLevel() int {
	lvl := 1
	for lvl < skipMaxLevel && rand.Float64() < skipP {
		lvl++
	}
	return lvl
}

// before reports whether (aScore, aMember) sorts before (bScore, bMember).
func before(aScore float64, aMember string, bScore float64, bMember string) bool {
	return aScore < bScore || (aScore == bScore && aMember < bMember)
}

// insert adds a new pair. The caller guarantees the member is not present.
func (sl *skipList) insert(score float64, member string) {
	var update [skipMaxLevel]*skipNode // last node before the new one, per level
	var rank [skipMaxLevel]int         // rank of update[i]

	// 1. Walk down from the top level, moving right while the next node sorts
	//    before the new pair. Record where we stop on each level.
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i < sl.level-1 {
			rank[i] = rank[i+1]
		}
		for x.levels[i].next != nil && before(x.levels[i].next.score, x.levels[i].next.member, score, member) {
			rank[i] += x.levels[i].span
			x = x.levels[i].next
		}
		update[i] = x
	}

	// 2. Pick a height. New top levels start from head and span the whole list.
	lvl := randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].levels[i].span = sl.length
		}
		sl.level = lvl
	}

	// 3. Splice the node in on each of its levels and fix the spans.
	//    rank[0]-rank[i] is how far the new node is from update[i].
	n := &skipNode{member: member, score: score, levels: make([]skipLevel, lvl)}
	for i := 0; i < lvl; i++ {
		n.levels[i].next = update[i].levels[i].next
		update[i].levels[i].next = n
		n.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = rank[0] - rank[i] + 1
	}
	// Higher links that pass over the new node now cover one more step.
	for i := lvl; i < sl.level; i++ {
		update[i].levels[i].span++
	}
	sl.length++
}

// delete removes the pair and reports whether it was there.
func (sl *skipList) delete(score float64, member string) bool {
	var update [skipMaxLevel]*skipNode
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && before(x.levels[i].next.score, x.levels[i].next.member, score, member) {
			x = x.levels[i].next
		}
		update[i] = x
	}

	x = x.levels[0].next
	if x == nil || x.score != score || x.member != member {
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].levels[i].next == x {
			// Unlink x; the link now covers what x's link covered.
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].next = x.levels[i].next
		} else {
			// This link jumped over x; it now covers one step less.
			update[i].levels[i].span--
		}
	}
	for sl.level > 1 && sl.head.levels[sl.level-1].next == nil {
		sl.level--
	}
	sl.length--
	return true
}

// rank returns the 0-based position of the pair, or -1 if it is absent.
func (sl *skipList) rank(score float64, member string) int {
	r := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		// Move right while the next node is <= the target.
		for x.levels[i].next != nil && !before(score, member, x.levels[i].next.score, x.levels[i].next.member) {
			r += x.levels[i].span
			x = x.levels[i].next
		}
		if x != sl.head && x.score == score && x.member == member {
			return r - 1
		}
	}
	return -1
}

// nodeAt returns the node at 0-based position idx, or nil if out of range.
func (sl *skipList) nodeAt(idx int) *skipNode {
	target := idx + 1 // spans count from 1
	traversed := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && traversed+x.levels[i].span <= target {
			traversed += x.levels[i].span
			x = x.levels[i].next
		}
		if traversed == target {
			return x
		}
	}
	return nil
}
