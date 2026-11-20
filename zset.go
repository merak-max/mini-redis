package main

import "time"

// zset is a Redis sorted set: two structures that hold the same members.
//   - scores answers "what is the score of member X?" in O(1).
//   - sl keeps members in (score, member) order for ranks and ranges.
//
// Invariant: every member in scores is in sl with the same score.
// Only add and remove change them, so they always change together.
type zset struct {
	scores map[string]float64
	sl     *skipList
}

func newZSet() *zset {
	return &zset{scores: make(map[string]float64), sl: newSkipList()}
}

// add sets member's score and reports whether the member is new.
// Changing a score means removing the old node and inserting a new one,
// because the node's position in the skip list depends on its score.
func (z *zset) add(score float64, member string) bool {
	old, exists := z.scores[member]
	if exists {
		if old == score {
			return false
		}
		z.sl.delete(old, member)
	}
	z.sl.insert(score, member)
	z.scores[member] = score
	return !exists
}

func (z *zset) remove(member string) bool {
	score, exists := z.scores[member]
	if !exists {
		return false
	}
	z.sl.delete(score, member)
	delete(z.scores, member)
	return true
}

// ZMember is one (member, score) pair returned by ZRange.
type ZMember struct {
	Member string
	Score  float64
}

// normalizeRange turns Redis-style start/stop indexes (both inclusive,
// negative counts from the end) into 0-based bounds for a sequence of
// length n. ok is false when the range is empty. LRANGE can use it too.
func normalizeRange(start, stop, n int) (int, int, bool) {
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return 0, 0, false
	}
	return start, stop, true
}

// lookupZSet returns the live sorted set at key. A missing or expired key
// gives (nil, nil); a key of another type gives errWrongType.
// Caller must hold s.mu.
func (s *Store) lookupZSet(key string) (*zset, error) {
	if s.isExpired(key, time.Now()) {
		s.deleteKey(key)
		return nil, nil
	}
	value, exists := s.data[key]
	if !exists {
		return nil, nil
	}
	if value.typ != ZSetType {
		return nil, errWrongType
	}
	return value.zset, nil
}

// ZAdd adds or updates members and returns how many were new.
func (s *Store) ZAdd(key string, members []ZMember) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	z, err := s.lookupZSet(key)
	if err != nil {
		return 0, err
	}
	if z == nil {
		z = newZSet()
		s.data[key] = &Value{typ: ZSetType, zset: z}
	}

	added := 0
	for _, m := range members {
		if z.add(m.Score, m.Member) {
			added++
		}
	}
	return added, nil
}

// ZRem removes members and returns how many existed. Like lists, a sorted
// set that becomes empty is deleted.
func (s *Store) ZRem(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	z, err := s.lookupZSet(key)
	if err != nil || z == nil {
		return 0, err
	}

	removed := 0
	for _, m := range members {
		if z.remove(m) {
			removed++
		}
	}
	if len(z.scores) == 0 {
		s.deleteKey(key)
	}
	return removed, nil
}

func (s *Store) ZScore(key, member string) (float64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	z, err := s.lookupZSet(key)
	if err != nil || z == nil {
		return 0, false, err
	}
	score, ok := z.scores[member]
	return score, ok, nil
}

func (s *Store) ZCard(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	z, err := s.lookupZSet(key)
	if err != nil || z == nil {
		return 0, err
	}
	return len(z.scores), nil
}

// ZRank returns member's 0-based position in score order.
func (s *Store) ZRank(key, member string) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	z, err := s.lookupZSet(key)
	if err != nil || z == nil {
		return 0, false, err
	}
	score, ok := z.scores[member]
	if !ok {
		return 0, false, nil
	}
	return z.sl.rank(score, member), true, nil
}

// ZRange returns members from position start to stop (inclusive) in score
// order. It finds the first node by rank in O(log n), then walks level 0,
// so the cost is O(log n + count).
func (s *Store) ZRange(key string, start, stop int) ([]ZMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	z, err := s.lookupZSet(key)
	if err != nil || z == nil {
		return nil, err
	}
	start, stop, ok := normalizeRange(start, stop, z.sl.length)
	if !ok {
		return nil, nil
	}

	out := make([]ZMember, 0, stop-start+1)
	for n := z.sl.nodeAt(start); n != nil && len(out) < stop-start+1; n = n.levels[0].next {
		out = append(out, ZMember{Member: n.member, Score: n.score})
	}
	return out, nil
}
