package main

import (
	"container/list"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"
)

var errWrongType = errors.New(
	"WRONGTYPE Operation against a key holding the wrong kind of value",
)

type ValueType int

const (
	StringType ValueType = iota
	ListType
	ZSetType
)

type Value struct {
	typ  ValueType
	str  string
	list *list.List
	zset *zset // used when typ == ZSetType (see zset.go)
}

// Store is split into shards, each with its own lock and its own two maps,
// so commands on keys in different shards don't wait for each other.
// A key always lives in the same shard: shardIndex(key).
type Store struct {
	shards []shard
	mask   uint32 // len(shards)-1; len(shards) is a power of 2
	aof    *AOF   // nil: writes aren't logged (replay, most tests)
}

// shard keeps values and expiry times in two maps, like real Redis does.
// Invariant: every key in expires is also in data. A key in data but not
// in expires never expires. Only setKey and deleteKey change the maps,
// so the invariant lives in those two places.
//
// The padding makes a shard exactly one 128-byte cache line, the unit the
// CPU moves between cores (Apple M-series; x86 lines are 64 bytes). The
// fields take 24 bytes, so without it about five shards share a line, and
// locking one shard steals the line from cores using its neighbours: false
// sharing. Padding made parallel GET on 16 shards 35% faster.
// TestShardIsOneCacheLine fails if a new field breaks this.
type shard struct {
	mu      sync.Mutex
	data    map[string]*Value    // every key; Value.typ says the type
	expires map[string]time.Time // only keys that have a TTL
	_       [104]byte            // pad to 128 bytes: see above
}

// defaultShards is the number of shards the server uses, chosen by
// BenchmarkShards: on 8 cores, parallel GET takes 147 ns/op with 1 shard
// and 20 ns/op with 64. 256 is only ~17% faster again, so 64 is enough for
// a 10-core machine. More shards also cost a little: an active-expiry cycle
// visits every shard (about 27 us per cycle at 64, 95 us at 256).
const defaultShards = 64

func NewStore() *Store {
	return newStoreWithShards(defaultShards)
}

// newStoreWithShards makes a store with n shards. n must be a power of 2,
// so that hash & (n-1) picks a shard as cheaply as hash % n.
func newStoreWithShards(n int) *Store {
	if n <= 0 || n&(n-1) != 0 {
		panic(fmt.Sprintf("shard count %d is not a power of 2", n))
	}
	s := &Store{shards: make([]shard, n), mask: uint32(n - 1)}
	for i := range s.shards {
		s.shards[i].data = make(map[string]*Value)
		s.shards[i].expires = make(map[string]time.Time)
	}
	return s
}

// shardIndex hashes key with 32-bit FNV-1a: for each byte, XOR it into
// the hash, then multiply by a prime. Cheap, no allocation, and it spreads
// similar keys ("key:1", "key:2") across shards.
func (s *Store) shardIndex(key string) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h & s.mask)
}

// shardFor returns a pointer, never a copy: a shard holds a mutex.
func (s *Store) shardFor(key string) *shard {
	return &s.shards[s.shardIndex(key)]
}

// --- helpers: caller must hold sh.mu ---

// setKey stores value. A zero expiresAt means "no TTL", which also clears
// any TTL the key had before (SET without EX makes a key permanent).
func (sh *shard) setKey(key, value string, expiresAt time.Time) {
	sh.data[key] = &Value{typ: StringType, str: value}

	if expiresAt.IsZero() {
		delete(sh.expires, key)
	} else {
		sh.expires[key] = expiresAt
	}
}

func (sh *shard) deleteKey(key string) {
	delete(sh.data, key)
	delete(sh.expires, key)
}

// isExpired is the single expiry rule used by every read and delete path.
func (sh *shard) isExpired(key string, now time.Time) bool {
	at, hasTTL := sh.expires[key]
	return hasTTL && !now.Before(at)
}

// log appends a write command to the AOF, if one is attached.
//
// Every write method calls it under the key's shard lock, after
// validation and before changing memory. Two writes to the same key are
// in the same shard, so holding its lock keeps them in the file in the
// same order as in memory. Writes to different keys may reach the file in
// either order, which is fine: they don't affect each other, so replaying
// them in either order gives the same result. Logging before applying means a failed disk write leaves memory
// unchanged, so the client never gets OK for a write that wasn't saved.
// Lazy and active expiry are not logged: replay restores the deadline and
// expiry deletes the key again.
func (s *Store) log(args ...string) error {
	if s.aof == nil {
		return nil
	}
	if err := s.aof.Append(args...); err != nil {
		return fmt.Errorf("ERR AOF write failed: %w", err)
	}
	return nil
}
func (s *Store) Push(key string, front bool, values ...string) (int, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := time.Now()

	// Expired keys behave like missing keys.
	if sh.isExpired(key, now) {
		sh.deleteKey(key)
	}

	value, exists := sh.data[key]

	if exists && value.typ != ListType {
		return 0, errWrongType
	}

	cmd := "RPUSH"
	if front {
		cmd = "LPUSH"
	}
	if err := s.log(append([]string{cmd, key}, values...)...); err != nil {
		return 0, err
	}

	if !exists {
		value = &Value{
			typ:  ListType,
			list: list.New(),
		}
		sh.data[key] = value
	}

	for _, item := range values {
		if front {
			value.list.PushFront(item)
		} else {
			value.list.PushBack(item)
		}
	}

	return value.list.Len(), nil
}
func (s *Store) LLen(key string) (int, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.isExpired(key, time.Now()) {
		sh.deleteKey(key)
		return 0, nil
	}

	value, exists := sh.data[key]

	if !exists {
		return 0, nil
	}

	if value.typ != ListType {
		return 0, errWrongType
	}

	return value.list.Len(), nil
}

// Pop removes and returns one element from the front (LPOP) or back (RPOP).
// ok is false when the key is missing, expired, or the list is empty.
// When the last element is removed the key is deleted, as in Redis:
// Redis never keeps an empty list around.
func (s *Store) Pop(key string, front bool) (string, bool, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.isExpired(key, time.Now()) {
		sh.deleteKey(key)
		return "", false, nil
	}

	value, exists := sh.data[key]
	if !exists {
		return "", false, nil
	}
	if value.typ != ListType {
		return "", false, errWrongType
	}

	elem := value.list.Back()
	if front {
		elem = value.list.Front()
	}
	if elem == nil { // defensive: lists are deleted when they become empty
		sh.deleteKey(key)
		return "", false, nil
	}

	cmd := "RPOP"
	if front {
		cmd = "LPOP"
	}
	if err := s.log(cmd, key); err != nil {
		return "", false, err
	}

	value.list.Remove(elem)
	if value.list.Len() == 0 {
		sh.deleteKey(key)
	}

	// list.Element stores its value as `any`, so we assert it back to string.
	// Push only ever stores strings, so this cannot fail.
	return elem.Value.(string), true, nil
}

// LRange returns elements start..stop (inclusive, negative counts from the
// end). A missing key gives an empty result. container/list has no index
// lookup, so we walk from the front: O(start + count).
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.isExpired(key, time.Now()) {
		sh.deleteKey(key)
		return nil, nil
	}
	value, exists := sh.data[key]
	if !exists {
		return nil, nil
	}
	if value.typ != ListType {
		return nil, errWrongType
	}

	start, stop, ok := normalizeRange(start, stop, value.list.Len())
	if !ok {
		return nil, nil
	}

	out := make([]string, 0, stop-start+1)
	i := 0
	for e := value.list.Front(); e != nil && i <= stop; e = e.Next() {
		if i >= start {
			out = append(out, e.Value.(string))
		}
		i++
	}
	return out, nil
}

// --- public API ---

func (s *Store) Get(key string) (string, bool, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	value, exists := sh.data[key]

	if !exists {
		return "", false, nil
	}

	if sh.isExpired(key, time.Now()) {
		sh.deleteKey(key)
		return "", false, nil
	}

	if value.typ != StringType {
		return "", false, errWrongType
	}

	return value.str, true, nil
}

func (s *Store) Set(key, value string) error {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if err := s.log("SET", key, value); err != nil {
		return err // memory is unchanged
	}
	sh.setKey(key, value, time.Time{})
	return nil
}

// SetWithTTL stores value with a TTL counted from now.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) error {
	return s.SetAt(key, value, time.Now().Add(ttl))
}

// SetAt stores value with an absolute deadline, and is the only way a key
// gets a TTL. The AOF records the deadline (SET key value PXAT <unix-ms>),
// not the duration, so replay restores the same expiry time however long
// the server was down. A relative TTL would restart the countdown.
//
// The deadline is rounded down to the millisecond, so memory and the file
// hold exactly the same value. A deadline that has already passed deletes
// the key, like Redis. Replay depends on this: for "SET k old" followed by
// "SET k new PXAT <past>", k must end up missing, not "old".
func (s *Store) SetAt(key, value string, at time.Time) error {
	ms := at.UnixMilli()
	at = time.UnixMilli(ms)

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if err := s.log("SET", key, value, "PXAT", strconv.FormatInt(ms, 10)); err != nil {
		return err
	}
	if !time.Now().Before(at) {
		sh.deleteKey(key)
		return nil
	}
	sh.setKey(key, value, at)
	return nil
}

// Del deletes keys that may live in different shards, as one atomic step:
// it locks every shard involved, then logs and deletes.
//
// The shards are locked in increasing index order, each once. Without a
// fixed order, "DEL a b" and "DEL b a" running together could each lock
// one shard and wait forever for the other's (deadlock). Each is locked
// once because sync.Mutex isn't reentrant: locking it twice deadlocks.
func (s *Store) Del(keys ...string) (int, error) {
	idx := make([]int, len(keys))
	for i, key := range keys {
		idx[i] = s.shardIndex(key)
	}
	slices.Sort(idx)
	idx = slices.Compact(idx)
	for _, i := range idx {
		s.shards[i].mu.Lock()
	}
	defer func() {
		for _, i := range idx {
			s.shards[i].mu.Unlock()
		}
	}()

	now := time.Now()

	// Log only if a live key will actually be deleted.
	for _, key := range keys {
		sh := s.shardFor(key)
		if _, exists := sh.data[key]; exists && !sh.isExpired(key, now) {
			if err := s.log(append([]string{"DEL"}, keys...)...); err != nil {
				return 0, err
			}
			break
		}
	}

	count := 0
	for _, key := range keys {
		sh := s.shardFor(key)
		if _, exists := sh.data[key]; !exists {
			continue
		}
		// An expired key is already logically gone, so it doesn't count.
		if !sh.isExpired(key, now) {
			count++
		}
		sh.deleteKey(key)
	}
	return count, nil
}

// --- active expiry ---

const (
	expireSampleSize = 20 // keys checked per round
	expireMaxRounds  = 5  // cap on work per cleanup cycle
)

// removeExpired runs one active-expiry cycle on every shard.
func (s *Store) removeExpired() {
	for i := range s.shards {
		s.shards[i].removeExpired()
	}
}

// removeExpired runs one active-expiry cycle on one shard, the same
// algorithm Redis uses: check a small sample of keys that have a TTL and
// delete the expired ones. If more than 25% of the sample was expired,
// there are probably many more, so run another round. Work per cycle is
// bounded no matter how big the shard is, and the lock is held for one
// round of at most expireSampleSize keys at a time.
func (sh *shard) removeExpired() {
	for round := 0; round < expireMaxRounds; round++ {
		sh.mu.Lock()
		now := time.Now()
		checked, expired := 0, 0

		// Go starts each map range at a random position, so the first
		// keys we see form a cheap random sample. We stop after
		// expireSampleSize keys instead of walking the whole map.
		// Deleting during range is allowed in Go.
		for key, at := range sh.expires {
			if checked == expireSampleSize {
				break
			}
			checked++
			if !now.Before(at) {
				sh.deleteKey(key)
				expired++
			}
		}
		sh.mu.Unlock() // let clients run between rounds

		if checked == 0 || expired*4 <= checked { // 25% or fewer expired
			return
		}
	}
}

func (s *Store) StartCleanup(interval time.Duration) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		s.removeExpired()
	}
}
