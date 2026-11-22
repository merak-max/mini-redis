package main

import (
	"container/list"
	"errors"
	"fmt"
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

// Store keeps values and expiry times in two maps, like real Redis does.
// Invariant: every key in expires is also in data. A key in data but not
// in expires never expires. Only setKey and deleteKey change the maps,
// so the invariant lives in those two places.
type Store struct {
	mu      sync.RWMutex
	data    map[string]*Value    // every key; Value.typ says string or list
	expires map[string]time.Time // only keys that have a TTL
	aof     *AOF                 // nil: writes aren't logged (replay, most tests)
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]*Value),
		expires: make(map[string]time.Time),
	}
}

// --- helpers: caller must hold s.mu ---

// setKey stores value. A zero expiresAt means "no TTL", which also clears
// any TTL the key had before (SET without EX makes a key permanent).
func (s *Store) setKey(key, value string, expiresAt time.Time) {
	s.data[key] = &Value{typ: StringType, str: value}

	if expiresAt.IsZero() {
		delete(s.expires, key)
	} else {
		s.expires[key] = expiresAt
	}
}

func (s *Store) deleteKey(key string) {
	delete(s.data, key)
	delete(s.expires, key)
}

// isExpired is the single expiry rule used by every read and delete path.
func (s *Store) isExpired(key string, now time.Time) bool {
	at, hasTTL := s.expires[key]
	return hasTTL && !now.Before(at)
}

// log appends a write command to the AOF, if one is attached.
//
// Every write method calls it under s.mu, after validation and before
// changing memory. Holding the lock keeps the file in the same order as
// memory. Logging before applying means a failed disk write leaves memory
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
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	// Expired keys behave like missing keys.
	if s.isExpired(key, now) {
		s.deleteKey(key)
	}

	value, exists := s.data[key]

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
		s.data[key] = value
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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpired(key, time.Now()) {
		s.deleteKey(key)
		return 0, nil
	}

	value, exists := s.data[key]

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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpired(key, time.Now()) {
		s.deleteKey(key)
		return "", false, nil
	}

	value, exists := s.data[key]
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
		s.deleteKey(key)
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
		s.deleteKey(key)
	}

	// list.Element stores its value as `any`, so we assert it back to string.
	// Push only ever stores strings, so this cannot fail.
	return elem.Value.(string), true, nil
}

// LRange returns elements start..stop (inclusive, negative counts from the
// end). A missing key gives an empty result. container/list has no index
// lookup, so we walk from the front: O(start + count).
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isExpired(key, time.Now()) {
		s.deleteKey(key)
		return nil, nil
	}
	value, exists := s.data[key]
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
	s.mu.Lock()
	defer s.mu.Unlock()

	value, exists := s.data[key]

	if !exists {
		return "", false, nil
	}

	if s.isExpired(key, time.Now()) {
		s.deleteKey(key)
		return "", false, nil
	}

	if value.typ != StringType {
		return "", false, errWrongType
	}

	return value.str, true, nil
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log("SET", key, value); err != nil {
		return err // memory is unchanged
	}
	s.setKey(key, value, time.Time{})
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

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.log("SET", key, value, "PXAT", strconv.FormatInt(ms, 10)); err != nil {
		return err
	}
	if !time.Now().Before(at) {
		s.deleteKey(key)
		return nil
	}
	s.setKey(key, value, at)
	return nil
}

func (s *Store) Del(keys ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	// Log only if a live key will actually be deleted.
	for _, key := range keys {
		if _, exists := s.data[key]; exists && !s.isExpired(key, now) {
			if err := s.log(append([]string{"DEL"}, keys...)...); err != nil {
				return 0, err
			}
			break
		}
	}

	count := 0
	for _, key := range keys {
		if _, exists := s.data[key]; !exists {
			continue
		}
		// An expired key is already logically gone, so it doesn't count.
		if !s.isExpired(key, now) {
			count++
		}
		s.deleteKey(key)
	}
	return count, nil
}

// --- active expiry ---

const (
	expireSampleSize = 20 // keys checked per round
	expireMaxRounds  = 5  // cap on work per cleanup cycle
)

// removeExpired runs one active-expiry cycle, the same algorithm Redis uses:
// check a small sample of keys that have a TTL and delete the expired ones.
// If more than 25% of the sample was expired, there are probably many more,
// so run another round. Work per cycle is bounded no matter how big the
// store is.
func (s *Store) removeExpired() {
	for round := 0; round < expireMaxRounds; round++ {
		s.mu.Lock()
		now := time.Now()
		checked, expired := 0, 0

		// Go starts each map range at a random position, so the first
		// keys we see form a cheap random sample. We stop after
		// expireSampleSize keys instead of walking the whole map.
		// Deleting during range is allowed in Go.
		for key, at := range s.expires {
			if checked == expireSampleSize {
				break
			}
			checked++
			if !now.Before(at) {
				s.deleteKey(key)
				expired++
			}
		}
		s.mu.Unlock() // let clients run between rounds

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
