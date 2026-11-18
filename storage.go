package main

import (
	"container/list"
	"errors"
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
)

type Value struct {
	typ  ValueType
	str  string
	list *list.List
}

// Store keeps values and expiry times in two maps, like real Redis does.
// Invariant: every key in expires is also in data. A key in data but not
// in expires never expires. Only setKey and deleteKey change the maps,
// so the invariant lives in those two places.
type Store struct {
	mu      sync.RWMutex
	data    map[string]*Value    // every key; Value.typ says string or list
	expires map[string]time.Time // only keys that have a TTL
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

	value.list.Remove(elem)
	if value.list.Len() == 0 {
		s.deleteKey(key)
	}

	// list.Element stores its value as `any`, so we assert it back to string.
	// Push only ever stores strings, so this cannot fail.
	return elem.Value.(string), true, nil
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

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setKey(key, value, time.Time{})
}

func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setKey(key, value, time.Now().Add(ttl))
}

func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
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
	return count
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
