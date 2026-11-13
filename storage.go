package main

import (
	"sync"
	"time"
)

// Store keeps values and expiry times in two maps, like real Redis does.
// Invariant: every key in expires is also in data. A key in data but not
// in expires never expires. Only setKey and deleteKey change the maps,
// so the invariant lives in those two places.
type Store struct {
	mu      sync.RWMutex
	data    map[string]string
	expires map[string]time.Time // only keys that have a TTL
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]string),
		expires: make(map[string]time.Time),
	}
}

// --- helpers: caller must hold s.mu ---

// setKey stores value. A zero expiresAt means "no TTL", which also clears
// any TTL the key had before (SET without EX makes a key permanent).
func (s *Store) setKey(key, value string, expiresAt time.Time) {
	s.data[key] = value
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

// --- public API ---

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, exists := s.data[key]
	if !exists {
		return "", false
	}
	if s.isExpired(key, time.Now()) {
		s.deleteKey(key) // lazy expiry
		return "", false
	}
	return value, true
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
