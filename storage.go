package main

import (
	"sync"
	"time"
)

func (s *Store) removeExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for key, entry := range s.data {
		if entry.expired(now) {
			delete(s.data, key)
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

type Entry struct {
	value     string
	expiresAt time.Time // zero means the key never expires
}

// expired is the single expiry rule used by every read and delete path.
func (e Entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]Entry),
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]

	if !exists {
		return "", false
	}

	if entry.expired(time.Now()) {
		delete(s.data, key)
		return "", false
	}

	return entry.value, true
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = Entry{
		value: value,
	}
}

func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = Entry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	count := 0

	for _, key := range keys {
		entry, exists := s.data[key]
		if !exists {
			continue
		}
		delete(s.data, key)
		// An expired key is already logically gone, so it doesn't count.
		if !entry.expired(now) {
			count++
		}
	}

	return count
}
