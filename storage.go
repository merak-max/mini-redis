package main

import (
	"math/rand"
	"sync"
	"time"
)

func (s *Store) removeExpired() {
	maxRounds := 5

	for round := 0; round < maxRounds; round++ {
		s.mu.Lock()

		keys := make([]string, 0)

		for key := range s.data {
			keys = append(keys, key)
		}

		if len(keys) == 0 {
			s.mu.Unlock()
			break
		}

		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})

		sampleSize := 20
		if len(keys) < sampleSize {
			sampleSize = len(keys)
		}

		sample := keys[:sampleSize]

		expiredCount := 0

		for _, key := range sample {
			entry := s.data[key]

			if !entry.expiresAt.IsZero() &&
				!time.Now().Before(entry.expiresAt) {
				delete(s.data, key)
				expiredCount++
			}
		}

		percentage := float64(expiredCount) /
			float64(sampleSize) * 100

		s.mu.Unlock()

		if percentage <= 25 {
			break
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
