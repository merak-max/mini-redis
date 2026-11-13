package main

import (
	"sync"
	"time"
)

func (s *Store) removeExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, entry := range s.data {
		if !entry.expiresAt.IsZero() &&
			!time.Now().Before(entry.expiresAt) {
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
	expiresAt time.Time
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

	if !entry.expiresAt.IsZero() &&
		!time.Now().Before(entry.expiresAt) {
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

	count := 0

	for _, key := range keys {
		if _, exists := s.data[key]; exists {
			delete(s.data, key)
			count++
		}
	}

	return count
}
