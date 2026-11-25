package main

import (
	"container/list"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetReplacesExistingType(t *testing.T) {
	s := NewStore()

	s.put("key", &Value{typ: ListType, list: listOf("Go", "Redis")})

	s.Set("key", "Hello")

	value := s.raw("key")

	if value.typ != StringType {
		t.Fatal("SET should replace existing value with StringType")
	}

	if value.str != "Hello" {
		t.Fatalf("Got %q, want Hello", value.str)
	}
}
func TestStoreBasic(t *testing.T) {
	s := NewStore()

	if _, ok, _ := s.Get("missing"); ok {
		t.Fatal("Get on missing key: want ok=false")
	}

	s.Set("k", "v1")
	s.Set("k", "v2") // overwrite
	if v, ok, _ := s.Get("k"); !ok || v != "v2" {
		t.Fatalf("Get(k) = %q, %v; want \"v2\", true", v, ok)
	}

	s.Set("a", "1")
	// "k" and "a" exist, "nope" does not, "k" is listed twice.
	if n, _ := s.Del("k", "a", "nope", "k"); n != 2 {
		t.Fatalf("Del count = %d, want 2", n)
	}
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("key still present after Del")
	}
}
func TestStoreTTL(t *testing.T) {
	s := NewStore()

	s.SetWithTTL("name", "Hemant", 100*time.Millisecond)

	value, ok, _ := s.Get("name")
	if !ok || value != "Hemant" {
		t.Fatal("Expected key to exist before expiration")
	}

	time.Sleep(150 * time.Millisecond)

	value, ok, _ = s.Get("name")
	if ok {
		t.Fatalf("Expected key to expire, got value: %s", value)
	}
}
func TestSampledExpirationMixedKeys(t *testing.T) {
	s := NewStore()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("expired%d", i)
		s.SetWithTTL(key, "value", 100*time.Millisecond)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("live%d", i)
		s.Set(key, "value")
	}

	// Keys with a TTL that hasn't passed are in the expires map, so the
	// sampler sees them and must leave them alone.
	for i := 0; i < 10; i++ {
		s.SetWithTTL(fmt.Sprintf("future%d", i), "value", time.Hour)
	}

	time.Sleep(150 * time.Millisecond)
	s.removeExpired()

	for i := 0; i < 10; i++ {
		if s.raw(fmt.Sprintf("live%d", i)) == nil {
			t.Fatalf("Live key live%d was incorrectly deleted", i)
		}
		if s.raw(fmt.Sprintf("future%d", i)) == nil {
			t.Fatalf("Key future%d with a future TTL was deleted", i)
		}
	}

	for i := 0; i < 10; i++ {
		if s.raw(fmt.Sprintf("expired%d", i)) != nil {
			t.Fatalf("Expired key expired%d was not deleted", i)
		}
	}
}

func TestActiveExpiration(t *testing.T) {
	s := NewStore()

	s.SetWithTTL("name", "Hemant", 100*time.Millisecond)

	go s.StartCleanup(20 * time.Millisecond)

	time.Sleep(200 * time.Millisecond)

	if s.raw("name") != nil {
		t.Fatal("Expected active cleanup to delete expired key")
	}
}

// SET without EX must make a key permanent again, as in Redis.
// This also checks the two maps stay in sync.
func TestSetClearsTTL(t *testing.T) {
	s := NewStore()
	s.SetWithTTL("k", "old", 20*time.Millisecond)
	s.Set("k", "new")
	time.Sleep(40 * time.Millisecond)

	if v, ok, _ := s.Get("k"); !ok || v != "new" {
		t.Fatalf("Get(k) = %q, %v; want \"new\", true (TTL should be cleared)", v, ok)
	}
	if _, hasTTL := s.deadline("k"); hasTTL {
		t.Fatal("k still listed in expires after plain SET")
	}
}

// One cleanup cycle must do bounded work, however many keys are expired.
func TestActiveExpiryBoundedWork(t *testing.T) {
	s := NewStore()
	for i := 0; i < 1000; i++ {
		s.SetWithTTL(fmt.Sprintf("k%d", i), "v", time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)

	// Count from what is stored now: with a 1ms TTL, a slow run can expire
	// a key before SetAt stores it, and SetAt deletes it right away.
	before, _ := s.sizes()
	s.removeExpired()
	after, _ := s.sizes()
	removed := before - after
	if max := expireSampleSize * expireMaxRounds; removed == 0 || removed > max {
		t.Fatalf("one cycle removed %d keys, want between 1 and %d", removed, max)
	}
}

// A few expired keys hidden among many permanent ones must still be found,
// because the sample only looks at keys that have a TTL.
func TestActiveExpiryFindsFewAmongMany(t *testing.T) {
	s := NewStore()
	for i := 0; i < 10000; i++ {
		s.Set(fmt.Sprintf("perm%d", i), "v")
	}
	for i := 0; i < 50; i++ {
		s.SetWithTTL(fmt.Sprintf("tmp%d", i), "v", time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)

	for c := 0; c < 5; c++ {
		s.removeExpired()
	}

	if keys, withTTL := s.sizes(); keys != 10000 || withTTL != 0 {
		t.Fatalf("after cleanup: %d keys, %d with TTL; want 10000 and 0", keys, withTTL)
	}
}

// DEL must not count a key whose TTL has passed, even if the
// background cleanup hasn't removed it yet.
func TestStoreDelIgnoresExpired(t *testing.T) {
	s := NewStore()
	s.SetWithTTL("old", "v", 10*time.Millisecond)
	s.Set("live", "v")
	time.Sleep(30 * time.Millisecond)

	if n, _ := s.Del("old", "live"); n != 1 {
		t.Fatalf("Del count = %d, want 1 (only the live key)", n)
	}
}

// Many goroutines hit the store at once, like many clients.
// Without the mutex this crashes with "concurrent map writes",
// and `go test -race` reports the data race.
func TestStoreConcurrent(t *testing.T) {
	s := NewStore()
	const workers, ops = 50, 1000

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key-%d", i%20) // shared keys, so writers collide
				s.Set(key, fmt.Sprintf("w%d", w))
				s.Get(key)
				if i%10 == 0 {
					s.Del(key)
				}
			}
		}(w)
	}
	wg.Wait()

	// Each worker writes its own key once; all of them must be there.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("own-%d", w), "x")
		}(w)
	}
	wg.Wait()
	for w := 0; w < workers; w++ {
		if _, ok, _ := s.Get(fmt.Sprintf("own-%d", w)); !ok {
			t.Fatalf("own-%d lost", w)
		}
	}
}

// listOf builds a *list.List from values, for tests that set up a list directly.
func listOf(items ...string) *list.List {
	l := list.New()
	for _, it := range items {
		l.PushBack(it)
	}
	return l
}

// --- test helpers: read and write a store's internals under the lock ---

// SetWithTTL stores value with a TTL counted from now. The server itself
// always goes through SetAt (dispatch turns EX into a deadline first).
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) error {
	return s.SetAt(key, value, time.Now().Add(ttl))
}

// raw returns the stored value for key without any expiry check, or nil.
func (s *Store) raw(key string) *Value {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[key]
}

// put stores v at key directly, for setting up types the API can't make.
func (s *Store) put(key string, v *Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = v
}

// deadline returns key's expiry time, if it has one.
func (s *Store) deadline(key string) (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	at, ok := s.expires[key]
	return at, ok
}

// sizes counts all keys, and the keys with a TTL.
func (s *Store) sizes() (keys, withTTL int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data), len(s.expires)
}

// A read must not delete an expired key: readers share the read lock, so
// a write from one of them would race with the others. Active expiry
// removes it instead.
func TestReadLeavesExpiredKey(t *testing.T) {
	s := NewStore()
	s.SetWithTTL("k", "v", time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("GET returned an expired key")
	}
	if s.raw("k") == nil {
		t.Fatal("GET deleted the key; reads must not change the maps")
	}
	s.removeExpired()
	if s.raw("k") != nil {
		t.Fatal("active expiry didn't remove the expired key")
	}
}
