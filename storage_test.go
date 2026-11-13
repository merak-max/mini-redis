package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestStoreBasic(t *testing.T) {
	s := NewStore()

	if _, ok := s.Get("missing"); ok {
		t.Fatal("Get on missing key: want ok=false")
	}

	s.Set("k", "v1")
	s.Set("k", "v2") // overwrite
	if v, ok := s.Get("k"); !ok || v != "v2" {
		t.Fatalf("Get(k) = %q, %v; want \"v2\", true", v, ok)
	}

	s.Set("a", "1")
	// "k" and "a" exist, "nope" does not, "k" is listed twice.
	if n := s.Del("k", "a", "nope", "k"); n != 2 {
		t.Fatalf("Del count = %d, want 2", n)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("key still present after Del")
	}
}
func TestStoreTTL(t *testing.T) {
	s := NewStore()

	s.SetWithTTL("name", "Hemant", 100*time.Millisecond)

	value, ok := s.Get("name")
	if !ok || value != "Hemant" {
		t.Fatal("Expected key to exist before expiration")
	}

	time.Sleep(150 * time.Millisecond)

	value, ok = s.Get("name")
	if ok {
		t.Fatalf("Expected key to expire, got value: %s", value)
	}
}
func TestSampledExpirationAllExpired(t *testing.T) {
	s := NewStore()

	// 1. Insert 20 keys with 100ms TTL
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		s.SetWithTTL(key, "value", 100*time.Millisecond)
	}

	// 2. Wait for keys to expire
	time.Sleep(150 * time.Millisecond)

	// 3. Run sampled cleanup
	s.removeExpired()

	// 4. Check remaining keys
	s.mu.RLock()
	remaining := len(s.data)
	s.mu.RUnlock()

	if remaining != 0 {
		t.Fatalf("Expected 0 keys, got %d", remaining)
	}
}
func TestSampledExpirationNoneExpired(t *testing.T) {
	s := NewStore()

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		s.Set(key, "value")
	}

	s.removeExpired()

	s.mu.RLock()
	remaining := len(s.data)
	s.mu.RUnlock()

	if remaining != 20 {
		t.Fatalf("Expected 20 keys, got %d", remaining)
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

	time.Sleep(150 * time.Millisecond)
	s.removeExpired()

	s.mu.RLock()
	defer s.mu.RUnlock()

	for i := 0; i < 10; i++ {
		if _, exists := s.data[fmt.Sprintf("live%d", i)]; !exists {
			t.Fatalf("Live key live%d was incorrectly deleted", i)
		}
	}

	for i := 0; i < 10; i++ {
		if _, exists := s.data[fmt.Sprintf("expired%d", i)]; exists {
			t.Fatalf("Expired key expired%d was not deleted", i)
		}
	}
}

func TestActiveExpiration(t *testing.T) {
	s := NewStore()

	s.SetWithTTL("name", "Hemant", 100*time.Millisecond)

	go s.StartCleanup(20 * time.Millisecond)

	time.Sleep(200 * time.Millisecond)

	s.mu.RLock()
	_, exists := s.data["name"]
	s.mu.RUnlock()

	if exists {
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

	if v, ok := s.Get("k"); !ok || v != "new" {
		t.Fatalf("Get(k) = %q, %v; want \"new\", true (TTL should be cleared)", v, ok)
	}
	s.mu.RLock()
	_, hasTTL := s.expires["k"]
	s.mu.RUnlock()
	if hasTTL {
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

	s.removeExpired()

	s.mu.RLock()
	removed := 1000 - len(s.data)
	s.mu.RUnlock()
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

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.data) != 10000 || len(s.expires) != 0 {
		t.Fatalf("after cleanup: %d keys, %d with TTL; want 10000 and 0", len(s.data), len(s.expires))
	}
}

// DEL must not count a key whose TTL has passed, even if the
// background cleanup hasn't removed it yet.
func TestStoreDelIgnoresExpired(t *testing.T) {
	s := NewStore()
	s.SetWithTTL("old", "v", 10*time.Millisecond)
	s.Set("live", "v")
	time.Sleep(30 * time.Millisecond)

	if n := s.Del("old", "live"); n != 1 {
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
		if _, ok := s.Get(fmt.Sprintf("own-%d", w)); !ok {
			t.Fatalf("own-%d lost", w)
		}
	}
}
