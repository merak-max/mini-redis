package main

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

// Benchmarks. Run them with:
//
//	go test -run '^$' -bench . -benchmem
//
// -run '^$' skips the normal tests. Each benchmark builds its data before
// b.Loop(), so only the loop body is timed.

// benchKeys returns n distinct keys, built outside the timed loop so key
// formatting isn't counted as part of the command.
func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", i)
	}
	return keys
}

// SET with and without the AOF: the difference is the cost of logging,
// one write() syscall per command while holding the store lock.
func BenchmarkSet(b *testing.B) {
	keys := benchKeys(1024)

	b.Run("memory", func(b *testing.B) {
		s := NewStore()
		i := 0
		for b.Loop() {
			s.Set(keys[i%len(keys)], "value")
			i++
		}
	})

	b.Run("aof", func(b *testing.B) {
		a, err := OpenAOF(filepath.Join(b.TempDir(), "appendonly.aof"))
		if err != nil {
			b.Fatal(err)
		}
		defer a.Close()
		s := NewStore()
		s.aof = a
		i := 0
		for b.Loop() {
			if err := s.Set(keys[i%len(keys)], "value"); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkGet(b *testing.B) {
	keys := benchKeys(100_000)
	s := NewStore()
	for _, k := range keys {
		s.Set(k, "value")
	}
	i := 0
	for b.Loop() {
		if _, ok, _ := s.Get(keys[i%len(keys)]); !ok {
			b.Fatal("key missing") // proves the lookup really ran and hit
		}
		i++
	}
}

// ZRANK on growing sorted sets. If rank is O(log n), going from 1K to 1M
// members (1000x) should cost only a few times more per call.
func BenchmarkZRank(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			s := NewStore()
			members := make([]ZMember, n)
			for i := range members {
				members[i] = ZMember{Member: fmt.Sprintf("m%d", i), Score: float64(i)}
			}
			if _, err := s.ZAdd("z", members); err != nil {
				b.Fatal(err)
			}
			// Random order, so the CPU cache can't make every lookup cheap.
			r := rand.New(rand.NewSource(1))
			order := r.Perm(n)
			i := 0
			for b.Loop() {
				m := members[order[i%n]]
				rank, ok, err := s.ZRank("z", m.Member)
				if err != nil || !ok || rank != int(m.Score) {
					b.Fatalf("ZRank(%s) = %d, %v, %v; want %d", m.Member, rank, ok, err, int(m.Score))
				}
				i++
			}
		})
	}
}

// GET and SET from many goroutines at once. Run with -cpu 1,2,4,8 to see
// whether throughput grows with cores. With one global lock, it shouldn't.
func BenchmarkParallel(b *testing.B) {
	keys := benchKeys(100_000)

	b.Run("get", func(b *testing.B) {
		s := NewStore()
		for _, k := range keys {
			s.Set(k, "value")
		}
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Intn(len(keys))
			for pb.Next() {
				if _, ok, _ := s.Get(keys[i%len(keys)]); !ok {
					b.Error("key missing")
					return
				}
				i++
			}
		})
	})

	b.Run("set", func(b *testing.B) {
		s := NewStore()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Intn(len(keys))
			for pb.Next() {
				s.Set(keys[i%len(keys)], "value")
				i++
			}
		})
	})
}

// One active-expiry cycle with 1M keys that have a TTL but haven't expired.
// The cycle samples from expires instead of scanning, so its cost shouldn't
// depend on how many keys there are.
func BenchmarkActiveExpiryCycle(b *testing.B) {
	s := NewStore()
	for i := 0; i < 1_000_000; i++ {
		s.SetWithTTL(fmt.Sprintf("k%d", i), "v", time.Hour)
	}
	for b.Loop() {
		s.removeExpired()
	}
	if keys, _ := s.sizes(); keys != 1_000_000 {
		b.Fatalf("%d keys left, want 1000000: nothing should have expired", keys)
	}
}

// Parallel GET and SET with different shard counts, to choose
// defaultShards. Compare with -cpu 1,8: with 1 shard this is the old
// single-lock store.
func BenchmarkShards(b *testing.B) {
	keys := benchKeys(100_000)
	for _, n := range []int{1, 4, 16, 64, 256} {
		s := newStoreWithShards(n)
		for _, k := range keys {
			s.Set(k, "value")
		}
		b.Run(fmt.Sprintf("get/shards=%d", n), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Intn(len(keys))
				for pb.Next() {
					if _, ok, _ := s.Get(keys[i%len(keys)]); !ok {
						b.Error("key missing")
						return
					}
					i++
				}
			})
		})
		b.Run(fmt.Sprintf("set/shards=%d", n), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Intn(len(keys))
				for pb.Next() {
					s.Set(keys[i%len(keys)], "value")
					i++
				}
			})
		})
	}
}
