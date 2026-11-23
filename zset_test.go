package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// Run just these:  go test -race -run 'ZSet|SkipList|NormalizeRange' ./...

func TestZSetBasics(t *testing.T) {
	c := newConn(t)
	k := key(t, "board")

	c.expect(":0\r\n", "ZCARD", k) // missing key = empty set
	c.expect(":3\r\n", "ZADD", k, "100", "alice", "50", "bob", "75", "carol")
	c.expect(":3\r\n", "ZCARD", k)
	c.expect("$3\r\n100\r\n", "ZSCORE", k, "alice")
	c.expect("$-1\r\n", "ZSCORE", k, "nobody")

	// Lowest score first.
	c.expect(arr("bob", "carol", "alice"), "ZRANGE", k, "0", "-1")
	c.expect(arr("bob", "50", "carol", "75", "alice", "100"), "ZRANGE", k, "0", "-1", "WITHSCORES")
	c.expect(":0\r\n", "ZRANK", k, "bob")
	c.expect(":2\r\n", "ZRANK", k, "alice")
	c.expect("$-1\r\n", "ZRANK", k, "nobody")
}

// ZADD on an existing member updates its score and moves it.
// The reply counts only new members.
func TestZSetUpdateScore(t *testing.T) {
	c := newConn(t)
	k := key(t, "board")

	c.expect(":2\r\n", "ZADD", k, "1", "a", "2", "b")
	c.expect(":1\r\n", "ZADD", k, "10", "a", "3", "c") // a updated, c new
	c.expect(arr("b", "c", "a"), "ZRANGE", k, "0", "-1")
	c.expect("$2\r\n10\r\n", "ZSCORE", k, "a")
	c.expect(":0\r\n", "ZADD", k, "10", "a") // same score: nothing changes
	c.expect(":3\r\n", "ZCARD", k)
}

func TestZSetTiesAndScores(t *testing.T) {
	c := newConn(t)
	k := key(t, "z")

	// Equal scores are ordered by member name.
	c.expect(":3\r\n", "ZADD", k, "5", "pear", "5", "apple", "5", "mango")
	c.expect(arr("apple", "mango", "pear"), "ZRANGE", k, "0", "-1")

	// Decimal, negative and infinite scores.
	c.expect(":3\r\n", "ZADD", k, "1.5", "half", "-2", "neg", "inf", "top")
	c.expect("$3\r\n1.5\r\n", "ZSCORE", k, "half")
	c.expect("$3\r\ninf\r\n", "ZSCORE", k, "top")
	c.expect(arr("neg", "half", "apple", "mango", "pear", "top"), "ZRANGE", k, "0", "-1")
}

func TestZSetRangeIndexes(t *testing.T) {
	c := newConn(t)
	k := key(t, "z")
	c.expect(":5\r\n", "ZADD", k, "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")

	c.expect(arr("b", "c", "d"), "ZRANGE", k, "1", "3")
	c.expect(arr("d", "e"), "ZRANGE", k, "-2", "-1")
	c.expect(arr("a", "b", "c", "d", "e"), "ZRANGE", k, "0", "100")
	c.expect(arr(), "ZRANGE", k, "5", "10")
	c.expect(arr(), "ZRANGE", k, "3", "1")
	c.expect(arr(), "ZRANGE", key(t, "missing"), "0", "-1")
}

func TestZSetRemove(t *testing.T) {
	c := newConn(t)
	k := key(t, "z")

	c.expect(":3\r\n", "ZADD", k, "1", "a", "2", "b", "3", "c")
	c.expect(":2\r\n", "ZREM", k, "a", "c", "nope")
	c.expect(arr("b"), "ZRANGE", k, "0", "-1")
	c.expect(":0\r\n", "ZRANK", k, "b") // ranks shift after removal

	// Removing the last member deletes the key, like an empty list.
	c.expect(":1\r\n", "ZREM", k, "b")
	c.expect(":0\r\n", "ZCARD", k)
	c.expect("$-1\r\n", "GET", k) // a missing key, not WRONGTYPE
}

func TestZSetWrongType(t *testing.T) {
	c := newConn(t)
	s, l, z := key(t, "str"), key(t, "list"), key(t, "zset")

	c.expect("+OK\r\n", "SET", s, "v")
	c.expect(":1\r\n", "RPUSH", l, "x")
	for _, k := range []string{s, l} {
		c.expect(wrongType, "ZADD", k, "1", "m")
		c.expect(wrongType, "ZREM", k, "m")
		c.expect(wrongType, "ZSCORE", k, "m")
		c.expect(wrongType, "ZCARD", k)
		c.expect(wrongType, "ZRANK", k, "m")
		c.expect(wrongType, "ZRANGE", k, "0", "-1")
	}

	c.expect(":1\r\n", "ZADD", z, "1", "m")
	c.expect(wrongType, "GET", z)
	c.expect(wrongType, "LPUSH", z, "x")
	c.expect(":1\r\n", "DEL", z) // DEL works on any type
}

func TestZSetArgumentErrors(t *testing.T) {
	c := newConn(t)
	k := key(t, "z")

	c.expect("-ERR wrong number of arguments for 'zadd' command\r\n", "ZADD", k, "1")
	c.expect("-ERR syntax error\r\n", "ZADD", k, "1", "a", "2")
	c.expect("-ERR value is not a valid float\r\n", "ZADD", k, "abc", "a")
	c.expect("-ERR value is not a valid float\r\n", "ZADD", k, "nan", "a")
	c.expect(":0\r\n", "ZCARD", k) // the failed ZADDs stored nothing
	c.expect("-ERR wrong number of arguments for 'zrem' command\r\n", "ZREM", k)
	c.expect("-ERR wrong number of arguments for 'zscore' command\r\n", "ZSCORE", k)
	c.expect("-ERR wrong number of arguments for 'zrange' command\r\n", "ZRANGE", k, "0")
	c.expect("-ERR syntax error\r\n", "ZRANGE", k, "0", "1", "BOGUS")
	c.expect("-ERR value is not an integer or out of range\r\n", "ZRANGE", k, "a", "1")
	c.expect("+PONG\r\n", "PING")
}

// 20 clients add members to one sorted set at once; none may be lost,
// and the skip list must stay consistent.
func TestZSetConcurrentAdd(t *testing.T) {
	k := key(t, "z")
	const clients, each = 20, 50

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := newConn(t)
			for j := 0; j < each; j++ {
				got := c.do("ZADD", k, fmt.Sprint(j), fmt.Sprintf("m%d-%d", i, j))
				if !strings.HasPrefix(got, ":") {
					t.Errorf("ZADD: got %q", got)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	c := newConn(t)
	c.expect(fmt.Sprintf(":%d\r\n", clients*each), "ZCARD", k)

	z := store.raw(k).zset // all writers are done, so reading z unlocked is safe
	want := make([]pair, 0, len(z.scores))
	for m, s := range z.scores {
		want = append(want, pair{s, m})
	}
	sortPairs(want)
	checkSkipList(t, z.sl, want)
}
