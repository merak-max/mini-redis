package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// Run just these while building lists:  go test -race -run List ./...

func TestListPushAndLen(t *testing.T) {
	c := newConn(t)
	k := key(t, "k")

	c.expect(":0\r\n", "LLEN", k)                 // missing key = empty list
	c.expect(":3\r\n", "LPUSH", k, "a", "b", "c") // each goes to the front: c b a
	c.expect(":3\r\n", "LLEN", k)
	c.expect(":4\r\n", "RPUSH", k, "d")
	c.expect(arr("c", "b", "a", "d"), "LRANGE", k, "0", "-1")
	c.expect(":6\r\n", "rpush", k, "e", "f") // lowercase works
	c.expect(arr("c", "b", "a", "d", "e", "f"), "LRANGE", k, "0", "-1")
}

func TestListPop(t *testing.T) {
	c := newConn(t)
	k := key(t, "k")

	c.expect("$-1\r\n", "LPOP", k) // missing key
	c.expect(":3\r\n", "RPUSH", k, "a", "b", "c")
	c.expect("$1\r\na\r\n", "LPOP", k)
	c.expect("$1\r\nc\r\n", "RPOP", k)
	c.expect(":1\r\n", "LLEN", k)
	c.expect("$1\r\nb\r\n", "LPOP", k)

	// Popping the last element deletes the key.
	c.expect("$-1\r\n", "LPOP", k)
	c.expect(":0\r\n", "LLEN", k)
	c.expect(":0\r\n", "DEL", k)                  // nothing left to delete
	c.expect("+OK\r\n", "SET", k, "now-a-string") // so the key is free for any type
}

func TestListRange(t *testing.T) {
	c := newConn(t)
	k := key(t, "k")
	c.expect(":5\r\n", "RPUSH", k, "a", "b", "c", "d", "e")

	tests := []struct {
		start, stop string
		want        []string
	}{
		{"0", "-1", []string{"a", "b", "c", "d", "e"}},  // whole list
		{"1", "3", []string{"b", "c", "d"}},             // stop is inclusive
		{"-2", "-1", []string{"d", "e"}},                // negative = from the end
		{"0", "100", []string{"a", "b", "c", "d", "e"}}, // stop past the end is clamped
		{"-100", "1", []string{"a", "b"}},               // start before the beginning is clamped
		{"5", "10", []string{}},                         // start past the end = empty
		{"3", "1", []string{}},                          // start > stop = empty
		{"2", "2", []string{"c"}},                       // single element
	}
	for _, tt := range tests {
		c.expect(arr(tt.want...), "LRANGE", k, tt.start, tt.stop)
	}
	c.expect(arr(), "LRANGE", key(t, "missing"), "0", "-1") // missing key = empty array
}

func TestListWrongType(t *testing.T) {
	c := newConn(t)
	s, l := key(t, "str"), key(t, "list")

	c.expect("+OK\r\n", "SET", s, "v")
	c.expect(wrongType, "LPUSH", s, "x")
	c.expect(wrongType, "RPUSH", s, "x")
	c.expect(wrongType, "LPOP", s)
	c.expect(wrongType, "RPOP", s)
	c.expect(wrongType, "LLEN", s)
	c.expect(wrongType, "LRANGE", s, "0", "-1")
	c.expect("$1\r\nv\r\n", "GET", s) // the string is untouched

	c.expect(":1\r\n", "RPUSH", l, "a")
	c.expect(wrongType, "GET", l)

	// SET replaces a value of any type, and DEL removes any type.
	c.expect("+OK\r\n", "SET", l, "now-string")
	c.expect("$10\r\nnow-string\r\n", "GET", l)
	c.expect(":1\r\n", "RPUSH", key(t, "l2"), "a")
	c.expect(":1\r\n", "DEL", key(t, "l2"))
	c.expect("+PONG\r\n", "PING") // WRONGTYPE never closes the connection
}

func TestListArgumentErrors(t *testing.T) {
	c := newConn(t)
	k := key(t, "k")

	c.expect("-ERR wrong number of arguments for 'lpush' command\r\n", "LPUSH", k)
	c.expect("-ERR wrong number of arguments for 'rpush' command\r\n", "RPUSH", k)
	c.expect("-ERR wrong number of arguments for 'lpop' command\r\n", "LPOP")
	c.expect("-ERR wrong number of arguments for 'rpop' command\r\n", "RPOP")
	c.expect("-ERR wrong number of arguments for 'llen' command\r\n", "LLEN")
	c.expect("-ERR wrong number of arguments for 'lrange' command\r\n", "LRANGE", k, "0")
	c.expect("-ERR value is not an integer or out of range\r\n", "LRANGE", k, "a", "1")
	c.expect("+PONG\r\n", "PING")
}

// 20 clients push to the same list at once; no element may be lost.
func TestListConcurrentPush(t *testing.T) {
	k := key(t, "k")
	const clients, each = 20, 50

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := newConn(t)
			for j := 0; j < each; j++ {
				if got := c.do("RPUSH", k, fmt.Sprintf("%d-%d", i, j)); !strings.HasPrefix(got, ":") {
					t.Errorf("RPUSH: got %q", got)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	c := newConn(t)
	c.expect(fmt.Sprintf(":%d\r\n", clients*each), "LLEN", k)
}
