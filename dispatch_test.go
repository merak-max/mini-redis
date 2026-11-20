package main

import (
	"bytes"
	"errors"
	"testing"
)

// run sends one command to dispatch and returns the reply bytes.
// No network: the reply goes into a bytes.Buffer.
func run(t *testing.T, s *Store, args ...string) string {
	t.Helper()
	var buf bytes.Buffer
	if err := dispatch(s, &buf, args); err != nil {
		t.Fatalf("dispatch(%q): %v", args, err)
	}
	return buf.String()
}

// Each step runs on the same fresh Store, in order, so later steps see
// earlier writes. The global store is never touched.
func TestDispatchWithBuffer(t *testing.T) {
	s := NewStore()
	steps := []struct {
		args []string
		want string
	}{
		{[]string{"PING"}, "+PONG\r\n"},
		{[]string{"SET", "k", "v"}, "+OK\r\n"},
		{[]string{"GET", "k"}, "$1\r\nv\r\n"},
		{[]string{"GET", "missing"}, "$-1\r\n"},
		{[]string{"RPUSH", "l", "a", "b", "c"}, ":3\r\n"},
		{[]string{"LPOP", "l"}, "$1\r\na\r\n"},
		{[]string{"LRANGE", "l", "0", "-1"}, arr("b", "c")},
		{[]string{"ZADD", "z", "2", "b", "1.5", "a"}, ":2\r\n"},
		{[]string{"ZRANGE", "z", "0", "-1", "WITHSCORES"}, arr("a", "1.5", "b", "2")},
		{[]string{"GET", "l"}, wrongType},
		{[]string{"DEL", "k", "l", "z", "missing"}, ":3\r\n"},
		{[]string{"SET", "k"}, "-ERR wrong number of arguments for 'set' command\r\n"},
		{[]string{"NOSUCHCMD"}, "-ERR unknown command\r\n"},
		{[]string{}, ""}, // "*0\r\n": no reply, as before the refactor
	}
	for _, st := range steps {
		if got := run(t, s, st.args...); got != st.want {
			t.Fatalf("%q: got %q, want %q", st.args, got, st.want)
		}
	}

	// dispatch used s, not the global store.
	if _, ok, _ := store.Get("k"); ok {
		t.Fatal("dispatch wrote to the global store")
	}
}

// failWriter rejects every write, like a connection the client closed.
type failWriter struct{}

var errClosed = errors.New("closed")

func (failWriter) Write([]byte) (int, error) { return 0, errClosed }

// handleConnection stops serving a client when a reply can't be written,
// so dispatch must return write errors instead of swallowing them.
func TestDispatchReturnsWriteError(t *testing.T) {
	s := NewStore()
	for _, args := range [][]string{
		{"PING"},
		{"SET", "k", "v"},
		{"GET", "k"},
		{"ZRANGE", "z", "0", "-1"}, // writeArray path
		{"NOSUCHCMD"},
	} {
		if err := dispatch(s, failWriter{}, args); !errors.Is(err, errClosed) {
			t.Fatalf("%q: got %v, want the write error", args, err)
		}
	}
}
