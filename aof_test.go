package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

// readAll parses every command in data with the real RESP parser.
// It fails the test if data has anything other than whole commands.
func readAll(t *testing.T, data []byte) [][]string {
	t.Helper()
	r := bufio.NewReader(bytes.NewReader(data))
	var cmds [][]string
	for {
		args, err := readCommand(r)
		if err == io.EOF {
			return cmds
		}
		if err != nil {
			t.Fatalf("command %d: %v", len(cmds), err)
		}
		cmds = append(cmds, args)
	}
}

func TestEncodeCommand(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"normal", []string{"SET", "k", "v"}, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"empty arg", []string{"SET", "k", ""}, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n"},
		{"spaces", []string{"SET", "my key", "a b c"}, "*3\r\n$3\r\nSET\r\n$6\r\nmy key\r\n$5\r\na b c\r\n"},
		{"CRLF inside", []string{"SET", "k", "a\r\nb"}, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n"},
		{"binary", []string{"SET", "k", "\x00\xff"}, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n\x00\xff\r\n"},
		{"no args", []string{}, "*0\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeCommand(tt.args)
			if string(got) != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
			// The parser must read back exactly the args we encoded.
			if len(tt.args) > 0 {
				back := readAll(t, got)
				if len(back) != 1 || !reflect.DeepEqual(back[0], tt.args) {
					t.Fatalf("readCommand gave %q, want %q", back, tt.args)
				}
			}
		})
	}
}

func TestOpenAOFCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file should not exist yet: %v", err)
	}

	a, err := OpenAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("file was not created: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("new file has %d bytes, want 0", info.Size())
	}
}

func TestOpenAOFMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such-dir", "appendonly.aof")
	if _, err := OpenAOF(path); err == nil {
		t.Fatal("expected an error for a missing directory")
	}
}

func TestAOFAppendAndReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	cmds := [][]string{
		{"SET", "k", "v"},
		{"RPUSH", "l", "a", "", "c d"},
		{"ZADD", "z", "1.5", "line\r\nbreak"},
	}

	a, err := OpenAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cmds {
		if err := a.Append(c...); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	for _, c := range cmds {
		want = append(want, encodeCommand(c)...)
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("file:\n%q\nwant:\n%q", data, want)
	}
	if got := readAll(t, data); !reflect.DeepEqual(got, cmds) {
		t.Fatalf("read back %q, want %q", got, cmds)
	}
}

// Reopening an existing file must add to it, never overwrite it.
// Otherwise every restart would wipe the data.
func TestAOFReopenAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	for _, c := range [][]string{{"SET", "a", "1"}, {"SET", "b", "2"}} {
		a, err := OpenAOF(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Append(c...); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"SET", "a", "1"}, {"SET", "b", "2"}}
	if got := readAll(t, data); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// 20 goroutines append at once. Every command must appear exactly once and
// whole: no lost writes, and no two commands mixed together.
func TestAOFConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := OpenAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	const writers, each = 20, 50

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				if err := a.Append("SET", fmt.Sprintf("k%d-%d", i, j), "some value"); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cmds := readAll(t, data) // fails if two commands were interleaved
	if len(cmds) != writers*each {
		t.Fatalf("got %d commands, want %d", len(cmds), writers*each)
	}
	seen := map[string]bool{}
	for _, c := range cmds {
		if len(c) != 3 || c[0] != "SET" || c[2] != "some value" || seen[c[1]] {
			t.Fatalf("bad or duplicate command %q", c)
		}
		seen[c[1]] = true
	}
}

func TestAOFAppendAfterClose(t *testing.T) {
	a, err := OpenAOF(filepath.Join(t.TempDir(), "appendonly.aof"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Append("SET", "k", "v"); err == nil {
		t.Fatal("Append after Close should return an error")
	}
}

// newLoggedStore returns a store that logs to an AOF in a temp directory,
// and a function that returns every command in the file so far.
func newLoggedStore(t *testing.T) (*Store, func() [][]string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := OpenAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	s := NewStore()
	s.aof = a
	logged := func() [][]string {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return readAll(t, data)
	}
	return s, logged
}

// Every successful write is logged, in order. Commands that fail or change
// nothing (WRONGTYPE, popping a missing key, deleting a missing key) are not.
func TestStoreLogsWrites(t *testing.T) {
	s, logged := newLoggedStore(t)

	run := func(args ...string) {
		t.Helper()
		var out bytes.Buffer
		if err := dispatch(s, &out, args); err != nil {
			t.Fatalf("dispatch %v: %v", args, err)
		}
	}
	run("SET", "name", "hemant")
	run("SET", "tmp", "v", "EX", "100")
	run("RPUSH", "jobs", "a", "b")
	run("LPUSH", "jobs", "z")
	run("LPOP", "jobs")
	run("RPOP", "missing")    // no key: not logged
	run("LPUSH", "name", "x") // WRONGTYPE: not logged
	run("ZADD", "board", "1.5", "alice", "-inf", "bob")
	run("ZADD", "board", "abc", "carol") // bad score: not logged
	run("ZREM", "board", "bob")
	run("ZREM", "nosuchset", "x") // no key: not logged
	run("DEL", "nope")            // nothing deleted: not logged
	run("DEL", "name", "nope")
	run("GET", "tmp") // reads are never logged

	want := [][]string{
		{"SET", "name", "hemant"},
		{"SET", "tmp", "v", "EX", "100"},
		{"RPUSH", "jobs", "a", "b"},
		{"LPUSH", "jobs", "z"},
		{"LPOP", "jobs"},
		{"ZADD", "board", "1.5", "alice", "-inf", "bob"},
		{"ZREM", "board", "bob"},
		{"DEL", "name", "nope"},
	}
	if got := logged(); !reflect.DeepEqual(got, want) {
		t.Fatalf("AOF contents:\n got %q\nwant %q", got, want)
	}
}

// If the AOF write fails, the client gets an error and memory is unchanged:
// the server never says OK for a write it didn't save.
func TestStoreAOFFailureChangesNothing(t *testing.T) {
	s, _ := newLoggedStore(t)
	var out bytes.Buffer
	dispatch(s, &out, []string{"SET", "k", "old"})
	dispatch(s, &out, []string{"RPUSH", "l", "a"})
	s.aof.Close() // every Append now fails

	cases := [][]string{
		{"SET", "k", "new"},
		{"SET", "k", "new", "EX", "10"},
		{"RPUSH", "l", "b"},
		{"LPOP", "l"},
		{"DEL", "k"},
		{"ZADD", "z", "1", "m"},
	}
	for _, args := range cases {
		out.Reset()
		if err := dispatch(s, &out, args); err != nil {
			t.Fatalf("dispatch %v: %v", args, err)
		}
		if !bytes.HasPrefix(out.Bytes(), []byte("-ERR AOF write failed")) {
			t.Errorf("%v replied %q, want an AOF error", args, out.String())
		}
	}

	if v, ok, _ := s.Get("k"); !ok || v != "old" {
		t.Errorf("k = %q, %v; want \"old\", true", v, ok)
	}
	if items, _ := s.LRange("l", 0, -1); !reflect.DeepEqual(items, []string{"a"}) {
		t.Errorf("l = %q, want [a]", items)
	}
	if n, _ := s.ZCard("z"); n != 0 {
		t.Errorf("ZCARD z = %d, want 0", n)
	}
}

// Writes logged by one store rebuild the same data in a fresh store.
func TestReplayRestoresState(t *testing.T) {
	s, _ := newLoggedStore(t)
	cmds := [][]string{
		{"SET", "name", "hemant"},
		{"SET", "name", "max"}, // overwrite: replay keeps the last value
		{"SET", "gone", "x"},
		{"DEL", "gone"},
		{"RPUSH", "jobs", "a", "b", "c"},
		{"LPOP", "jobs"},
		{"ZADD", "board", "0.1", "alice", "50", "bob", "-inf", "carol"},
		{"ZADD", "board", "90", "bob"}, // score update
		{"ZREM", "board", "carol"},
	}
	var out bytes.Buffer
	for _, c := range cmds {
		dispatch(s, &out, c)
	}
	data, err := os.ReadFile(s.aof.f.Name())
	if err != nil {
		t.Fatal(err)
	}

	fresh := NewStore()
	n, err := replay(fresh, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if n != len(cmds) {
		t.Fatalf("replayed %d commands, want %d", n, len(cmds))
	}

	// Compare what clients would see, byte for byte.
	for _, q := range [][]string{
		{"GET", "name"},
		{"GET", "gone"},
		{"LRANGE", "jobs", "0", "-1"},
		{"ZRANGE", "board", "0", "-1", "WITHSCORES"},
	} {
		var want, got bytes.Buffer
		dispatch(s, &want, q)
		dispatch(fresh, &got, q)
		if got.String() != want.String() {
			t.Errorf("%v after replay = %q, want %q", q, got.String(), want.String())
		}
	}
}

func TestReplayEmptyFile(t *testing.T) {
	n, err := replay(NewStore(), bytes.NewReader(nil))
	if n != 0 || err != nil {
		t.Fatalf("replay(empty) = %d, %v; want 0, nil", n, err)
	}
}

// A damaged file stops replay instead of loading part of the data.
func TestReplayRejectsDamagedFile(t *testing.T) {
	good := encodeCommand([]string{"SET", "k", "v"})
	cases := map[string][]byte{
		// The process died halfway through writing the last command.
		// Milestone 6 will truncate this instead of failing.
		"torn last command": append(append([]byte{}, good...), "*3\r\n$3\r\nSET\r\n$1\r\nk"...),
		"garbage":           append(append([]byte{}, good...), "hello\r\n"...),
		// Each command parses, but the second can't have succeeded originally.
		"failing command": append(append([]byte{}, good...), encodeCommand([]string{"LPUSH", "k", "x"})...),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			n, err := replay(NewStore(), bytes.NewReader(data))
			if err == nil {
				t.Fatal("replay succeeded, want an error")
			}
			if n != 1 {
				t.Errorf("replayed %d commands before the error, want 1", n)
			}
		})
	}
}
