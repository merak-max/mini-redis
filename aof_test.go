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
