package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// These tests expect, in package main:
//
//	func readCommand(r *bufio.Reader) ([]string, error)
//	const maxArrayLen = ...  // max number of arguments in one command
//	const maxBulkLen  = ...  // max bytes in one bulk string
//
// readCommand reads exactly one RESP2 array of bulk strings from r.
// It returns io.EOF only when r ends before the first byte of a command.

func TestReadCommandValid(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"PING", "*1\r\n$4\r\nPING\r\n", []string{"PING"}},
		{"GET", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", []string{"GET", "key"}},
		{"SET", "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", []string{"SET", "key", "value"}},
		{"DEL many", "*4\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n", []string{"DEL", "a", "b", "c"}},
		{"empty bulk string", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n", []string{"SET", "k", ""}},
		{"value contains CRLF", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n", []string{"SET", "k", "a\r\nb"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readCommand(bufio.NewReader(strings.NewReader(tt.input)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TCP can deliver a command in pieces. OneByteReader returns one byte per Read.
func TestReadCommandSplitAcrossReads(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	r := bufio.NewReader(iotest.OneByteReader(strings.NewReader(input)))
	got, err := readCommand(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"SET", "key", "value"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TCP can also deliver several commands in one read (pipelining).
func TestReadCommandPipelined(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	r := bufio.NewReader(strings.NewReader(input))
	for _, want := range [][]string{{"PING"}, {"GET", "k"}} {
		got, err := readCommand(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	if _, err := readCommand(r); !errors.Is(err, io.EOF) {
		t.Fatalf("after last command: got %v, want io.EOF", err)
	}
}

// A client that disconnects cleanly between commands is not an error.
func TestReadCommandCleanEOF(t *testing.T) {
	_, err := readCommand(bufio.NewReader(strings.NewReader("")))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestReadCommandMalformed(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"not an array", "+PING\r\n"},
		{"array length not a number", "*a\r\n"},
		{"array length empty", "*\r\n"},
		{"negative array length", "*-1\r\n"},
		{"array length with plus sign", "*+1\r\n$4\r\nPING\r\n"},
		{"bulk length with plus sign", "*1\r\n$+4\r\nPING\r\n"},
		{"array too long", fmt.Sprintf("*%d\r\n", maxArrayLen+1)},
		{"element is not a bulk string", "*1\r\n:4\r\n"},
		{"bulk length not a number", "*1\r\n$x\r\nPING\r\n"},
		{"negative bulk length", "*1\r\n$-1\r\n"},
		{"bulk too long", fmt.Sprintf("*1\r\n$%d\r\n", maxBulkLen+1)},
		{"data longer than length", "*1\r\n$4\r\nPINGX\r\n"},
		{"data shorter than length", "*1\r\n$5\r\nPING\r\n"},
		{"bulk missing CRLF terminator", "*1\r\n$4\r\nPING\n\n"},
		{"header ends with bare LF", "*1\n$4\r\nPING\r\n"},
		{"truncated in header", "*2\r\n$3\r\nGE"},
		{"truncated before second element", "*2\r\n$3\r\nGET\r\n"},
		{"truncated in data", "*1\r\n$4\r\nPI"},
		{"header longer than limit", "*" + strings.Repeat("1", 100) + "\r\n"},
		{"header with no newline", "*" + strings.Repeat("1", 10000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readCommand(bufio.NewReader(strings.NewReader(tt.input)))
			if err == nil {
				t.Fatalf("expected error, got command %q", got)
			}
			// Input ended mid-command, so this is not a clean disconnect.
			if errors.Is(err, io.EOF) {
				t.Fatalf("got bare io.EOF; mid-command EOF should be a different error (e.g. io.ErrUnexpectedEOF)")
			}
		})
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
