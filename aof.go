package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// AOF is an append-only file of write commands, stored in RESP format:
// the same bytes a client sends, so readCommand can read them back.
type AOF struct {
	mu sync.Mutex // one Append at a time, so commands never interleave
	f  *os.File
}

// OpenAOF opens the file at path, creating it if it doesn't exist.
//
// O_APPEND makes the OS put every write at the current end of the file,
// so existing commands are never overwritten. O_RDWR also allows reading,
// which replay will need later.
func OpenAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &AOF{f: f}, nil
}

// encodeCommand encodes args as a RESP array of bulk strings:
//
//	*<count>\r\n  then, per arg:  $<len>\r\n<bytes>\r\n
//
// Each length comes before its bytes, so an arg may hold anything:
// spaces, "\r\n", zero bytes, or nothing at all.
func encodeCommand(args []string) []byte {
	b := make([]byte, 0, 16+len(args)*16)
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	return b
}

// Append writes one command to the end of the file.
//
// The command is encoded first, outside the lock, and written with a
// single Write. os.File.Write keeps writing until every byte is written
// or an error occurs, and returns an error whenever it wrote fewer bytes.
// The n check below makes that guarantee explicit. If a write fails
// halfway, the file ends in a partial command, which replay will detect
// as an incomplete final command.
func (a *AOF) Append(args ...string) error {
	buf := encodeCommand(args)

	a.mu.Lock()
	defer a.mu.Unlock()

	n, err := a.f.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}
	return nil
}

// Close closes the file. It takes the lock so it can't close the file
// in the middle of an Append. Appending after Close returns an error.
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Close()
}

// errTornTail means the AOF ends partway through a command: the process
// died, or the disk filled, in the middle of an Append. That command was
// never acknowledged, because the reply comes after Append returns, so it
// is safe to drop.
var errTornTail = errors.New("AOF ends with a partial command")

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// replay runs every command in r against s. It returns how many commands
// it ran and good, the byte offset where the last complete command ends.
// s must not have an AOF attached, or replay would append each command
// back onto the file it is reading.
//
// replay only reads and reports. The caller owns the file and decides what
// to do with it, so tests can replay from a bytes.Reader.
//
//   - The file ends partway through a command: errTornTail. The caller
//     should truncate the file to good and start.
//   - Any other parse error, or a command that gets an error reply: the
//     file is damaged in the middle. The file only ever holds commands that
//     succeeded, and dropping what follows would lose acknowledged writes,
//     so the caller should refuse to start.
func replay(s *Store, r io.Reader) (n int, good int64, err error) {
	counter := &countingReader{r: r}
	reader := bufio.NewReader(counter)
	var reply bytes.Buffer
	for ; ; n++ {
		args, err := readCommand(reader)
		if err == io.EOF {
			return n, good, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return n, good, errTornTail
		}
		if err != nil {
			return n, good, fmt.Errorf("command %d at byte %d: %w", n+1, good, err)
		}

		reply.Reset()
		dispatch(s, &reply, args) // writes to a bytes.Buffer never fail
		if bytes.HasPrefix(reply.Bytes(), []byte("-")) {
			return n, good, fmt.Errorf("command %d %q at byte %d: %s", n+1, args, good, strings.TrimSpace(reply.String()))
		}

		// bufio reads ahead, so the bytes consumed so far are the bytes
		// pulled from r minus the bytes still waiting in the buffer.
		good = counter.n - int64(reader.Buffered())
	}
}
