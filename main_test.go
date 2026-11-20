package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// conn is a test client wired to handleConnection through net.Pipe,
// an in-memory connection, so no real port is needed.
type conn struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func newConn(t *testing.T) *conn {
	t.Helper()
	server, client := net.Pipe()
	go handleConnection(server)
	t.Cleanup(func() { client.Close() })
	_ = client.SetDeadline(time.Now().Add(5 * time.Second)) // a hung server fails the test instead of hanging it
	return &conn{t: t, c: client, r: bufio.NewReader(client)}
}

// do sends one command as a RESP array and returns the raw reply.
func (c *conn) do(args ...string) string {
	c.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := io.WriteString(c.c, b.String()); err != nil {
		c.t.Fatalf("write %q: %v", args, err)
	}
	return c.readReply()
}

// readReply reads exactly one RESP reply of any type.
func (c *conn) readReply() string {
	c.t.Helper()
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	switch line[0] {
	case '$': // bulk string: read the announced bytes too
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		if n < 0 {
			return line
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			c.t.Fatalf("read bulk: %v", err)
		}
		return line + string(buf)
	case '*': // array (for LRANGE later): read each element
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		for i := 0; i < n; i++ {
			line += c.readReply()
		}
		return line
	}
	return line
}

func (c *conn) expect(want string, args ...string) {
	c.t.Helper()
	if got := c.do(args...); got != want {
		c.t.Fatalf("%q: got %q, want %q", args, got, want)
	}
}

// The handler uses one global store, so each test uses its own key names,
// and deletes them afterwards so `go test -count=N` reruns start clean.
func key(t *testing.T, name string) string {
	k := t.Name() + ":" + name
	t.Cleanup(func() { store.Del(k) })
	return k
}

func TestPing(t *testing.T) {
	c := newConn(t)
	c.expect("+PONG\r\n", "PING")
	c.expect("+PONG\r\n", "ping") // commands are case-insensitive
	c.expect("$5\r\nhello\r\n", "PING", "hello")
	c.expect("$4\r\na\r\nb\r\n", "PING", "a\r\nb") // bulk reply is binary-safe
	c.expect("-ERR wrong number of arguments for 'ping' command\r\n", "PING", "a", "b")
}

func TestSetGetDel(t *testing.T) {
	c := newConn(t)
	k, missing := key(t, "k"), key(t, "missing")

	c.expect("$-1\r\n", "GET", k)
	c.expect("+OK\r\n", "SET", k, "v1")
	c.expect("$2\r\nv1\r\n", "GET", k)
	c.expect("+OK\r\n", "set", k, "v2") // overwrite, lowercase
	c.expect("$2\r\nv2\r\n", "get", k)
	c.expect(":1\r\n", "DEL", k, missing)
	c.expect("$-1\r\n", "GET", k)
	c.expect(":0\r\n", "DEL", k)
}

func TestSetEX(t *testing.T) {
	c := newConn(t)
	k := key(t, "k")

	c.expect("+OK\r\n", "SET", k, "v", "EX", "100")
	c.expect("$1\r\nv\r\n", "GET", k)

	c.expect("+OK\r\n", "SET", k, "v", "ex", "1") // EX is case-insensitive
	time.Sleep(1100 * time.Millisecond)
	c.expect("$-1\r\n", "GET", k) // expired
}

// Bad arguments get -ERR but the connection stays open,
// because the command itself was well-formed RESP.
func TestArgumentErrorsKeepConnection(t *testing.T) {
	c := newConn(t)
	k := key(t, "k")

	c.expect("-ERR syntax error\r\n", "SET", k, "v", "XX", "5")
	c.expect("-ERR invalid expire time in 'set' command\r\n", "SET", k, "v", "EX", "-5")
	c.expect("-ERR invalid expire time in 'set' command\r\n", "SET", k, "v", "EX", "abc")
	c.expect("-ERR wrong number of arguments for 'set' command\r\n", "SET", k)
	c.expect("-ERR wrong number of arguments for 'get' command\r\n", "GET")
	c.expect("-ERR wrong number of arguments for 'del' command\r\n", "DEL")
	c.expect("$-1\r\n", "GET", k) // none of the failed SETs stored anything

	if got := c.do("NOSUCHCMD"); !strings.HasPrefix(got, "-ERR unknown command") {
		t.Fatalf("unknown command: got %q", got)
	}
	c.expect("+PONG\r\n", "PING") // still connected after all of that
}

// Malformed RESP gets -ERR and then the server closes the connection.
func TestParseErrorClosesConnection(t *testing.T) {
	c := newConn(t)
	if _, err := io.WriteString(c.c, "hello\r\n"); err != nil {
		t.Fatal(err)
	}
	if got := c.readReply(); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("got %q, want an -ERR reply", got)
	}
	if _, err := c.r.ReadByte(); err != io.EOF {
		t.Fatalf("after parse error: got %v, want io.EOF (connection closed)", err)
	}
}

// Two commands written at once must get two replies, in order.
func TestPipelinedCommands(t *testing.T) {
	c := newConn(t)
	k := key(t, "k")
	msg := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(k), k, len(k), k)
	if _, err := io.WriteString(c.c, msg); err != nil {
		t.Fatal(err)
	}
	if got := c.readReply(); got != "+OK\r\n" {
		t.Fatalf("reply 1: got %q", got)
	}
	if got := c.readReply(); got != "$1\r\nv\r\n" {
		t.Fatalf("reply 2: got %q", got)
	}
}

// The server must never reply +OK without storing the key.
// 9300000000 seconds overflows time.Duration (int64 nanoseconds).
func TestSetEXOverflowNeverLies(t *testing.T) {
	c := newConn(t)
	k := key(t, "big")

	got := c.do("SET", k, "v", "EX", "9300000000")
	switch {
	case strings.HasPrefix(got, "-ERR"):
		// Rejecting it is fine.
	case got == "+OK\r\n":
		c.expect("$1\r\nv\r\n", "GET", k) // accepting it is fine only if it's really stored
	default:
		t.Fatalf("unexpected reply %q", got)
	}
}

const wrongType = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

// arr builds the expected RESP array reply for LRANGE.
func arr(items ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(items))
	for _, it := range items {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(it), it)
	}
	return b.String()
}
