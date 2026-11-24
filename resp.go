package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	maxArrayLen = 1024
	maxBulkLen  = 1 << 20 // 1 MB
)

func readCommand(reader *bufio.Reader) ([]string, error) {

	// Step 1: Read the first byte
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err // Clean EOF if no command has started
	}

	// Step 2: Check RESP array prefix
	if prefix != '*' {
		return nil, fmt.Errorf("expected array prefix '*', got %q", prefix)
	}

	// Step 3: Read array length
	lengthStr, err := readRESPLine(reader)
	if err != nil {
		return nil, err
	}

	// Step 4: Parse and validate array length
	arrayLength, err := parseLength(lengthStr, maxArrayLen)
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}

	args := make([]string, 0, arrayLength)

	// Step 5: Read each bulk string
	for i := 0; i < arrayLength; i++ {

		prefix, err := reader.ReadByte()
		if err != nil {
			return nil, incompleteError(err)
		}

		if prefix != '$' {
			return nil, fmt.Errorf("expected bulk string prefix '$', got %q", prefix)
		}

		// Read bulk string length
		lengthStr, err := readRESPLine(reader)
		if err != nil {
			return nil, err
		}

		// Validate before allocating memory
		bulkLength, err := parseLength(lengthStr, maxBulkLen)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}

		// Read exact number of bytes
		buffer := make([]byte, bulkLength)

		_, err = io.ReadFull(reader, buffer)
		if err != nil {
			return nil, incompleteError(err)
		}

		// Validate trailing CRLF
		terminator := make([]byte, 2)

		_, err = io.ReadFull(reader, terminator)
		if err != nil {
			return nil, incompleteError(err)
		}

		if string(terminator) != "\r\n" {
			return nil, fmt.Errorf("invalid RESP terminator")
		}

		args = append(args, string(buffer))
	}

	// Step 6: Return complete command
	return args, nil
}

// Reads one RESP header line and returns it without the trailing CRLF.
func readRESPLine(reader *bufio.Reader) (string, error) {
	// ReadSlice never grows past the reader's buffer (4096 bytes by default),
	// so a client cannot make us buffer an endless header. A long header
	// that does fit is still rejected by parseLength.
	line, err := reader.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			return "", fmt.Errorf("RESP header too long")
		}
		return "", incompleteError(err)
	}

	// Copy out of the reader's buffer before the next read overwrites it.
	s := string(line)
	if !strings.HasSuffix(s, "\r\n") {
		return "", fmt.Errorf("invalid RESP line ending")
	}

	return strings.TrimSuffix(s, "\r\n"), nil
}

// Parses a length that must be plain digits (no sign, no spaces) in [0, max].
// strconv.Atoi alone would accept "+3", which RESP does not allow.
func parseLength(s string, max int) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("%q is not a non-negative integer", s)
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n > max {
		return 0, fmt.Errorf("%q exceeds limit %d", s, max)
	}
	return n, nil
}

// Distinguishes incomplete commands from clean disconnects
func incompleteError(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
