package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

const listenAddr = ":6380"

func main() {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("Redis server listening on", listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection error:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Println("Client connected:", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		args, err := readCommand(reader)

		if err != nil {
			if err == io.EOF {
				log.Println("Client disconnected:", conn.RemoteAddr())
			} else {
				log.Println("Command parsing error:", err)
				// Ignore the write error: we close the connection either way.
				_, _ = io.WriteString(conn, "-ERR "+err.Error()+"\r\n")
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		switch {
		case strings.EqualFold(args[0], "PING"):
			switch len(args) {
			case 1:
				_, err = io.WriteString(conn, "+PONG\r\n")
			case 2:
				err = writeBulk(conn, args[1]) // PING <message> echoes the message
			default:
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'ping' command\r\n")
			}

		default:
			_, err = io.WriteString(conn, "-ERR unknown command\r\n")
		}

		// Handle any failed response write
		if err != nil {
			log.Println("Response write error:", err)
			return
		}
	}
}

// writeBulk sends s as a RESP bulk string: $<len>\r\n<bytes>\r\n.
// Unlike a +simple string, this is safe for any bytes, including \r\n.
func writeBulk(w io.Writer, s string) error {
	_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
	return err
}
