package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

const listenAddr = ":6380"

// maxExpireSeconds is the largest EX value that fits in a time.Duration
// (int64 nanoseconds), about 292 years. Anything larger would overflow.
const maxExpireSeconds = int64(math.MaxInt64) / int64(time.Second)

// One shared Store for all client connections.
var store = NewStore()

func main() {
	go store.StartCleanup(time.Second)

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
				_, _ = io.WriteString(conn, "-ERR "+err.Error()+"\r\n")
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		switch strings.ToUpper(args[0]) {

		case "PING":
			switch len(args) {
			case 1:
				_, err = io.WriteString(conn, "+PONG\r\n")
			case 2:
				err = writeBulk(conn, args[1])
			default:
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'ping' command\r\n")
			}

		case "SET":
			switch len(args) {
			case 3:
				store.Set(args[1], args[2])
				_, err = io.WriteString(conn, "+OK\r\n")

			case 5:
				if !strings.EqualFold(args[3], "EX") {
					_, err = io.WriteString(conn, "-ERR syntax error\r\n")
					break
				}

				seconds, convErr := strconv.Atoi(args[4])
				if convErr != nil || seconds <= 0 || int64(seconds) > maxExpireSeconds {
					_, err = io.WriteString(conn, "-ERR invalid expire time in 'set' command\r\n")
					break
				}

				ttl := time.Duration(seconds) * time.Second
				store.SetWithTTL(args[1], args[2], ttl)

				_, err = io.WriteString(conn, "+OK\r\n")

			default:
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'set' command\r\n")
			}

		case "GET":
			if len(args) != 2 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'get' command\r\n")
				break
			}

			value, exists := store.Get(args[1])

			if exists {
				err = writeBulk(conn, value)
			} else {
				_, err = io.WriteString(conn, "$-1\r\n")
			}

		case "DEL":
			if len(args) < 2 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'del' command\r\n")
				break
			}

			count := store.Del(args[1:]...)
			_, err = fmt.Fprintf(conn, ":%d\r\n", count)

		default:
			_, err = io.WriteString(conn, "-ERR unknown command\r\n")
		}

		if err != nil {
			log.Println("Response write error:", err)
			return
		}
	}
}

// Sends a value as a RESP bulk string.
func writeBulk(w io.Writer, s string) error {
	_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
	return err
}
