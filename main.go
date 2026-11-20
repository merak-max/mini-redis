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

			value, exists, getErr := store.Get(args[1])

			switch {
			case getErr != nil: // e.g. the key holds a list
				_, err = io.WriteString(conn, "-"+getErr.Error()+"\r\n")
			case exists:
				err = writeBulk(conn, value)
			default:
				_, err = io.WriteString(conn, "$-1\r\n")
			}
		case "LPUSH", "RPUSH":
			if len(args) < 3 {
				// One case serves both commands, so the name comes from args[0].
				_, err = fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", strings.ToLower(args[0]))
				break
			}

			front := strings.EqualFold(args[0], "LPUSH")

			length, pushErr := store.Push(args[1], front, args[2:]...)

			if pushErr != nil {
				_, err = fmt.Fprintf(conn, "-%s\r\n", pushErr.Error())
			} else {
				_, err = fmt.Fprintf(conn, ":%d\r\n", length)
			}

		case "LPOP", "RPOP":
			if len(args) != 2 {
				_, err = fmt.Fprintf(conn, "-ERR wrong number of arguments for '%s' command\r\n", strings.ToLower(args[0]))
				break
			}

			front := strings.EqualFold(args[0], "LPOP")
			item, ok, popErr := store.Pop(args[1], front)

			switch {
			case popErr != nil: // the key holds a string
				_, err = fmt.Fprintf(conn, "-%s\r\n", popErr.Error())
			case ok:
				err = writeBulk(conn, item)
			default: // missing key or empty list
				_, err = io.WriteString(conn, "$-1\r\n")
			}

		case "LLEN":
			if len(args) != 2 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'llen' command\r\n")
				break
			}

			length, listErr := store.LLen(args[1])

			if listErr != nil {
				_, err = fmt.Fprintf(conn, "-%s\r\n", listErr.Error())
			} else {
				_, err = fmt.Fprintf(conn, ":%d\r\n", length)
			}

		case "LRANGE": // LRANGE key start stop
			if len(args) != 4 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'lrange' command\r\n")
				break
			}
			start, err1 := strconv.Atoi(args[2])
			stop, err2 := strconv.Atoi(args[3])
			if err1 != nil || err2 != nil {
				_, err = io.WriteString(conn, "-ERR value is not an integer or out of range\r\n")
				break
			}
			items, listErr := store.LRange(args[1], start, stop)
			if listErr != nil {
				_, err = fmt.Fprintf(conn, "-%s\r\n", listErr.Error())
				break
			}
			err = writeArray(conn, items)

		case "DEL":
			if len(args) < 2 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'del' command\r\n")
				break
			}

			count := store.Del(args[1:]...)
			_, err = fmt.Fprintf(conn, ":%d\r\n", count)

		case "ZADD": // ZADD key score member [score member ...]
			if len(args) < 4 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'zadd' command\r\n")
				break
			}
			if len(args)%2 != 0 { // a score without a member
				_, err = io.WriteString(conn, "-ERR syntax error\r\n")
				break
			}
			members := make([]ZMember, 0, (len(args)-2)/2)
			badScore := false
			for i := 2; i < len(args); i += 2 {
				score, ok := parseScore(args[i])
				if !ok {
					badScore = true
					break
				}
				members = append(members, ZMember{Member: args[i+1], Score: score})
			}
			if badScore {
				_, err = io.WriteString(conn, "-ERR value is not a valid float\r\n")
				break
			}
			added, zErr := store.ZAdd(args[1], members)
			if zErr != nil {
				_, err = fmt.Fprintf(conn, "-%s\r\n", zErr.Error())
			} else {
				_, err = fmt.Fprintf(conn, ":%d\r\n", added)
			}

		case "ZREM": // ZREM key member [member ...]
			if len(args) < 3 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'zrem' command\r\n")
				break
			}
			removed, zErr := store.ZRem(args[1], args[2:]...)
			if zErr != nil {
				_, err = fmt.Fprintf(conn, "-%s\r\n", zErr.Error())
			} else {
				_, err = fmt.Fprintf(conn, ":%d\r\n", removed)
			}

		case "ZSCORE": // ZSCORE key member
			if len(args) != 3 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'zscore' command\r\n")
				break
			}
			score, ok, zErr := store.ZScore(args[1], args[2])
			switch {
			case zErr != nil:
				_, err = fmt.Fprintf(conn, "-%s\r\n", zErr.Error())
			case ok:
				err = writeBulk(conn, formatScore(score))
			default:
				_, err = io.WriteString(conn, "$-1\r\n")
			}

		case "ZCARD": // ZCARD key
			if len(args) != 2 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'zcard' command\r\n")
				break
			}
			n, zErr := store.ZCard(args[1])
			if zErr != nil {
				_, err = fmt.Fprintf(conn, "-%s\r\n", zErr.Error())
			} else {
				_, err = fmt.Fprintf(conn, ":%d\r\n", n)
			}

		case "ZRANK": // ZRANK key member
			if len(args) != 3 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'zrank' command\r\n")
				break
			}
			rank, ok, zErr := store.ZRank(args[1], args[2])
			switch {
			case zErr != nil:
				_, err = fmt.Fprintf(conn, "-%s\r\n", zErr.Error())
			case ok:
				_, err = fmt.Fprintf(conn, ":%d\r\n", rank)
			default:
				_, err = io.WriteString(conn, "$-1\r\n")
			}

		case "ZRANGE": // ZRANGE key start stop [WITHSCORES]
			if len(args) != 4 && len(args) != 5 {
				_, err = io.WriteString(conn, "-ERR wrong number of arguments for 'zrange' command\r\n")
				break
			}
			withScores := len(args) == 5
			if withScores && !strings.EqualFold(args[4], "WITHSCORES") {
				_, err = io.WriteString(conn, "-ERR syntax error\r\n")
				break
			}
			start, err1 := strconv.Atoi(args[2])
			stop, err2 := strconv.Atoi(args[3])
			if err1 != nil || err2 != nil {
				_, err = io.WriteString(conn, "-ERR value is not an integer or out of range\r\n")
				break
			}
			items, zErr := store.ZRange(args[1], start, stop)
			if zErr != nil {
				_, err = fmt.Fprintf(conn, "-%s\r\n", zErr.Error())
				break
			}
			out := make([]string, 0, 2*len(items))
			for _, it := range items {
				out = append(out, it.Member)
				if withScores {
					out = append(out, formatScore(it.Score))
				}
			}
			err = writeArray(conn, out)

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

// writeArray sends items as a RESP array of bulk strings:
// *<count>\r\n followed by one bulk string per item.
func writeArray(w io.Writer, items []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(items)); err != nil {
		return err
	}
	for _, it := range items {
		if err := writeBulk(w, it); err != nil {
			return err
		}
	}
	return nil
}

// parseScore reads a sorted-set score. Redis accepts "inf" and "-inf"
// but never NaN, because NaN can't be ordered.
func parseScore(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}

// formatScore prints a score the way Redis does: shortest form ("1.5",
// "3", "1e+20") and "inf"/"-inf" for infinities.
func formatScore(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
