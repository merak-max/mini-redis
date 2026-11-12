package main

import (
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("Redis server running on port 6380")

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

	buffer := make([]byte, 1024)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			break
		}

		log.Printf("Received: %q", string(buffer[:n]))
	}
}
