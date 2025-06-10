package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func parseResp(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	fmt.Println(line)
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("invalid RESP array: %s", line)
	}

	numElemts, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %s", line)
	}

	tokens := make([]string, 0, numElemts)

	for i := 0; i < numElemts; i++ {
		_, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		// read the actual value
		value, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, strings.TrimSpace(value))
	}

	return tokens, nil

}

func handleConcurrent(conn net.Conn) {
	defer conn.Close()

	var store = make(map[string]string)
	reader := bufio.NewReader(conn)

	for {
		tokens, err := parseResp(reader)
		if err != nil {
			return
		}
		if len(tokens) == 0 {
			continue
		}
		//fmt.Println(tokens)
		switch strings.ToUpper(tokens[0]) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(tokens) >= 2 {
				arg := tokens[1]
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))

			}
		case "GET":
			if len(tokens) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'GET'\r\n"))
				return
			}
			key := tokens[1]
			val, exists := store[key]
			if !exists {
				conn.Write([]byte("$-1\r\n"))
				return
			}
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
			conn.Write([]byte(response))
		case "SET":
			if len(tokens) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'SET'\r\n"))
				return
			}
			key := tokens[1]
			val := tokens[2]
			store[key] = val
			conn.Write([]byte("+OK\r\n"))

		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}

}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConcurrent(conn)

	}

}
