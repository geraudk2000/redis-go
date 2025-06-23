package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var _ = net.Listen
var _ = os.Exit
var dir = flag.String("dir", "", "Path to data directory")
var dbfilename = flag.String("dbfilename", "", "Name of RDB file")
var port_replication = flag.String("port", "6380", "Replication port")

var store = make(map[string]string)
var expiries = make(map[string]time.Time)

func parseResp(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

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

func globToRegex(glob string) string {
	var re strings.Builder
	re.WriteString("^")

	for _, ch := range glob {
		switch ch {
		case '*':
			re.WriteString(".*")
		case '?':
			re.WriteString(".")
		default:
			re.WriteRune(ch)
		}
	}
	re.WriteString("$")
	return re.String()
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Prase the flag
	flag.Parse()

	filename := filepath.Join(*dir, *dbfilename)
	file, err := os.Open(filename)
	if err == nil {
		defer file.Close()
		err = loadRDB(file)
		if err != nil {
			fmt.Println("Failed to load RDB:", err)
		}
	}

	// Uncomment this block to pass the first stage
	//
	l1, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Printf("Listening on 6379")

	l2, err := net.Listen("tcp", "0.0.0.0:"+*port_replication)
	if err != nil {
		fmt.Println("Failed to bind to port 6380")
		os.Exit(1)
	}

	// Start goroutine for first listener

	go func() {

		for {
			conn, err := l1.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
				continue
			}
			go handleConcurrent(conn)
		}

	}()

	for {
		conn, err := l2.Accept()
		if err != nil {
			fmt.Println("Error acception connection:", err.Error())
			continue
		}
		go handleReplication(conn)
	}

}
