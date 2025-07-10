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
var port = flag.String("port", "6379", "Port to listen on")
var replicaof = flag.String("replicaof", "", "host and port of the master")

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
	//fmt.Println("Logs from your program will appear here!")

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

	l1, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", *port)
		os.Exit(1)
	}
	fmt.Printf("Listening on %s", *port)

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

	if *replicaof != "" {
		parts := strings.Split(*replicaof, " ")
		if len(parts) != 2 {
			fmt.Println("Invalide --replicaof format, expected 'host port'")
			os.Exit(1)
		}

		host, masterPort := parts[0], parts[1]

		go func() {
			for {
				conn, err := net.Dial("tcp", host+":"+masterPort)
				if err != nil {
					fmt.Println("Failed to connect to master, retrying...")
					time.Sleep(2 * time.Second)
					continue
				}
				fmt.Println("Connected to master:", host+":"+masterPort)
				fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n")
				handleConcurrent(conn)
				//handleMasterConnection(conn)
				//conn.Close()
				//break
			}
		}()
	}

	select {}

}
