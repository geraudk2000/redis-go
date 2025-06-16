package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var _ = net.Listen
var _ = os.Exit
var dir = flag.String("dir", "", "Path to data directory")
var dbfilename = flag.String("dbfilename", "", "Name of RDB file")

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

func handleConcurrent(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		tokens, err := parseResp(reader)

		if err != nil {
			return
		}
		if len(tokens) == 0 {
			continue
		}

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

			if expiry, exists := expiries[key]; exists {
				if time.Now().After(expiry) {
					delete(store, key)
					delete(expiries, key)
					conn.Write([]byte("$-1\r\n"))
					return
				}
			}

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

			if len(tokens) == 5 && strings.ToUpper(tokens[3]) == "PX" {

				ms, err := strconv.Atoi(tokens[4])
				if err == nil {
					expiries[key] = time.Now().Add(time.Duration(ms) * time.Millisecond)
				}

			}

			conn.Write([]byte("+OK\r\n"))

		case "CONFIG":

			if len(tokens) >= 3 && strings.ToUpper(tokens[1]) == "GET" {
				param := tokens[2]
				var val string
				switch param {
				case "dir":
					val = *dir
				case "dbfilename":
					val = *dbfilename
				default:
					conn.Write([]byte("*0\r\n"))
					return
				}

				response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(param), param, len(val), val)
				conn.Write([]byte(response))
			}

		case "KEYS":
			if len(tokens) == 2 {
				pattern := tokens[1]
				regex := regexp.MustCompile(globToRegex(pattern))

				matched := []string{}
				for key := range store {
					if regex.MatchString(key) {
						matched = append(matched, key)
					}
				}
				response := fmt.Sprintf("*%d\r\n", len(matched))
				for _, k := range matched {
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
				}
				conn.Write([]byte(response))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'KEYS'\r\n"))
			}

		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}

}

func readLength(r io.Reader) (int, error) {

	first := make([]byte, 1)
	_, err := io.ReadFull(r, first)
	if err != nil {
		return 0, nil
	}

	prefix := first[0] >> 6
	switch prefix {
	case 0:
		return int(first[0] & 0x3F), nil
	case 1:
		next := make([]byte, 1)
		io.ReadFull(r, next)
		return int(first[0]&0x3F)<<8 | int(next[0]), nil
	case 2:
		buf := make([]byte, 4)
		io.ReadFull(r, buf)
		return int(binary.BigEndian.Uint32(buf)), nil
	default:
		return 0, fmt.Errorf("unsupported length encoding")
	}

}

func readString(r io.Reader) ([]byte, error) {
	length, err := readLength(r)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)
	return buf, err
}

func loadRDB(file *os.File) error {

	reader := bufio.NewReader(file)
	header := make([]byte, 9)
	if _, err := io.ReadFull(reader, header); err != nil {
		return err
	}
	if string(header) != "REDIS0011" {
		return fmt.Errorf("invalid RDB header")

	}
	// skip metadata

	for {
		b, err := reader.ReadByte()
		if err != nil {
			return err
		}
		if b != 0xFA {
			reader.UnreadByte()
			break
		}
		// Read and discard metadata key + value
		_, _ = readString(reader)
		_, _ = readString(reader)

	}
	// databae selector (0xFE)
	b, _ := reader.ReadByte()
	if b != 0xFE {
		return fmt.Errorf("expected 0xFE")
	}
	_, _ = readLength(reader) // db index

	if marker, _ := reader.ReadByte(); marker == 0xFB {
		_, _ = readLength(reader)
		_, _ = readLength(reader)
	}

	// Parse one key
	_, _ = reader.ReadByte()
	key, _ := readString(reader)
	val, _ := readString(reader)

	store[string(key)] = string(val)
	return nil

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
