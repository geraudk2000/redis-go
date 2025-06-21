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
					break
				}
			}

			val, exists := store[key]
			if !exists {
				conn.Write([]byte("$-1\r\n"))
				break
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
	case 0b00:
		return int(first[0] & 0x3F), nil
	case 0b01:
		next := make([]byte, 1)
		io.ReadFull(r, next)
		return int(first[0]&0x3F)<<8 | int(next[0]), nil
	case 0b10:
		buf := make([]byte, 4)
		io.ReadFull(r, buf)
		return int(binary.BigEndian.Uint32(buf)), nil
	case 0b11:
		return 0, fmt.Errorf("readLength: got special string encoding prefix (0b11), should be handled in readString")
	default:
		return 0, fmt.Errorf("unsupported length encoding")
	}

}

func readString(r *bufio.Reader) ([]byte, error) {
	b, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	if b[0]>>6 == 0b11 {
		r.ReadByte()
		typ := b[0] & 0x3F
		switch typ {
		case 0:
			val, _ := r.ReadByte()
			return []byte(strconv.Itoa(int(val))), nil
		case 1:
			buf := make([]byte, 2)
			io.ReadFull(r, buf)
			val := binary.LittleEndian.Uint16(buf)
			return []byte(strconv.Itoa(int(val))), nil
		case 2:
			buf := make([]byte, 4)
			io.ReadFull(r, buf)
			val := binary.LittleEndian.Uint32(buf)
			return []byte(strconv.Itoa(int(val))), nil
		default:
			fmt.Printf("readString: got unknown special encoding type 0x%X\n", typ)
			return nil, fmt.Errorf("unsupported special encoding: 0x%x", typ)

		}
	}

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

		// Read the value
		_, err = readString(reader)
		if err != nil {
			return err
		}

		// Read the key
		_, err = readString(reader)
		if err != nil {
			return err
		}

	}
	// databae selector (0xFE)
	//
	b, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if b != 0xFE {
		return fmt.Errorf("expected 0xFE but got 0x%x", b)
	}
	_, err = readLength(reader) // DB index
	if err != nil {
		return err
	}

	// Expect 0xFB (hash table sizes)
	b, err = reader.ReadByte()
	if err != nil {
		return err
	}
	if b != 0xFB {
		return fmt.Errorf("expected 0xFB but got 0x%x", b)
	}
	_, err = readLength(reader) // key-value pair count
	if err != nil {
		return err
	}
	_, err = readLength(reader) // number of expiring keys
	if err != nil {
		return err
	}

	// Read all key-value pairs until 0xFF (EOF)
	for {
		b, err := reader.Peek(1)
		if err != nil {
			break
		}
		if b[0] == 0xFF {
			break
		}
		// Handle optional expiry

		var expiry time.Time

		b, _ = reader.Peek(1)
		switch b[0] {
		case 0xFC:
			reader.ReadByte() // consume 0xFC
			ts := make([]byte, 8)
			io.ReadFull(reader, ts)
			ms := binary.LittleEndian.Uint64(ts)
			expiry = time.UnixMilli(int64(ms))
		case 0xFD:
			reader.ReadByte()
			ts := make([]byte, 4)
			io.ReadFull(reader, ts)
			sec := binary.LittleEndian.Uint32(ts)
			expiry = time.Unix(int64(sec), 0)
		}
		// value type

		typ, _ := reader.ReadByte()
		if typ != 0x00 {
			return fmt.Errorf("unsupported value type: 0x%x", typ)
		}

		key, err := readString(reader)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			continue // skip blank/bad keys
		}
		val, err := readString(reader)
		if err != nil {
			return err
		}

		store[string(key)] = string(val)
		if !expiry.IsZero() {
			expiries[string(key)] = expiry
		}
	}

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
