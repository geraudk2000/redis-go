package main

import (
	"bufio"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

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
		case "INFO":
			if len(tokens) >= 2 && strings.ToUpper(tokens[1]) == "REPLICATION" {

				role := "master"
				if *replicaof != "" {
					role = "slave"
				}
				info := fmt.Sprintf("role:%s\r\n", role)

				// RESP Bulk String:
				response := fmt.Sprintf("$%d\r\n%s", len(info), info)
				conn.Write([]byte(response + "\r\n"))

			} else {
				conn.Write([]byte("-ERR wrong arguments\r\n"))
			}

		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}

}
