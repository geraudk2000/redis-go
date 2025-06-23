package main

import "net"

func handleReplication(conn net.Conn) error {
	defer conn.Close()

	return nil
}
