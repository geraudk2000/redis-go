package main

import (
	"fmt"
	"net"
)

func handleReplication(conn net.Conn) error {
	defer conn.Close()

	return nil
}

func handleMasterConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024)

	for {
		_, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Lost connection to master:", err.Error())
			break
		}
	}

}
