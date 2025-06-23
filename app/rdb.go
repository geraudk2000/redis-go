package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

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
