package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"strconv"
)

func sendFile(file string, port int) (int64, error) {
	f, err := os.Open(file)
	if err != nil {
		return -1, err
	}
	defer f.Close()
	bf := bufio.NewReader(f)

	conn, err := net.Dial("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	written, err := io.Copy(conn, bf)
	if err != nil {
		return -1, err
	}
	return written, nil
}
