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

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Half-close the write side so the relay on the other end sees a clean EOF, then drain
		// (and discard) anything it sends back before letting the deferred Close() run: closing
		// a socket that still has unread inbound data queued turns into a RST that can truncate
		// the relay's in-flight forward to the remote end. This also means we don't return until
		// the relay has actually finished forwarding everything, instead of just handing the
		// bytes to the local kernel buffer.
		_ = tcpConn.CloseWrite()
		_, _ = io.Copy(io.Discard, conn)
	}

	return written, nil
}
