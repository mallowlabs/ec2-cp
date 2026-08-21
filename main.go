package main

import (
	"os"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		println("Usage: ec2-cp /path/to/local/file target:/path/to/remote/file")
		os.Exit(1)
	}
	arg2 := strings.Split(os.Args[2], ":")
	if len(arg2) != 2 || arg2[0] == "" || arg2[1] == "" {
		println("Invalid target format. Must be target:/path/to/remote/file")
		os.Exit(1)
	}

	src := os.Args[1]  // /path/to/local/file
	dest := os.Args[2] // target:/path/to/remote/file

	ec2Cp(src, dest)
}
