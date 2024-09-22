package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/mmmorris1975/ssm-session-client/datachannel"
)

func ec2Cp(localFile string, dest string, port int) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	target := strings.Split(dest, ":")[0]
	destFile := strings.Split(dest, ":")[1]
	if strings.HasSuffix(destFile, "/") {
		destFile = destFile + filepath.Base(localFile)
	}

	log.Print("Executing nc command (nc)")
	cnc, err := runNc(cfg, target, destFile, port)
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the nc listener to start
	time.Sleep(2 * time.Second)

	log.Print("Starting port forwarding session")
	cpw, err := openPortForwarding(cfg, target, port, port)
	if err != nil {
		log.Fatal(err)
	}
	go startPortForwarding(cpw, port)

	defer func() {
		// Wait for nc to be finished
		time.Sleep(1 * time.Second)

		log.Print("Closing data channel (nc)")
		closeNc(cnc)

		log.Print("Closing data channel (port forwarding)")
		closePortForwarding(cpw)

		log.Print("Finished")
	}()
	installSignalHandler(cnc, cpw)

	// Wait for the port forwarding session to be established
	time.Sleep(2 * time.Second)

	log.Print("Sending file to target")
	written, err := sendFile(localFile, port)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Sent %d bytes", written)
}

func installSignalHandler(cnc *datachannel.SsmDataChannel, cpw *datachannel.SsmDataChannel) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Got signal: %s, shutting down", sig.String())

		log.Print("Closing data channel (nc)")
		closeNc(cnc)

		log.Print("Closing data channel (port forwarding)")
		closePortForwarding(cpw)

		os.Exit(0)
	}()
}
