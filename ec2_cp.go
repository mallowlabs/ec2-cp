package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/mmmorris1975/ssm-session-client/datachannel"
)

// The AWS SSM port-forwarding relay used to reach tncl is not fully reliable for bulk transfers:
// under sustained load a small fraction of the data can go missing between the SSM agent and the
// target port without either side reporting an error (see throttledCopy in port_forwarding.go).
// Verifying the remote checksum and retrying on mismatch turns that into a rare extra attempt
// instead of silent corruption.
const maxAttempts = 5

// chunkSizes are cycled across attempts. The loss throttledCopy can't avoid lands on the same
// bytes every time for a given chunk size, so retrying with the same size would just fail the same
// way again; varying it changes which message/write boundaries the data falls on.
var chunkSizes = []int{4096, 2731, 6151, 1523, 8209}

func ec2Cp(localFile string, dest string) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	target := strings.Split(dest, ":")[0]
	destFile := strings.Split(dest, ":")[1]
	if strings.HasSuffix(destFile, "/") {
		destFile = destFile + filepath.Base(localFile)
	}

	localSum, err := sha256File(localFile)
	if err != nil {
		log.Fatal(err)
	}

	info, err := os.Stat(localFile)
	if err != nil {
		log.Fatal(err)
	}
	fileSize := info.Size()

	active := &activeSession{}
	installSignalHandler(active)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// A fixed port can collide with a service already listening on the target instance, in
		// which case the "transfer" silently talks to that unrelated service instead of tncl.
		port := 20000 + rand.Intn(20000)
		log.Printf("Attempt %d/%d (port %d)", attempt, maxAttempts, port)

		chunkSize := chunkSizes[(attempt-1)%len(chunkSizes)]
		ok, err := attemptTransfer(cfg, target, destFile, localFile, localSum, fileSize, port, chunkSize, active)
		if err != nil {
			log.Printf("Attempt failed: %v", err)
			continue
		}
		if ok {
			log.Print("Transfer verified OK")
			return
		}
		log.Print("Remote checksum did not match, retrying...")
	}

	log.Fatalf("failed to transfer %s after %d attempts", localFile, maxAttempts)
}

func attemptTransfer(cfg aws.Config, target, destFile, localFile, localSum string, fileSize int64, port int, chunkSize int, active *activeSession) (bool, error) {
	// The remote hop (SSM agent -> tncl) has historically drained no faster than the throttled
	// send rate, and sometimes noticeably slower, so give it a generous multiple of the time the
	// send itself is expected to take rather than a fixed timeout that doesn't scale with size.
	expectedTransferTime := time.Duration(fileSize) * time.Second / throttleBytesPerSec
	relayTimeout := 60*time.Second + 2*expectedTransferTime
	remoteDrainTimeout := 60*time.Second + 4*expectedTransferTime
	log.Print("Executing nc command (nc)")
	// runNc blocks until the remote agent has confirmed it is about to start listening.
	cnc, remoteDone, err := runNc(cfg, target, destFile, port)
	if err != nil {
		return false, err
	}
	active.set(cnc, nil)
	defer func() {
		closeNc(cnc)
		active.set(nil, nil)
	}()

	log.Print("Starting port forwarding session")
	cpw, err := openPortForwarding(cfg, target, port, port)
	if err != nil {
		return false, err
	}
	active.set(cnc, cpw)
	defer func() {
		closePortForwarding(cpw)
		active.set(nil, nil)
	}()

	ready := make(chan struct{})
	transferDone := make(chan struct{})
	go startPortForwarding(cpw, port, chunkSize, ready, transferDone)

	select {
	case <-ready:
	case <-time.After(30 * time.Second):
		return false, errors.New("timed out waiting for the local port forwarding listener to be ready")
	}

	log.Print("Sending file to target")
	written, err := sendFile(localFile, port)
	if err != nil {
		return false, err
	}
	log.Printf("Sent %d bytes", written)

	// Wait for the local relay to finish forwarding the data onto the websocket...
	select {
	case <-transferDone:
	case <-time.After(relayTimeout):
		return false, errors.New("timed out waiting for the local relay to finish")
	}

	// ...and then for the remote side to confirm it actually received all of it. There's a
	// separate, independently-buffered hop from the SSM agent to tncl on the instance that has
	// historically drained slower than this local relay, so the local relay finishing doesn't
	// guarantee the file is complete yet.
	select {
	case <-remoteDone:
	case <-time.After(remoteDrainTimeout):
		return false, errors.New("timed out waiting for the remote agent to confirm the transfer finished")
	}

	remoteSum, err := verifyRemoteSha256(cnc, destFile, 30*time.Second)
	if err != nil {
		return false, fmt.Errorf("verifying remote checksum: %w", err)
	}

	if remoteSum != localSum {
		log.Printf("checksum mismatch: local=%s remote=%s", localSum, remoteSum)
		return false, nil
	}

	return true, nil
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// activeSession tracks whichever channels the current retry attempt has open, so a signal
// received mid-retry can still clean up the right thing instead of whatever the first attempt had.
type activeSession struct {
	mu  sync.Mutex
	cnc *datachannel.SsmDataChannel
	cpw *datachannel.SsmDataChannel
}

func (a *activeSession) set(cnc, cpw *datachannel.SsmDataChannel) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cnc, a.cpw = cnc, cpw
}

func installSignalHandler(active *activeSession) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Got signal: %s, shutting down", sig.String())

		active.mu.Lock()
		defer active.mu.Unlock()
		if active.cnc != nil {
			log.Print("Closing data channel (nc)")
			closeNc(active.cnc)
		}
		if active.cpw != nil {
			log.Print("Closing data channel (port forwarding)")
			closePortForwarding(active.cpw)
		}

		os.Exit(0)
	}()
}
