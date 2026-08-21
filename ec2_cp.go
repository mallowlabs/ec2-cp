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

// Acknowledgements only cover the path as far as the SSM agent; the agent's own hop to tncl and
// tncl's write to the file are unobserved, and runNc documents that hop failing silently. The
// checksum is the only end-to-end check, so retrying on a mismatch stays. Note this now only helps
// against transient failures -- with the chunk size cycling gone, the attempts are identical, so
// anything deterministic (a raised maxPayloadSize, say) just fails three times.
const maxAttempts = 3

// minExpectedBytesPerSec is a deliberately pessimistic floor on how fast a transfer runs, used only
// so the timeouts below scale with the size of the file instead of being a fixed value that a large
// transfer trips over. It is not a rate limit.
const minExpectedBytesPerSec = 128 * 1024

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

	localSum, fileSize, err := sha256File(localFile)
	if err != nil {
		log.Fatal(err)
	}

	active := &activeSession{}
	installSignalHandler(active)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// A fixed port can collide with a service already listening on the target instance, in
		// which case the "transfer" silently talks to that unrelated service instead of tncl.
		port := 20000 + rand.Intn(20000)
		log.Printf("Attempt %d/%d (port %d)", attempt, maxAttempts, port)

		ok, err := attemptTransfer(cfg, target, destFile, localFile, localSum, fileSize, port, active)
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

func attemptTransfer(cfg aws.Config, target, destFile, localFile, localSum string, fileSize int64, port int, active *activeSession) (bool, error) {
	expectedTransferTime := time.Duration(fileSize) * time.Second / minExpectedBytesPerSec
	// An outer bound, not a health check: the relay decides for itself whether the channel has gone
	// quiet (see stalledLocked), so this only exists so a wedged goroutine can't hang the program.
	relayTimeout := 120*time.Second + 2*expectedTransferTime
	// The SSM agent has acknowledged every byte by the time the relay finishes, so all that's left
	// here is the agent handing them to tncl on the instance.
	remoteDrainTimeout := 60*time.Second + expectedTransferTime
	log.Print("Executing nc command (nc)")
	// runNc blocks until the remote agent has confirmed it is about to start listening.
	cnc, remoteDone, err := runNc(cfg, target, destFile, port)
	if err != nil {
		return false, err
	}
	active.set(cnc, nil)
	defer active.set(nil, nil)
	defer closeNc(cnc)

	log.Print("Starting port forwarding session")
	cpw, err := openPortForwarding(cfg, target, port, port)
	if err != nil {
		return false, err
	}
	active.set(cnc, cpw)
	defer func() { _ = cpw.Close() }()

	ready := make(chan struct{})
	relayDone := make(chan error, 1)
	go func() { relayDone <- startPortForwarding(cpw, port, ready, active) }()

	if err := waitOrTimeout(ready, 30*time.Second, "timed out waiting for the local port forwarding listener to be ready"); err != nil {
		return false, err
	}

	log.Print("Sending file to target")
	started := time.Now()
	written, sendErr := sendFile(localFile, port)

	// The relay closes the local connection once it's done, so sendFile returning is immediately
	// followed by the relay's result. Read that first: when the relay is what failed, sendFile's
	// error is just the fallout of it hanging up, and the relay's is the one worth reporting.
	select {
	case err := <-relayDone:
		if err != nil {
			return false, fmt.Errorf("relaying the file over SSM: %w", err)
		}
	case <-time.After(relayTimeout):
		return false, errors.New("timed out waiting for the local relay to finish")
	}
	if sendErr != nil {
		return false, sendErr
	}

	elapsed := time.Since(started)
	log.Printf("Sent %d bytes in %s (%.0f KiB/s)", written, elapsed.Round(time.Millisecond),
		float64(written)/1024/elapsed.Seconds())

	// The relay finishing means the SSM agent acknowledged every byte, but there's still a
	// separately-buffered hop from the agent to tncl on the instance, so the file isn't necessarily
	// complete yet.
	if err := waitOrTimeout(remoteDone, remoteDrainTimeout, "timed out waiting for the remote agent to confirm the transfer finished"); err != nil {
		return false, err
	}

	remoteSum, err := verifyRemoteSha256(cnc, destFile)
	if err != nil {
		return false, fmt.Errorf("verifying remote checksum: %w", err)
	}

	if remoteSum != localSum {
		log.Printf("checksum mismatch: local=%s remote=%s", localSum, remoteSum)
		return false, nil
	}

	return true, nil
}

// waitOrTimeout blocks until ch is closed or d elapses, in which case it returns an error with msg.
func waitOrTimeout(ch <-chan struct{}, d time.Duration, msg string) error {
	select {
	case <-ch:
		return nil
	case <-time.After(d):
		return errors.New(msg)
	}
}

func sha256File(path string) (sum string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", 0, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), info.Size(), nil
}

// activeSession tracks whichever channels the current retry attempt has open, so a signal
// received mid-retry can still clean up the right thing instead of whatever the first attempt had.
type activeSession struct {
	mu  sync.Mutex
	cnc *datachannel.SsmDataChannel
	cpw *datachannel.SsmDataChannel
	rc  *reliableChannel
}

func (a *activeSession) set(cnc, cpw *datachannel.SsmDataChannel) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cnc, a.cpw, a.rc = cnc, cpw, nil
}

// setRelay records the relay layered over cpw, which owns the sequence numbering the port
// forwarding session's terminate message has to use.
func (a *activeSession) setRelay(rc *reliableChannel) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rc = rc
}

func (a *activeSession) closeAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cnc != nil {
		log.Print("Closing data channel (nc)")
		closeNc(a.cnc)
	}
	if a.cpw != nil {
		log.Print("Closing data channel (port forwarding)")
		if a.rc != nil {
			a.rc.close()
		} else {
			_ = a.cpw.Close()
		}
	}
}

func installSignalHandler(active *activeSession) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Got signal: %s, shutting down", sig.String())
		active.closeAll()
		os.Exit(0)
	}()
}
