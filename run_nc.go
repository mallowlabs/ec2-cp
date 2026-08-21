package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/mmmorris1975/ssm-session-client/datachannel"
)

// readyMarker is printed by the remote shell right before it hands control to tncl, so we can
// detect (instead of guessing with a sleep) the moment the remote listener is about to start.
const readyMarker = "___EC2_CP_READY___"

// doneMarker is echoed by the remote shell itself right after the tncl command line finishes. This
// fires whether tncl exits cleanly or crashes -- tncl's own "client closed connection" log line is
// not a reliable signal, since tncl has been observed to disappear without ever printing it (e.g. a
// crash). The local relay finishing only means our bytes made it onto the websocket; there's still
// a separately-buffered hop from the SSM agent to tncl on the instance, so this is the only
// reliable "the transfer attempt is over" signal (whether or not it actually succeeded -- that's
// what the checksum step is for).
const doneMarker = "___EC2_CP_DONE___"

// checksumDoneMarker terminates the checksum command's output so we know when to stop waiting.
const checksumDoneMarker = "___EC2_CP_CHECKSUM_DONE___"

var sha256LinePattern = regexp.MustCompile(`\b([0-9a-f]{64})\b`)

// echoQuoted splits a marker into two halves joined as adjacent quoted strings, e.g. "ABCD" becomes
// `"AB""CD"`. Bash concatenates adjacent quoted strings, so the command's actual output is the
// intact marker -- but the command's own source text (which the PTY echoes back to us as soon as we
// send it, well before the shell has actually executed anything) never contains the marker as an
// unbroken substring. That's what lets watchers below tell "the marker was really printed" apart
// from "the marker just scrolled by because we typed it".
func echoQuoted(marker string) string {
	mid := len(marker) / 2
	return fmt.Sprintf("%q%q", marker[:mid], marker[mid:])
}

type bootAgentTmplData struct {
	Cmd           string
	Port          int
	Filename      string
	ReadyEchoExpr string
	DoneEchoExpr  string
}

// tncl exits its whole process (via std.process.exit) the instant its own stdin reaches EOF, which
// happens almost immediately when it inherits the SSM shell session's stdin directly. Feeding it
// from `sleep infinity` via process substitution (rather than a plain pipe) keeps that stdin open
// for the life of the transfer, while still letting the shell regain control as soon as tncl exits
// -- a plain `sleep infinity | tncl ...` pipe would leave the shell waiting on the whole pipeline,
// which never finishes since sleep never exits, so it'd never see our next command.
var bootAgentTmpl = template.Must(template.New("").Parse(
	`sudo su
cd /root/
curl --silent -L -o {{.Cmd}} "https://github.com/fujiwara/tncl/releases/download/v0.0.4/tncl-x86_64-linux-musl"
chmod +x {{.Cmd}}
echo {{.ReadyEchoExpr}}
{{.Cmd}} {{.Port}} < <(sleep infinity) > "{{.Filename}}"
echo {{.DoneEchoExpr}}
`))

// runNc boots the remote listener and returns the shell session's data channel along with a
// channel that is closed once the remote side confirms the transfer attempt is over. Once
// remoteDone fires, streamRemoteOutput's reader goroutine has exited and the channel is safe to
// read from again (e.g. via verifyRemoteSha256).
func runNc(cfg aws.Config, target string, remoteFile string, port int) (*datachannel.SsmDataChannel, <-chan struct{}, error) {
	buf := &strings.Builder{}
	bootAgentTmpl.Execute(buf, bootAgentTmplData{
		Cmd: "/tmp/tncl", Port: port, Filename: remoteFile,
		ReadyEchoExpr: echoQuoted(readyMarker), DoneEchoExpr: echoQuoted(doneMarker)})
	cmd := buf.String()

	c := new(datachannel.SsmDataChannel)
	if err := c.Open(cfg, &ssm.StartSessionInput{Target: aws.String(target)}); err != nil {
		return c, nil, err
	}

	r := strings.NewReader(cmd)
	if _, err := io.Copy(c, r); err != nil {
		return c, nil, err
	}

	ready := make(chan error, 1)
	remoteDone := make(chan struct{})
	go streamRemoteOutput(c, ready, remoteDone)
	// This shell session sits completely idle for the whole transfer -- all the actual traffic
	// flows over the separate port-forwarding channel -- and AWS API Gateway websocket
	// connections are dropped after ~10 minutes of inactivity. A large transfer easily takes
	// longer than that, so without this the channel goes stale and remoteDone never fires. A bare
	// newline is a harmless no-op once bash reads it, whether that's now (idle at the prompt) or
	// later (queued behind the still-running tncl command).
	go sendKeepalive(c, remoteDone)

	select {
	case err := <-ready:
		if err != nil {
			return c, remoteDone, err
		}
	case <-time.After(60 * time.Second):
		return c, remoteDone, errors.New("timed out waiting for remote agent to become ready")
	}

	return c, remoteDone, nil
}

// sendKeepalive periodically writes a no-op newline to c so the shell session's websocket
// connection doesn't sit idle long enough for AWS to drop it during a long transfer. Stops once
// remoteDone fires.
func sendKeepalive(c *datachannel.SsmDataChannel, remoteDone <-chan struct{}) {
	ticker := time.NewTicker(4 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, _ = io.Copy(c, strings.NewReader("\n"))
		case <-remoteDone:
			return
		}
	}
}

// maxSeenBuffer bounds how much trailing output streamRemoteOutput and verifyRemoteSha256 keep
// around while scanning for a marker, so a long-idle session -- kept alive by periodic keepalive
// newlines during a long transfer -- doesn't grow that buffer for as long as the session is open.
// It's generous relative to the longest marker/pattern being searched for (well under 100 bytes),
// leaving plenty of overlap to catch one split across two reads.
const maxSeenBuffer = 4096

func trimSeen(b *strings.Builder) {
	if b.Len() <= maxSeenBuffer {
		return
	}
	tail := b.String()[b.Len()-maxSeenBuffer:]
	b.Reset()
	b.WriteString(tail)
}

// readPayload reads and decodes the next message from c. err is set only for a genuine failure to
// read or decode; a clean channel closure is reported via eof instead (with payload still holding
// any final bytes that arrived alongside it, which callers should process before checking eof).
func readPayload(c *datachannel.SsmDataChannel, buf []byte) (payload []byte, eof bool, err error) {
	n, err := c.Read(buf)
	if err != nil {
		return nil, false, err
	}

	payload, handleErr := c.HandleMsg(buf[:n])
	if handleErr != nil {
		if errors.Is(handleErr, io.EOF) {
			return payload, true, nil
		}
		return payload, false, handleErr
	}
	return payload, false, nil
}

// streamRemoteOutput drains and logs the shell session's output (prefixed with "[remote]") so
// failures on the remote side (a failed curl, a tncl crash, ...) are visible instead of silently
// vanishing. It reports on ready exactly once, the moment readyMarker is actually printed (not just
// echoed as typed input), and closes remoteDone and returns exactly once, the moment doneMarker is
// actually printed -- at which point nothing else will read from c, and the caller may safely start
// reading it again.
func streamRemoteOutput(c *datachannel.SsmDataChannel, ready chan<- error, remoteDone chan<- struct{}) {
	buf := make([]byte, 4096)
	var seen strings.Builder
	readyFound := false

	for {
		payload, eof, err := readPayload(c, buf)
		if len(payload) > 0 {
			for line := range strings.SplitSeq(strings.TrimRight(string(payload), "\r\n"), "\n") {
				if line != "" {
					log.Printf("[remote] %s", line)
				}
			}

			seen.Write(payload)
			trimSeen(&seen)
			if !readyFound && strings.Contains(seen.String(), readyMarker) {
				readyFound = true
				ready <- nil
			}
			if strings.Contains(seen.String(), doneMarker) {
				close(remoteDone)
				return
			}
		}

		if err != nil {
			if !readyFound {
				ready <- fmt.Errorf("waiting for remote agent to become ready: %w", err)
			}
			return
		}
		if eof {
			return
		}
	}
}

// verifyRemoteSha256 runs sha256sum on the remote file over the still-open shell session and
// returns the hash it reports. Must only be called after remoteDone has fired, so nothing else is
// concurrently reading c.
func verifyRemoteSha256(c *datachannel.SsmDataChannel, remoteFile string, timeout time.Duration) (string, error) {
	cmd := fmt.Sprintf("sha256sum %q; echo %s\n", remoteFile, checksumDoneMarker)
	if _, err := io.Copy(c, strings.NewReader(cmd)); err != nil {
		return "", err
	}

	buf := make([]byte, 4096)
	var seen strings.Builder
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		payload, eof, err := readPayload(c, buf)
		if len(payload) > 0 {
			seen.Write(payload)
			trimSeen(&seen)
			if m := sha256LinePattern.FindStringSubmatch(seen.String()); m != nil {
				return m[1], nil
			}
		}

		if err != nil {
			return "", err
		}
		if eof {
			return "", errors.New("remote session closed before checksum was reported")
		}
	}

	return "", errors.New("timed out waiting for remote checksum")
}

func closeNc(c *datachannel.SsmDataChannel) {
	io.Copy(c, strings.NewReader("exit\n")) // root
	io.Copy(c, strings.NewReader("exit\n")) // ssm-user
	time.Sleep(200 * time.Millisecond)
	_ = c.DisconnectPort()
	_ = c.TerminateSession()
	_ = c.Close()
}
