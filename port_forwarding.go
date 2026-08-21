package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/mmmorris1975/ssm-session-client/datachannel"
	"github.com/mmmorris1975/ssm-session-client/ssmclient"
	"golang.org/x/net/netutil"
)

// This sorce code is based on the following source code.
// https://github.com/mmmorris1975/ssm-session-client/blob/1aadba9b4dbbfd2a052f879f5ac1eefeca8e149d/ssmclient/port_forwarding.go
// The originl source code is licensed under the MIT License.
func openPortForwarding(cfg aws.Config, target string, remotePort int, localPort int) (*datachannel.SsmDataChannel, error) {
	opts := ssmclient.PortForwardingInput{
		Target:     target,
		RemotePort: remotePort,
		LocalPort:  localPort,
	}

	c, err := openDataChannel(cfg, &opts)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// PortForwardingSession starts a port forwarding session using the PortForwardingInput parameters to
// configure the session.  The aws.Config parameter will be used to call the AWS SSM StartSession
// API, which is used as part of establishing the websocket communication channel.
//
//nolint:funlen,gocognit // it's long, but not overly hard to read despite what the gocognit says
func startPortForwarding(c *datachannel.SsmDataChannel, localPort int, chunkSize int, ready chan<- struct{}, transferDone chan<- struct{}) error {
	if err := c.WaitForHandshakeComplete(context.Background()); err != nil {
		close(ready)
		return err
	}

	lsnr, err := createListener(localPort)
	if err != nil {
		close(ready)
		return err
	}
	defer lsnr.Close()
	log.Printf("listening on %s", lsnr.Addr())
	// Signal readiness now (the caller starts sending as soon as this fires), not via a deferred
	// close at function exit -- this function keeps running the accept loop below for the life of
	// the whole transfer, well after the listener is actually ready to accept.
	close(ready)

	doneCh := make(chan bool)
	errCh := make(chan error)
	inCh := messageChannel(c, errCh)

	closeTransferDone := func() {
		if transferDone != nil {
			close(transferDone)
			transferDone = nil
		}
	}

outer:
	for {
		var conn net.Conn
		conn, err = lsnr.Accept()
		if err != nil {
			// not fatal, just wait for next (maybe unless lsnr is dead?)
			log.Print(err)
			continue
		}

		go func() {
			// Forward local -> remote. The SSM agent's own hop from the websocket to the
			// target port on the instance can't sustain the same throughput as this local
			// relay; pushing data in as fast as io.Copy would silently loses most of it
			// somewhere along that path, so pace it instead.
			if e := throttledCopy(c, conn, chunkSize); e != nil {
				errCh <- e
			}
			doneCh <- true
		}()

	inner:
		for {
			select {
			case <-doneCh:
				// basic (non-muxing) connections support DisconnectPort to signal to the remote agent that
				// we are shutting down this particular connection on our end, and possibly expect a new one.
				_ = c.DisconnectPort()
				closeTransferDone()
				break inner
			case data, ok := <-inCh:
				if !ok {
					// incoming websocket channel is closed, which is fatal
					_ = conn.Close()
					break outer
				}

				if _, err = conn.Write(data); err != nil {
					log.Print(err)
				}
			case er, ok := <-errCh:
				if !ok {
					// I can't think of a good reason why we'd ever end up here, but if we do
					// we should stop the world
					log.Print("errCh closed")
					_ = conn.Close()
					break outer
				}

				// any write to errCh means at least 1 of the goroutines has exited
				log.Print(er)
				closeTransferDone()
				break inner
			}
		}

		_ = conn.Close()
	}
	return nil
}

// throttleBytesPerSec is the target sustained rate for throttledCopy; ec2_cp.go uses it to size
// how long it's worth waiting for the remote side to confirm a transfer actually completed.
const throttleBytesPerSec = 32 * 1024

// throttledCopy is like io.Copy but paces its writes: the SSM agent's hop from the websocket to
// the target port on the instance has much less headroom than this local, kernel-buffered relay,
// and writing at full speed overruns it, with the excess getting dropped rather than backed up.
// The loss that remains at a sustainable rate lands on the same bytes every time for a given
// chunkSize, so callers should vary chunkSize across retries rather than just retrying as-is.
func throttledCopy(dst io.Writer, src io.Reader, chunkSize int) error {
	pause := time.Duration(chunkSize) * time.Second / throttleBytesPerSec

	buf := make([]byte, chunkSize)
	for {
		nr, err := src.Read(buf)
		if nr > 0 {
			if _, werr := dst.Write(buf[:nr]); werr != nil {
				return werr
			}
			time.Sleep(pause)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func closePortForwarding(c *datachannel.SsmDataChannel) {
	_ = c.DisconnectPort()
	_ = c.TerminateSession()
	_ = c.Close()
}

func openDataChannel(cfg aws.Config, opts *ssmclient.PortForwardingInput) (*datachannel.SsmDataChannel, error) {
	in := &ssm.StartSessionInput{
		DocumentName: aws.String("AWS-StartPortForwardingSession"),
		Target:       aws.String(opts.Target),
		Parameters: map[string][]string{
			"localPortNumber": {strconv.Itoa(opts.LocalPort)},
			"portNumber":      {strconv.Itoa(opts.RemotePort)},
		},
	}

	c := new(datachannel.SsmDataChannel)
	if err := c.Open(cfg, in); err != nil {
		return nil, err
	}
	return c, nil
}

// read messages from websocket and write payload to the returned channel.
func messageChannel(c datachannel.DataChannel, errCh chan error) chan []byte {
	inCh := make(chan []byte)

	buf := make([]byte, 4096)
	var payload []byte

	go func() {
		defer close(inCh)

		for {
			nr, err := c.Read(buf)
			if err != nil {
				errCh <- err
				return
			}

			payload, err = c.HandleMsg(buf[:nr])
			if err != nil {
				errCh <- err
				return
			}

			if len(payload) > 0 {
				inCh <- payload
			}
		}
	}()

	return inCh
}

func createListener(port int) (net.Listener, error) {
	l, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	// use limit listener for now, eventually maybe we'll add muxing
	// REF: https://github.com/aws/amazon-ssm-agent/blob/master/agent/session/plugins/port/port_mux.go
	return netutil.LimitListener(l, 1), nil
}
