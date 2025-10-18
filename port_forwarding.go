package main

import (
	"context"
	"io"
	"log"
	"net"
	"strconv"

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
func startPortForwarding(c *datachannel.SsmDataChannel, localPort int) error {
	if err := c.WaitForHandshakeComplete(context.Background()); err != nil {
		return err
	}

	lsnr, err := createListener(localPort)
	if err != nil {
		return err
	}
	defer lsnr.Close()
	log.Printf("listening on %s", lsnr.Addr())

	doneCh := make(chan bool)
	errCh := make(chan error)
	inCh := messageChannel(c, errCh)

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
			// handle incoming messages from AWS in the background
			if _, e := io.Copy(c, conn); e != nil {
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
				break inner
			}
		}

		_ = conn.Close()
	}
	return nil
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
