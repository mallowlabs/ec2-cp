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

// startPortForwarding accepts the single local connection carrying the file and relays it over the
// SSM data channel, returning once the SSM agent has acknowledged every byte of it (or the relay
// fails). ready is closed as soon as the local listener can be dialled.
func startPortForwarding(c *datachannel.SsmDataChannel, localPort int, ready chan<- struct{}, active *activeSession) error {
	// Until the relay below takes the channel over, this is the only way to shut it down: the
	// library's own terminate is still the right one to use, because nothing has diverged from its
	// sequence counter yet.
	fail := func(err error) error {
		close(ready)
		_ = c.TerminateSession()
		_ = c.Close()
		return err
	}

	if err := c.WaitForHandshakeComplete(context.Background()); err != nil {
		return fail(err)
	}

	lsnr, err := createListener(localPort)
	if err != nil {
		return fail(err)
	}
	defer lsnr.Close()
	log.Printf("listening on %s", lsnr.Addr())
	// Signal readiness now, before the blocking Accept below: the caller starts sending as soon as
	// this fires, and that connection is what Accept is waiting for.
	close(ready)

	rc := newReliableChannel(c)
	active.setRelay(rc)
	defer rc.close()

	conn, err := lsnr.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()

	go func() {
		for data := range rc.inbound {
			if _, err := conn.Write(data); err != nil {
				log.Print(err)
				return
			}
		}
	}()

	// Forward local -> remote. rc paces this by itself: it blocks whenever the send window is full
	// of unacknowledged messages, so there is no need to guess at a safe rate.
	if _, err := io.Copy(rc, conn); err != nil {
		return err
	}

	// io.Copy returning only means the bytes are on the websocket. Wait for the agent to
	// acknowledge all of them before telling it to hang up on the remote listener, otherwise the
	// disconnect can overtake a message still being retransmitted.
	if err := rc.waitAcked(); err != nil {
		return err
	}
	return rc.disconnectPort()
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

func createListener(port int) (net.Listener, error) {
	l, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}

	// use limit listener for now, eventually maybe we'll add muxing
	// REF: https://github.com/aws/amazon-ssm-agent/blob/master/agent/session/plugins/port/port_mux.go
	return netutil.LimitListener(l, 1), nil
}
