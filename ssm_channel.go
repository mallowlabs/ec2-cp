package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"sync"
	"time"

	"github.com/mmmorris1975/ssm-session-client/datachannel"
)

// reliableChannel layers acknowledgement tracking and retransmission on top of an open,
// handshake-complete SSM data channel. The datachannel package's own send path can't be used for
// bulk transfers because it loses data in two separate ways, neither of which surfaces as an error:
//
//   - WriteMsg silently discards any message written while the service has asked us to pause
//     publication, and reports it as sent.
//   - Its retransmit buffer keys acknowledgements off the message header's sequence number, but the
//     service always sends acknowledgements with a header sequence number of 0 and carries the real
//     one in the JSON payload, so nothing is ever removed from that buffer. For a port forwarding
//     session it's moot anyway: WaitForHandshakeComplete disables the buffer entirely.
//
// The workaround that used to compensate for this was to send at 32 KiB/s and verify the result
// with a checksum. This instead does what the AWS session-manager-plugin does -- hold every
// unacknowledged message, resend it if its acknowledgement doesn't arrive within an RTT-derived
// timeout, and keep a window of them in flight -- which makes the transfer correct at full speed.
//
// It deliberately drives the underlying channel through only Read/WriteMsg and handles every
// message type itself, with one exception noted in readLoop. In particular it never hands a
// pause_publication message to HandleMsg, so the pausePub flag behind WriteMsg's discard stays
// false and the discard is unreachable.
type reliableChannel struct {
	c agentChannel

	mu      sync.Mutex
	cond    *sync.Cond
	paused  bool
	err     error
	seqNum  int64
	pending map[int64]*pendingMessage
	lastAck time.Time
	resends int

	// oldestUnacked is where resendOverdueLocked starts its scan. Sequence numbers are dense, so
	// tracking the front of the window lets it walk pending in order without sorting the map's keys.
	oldestUnacked int64

	// Jacobson/Karels round trip estimate, driving the retransmission timeout.
	rtt    float64
	rttVar float64
	rto    time.Duration

	inbound  chan []byte
	stopCh   chan struct{}
	stopOnce sync.Once
}

type pendingMessage struct {
	msg      *datachannel.AgentMessage
	sentAt   time.Time
	attempts int
}

const (
	// maxPayloadSize is how much data goes into a single agent message. This is a hard constraint
	// rather than a tuning knob: from 4096 bytes up, the service acknowledges every message and
	// then delivers only part of it to the agent, so a transfer "succeeds" with a corrupt file.
	// Measured over a 10 MiB transfer at 8192, 48% of the bytes arrived, each message contributing
	// only payload[120*k:120*k+4096] -- drifting by exactly the 120 byte header per message. The
	// AWS session-manager-plugin uses 1024; 2048 is the largest size that survived testing, and
	// anything larger must be re-tested against a real instance before being adopted.
	maxPayloadSize = 2048

	// sendWindow is how many messages may be awaiting acknowledgement at once, and is the main
	// speed knob. A larger window loses more messages to the service, but up to a point resending
	// them costs less than the throughput a smaller window gives up. Measured over a 10 MiB
	// transfer, in MiB/s: 32 -> 1.8, 64 -> 2.2, 128 -> 2.5, 256 -> 3.3, 512 -> 4.3, 1024 -> 5.5,
	// 2048 -> 4.0. Past 1024 the service drops enough that retransmission dominates and throughput
	// goes backwards, so this sits at the peak rather than as high as it will go.
	sendWindow = 1024

	resendInterval = 100 * time.Millisecond

	// The service drops messages under load rather than merely delaying them, so a longer timeout
	// only delays recovery: raising minRTO to 1s cost about a third of the throughput.
	minRTO = 200 * time.Millisecond
	maxRTO = 5 * time.Second

	// maxBackoffShift caps the exponential backoff applied to a message's retransmission timeout.
	// Without a backoff, a service that is merely slow to acknowledge gets a fresh copy of every
	// message every RTO, which is exactly the extra load that keeps it slow.
	maxBackoffShift = 5

	// ackStallTimeout bounds how long the send path will sit unable to make progress before giving
	// up, so a peer that stops acknowledging fails loudly instead of hanging until the caller's
	// timeout fires.
	ackStallTimeout = 60 * time.Second
)

// Same constants the AWS session-manager-plugin uses for its round trip time estimate.
const (
	rttConstant      = 1.0 / 8.0
	rttvConstant     = 1.0 / 4.0
	clockGranularity = 10 * time.Millisecond
)

var errChannelStopped = errors.New("data channel stopped")

// agentChannel is the part of datachannel.SsmDataChannel that reliableChannel drives. Narrowing it
// this far is what makes the acknowledgement and retransmission logic testable without a websocket.
type agentChannel interface {
	Read(data []byte) (int, error)
	WriteMsg(msg *datachannel.AgentMessage) (int, error)
	HandleMsg(data []byte) ([]byte, error)
	Close() error
}

func newReliableChannel(c agentChannel) *reliableChannel {
	r := &reliableChannel{
		c:       c,
		pending: make(map[int64]*pendingMessage),
		// WaitForHandshakeComplete sent the handshake response as sequence number 0 (WriteMsg
		// forces the first message it sends to be the Syn), so the agent expects 1 next.
		seqNum:        0,
		oldestUnacked: 1,
		lastAck:       time.Now(),
		rtt:           float64(100 * time.Millisecond),
		rto:           minRTO,
		inbound:       make(chan []byte, 16),
		stopCh:        make(chan struct{}),
	}
	r.cond = sync.NewCond(&r.mu)

	go r.readLoop()
	go r.resendLoop()

	return r
}

// Write splits p across as many agent messages as it takes and blocks until each has been handed to
// the websocket, which won't happen until there's room in the send window for it.
func (r *reliableChannel) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := min(len(p), maxPayloadSize)
		if err := r.writeChunk(p[:n]); err != nil {
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

func (r *reliableChannel) writeChunk(b []byte) error {
	msg := datachannel.NewAgentMessage()
	msg.MessageType = datachannel.InputStreamData
	msg.Flags = datachannel.Data
	msg.PayloadType = datachannel.Output
	// Kept until acknowledged so it can be resent, so it must not alias the caller's buffer.
	msg.Payload = append([]byte(nil), b...)

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.awaitSendSlot(); err != nil {
		return err
	}

	r.seqNum++
	msg.SequenceNumber = r.seqNum
	r.pending[msg.SequenceNumber] = &pendingMessage{msg: msg, sentAt: time.Now()}

	if _, err := r.c.WriteMsg(msg); err != nil {
		r.setErrLocked(err)
		return err
	}
	return nil
}

// awaitSendSlot blocks until the window has room and the service isn't asking us to hold off. The
// caller must hold r.mu; resendLoop's periodic broadcast is what lets the stall check run.
func (r *reliableChannel) awaitSendSlot() error {
	for {
		if r.err != nil {
			return r.err
		}
		if !r.paused && len(r.pending) < sendWindow {
			return nil
		}
		if err := r.stalledLocked(); err != nil {
			return err
		}
		r.cond.Wait()
	}
}

// stalledLocked reports the error to fail with when nothing has been acknowledged for long enough
// that the channel should be treated as dead rather than merely slow, and nil otherwise. This is the
// only definition of "stuck" the channel has: a deadline derived from how big the transfer is would
// either cut off a slow but healthy channel or, on a large transfer, never fire at all.
func (r *reliableChannel) stalledLocked() error {
	if time.Since(r.lastAck) <= ackStallTimeout {
		return nil
	}
	return fmt.Errorf("no acknowledgement from the SSM service for %s (%d messages in flight, paused=%t)",
		ackStallTimeout, len(r.pending), r.paused)
}

// waitAcked blocks until the service has acknowledged every message written so far, which is the
// point at which the data is known to have reached the SSM agent rather than merely the websocket.
func (r *reliableChannel) waitAcked() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for len(r.pending) > 0 {
		if r.err != nil {
			return r.err
		}
		if err := r.stalledLocked(); err != nil {
			return err
		}
		r.cond.Wait()
	}

	// Retransmissions are routine on this channel, not a warning sign, but the count is the first
	// thing worth knowing when a transfer is slower than expected.
	log.Printf("Relay finished: %d messages sent, %d retransmitted", r.seqNum, r.resends)
	return nil
}

// disconnectPort tells the agent to close its connection to the target port, which is what makes
// the remote listener see a clean EOF. Only meaningful once waitAcked has returned: the agent
// applies input messages in sequence order, so sending it earlier would cut off data still in
// flight.
func (r *reliableChannel) disconnectPort() error {
	return r.sendFlag(datachannel.DisconnectToPort, datachannel.Data)
}

func (r *reliableChannel) terminateSession() error {
	return r.sendFlag(datachannel.TerminateSession, datachannel.Fin)
}

func (r *reliableChannel) sendFlag(flag datachannel.PayloadTypeFlag, msgFlag datachannel.AgentMessageFlag) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(flag))

	msg := datachannel.NewAgentMessage()
	msg.MessageType = datachannel.InputStreamData
	msg.Flags = msgFlag
	msg.PayloadType = datachannel.Flag
	msg.Payload = payload

	r.mu.Lock()
	defer r.mu.Unlock()

	// Control messages share the data stream's numbering, so they have to come from the same
	// counter or the agent will treat them as out of order.
	r.seqNum++
	msg.SequenceNumber = r.seqNum

	_, err := r.c.WriteMsg(msg)
	return err
}

// close shuts the channel down in the order the SSM protocol expects: tell the agent the session is
// over -- which has to go through this type's sequence counter rather than the library's, which the
// handshake left pointing somewhere else -- then release anything blocked here, then drop the
// websocket. Holding this instead of the underlying channel is what keeps that ordering in one
// place.
func (r *reliableChannel) close() {
	_ = r.terminateSession()
	r.stop()
	_ = r.c.Close()
}

// stop releases anything blocked on this channel. readLoop is left to exit on its own once the
// underlying websocket is closed, since it's parked in a blocking read until then.
func (r *reliableChannel) stop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
		r.setErr(errChannelStopped)
	})
}

func (r *reliableChannel) readLoop() {
	defer close(r.inbound)

	// Sized well above the largest message the service sends: SsmDataChannel.Read copies into this
	// slice without bounds-checking it against the frame it just read.
	buf := make([]byte, 64*1024)

	for {
		n, err := r.c.Read(buf)
		if err != nil {
			r.setErr(err)
			return
		}

		m := new(datachannel.AgentMessage)
		if err := m.UnmarshalBinary(buf[:n]); err != nil {
			log.Printf("Dropping malformed message from the SSM service: %v", err)
			continue
		}

		switch m.MessageType {
		case datachannel.Acknowledge:
			r.handleAck(m)
		case datachannel.PausePublication:
			r.setPaused(true)
		case datachannel.StartPublication:
			r.setPaused(false)
		case datachannel.ChannelClosed:
			r.setErr(io.EOF)
			return
		case datachannel.OutputStreamData:
			if !r.deliverOutput(m.PayloadType, buf[:n]) {
				return
			}
		}
	}
}

// deliverOutput acknowledges an output message and forwards its payload, reporting whether the read
// loop should keep running. Nothing sent over a port forwarding session provokes a reply, so this
// direction is drained in arrival order rather than reassembled -- acknowledging it and keeping the
// socket moving is all it is here for.
func (r *reliableChannel) deliverOutput(payloadType datachannel.PayloadType, raw []byte) bool {
	if payloadType != datachannel.Output {
		return true // handshake traffic, which WaitForHandshakeComplete already dealt with
	}

	// The one place the library is still the right thing to call: with its inbound buffer disabled
	// this only acknowledges the message and hands back the payload, and it saves digging the
	// message ID the acknowledgement needs out of the wire format by hand.
	payload, err := r.c.HandleMsg(raw)
	if err != nil {
		r.setErr(err)
		return false
	}
	if len(payload) == 0 {
		return true
	}

	select {
	// payload aliases the read buffer, which the next read overwrites.
	case r.inbound <- append([]byte(nil), payload...):
		return true
	case <-r.stopCh:
		return false
	}
}

func (r *reliableChannel) resendLoop() {
	ticker := time.NewTicker(resendInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
		}

		r.mu.Lock()
		if r.err == nil && !r.paused {
			r.resendOverdueLocked()
		}
		// Unconditional, so that a writer parked in awaitSendSlot gets to re-check the stall
		// timeout even while nothing is arriving.
		r.cond.Broadcast()
		r.mu.Unlock()
	}
}

func (r *reliableChannel) resendOverdueLocked() {
	now := time.Now()
	front := int64(-1)

	// Oldest first: the agent applies the input stream in sequence order, so an unfilled gap holds
	// up everything queued behind it. Sequence numbers are handed out densely, so walking the range
	// visits them in order without having to sort the map's keys on every tick. Gaps are normal --
	// acknowledged messages are removed, and control messages never enter pending at all.
	for seq := r.oldestUnacked; seq <= r.seqNum; seq++ {
		p, ok := r.pending[seq]
		if !ok {
			continue
		}
		if front < 0 {
			front = seq
		}

		if now.Sub(p.sentAt) < r.rto<<min(p.attempts, maxBackoffShift) {
			continue
		}

		p.sentAt = now
		p.attempts++
		r.resends++

		if _, err := r.c.WriteMsg(p.msg); err != nil {
			r.setErrLocked(err)
			return
		}
	}

	if front < 0 {
		front = r.seqNum + 1
	}
	r.oldestUnacked = front
}

func (r *reliableChannel) handleAck(m *datachannel.AgentMessage) {
	var ack struct {
		SequenceNumber int64 `json:"AcknowledgedMessageSequenceNumber"`
	}
	if err := json.Unmarshal(m.Payload, &ack); err != nil {
		log.Printf("Dropping unparseable acknowledgement: %v", err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	p, ok := r.pending[ack.SequenceNumber]
	if !ok {
		return // acknowledgement for something already retired
	}
	delete(r.pending, ack.SequenceNumber)
	r.lastAck = time.Now()

	// Karn's algorithm: a resent message can't tell us which of its copies this acknowledges, so
	// its round trip time isn't a usable sample.
	if p.attempts == 0 {
		r.updateRTOLocked(time.Since(p.sentAt))
	}

	r.cond.Broadcast()
}

func (r *reliableChannel) updateRTOLocked(sample time.Duration) {
	s := float64(sample)
	r.rttVar = (1-rttvConstant)*r.rttVar + rttvConstant*math.Abs(r.rtt-s)
	r.rtt = (1-rttConstant)*r.rtt + rttConstant*s

	rto := time.Duration(r.rtt + max(float64(clockGranularity), 4*r.rttVar))
	r.rto = min(max(rto, minRTO), maxRTO)
}

func (r *reliableChannel) setPaused(paused bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.paused != paused {
		verb := "resume"
		if paused {
			verb = "pause"
		}
		log.Printf("SSM service asked us to %s publishing", verb)
	}
	r.paused = paused
	r.cond.Broadcast()
}

func (r *reliableChannel) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setErrLocked(err)
}

func (r *reliableChannel) setErrLocked(err error) {
	if r.err == nil {
		r.err = err
	}
	r.cond.Broadcast()
}
