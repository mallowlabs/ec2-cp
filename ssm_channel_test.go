package main

import (
	"encoding/json"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mmmorris1975/ssm-session-client/datachannel"
)

const (
	// stillBlockedFor is how long a call that should be blocked is given to prove it by not
	// returning; unblockedWithin is how long one that should have been released is given to return.
	stillBlockedFor = 300 * time.Millisecond
	unblockedWithin = 5 * time.Second
)

// fakeChannel stands in for the SSM websocket: it records everything written and lets a test feed
// messages back in as if they came from the service.
type fakeChannel struct {
	mu   sync.Mutex
	sent []*datachannel.AgentMessage
	in   chan []byte
}

func newTestRelay(t *testing.T) (*fakeChannel, *reliableChannel) {
	t.Helper()

	f := &fakeChannel{in: make(chan []byte, 64)}
	r := newReliableChannel(f)
	t.Cleanup(r.stop)

	return f, r
}

func (f *fakeChannel) WriteMsg(msg *datachannel.AgentMessage) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, msg)
	return len(msg.Payload), nil
}

func (f *fakeChannel) Read(data []byte) (int, error) {
	msg, ok := <-f.in
	if !ok {
		return 0, io.EOF
	}
	return copy(data, msg), nil
}

func (f *fakeChannel) HandleMsg(data []byte) ([]byte, error) {
	m := new(datachannel.AgentMessage)
	if err := m.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return m.Payload, nil
}

func (f *fakeChannel) Close() error { return nil }

// sendCount reports how many times the message with the given sequence number has been written,
// which is 1 plus the number of times it has been retransmitted.
func (f *fakeChannel) sendCount(seq int64) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	n := 0
	for _, msg := range f.sent {
		if msg.SequenceNumber == seq && msg.PayloadType == datachannel.Output {
			n++
		}
	}
	return n
}

func (f *fakeChannel) dataSequenceNumbers() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	var seqs []int64
	for _, msg := range f.sent {
		if msg.PayloadType == datachannel.Output && !slices.Contains(seqs, msg.SequenceNumber) {
			seqs = append(seqs, msg.SequenceNumber)
		}
	}
	return seqs
}

func (f *fakeChannel) ack(t *testing.T, seq int64) {
	t.Helper()

	payload, err := json.Marshal(map[string]any{"AcknowledgedMessageSequenceNumber": seq})
	if err != nil {
		t.Fatal(err)
	}
	// The service always sends acknowledgements with a header sequence number of 0 and carries the
	// real one in the payload; keying off the header is the library bug this package exists to fix.
	f.in <- frame(t, datachannel.Acknowledge, datachannel.Undefined, 0, payload)
}

func frame(t *testing.T, msgType datachannel.MessageType, payloadType datachannel.PayloadType, seq int64, payload []byte) []byte {
	t.Helper()

	msg := datachannel.NewAgentMessage()
	msg.MessageType = msgType
	msg.PayloadType = payloadType
	msg.SequenceNumber = seq
	msg.Payload = payload

	data, err := msg.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// eventually polls cond until it holds, so a test doesn't have to guess how long a background
// goroutine needs.
func eventually(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func assertBlocked(t *testing.T, done <-chan error, what string) {
	t.Helper()

	select {
	case err := <-done:
		t.Fatalf("%s returned (%v) instead of blocking", what, err)
	case <-time.After(stillBlockedFor):
	}
}

func assertUnblocked(t *testing.T, done <-chan error, what string) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(unblockedWithin):
		t.Fatalf("%s stayed blocked", what)
	}
}

func TestWriteNumbersMessagesFromOne(t *testing.T) {
	f, r := newTestRelay(t)

	// The library sent the handshake response as sequence number 0, so the agent expects 1 next.
	if _, err := r.Write(make([]byte, 3*maxPayloadSize)); err != nil {
		t.Fatal(err)
	}

	if got, want := f.dataSequenceNumbers(), []int64{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("sent sequence numbers %v, want %v", got, want)
	}
}

func TestUnacknowledgedMessageIsResent(t *testing.T) {
	f, r := newTestRelay(t)

	if _, err := r.Write(make([]byte, 3*maxPayloadSize)); err != nil {
		t.Fatal(err)
	}

	f.ack(t, 1)
	f.ack(t, 3)

	eventually(t, unblockedWithin, "message 2 to be resent", func() bool {
		return f.sendCount(2) > 1
	})

	// An acknowledged message must not be resent alongside it.
	if n := f.sendCount(1); n != 1 {
		t.Fatalf("acknowledged message 1 was sent %d times, want 1", n)
	}

	f.ack(t, 2)
	if err := r.waitAcked(); err != nil {
		t.Fatalf("waitAcked: %v", err)
	}
}

func TestWaitAckedBlocksUntilEverythingIsAcknowledged(t *testing.T) {
	f, r := newTestRelay(t)

	if _, err := r.Write(make([]byte, 2*maxPayloadSize)); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- r.waitAcked() }()

	f.ack(t, 1)
	assertBlocked(t, done, "waitAcked with message 2 still unacknowledged")

	f.ack(t, 2)
	assertUnblocked(t, done, "waitAcked after everything was acknowledged")
}

func TestWriteBlocksWhenTheWindowIsFull(t *testing.T) {
	f, r := newTestRelay(t)

	if _, err := r.Write(make([]byte, sendWindow*maxPayloadSize)); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := r.Write(make([]byte, maxPayloadSize))
		done <- err
	}()

	assertBlocked(t, done, "write past a full send window")

	f.ack(t, 1)
	assertUnblocked(t, done, "write after the window opened up")
}

func TestPauseStopsSendingUntilResumed(t *testing.T) {
	f, r := newTestRelay(t)

	f.in <- frame(t, datachannel.PausePublication, datachannel.Undefined, 0, nil)
	eventually(t, time.Second, "the pause to be applied", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.paused
	})

	done := make(chan error, 1)
	go func() {
		_, err := r.Write(make([]byte, maxPayloadSize))
		done <- err
	}()

	assertBlocked(t, done, "write during a pause")
	if n := len(f.dataSequenceNumbers()); n != 0 {
		t.Fatalf("%d messages were sent while publication was paused, want 0", n)
	}

	f.in <- frame(t, datachannel.StartPublication, datachannel.Undefined, 0, nil)
	assertUnblocked(t, done, "write after publication resumed")
}

func TestInboundDataIsForwardedInArrivalOrder(t *testing.T) {
	f, r := newTestRelay(t)

	for _, body := range []string{"one", "two", "three"} {
		f.in <- frame(t, datachannel.OutputStreamData, datachannel.Output, 0, []byte(body))
	}

	for _, want := range []string{"one", "two", "three"} {
		select {
		case got := <-r.inbound:
			if string(got) != want {
				t.Fatalf("received %q, want %q", got, want)
			}
		case <-time.After(unblockedWithin):
			t.Fatalf("timed out waiting for %q", want)
		}
	}
}
