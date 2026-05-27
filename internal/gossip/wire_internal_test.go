package gossip

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/membership"
)

func TestWire_RoundTrip(t *testing.T) {
	msg := &Message{
		Kind:          KindPing,
		SenderID:      "alpha",
		SenderAddress: "alpha:7100",
		SenderIncarn:  3,
		Target:        "beta",
		Piggyback: []membership.Event{
			{ID: "gamma", State: membership.StateAlive, Incarnation: 1},
		},
	}
	var buf bytes.Buffer
	if err := writeMessage(&buf, msg); err != nil {
		t.Fatalf("writeMessage: %v", err)
	}
	got, err := readMessage(&buf)
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if got.Kind != msg.Kind || got.SenderID != msg.SenderID || got.SenderIncarn != msg.SenderIncarn {
		t.Errorf("mismatch: got %+v want %+v", got, msg)
	}
	if len(got.Piggyback) != 1 || got.Piggyback[0].ID != "gamma" {
		t.Errorf("piggyback: got %+v", got.Piggyback)
	}
}

func TestWire_RejectsOversizedFrameOnRead(t *testing.T) {
	// Hand-craft a header announcing a size above the cap.
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], MaxMessageBytes+1)
	r := bytes.NewReader(header[:])
	_, err := readMessage(r)
	if err == nil || !strings.Contains(err.Error(), "refused to read") {
		t.Errorf("got %v, want oversize-frame error", err)
	}
}

func TestWire_RejectsOversizedPayloadOnWrite(t *testing.T) {
	// Fill the piggyback so JSON-encoded body exceeds MaxMessageBytes.
	big := make([]membership.Event, 0, 5000)
	for i := 0; i < 5000; i++ {
		big = append(big, membership.Event{
			ID:      "node-with-a-very-long-identifier-to-pad-the-frame-",
			Address: "long-host-name.example.invalid:65535",
			State:   membership.StateAlive,
		})
	}
	msg := &Message{Kind: KindPing, SenderID: "alpha", Piggyback: big}
	var buf bytes.Buffer
	err := writeMessage(&buf, msg)
	if err == nil || !strings.Contains(err.Error(), "cap is") {
		t.Errorf("got %v, want oversize-write error", err)
	}
}

func TestWire_RejectsEmptyFrame(t *testing.T) {
	// Zero length is reserved — refuse instead of decoding an empty
	// JSON object into a half-filled Message.
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 0)
	_, err := readMessage(bytes.NewReader(header[:]))
	if err == nil || !strings.Contains(err.Error(), "empty frame") {
		t.Errorf("got %v, want empty-frame error", err)
	}
}

func TestWire_BadHeaderEOF(t *testing.T) {
	_, err := readMessage(bytes.NewReader([]byte{1, 2}))
	if err == nil {
		t.Error("expected error for truncated header")
	}
}

func TestWire_BadBodyEOF(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 10)
	_, err := readMessage(bytes.NewReader(header[:]))
	if err == nil {
		t.Error("expected error for missing body")
	}
}

func TestWire_MarshalFailure(t *testing.T) {
	// time.Time fails to MarshalJSON for years outside [0, 9999]. We
	// stuff one into a piggyback Event to exercise the writeMessage
	// marshal-error branch without changing the production wire shape.
	yearOutOfRange := time.Date(-9999, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(-10000, 0, 0)
	msg := &Message{
		Kind: KindPing,
		Piggyback: []membership.Event{
			{ID: "p", State: membership.StateAlive, At: yearOutOfRange},
		},
	}
	var buf bytes.Buffer
	err := writeMessage(&buf, msg)
	if err == nil || !strings.Contains(err.Error(), "encode") {
		t.Errorf("got %v, want encode failure", err)
	}
}

func TestWire_BadJSON(t *testing.T) {
	body := []byte("not json")
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(body)))
	combined := append(header[:], body...)
	_, err := readMessage(bytes.NewReader(combined))
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Errorf("got %v, want decode error", err)
	}
}

// failingWriter returns an error after acceptN bytes.
type failingWriter struct {
	acceptN int
	written int
}

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.written >= f.acceptN {
		return 0, io.ErrUnexpectedEOF
	}
	n := len(p)
	if n > f.acceptN-f.written {
		n = f.acceptN - f.written
		f.written += n
		return n, io.ErrUnexpectedEOF
	}
	f.written += n
	return n, nil
}

func TestWire_WriteHeaderFailure(t *testing.T) {
	msg := &Message{Kind: KindPing, SenderID: "alpha"}
	w := &failingWriter{acceptN: 0}
	err := writeMessage(w, msg)
	if err == nil || !strings.Contains(err.Error(), "frame header") {
		t.Errorf("got %v, want header-write failure", err)
	}
}

func TestWire_WriteBodyFailure(t *testing.T) {
	msg := &Message{Kind: KindPing, SenderID: "alpha"}
	w := &failingWriter{acceptN: 4}
	err := writeMessage(w, msg)
	if err == nil || !strings.Contains(err.Error(), "frame body") {
		t.Errorf("got %v, want body-write failure", err)
	}
}

func TestApplyTimeouts_NoOpOnZero(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	if err := applyTimeouts(server, 0); err != nil {
		t.Errorf("applyTimeouts(0): got %v", err)
	}
	if err := applyTimeouts(server, time.Second); err != nil {
		t.Errorf("applyTimeouts(1s): got %v", err)
	}
}

// closedConn implements net.Conn but returns an error from SetDeadline.
type closedConn struct {
	net.Conn
}

func (closedConn) SetDeadline(time.Time) error { return io.ErrClosedPipe }

func TestApplyTimeouts_PropagatesSetDeadlineError(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	err := applyTimeouts(closedConn{Conn: server}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "TCP deadline") {
		t.Errorf("got %v, want set-deadline error", err)
	}
}
