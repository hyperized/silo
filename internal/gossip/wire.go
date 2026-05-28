// Package gossip implements SWIM-style failure detection and
// piggybacked-event broadcast over JSON-framed TCP with mTLS. The
// membership state machine lives in internal/membership; this package
// is the I/O around it: probe loop, anti-entropy syncs, wire format.
package gossip

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hyperized/silo/internal/membership"
)

// MessageKind discriminates the JSON envelopes that travel over the
// wire. Kept as a string so a packet capture is readable without a
// proto descriptor — gossip diagnostics happen on operator machines
// without buf installed.
type MessageKind string

const (
	// KindPing is a direct probe to one peer.
	KindPing MessageKind = "ping"
	// KindAck is the reply to a Ping or PingReq.
	KindAck MessageKind = "ack"
	// KindPingReq asks a peer to probe a third party on our behalf
	// when our own direct probe fails.
	KindPingReq MessageKind = "ping-req"
	// KindSyncReq opens an anti-entropy exchange: we send our membership
	// view and ask the peer for theirs in return.
	KindSyncReq MessageKind = "sync-req"
	// KindSyncResp is the reply to SyncReq, carrying the responder's
	// full membership view.
	KindSyncResp MessageKind = "sync-resp"
)

// MaxMessageBytes caps a single framed JSON payload. A typical Ping
// with a piggyback slice is a few hundred bytes; full SyncResps grow
// with the cluster but stay well under the cap until thousands of
// nodes are in the table. The instruction-shaped error names the cap
// and tells operators where to file a bug, which is the only
// reasonable course when this trips on a normally-sized cluster.
const MaxMessageBytes = 256 * 1024

// Message is one envelope on the gossip wire. The piggyback slice is
// where SWIM's eventual consistency lives — every Ping/Ack carries
// recent membership changes so the table converges without a dedicated
// broadcast channel.
type Message struct {
	Kind           MessageKind        `json:"kind"`
	SenderID       string             `json:"sender_id"`
	SenderAddress  string             `json:"sender_address,omitempty"`
	SenderIncarn   uint64             `json:"sender_incarnation"`
	Target         string             `json:"target,omitempty"`
	TargetAddress  string             `json:"target_address,omitempty"`
	Piggyback      []membership.Event `json:"piggyback,omitempty"`
	MembershipView []membership.Event `json:"membership_view,omitempty"`
}

// writeMessage serialises msg as length-prefixed JSON and writes it.
// The 4-byte big-endian length lets the reader allocate exactly the
// right buffer instead of streaming-parsing JSON, which would invite
// half-decoded messages on a partial read.
func writeMessage(w io.Writer, msg *Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("gossip: could not encode %s message (%w); this is a programmer bug, please file a bug at https://github.com/hyperized/silo/issues", msg.Kind, err)
	}
	if len(body) > MaxMessageBytes {
		return fmt.Errorf("gossip: %s message is %d bytes; the per-message cap is %d — this usually means the membership table is unexpectedly large, please file a bug at https://github.com/hyperized/silo/issues with the cluster size", msg.Kind, len(body), MaxMessageBytes)
	}
	var header [4]byte
	// len(body) is bounded by MaxMessageBytes (256 KiB) — well under
	// 2^32, so the uint32 conversion is safe.
	binary.BigEndian.PutUint32(header[:], uint32(len(body))) //nolint:gosec // bounded above by MaxMessageBytes
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("gossip: could not write %s frame header (%w)", msg.Kind, err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("gossip: could not write %s frame body (%w)", msg.Kind, err)
	}
	return nil
}

// readMessage reads one length-prefixed JSON frame from r and decodes
// it. Bound by MaxMessageBytes so a hostile or buggy peer cannot
// allocate arbitrary memory in this process.
func readMessage(r io.Reader) (*Message, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 {
		return nil, errors.New("gossip: received an empty frame; the peer is sending malformed gossip — check that both sides are on the same silod version")
	}
	if size > MaxMessageBytes {
		return nil, fmt.Errorf("gossip: refused to read a %d-byte frame; the per-message cap is %d (raise it only after auditing the sender)", size, MaxMessageBytes)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("gossip: could not read %d-byte frame body (%w)", size, err)
	}
	var msg Message
	if err := json.Unmarshal(buf, &msg); err != nil {
		return nil, fmt.Errorf("gossip: could not decode frame body (%w); the peer is sending malformed gossip — check that both sides are on the same silod version", err)
	}
	return &msg, nil
}

// applyTimeouts sets read/write deadlines on a single gossip connection.
// We use one deadline per RPC because gossip messages are one-shot:
// no long-lived streams, so a per-connection deadline is the same as
// a per-message deadline. The default of 2s is high enough to absorb a
// loaded laptop's docker network jitter and low enough that a stuck
// probe doesn't hold up the loop. Callers override via the configured
// timeout.
func applyTimeouts(c net.Conn, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	deadline := time.Now().Add(d)
	if err := c.SetDeadline(deadline); err != nil {
		return fmt.Errorf("gossip: could not set TCP deadline on %s (%w)", c.RemoteAddr(), err)
	}
	return nil
}
