package gossip

import (
	"bytes"
	"testing"

	"github.com/hyperized/silo/internal/membership"
)

// FuzzReadMessage hardens the gossip frame decoder, which parses bytes
// straight off the wire from another node. It must never panic on hostile
// or corrupt input — only decode a message or return an error. Seeded with
// well-formed frames plus degenerate framing so the fuzzer mutates outward
// from valid input into bad lengths and malformed JSON bodies.
func FuzzReadMessage(f *testing.F) {
	for _, msg := range []*Message{
		{Kind: KindPing, SenderID: "a"},
		{Kind: KindAck, SenderID: "b", SenderAddress: "b:7100", SenderDataAddress: "b:7000", Piggyback: []membership.Event{{ID: "c", State: membership.StateAlive, Incarnation: 1}}},
		{Kind: KindSyncResp, MembershipView: []membership.Event{{ID: "d", DataAddress: "d:7000", State: membership.StateAlive}}},
	} {
		var buf bytes.Buffer
		if err := writeMessage(&buf, msg); err == nil {
			f.Add(buf.Bytes())
		}
	}
	f.Add([]byte{})                        // no header at all
	f.Add([]byte{0, 0, 0, 0})              // zero-length frame
	f.Add([]byte{255, 255, 255, 255, 'x'}) // length far exceeding the body

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = readMessage(bytes.NewReader(data))
	})
}
