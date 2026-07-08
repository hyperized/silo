package nbdnl

import (
	"testing"
)

// FuzzParseMessages hardens the netlink frame parser, which consumes datagrams
// straight off a kernel socket. Arbitrary input must error cleanly — never
// panic, never let an attribute alias bytes outside the input.
func FuzzParseMessages(f *testing.F) {
	f.Add([]byte{})
	f.Add(nlBytes(uint32(20), nlmsgError, uint16(0), uint32(5), uint32(0), uint32(0)))
	f.Add(marshalMessage(0x1d, nlmFRequest, 1, 2, nbdCmdConnect, nbdGenlVersion,
		[]attr{u32Attr(nbdAttrIndex, 3), nest(nbdAttrSockets, nest(nbdSockItem, u32Attr(nbdSockFD, 9)))}))

	f.Fuzz(func(t *testing.T, data []byte) {
		msgs, err := parseMessages(data)
		if err != nil {
			return
		}
		for _, m := range msgs {
			for _, a := range m.Attrs {
				// Containment: parsed attributes may only alias the input.
				if len(a.Data) > len(data) {
					t.Fatalf("attribute of %d bytes cannot come from a %d-byte input", len(a.Data), len(data))
				}
				// Nested walks must stay panic-free too.
				if kids, err := a.Nested(); err == nil {
					for _, k := range kids {
						_, _ = k.Nested()
					}
				}
			}
		}
	})
}

// FuzzMarshalParseRoundTrip pins the encoder and decoder as inverses: whatever
// header fields and attribute payloads we marshal must parse back identically.
func FuzzMarshalParseRoundTrip(f *testing.F) {
	f.Add(uint16(0x1d), uint32(7), uint8(1), []byte("payload"), uint16(2))
	f.Add(uint16(16), uint32(0), uint8(5), []byte{}, uint16(1))

	f.Fuzz(func(t *testing.T, family uint16, seq uint32, cmd uint8, payload []byte, attrType uint16) {
		if family == nlmsgError || family == nlmsgDone || family < genlIDCtrl {
			return // reserved control types are not generic-netlink messages
		}
		attrType &= nlaTypeMask
		if attrType == 0 || len(payload) > 1<<14 {
			return
		}
		in := marshalMessage(family, nlmFRequest, seq, 0, cmd, nbdGenlVersion, []attr{
			{typ: attrType, data: payload},
			nest(attrType, attr{typ: attrType, data: payload}),
		})
		if len(in)%4 != 0 {
			t.Fatalf("marshalled message is %d bytes, not 4-byte aligned", len(in))
		}
		msgs, err := parseMessages(in)
		if err != nil {
			t.Fatalf("round-trip parse failed: %v", err)
		}
		if len(msgs) != 1 {
			t.Fatalf("round-trip produced %d messages, want 1", len(msgs))
		}
		m := msgs[0]
		if m.Type != family || m.Seq != seq || m.Cmd != cmd {
			t.Fatalf("header mismatch: got %+v", m)
		}
		if len(m.Attrs) != 2 {
			t.Fatalf("got %d attributes, want 2", len(m.Attrs))
		}
		if m.Attrs[0].Typ != attrType || string(m.Attrs[0].Data) != string(payload) {
			t.Fatalf("leaf attribute mismatch: %+v", m.Attrs[0])
		}
		kids, err := m.Attrs[1].Nested()
		if err != nil || len(kids) != 1 || string(kids[0].Data) != string(payload) {
			t.Fatalf("nested attribute mismatch: kids=%v err=%v", kids, err)
		}
	})
}
