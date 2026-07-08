package nbdnl

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
)

// nlBytes builds netlink wire bytes in native endianness for test fixtures.
func nlBytes(parts ...any) []byte {
	var out []byte
	for _, p := range parts {
		switch v := p.(type) {
		case uint16:
			out = binary.NativeEndian.AppendUint16(out, v)
		case uint32:
			out = binary.NativeEndian.AppendUint32(out, v)
		case uint64:
			out = binary.NativeEndian.AppendUint64(out, v)
		case []byte:
			out = append(out, v...)
		case string:
			out = append(out, v...)
		default:
			panic("nlBytes: unsupported fixture part")
		}
	}
	return out
}

func TestMarshalMessageLayout(t *testing.T) {
	msg := marshalMessage(0x1d, nlmFRequest|nlmFAck, 7, 99, nbdCmdConnect, nbdGenlVersion,
		[]attr{u32Attr(nbdAttrIndex, 3)})

	want := nlBytes(
		uint32(28),         // total length: 16 header + 4 genl + 8 attribute
		uint16(0x1d),       // family id
		uint16(0x1|0x4),    // NLM_F_REQUEST|NLM_F_ACK
		uint32(7),          // sequence
		uint32(99),         // port id
		[]byte{1, 1, 0, 0}, // genl: cmd=CONNECT version=1 reserved
		uint16(8), uint16(nbdAttrIndex), uint32(3),
	)
	if string(msg) != string(want) {
		t.Fatalf("marshalMessage layout mismatch\n got %v\nwant %v", msg, want)
	}
}

func TestMarshalAttrPadsToAlignment(t *testing.T) {
	got := marshalAttr(nil, stringAttr(ctrlAttrFamilyName, "nbd"))
	// 4 header + 4 payload ("nbd\0") happens to align; use a 2-byte payload to
	// force padding.
	if len(got)%4 != 0 {
		t.Fatalf("attribute is %d bytes, not 4-byte aligned", len(got))
	}
	got = marshalAttr(nil, attr{typ: 9, data: []byte{0xaa, 0xbb}})
	if len(got) != 8 {
		t.Fatalf("2-byte payload should marshal to 8 bytes (4 header + 2 data + 2 pad), got %d", len(got))
	}
	if length := binary.NativeEndian.Uint16(got[0:2]); length != 6 {
		t.Fatalf("attribute length field should exclude padding: got %d, want 6", length)
	}
}

func TestMarshalParseRoundTrip(t *testing.T) {
	in := marshalMessage(0x1e, nlmFRequest, 42, 4242, nbdCmdStatus, nbdGenlVersion, []attr{
		u32Attr(nbdAttrIndex, 12),
		u64Attr(nbdAttrSizeBytes, 5<<30),
		stringAttr(ctrlAttrFamilyName, "nbd"),
		nest(nbdAttrSockets, nest(nbdSockItem, u32Attr(nbdSockFD, 9))),
	})

	msgs, err := parseMessages(in)
	if err != nil {
		t.Fatalf("parseMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Type != 0x1e || m.Seq != 42 || m.Cmd != nbdCmdStatus || m.Version != nbdGenlVersion {
		t.Fatalf("header round-trip mismatch: %+v", m)
	}
	if len(m.Attrs) != 4 {
		t.Fatalf("got %d attributes, want 4", len(m.Attrs))
	}
	if v, err := m.Attrs[0].U32(); err != nil || v != 12 {
		t.Fatalf("u32 attribute round-trip: v=%d err=%v", v, err)
	}
	if s := m.Attrs[2].String(); s != "nbd" {
		t.Fatalf("string attribute round-trip: %q", s)
	}
	items, err := m.Attrs[3].Nested()
	if err != nil || len(items) != 1 {
		t.Fatalf("nested sockets: items=%d err=%v", len(items), err)
	}
	fields, err := items[0].Nested()
	if err != nil || len(fields) != 1 {
		t.Fatalf("nested socket item: fields=%d err=%v", len(fields), err)
	}
	if fd, err := fields[0].U32(); err != nil || fd != 9 {
		t.Fatalf("socket fd round-trip: fd=%d err=%v", fd, err)
	}
}

func TestParseMessagesRejectsMalformedInput(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"length below header size", nlBytes(uint32(8), uint16(0), uint16(0), uint32(0), uint32(0))},
		{"length past the buffer", nlBytes(uint32(64), uint16(0), uint16(0), uint32(0), uint32(0))},
		{"genl payload too short", nlBytes(uint32(18), uint16(0x1d), uint16(0), uint32(0), uint32(0), uint16(0))},
		{"error frame without a code", nlBytes(uint32(16), nlmsgError, uint16(0), uint32(0), uint32(0))},
		{"attribute overruns message", nlBytes(uint32(24), uint16(0x1d), uint16(0), uint32(0), uint32(0), []byte{1, 1, 0, 0}, uint16(40), uint16(1))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseMessages(tc.in); err == nil {
				t.Fatalf("parseMessages accepted malformed input")
			}
		})
	}
}

func TestParseMessagesAckAndError(t *testing.T) {
	ack := nlBytes(uint32(20), nlmsgError, uint16(0), uint32(5), uint32(0), uint32(0))
	msgs, err := parseMessages(ack)
	if err != nil || len(msgs) != 1 || !msgs[0].IsAck || msgs[0].Err != nil {
		t.Fatalf("ack parse: msgs=%+v err=%v", msgs, err)
	}

	// errno -2 (ENOENT) in native byte order, two's complement.
	nlerr := nlBytes(uint32(20), nlmsgError, uint16(0), uint32(5), uint32(0), uint32(0xfffffffe))
	msgs, err = parseMessages(nlerr)
	if err != nil || len(msgs) != 1 || msgs[0].Err == nil {
		t.Fatalf("error parse: msgs=%+v err=%v", msgs, err)
	}
	if !strings.Contains(msgs[0].Err.Error(), "2") {
		t.Fatalf("errno should surface in the error, got %v", msgs[0].Err)
	}
}

func TestFamilyReplyRoundTrip(t *testing.T) {
	reply := marshalMessage(genlIDCtrl, 0, 1, 0, 1, 2, []attr{
		stringAttr(ctrlAttrFamilyName, "nbd"),
		attr{typ: ctrlAttrFamilyID, data: binary.NativeEndian.AppendUint16(nil, 0x1d)},
		nest(ctrlAttrMcastGroups,
			nest(1,
				stringAttr(ctrlAttrMcastGrpName, nbdMcastGroup),
				u32Attr(ctrlAttrMcastGrpID, 7),
			),
		),
	})
	msgs, err := parseMessages(reply)
	if err != nil {
		t.Fatalf("parseMessages: %v", err)
	}
	info, err := parseFamilyReply(msgs[0])
	if err != nil {
		t.Fatalf("parseFamilyReply: %v", err)
	}
	if info.id != 0x1d {
		t.Fatalf("family id = %#x, want 0x1d", info.id)
	}
	if info.groups[nbdMcastGroup] != 7 {
		t.Fatalf("mcast group id = %d, want 7", info.groups[nbdMcastGroup])
	}
}

func TestParseFamilyReplyRequiresID(t *testing.T) {
	reply := marshalMessage(genlIDCtrl, 0, 1, 0, 1, 2, []attr{stringAttr(ctrlAttrFamilyName, "nbd")})
	msgs, err := parseMessages(reply)
	if err != nil {
		t.Fatalf("parseMessages: %v", err)
	}
	if _, err := parseFamilyReply(msgs[0]); err == nil {
		t.Fatal("parseFamilyReply accepted a reply without a family id")
	}
}

func TestConnectReplyAndLinkDead(t *testing.T) {
	reply := marshalMessage(0x1d, 0, 1, 0, nbdCmdConnect, nbdGenlVersion,
		[]attr{u32Attr(nbdAttrIndex, 5)})
	msgs, _ := parseMessages(reply)
	idx, err := parseConnectReply(msgs[0])
	if err != nil || idx != 5 {
		t.Fatalf("parseConnectReply: idx=%d err=%v", idx, err)
	}

	empty := marshalMessage(0x1d, 0, 1, 0, nbdCmdConnect, nbdGenlVersion, nil)
	msgs, _ = parseMessages(empty)
	if _, err := parseConnectReply(msgs[0]); err == nil {
		t.Fatal("parseConnectReply accepted a reply without an index")
	}

	dead := marshalMessage(0x1d, 0, 0, 0, nbdCmdLinkDead, nbdGenlVersion,
		[]attr{u32Attr(nbdAttrIndex, 9)})
	msgs, _ = parseMessages(dead)
	if idx, ok := parseLinkDead(msgs[0]); !ok || idx != 9 {
		t.Fatalf("parseLinkDead: idx=%d ok=%v", idx, ok)
	}
	if _, ok := parseLinkDead(msgs[0]); !ok {
		t.Fatal("parseLinkDead should be repeatable on the same message")
	}
	other := marshalMessage(0x1d, 0, 0, 0, nbdCmdStatus, nbdGenlVersion, nil)
	msgs, _ = parseMessages(other)
	if _, ok := parseLinkDead(msgs[0]); ok {
		t.Fatal("parseLinkDead matched a non-LINK_DEAD command")
	}
}

func TestParseStatusReply(t *testing.T) {
	reply := marshalMessage(0x1d, 0, 1, 0, nbdCmdStatus, nbdGenlVersion, []attr{
		nest(nbdAttrDeviceList,
			nest(nbdDeviceItem,
				u32Attr(nbdDeviceIndex, 2),
				attr{typ: nbdDeviceConnected, data: []byte{1}},
			),
			nest(nbdDeviceItem,
				u32Attr(nbdDeviceIndex, 3),
				attr{typ: nbdDeviceConnected, data: []byte{0}},
			),
		),
	})
	msgs, err := parseMessages(reply)
	if err != nil {
		t.Fatalf("parseMessages: %v", err)
	}
	if connected, err := parseStatusReply(msgs[0], 2); err != nil || !connected {
		t.Fatalf("device 2: connected=%v err=%v, want connected", connected, err)
	}
	if connected, err := parseStatusReply(msgs[0], 3); err != nil || connected {
		t.Fatalf("device 3: connected=%v err=%v, want disconnected", connected, err)
	}
	if _, err := parseStatusReply(msgs[0], 7); err == nil {
		t.Fatal("parseStatusReply invented a status for an unlisted device")
	}
}

func TestConnectRequestCarriesTimeouts(t *testing.T) {
	req := connectRequest(0x1d, 1, 0, ConnectConfig{
		SocketFD:        3,
		SizeBytes:       1 << 30,
		ServerFlags:     0x25,
		BlockSizeBytes:  512,
		IOTimeout:       45 * time.Second,
		DeadConnTimeout: 5 * time.Minute,
	})
	msgs, err := parseMessages(req)
	if err != nil {
		t.Fatalf("parseMessages: %v", err)
	}
	byType := map[uint16]rawAttr{}
	for _, a := range msgs[0].Attrs {
		byType[a.Typ] = a
	}
	if _, ok := byType[nbdAttrIndex]; ok {
		t.Fatal("connect request must not pin an index; the kernel chooses the device")
	}
	checks := []struct {
		typ  uint16
		want uint64
	}{
		{nbdAttrSizeBytes, 1 << 30},
		{nbdAttrServerFlags, 0x25},
		{nbdAttrBlockSizeBytes, 512},
		{nbdAttrTimeout, 45},
		{nbdAttrDeadConnTimeout, 300},
	}
	for _, c := range checks {
		a, ok := byType[c.typ]
		if !ok {
			t.Fatalf("connect request lacks attribute %d", c.typ)
		}
		v := binary.NativeEndian.Uint64(a.Data)
		if v != c.want {
			t.Fatalf("attribute %d = %d, want %d", c.typ, v, c.want)
		}
	}
}

func TestSecondsRoundsUp(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want uint64
	}{
		{0, 0},
		{-time.Second, 0},
		{time.Nanosecond, 1},
		{time.Second, 1},
		{1500 * time.Millisecond, 2},
		{5 * time.Minute, 300},
	}
	for _, c := range cases {
		if got := seconds(c.in); got != c.want {
			t.Fatalf("seconds(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestErrnoToErrorDefault(t *testing.T) {
	err := errnoToError(-13)
	if err == nil || !strings.Contains(err.Error(), "13") {
		t.Fatalf("default errno mapping should mention the code, got %v", err)
	}
	if errors.Is(err, errors.New("x")) {
		t.Fatal("sanity: errors.Is misbehaving")
	}
}
