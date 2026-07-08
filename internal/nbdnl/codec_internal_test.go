package nbdnl

import (
	"encoding/binary"
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
		uint16(8), nbdAttrIndex, uint32(3),
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

	// errno -2 (ENOENT) in native byte order, two's complement. The message
	// text is platform-specific (the linux build maps codes to real errnos),
	// so only the presence of an error is asserted here.
	nlerr := nlBytes(uint32(20), nlmsgError, uint16(0), uint32(5), uint32(0), uint32(0xfffffffe))
	msgs, err = parseMessages(nlerr)
	if err != nil || len(msgs) != 1 || msgs[0].Err == nil {
		t.Fatalf("error parse: msgs=%+v err=%v", msgs, err)
	}
	if msgs[0].Err.Error() == "" {
		t.Fatal("the errno error should carry a message")
	}
}

func TestFamilyReplyRoundTrip(t *testing.T) {
	reply := marshalMessage(genlIDCtrl, 0, 1, 0, 1, 2, []attr{
		stringAttr(ctrlAttrFamilyName, "nbd"),
		{typ: ctrlAttrFamilyID, data: binary.NativeEndian.AppendUint16(nil, 0x1d)},
		nest(
			ctrlAttrMcastGroups,
			nest(
				1,
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
		nest(
			nbdAttrDeviceList,
			nest(
				nbdDeviceItem,
				u32Attr(nbdDeviceIndex, 2),
				attr{typ: nbdDeviceConnected, data: []byte{1}},
			),
			nest(
				nbdDeviceItem,
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

func TestErrnoToError(t *testing.T) {
	// The mapping is platform-specific: the linux build swaps in real errnos
	// at init, elsewhere the numeric fallback names the code. Either way a
	// negative kernel code must become a non-empty error.
	err := errnoToError(-13)
	if err == nil || err.Error() == "" {
		t.Fatalf("errno mapping produced no error: %v", err)
	}
}

// TestRequestConstructors pins the wire shape of every command the linux
// driver sends, by parsing each constructed request back.
func TestRequestConstructors(t *testing.T) {
	requireAttr := func(t *testing.T, m message, typ uint16) rawAttr {
		t.Helper()
		a, ok := attrByType(m.Attrs, typ)
		if !ok {
			t.Fatalf("request lacks attribute %d", typ)
		}
		return a
	}
	parseOne := func(t *testing.T, buf []byte) message {
		t.Helper()
		msgs, err := parseMessages(buf)
		if err != nil || len(msgs) != 1 {
			t.Fatalf("parse: msgs=%d err=%v", len(msgs), err)
		}
		return msgs[0]
	}

	fam := parseOne(t, getFamilyRequest(1, 10, nbdFamilyName))
	if fam.Type != genlIDCtrl || fam.Cmd != ctrlCmdGetFamily {
		t.Fatalf("family request header: %+v", fam)
	}
	if name := requireAttr(t, fam, ctrlAttrFamilyName).String(); name != "nbd" {
		t.Fatalf("family name = %q", name)
	}

	reconf := parseOne(t, reconfigureRequest(0x1d, 2, 10, 4, 9, 0, 2*time.Minute))
	if reconf.Cmd != nbdCmdReconfigure {
		t.Fatalf("reconfigure cmd = %d", reconf.Cmd)
	}
	if idx, err := requireAttr(t, reconf, nbdAttrIndex).U32(); err != nil || idx != 4 {
		t.Fatalf("reconfigure index = (%d, %v)", idx, err)
	}
	if _, ok := attrByType(reconf.Attrs, nbdAttrTimeout); ok {
		t.Fatal("a zero io timeout must not be sent")
	}
	sockets, err := requireAttr(t, reconf, nbdAttrSockets).Nested()
	if err != nil || len(sockets) != 1 {
		t.Fatalf("reconfigure sockets: %v %v", sockets, err)
	}

	disc := parseOne(t, disconnectRequest(0x1d, 3, 10, 6))
	if disc.Cmd != nbdCmdDisconnect {
		t.Fatalf("disconnect cmd = %d", disc.Cmd)
	}
	if idx, _ := requireAttr(t, disc, nbdAttrIndex).U32(); idx != 6 {
		t.Fatalf("disconnect index = %d", idx)
	}

	stat := parseOne(t, statusRequest(0x1d, 4, 10, 2))
	if stat.Cmd != nbdCmdStatus {
		t.Fatalf("status cmd = %d", stat.Cmd)
	}
	if idx, _ := requireAttr(t, stat, nbdAttrIndex).U32(); idx != 2 {
		t.Fatalf("status index = %d", idx)
	}
}

func TestRawAttrShortInputs(t *testing.T) {
	short := rawAttr{Typ: 1, Data: []byte{0xaa}}
	if _, err := short.U16(); err == nil {
		t.Fatal("U16 accepted a 1-byte attribute")
	}
	if _, err := short.U32(); err == nil {
		t.Fatal("U32 accepted a 1-byte attribute")
	}
	if _, err := (rawAttr{Typ: 1}).U8(); err == nil {
		t.Fatal("U8 accepted an empty attribute")
	}
	// A netlink string without a NUL terminator is returned whole.
	if s := (rawAttr{Typ: 1, Data: []byte("nbd")}).String(); s != "nbd" {
		t.Fatalf("String without NUL = %q, want nbd", s)
	}
}

func TestParseRepliesTolerateSparseAttrs(t *testing.T) {
	// LINK_DEAD without an index is not a usable notification.
	dead := marshalMessage(0x1d, 0, 0, 0, nbdCmdLinkDead, nbdGenlVersion, nil)
	msgs, _ := parseMessages(dead)
	if _, ok := parseLinkDead(msgs[0]); ok {
		t.Fatal("parseLinkDead invented an index")
	}
	// A LINK_DEAD whose index attribute is too short is likewise ignored.
	deadShort := marshalMessage(0x1d, 0, 0, 0, nbdCmdLinkDead, nbdGenlVersion,
		[]attr{{typ: nbdAttrIndex, data: []byte{1}}})
	msgs, _ = parseMessages(deadShort)
	if _, ok := parseLinkDead(msgs[0]); ok {
		t.Fatal("parseLinkDead accepted a short index")
	}

	// Status items lacking index or connected fields are skipped, not fatal.
	status := marshalMessage(0x1d, 0, 1, 0, nbdCmdStatus, nbdGenlVersion, []attr{
		nest(nbdAttrDeviceList,
			nest(nbdDeviceItem, u32Attr(nbdDeviceIndex, 9)), // no connected flag
			nest(nbdDeviceItem,
				u32Attr(nbdDeviceIndex, 2),
				attr{typ: nbdDeviceConnected, data: []byte{1}},
			),
		),
	})
	msgs, _ = parseMessages(status)
	if connected, err := parseStatusReply(msgs[0], 2); err != nil || !connected {
		t.Fatalf("sparse status reply: connected=%v err=%v", connected, err)
	}

	// Malformed nested payloads surface as errors.
	badNested := marshalMessage(0x1d, 0, 1, 0, nbdCmdStatus, nbdGenlVersion,
		[]attr{{typ: nbdAttrDeviceList | nlaFNested, data: []byte{9, 0, 1}}})
	msgs, _ = parseMessages(badNested)
	if _, err := parseStatusReply(msgs[0], 2); err == nil {
		t.Fatal("parseStatusReply accepted a malformed device list")
	}

	// Family reply: groups missing name or id are skipped; malformed nested
	// group data errors.
	fam := marshalMessage(genlIDCtrl, 0, 1, 0, 1, 2, []attr{
		{typ: ctrlAttrFamilyID, data: binary.NativeEndian.AppendUint16(nil, 0x1d)},
		nest(ctrlAttrMcastGroups,
			nest(1, stringAttr(ctrlAttrMcastGrpName, "half")), // no id
		),
	})
	msgs, _ = parseMessages(fam)
	info, err := parseFamilyReply(msgs[0])
	if err != nil || len(info.groups) != 0 {
		t.Fatalf("sparse family reply: groups=%v err=%v", info.groups, err)
	}
	famBad := marshalMessage(genlIDCtrl, 0, 1, 0, 1, 2, []attr{
		{typ: ctrlAttrFamilyID, data: binary.NativeEndian.AppendUint16(nil, 0x1d)},
		{typ: ctrlAttrMcastGroups | nlaFNested, data: []byte{9, 0, 1}},
	})
	msgs, _ = parseMessages(famBad)
	if _, err := parseFamilyReply(msgs[0]); err == nil {
		t.Fatal("parseFamilyReply accepted malformed group data")
	}

	// A connect reply whose index is too short errors rather than misreading.
	conn := marshalMessage(0x1d, 0, 1, 0, nbdCmdConnect, nbdGenlVersion,
		[]attr{{typ: nbdAttrIndex, data: []byte{5}}})
	msgs, _ = parseMessages(conn)
	if _, err := parseConnectReply(msgs[0]); err == nil {
		t.Fatal("parseConnectReply accepted a short index")
	}
}
