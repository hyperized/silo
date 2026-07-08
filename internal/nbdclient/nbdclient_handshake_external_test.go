package nbdclient_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/nbdclient"
)

// Handshake wire constants, mirrored from the package under test so these
// black-box tests can hand-craft the server's side of the exchange.
const (
	hsMagicNBD   uint64 = 0x4e42444d41474943
	hsMagicIHAVE uint64 = 0x49484156454f5054
	hsMagicRep   uint64 = 0x0003e889045565a9

	hsFixedNewstyle uint16 = 1 << 0

	hsOptGo uint32 = 7

	hsRepAck  uint32 = 1
	hsRepInfo uint32 = 3
	hsRepErr  uint32 = 1 << 31
)

func hsGreeting(w io.Writer, flags uint16) error {
	b := binary.BigEndian.AppendUint64(nil, hsMagicNBD)
	b = binary.BigEndian.AppendUint64(b, hsMagicIHAVE)
	b = binary.BigEndian.AppendUint16(b, flags)
	_, err := w.Write(b)
	return err
}

// hsReadClientFlags consumes the client's 4-byte flags word so a net.Pipe write
// on the client side unblocks.
func hsReadClientFlags(r io.Reader) error {
	_, err := io.ReadFull(r, make([]byte, 4))
	return err
}

// hsReadGo consumes the client's NBD_OPT_GO message (16-byte option header plus
// its declared payload) so the pipe stays framed before the server replies.
func hsReadGo(r io.Reader) error {
	head := make([]byte, 16)
	if _, err := io.ReadFull(r, head); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(head[12:16])
	_, err := io.ReadFull(r, make([]byte, length))
	return err
}

func hsReply(opt, repType uint32, payload []byte) []byte {
	b := binary.BigEndian.AppendUint64(nil, hsMagicRep)
	b = binary.BigEndian.AppendUint32(b, opt)
	b = binary.BigEndian.AppendUint32(b, repType)
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

func hsExportInfo(size uint64, flags uint16) []byte {
	p := binary.BigEndian.AppendUint16(nil, 0) // NBD_INFO_EXPORT
	p = binary.BigEndian.AppendUint64(p, size)
	p = binary.BigEndian.AppendUint16(p, flags)
	return p
}

// TestNegotiateRejectsScriptedServers drives Negotiate over net.Pipe against a
// server goroutine that writes hand-crafted, protocol-violating byte sequences,
// asserting each failure is surfaced with an actionable message.
func TestNegotiateRejectsScriptedServers(t *testing.T) {
	cases := []struct {
		name   string
		script func(net.Conn)
		want   string
	}{
		{
			name:   "greeting lacks the fixed-newstyle flag",
			script: func(s net.Conn) { _ = hsGreeting(s, 0) },
			want:   "fixed-newstyle",
		},
		{
			name: "server hangs up before the client flags land",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = s.Close()
			},
			want: "sending client flags",
		},
		{
			name: "server hangs up before the GO option lands",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = s.Close()
			},
			want: "requesting export",
		},
		{
			name: "reply header truncated before the first reply",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = hsReadGo(s)
				_ = s.Close()
			},
			want: "reading an option reply",
		},
		{
			name: "reply carries a bad magic",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = hsReadGo(s)
				bad := hsReply(hsOptGo, hsRepAck, nil)
				binary.BigEndian.PutUint64(bad[0:8], 0xdeadbeef) // corrupt the reply magic
				_, _ = s.Write(bad)
			},
			want: "malformed",
		},
		{
			name: "reply payload exceeds the cap",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = hsReadGo(s)
				// Advertise a payload larger than the 1 MiB cap without sending it.
				_, _ = s.Write(hsReplyHeaderWithLength(hsOptGo, hsRepInfo, (1<<20)+1))
			},
			want: "exceeds the cap",
		},
		{
			name: "reply payload truncated mid-body",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = hsReadGo(s)
				// Promise 100 bytes then hang up: the header parses, the body does not.
				_, _ = s.Write(hsReplyHeaderWithLength(hsOptGo, hsRepInfo, 100))
				_ = s.Close()
			},
			want: "payload",
		},
		{
			name: "reply names a different option than GO",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = hsReadGo(s)
				_, _ = s.Write(hsReply(1, hsRepAck, nil)) // option 1, not GO
			},
			want: "pending",
		},
		{
			name: "unexpected reply type",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = hsReadGo(s)
				_, _ = s.Write(hsReply(hsOptGo, 2, nil)) // neither err, info, nor ack
			},
			want: "unexpected",
		},
		{
			name: "ACK without an export description",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = hsReadGo(s)
				_, _ = s.Write(hsReply(hsOptGo, hsRepAck, nil))
			},
			want: "without describing",
		},
		{
			name: "error reply with a server detail",
			script: func(s net.Conn) {
				_ = hsGreeting(s, hsFixedNewstyle)
				_ = hsReadClientFlags(s)
				_ = hsReadGo(s)
				_, _ = s.Write(hsReply(hsOptGo, hsRepErr|1, []byte("boom")))
			},
			want: "refused to serve",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer func() { _ = client.Close() }()
			defer func() { _ = server.Close() }()
			go tc.script(server)

			_, err := nbdclient.Negotiate(context.Background(), client, "/vol")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Negotiate err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

// hsReplyHeaderWithLength builds a 20-byte option-reply header declaring a
// payload of the given length without appending the payload itself.
func hsReplyHeaderWithLength(opt, repType, length uint32) []byte {
	b := binary.BigEndian.AppendUint64(nil, hsMagicRep)
	b = binary.BigEndian.AppendUint32(b, opt)
	b = binary.BigEndian.AppendUint32(b, repType)
	return binary.BigEndian.AppendUint32(b, length)
}

// TestNegotiateIgnoresAdvisoryInfoReplies proves an info reply that is not an
// NBD_INFO_EXPORT is skipped, and the export details come from the export info
// reply that follows before the ACK.
func TestNegotiateIgnoresAdvisoryInfoReplies(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	go func() {
		_ = hsGreeting(server, hsFixedNewstyle)
		_ = hsReadClientFlags(server)
		_ = hsReadGo(server)
		advisory := hsReply(hsOptGo, hsRepInfo, append(binary.BigEndian.AppendUint16(nil, 1), make([]byte, 10)...))
		export := hsReply(hsOptGo, hsRepInfo, hsExportInfo(1<<20, 0x25))
		ack := hsReply(hsOptGo, hsRepAck, nil)
		_, _ = server.Write(append(append(advisory, export...), ack...))
	}()

	got, err := nbdclient.Negotiate(context.Background(), client, "/vol")
	if err != nil {
		t.Fatalf("Negotiate: %v", err)
	}
	if got.Size != 1<<20 || got.TransmissionFlags != 0x25 {
		t.Fatalf("export = %+v, want size 1 MiB flags 0x25", got)
	}
}

// TestNegotiateSurfacesDeadlineFailure covers the SetDeadline error path: a
// closed pipe rejects SetDeadline, so a context deadline cannot be applied.
func TestNegotiateSurfacesDeadlineFailure(t *testing.T) {
	client, server := net.Pipe()
	_ = server.Close()
	_ = client.Close() // a closed pipe returns an error from SetDeadline
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := nbdclient.Negotiate(ctx, client, "/vol"); err == nil {
		t.Fatal("Negotiate should surface a SetDeadline failure on a closed connection")
	}
}
