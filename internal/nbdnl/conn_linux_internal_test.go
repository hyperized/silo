//go:build linux

package nbdnl

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestDialAgainstHostKernel exercises the netlink bootstrap against whatever
// kernel runs the tests. Without the nbd module the resolve must fail with the
// modprobe instruction; with it, the basic query surface must work. The full
// connect/reconfigure lifecycle needs a served export and lives in the
// kernel integration test (make test-nbd-kernel).
func TestDialAgainstHostKernel(t *testing.T) {
	conn, err := Dial()
	if err != nil {
		if s := err.Error(); s == "" {
			t.Fatal("Dial error should explain itself")
		}
		t.Logf("no NBD netlink family on this host (fine): %v", err)
		return
	}
	defer func() { _ = conn.Close() }()

	// Statusing a device index far beyond any real device must error cleanly,
	// not hang or panic.
	if _, err := conn.Connected(4294967294); err == nil {
		t.Fatal("Connected on an absurd index should error")
	}

	watcher, err := conn.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// Close must unblock a pending Next.
	done := make(chan struct{})
	go func() {
		_, _ = watcher.Next()
		close(done)
	}()
	if err := watcher.Close(); err != nil {
		t.Fatalf("watcher Close: %v", err)
	}
	<-done
}

// scriptedSocket stands in for the netlink socket: it records what was sent
// and answers reads from a script of datagrams and errors, ending with
// io.EOF once the script runs out.
type scriptedSocket struct {
	mu     sync.Mutex
	sent   [][]byte
	script []readStep
	closed bool
}

type readStep struct {
	data []byte
	err  error
}

func (s *scriptedSocket) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, append([]byte(nil), p...))
	return len(p), nil
}

func (s *scriptedSocket) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.script) == 0 {
		return 0, io.EOF
	}
	step := s.script[0]
	s.script = s.script[1:]
	if step.err != nil {
		return 0, step.err
	}
	return copy(p, step.data), nil
}

func (s *scriptedSocket) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// ackFor is the kernel's NLMSG_ERROR reply to seq; code 0 is a plain ack,
// a negative errno a refusal.
func ackFor(seq uint32, code int32) []byte {
	return nlBytes(uint32(20), nlmsgError, uint16(0), seq, uint32(0), uint32(code)) // #nosec G115 -- two's complement on purpose
}

func TestConnSetTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reply   []readStep
		wantErr error
	}{
		{"kernel acks", []readStep{{data: ackFor(1, 0)}}, nil},
		{"kernel refuses", []readStep{{data: ackFor(1, -int32(unix.ENOENT))}}, unix.ENOENT},
		{"socket fails", []readStep{{err: unix.EBADF}}, unix.EBADF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sock := &scriptedSocket{script: tc.reply}
			c := &Conn{f: sock, pid: 7, family: familyInfo{id: 0x1d}}
			err := c.SetTimeouts(3, 30*time.Second, 5*time.Minute)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("SetTimeouts = %v, want %v", err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "device 3") {
				t.Fatalf("error %q should name the device", err)
			}
			if len(sock.sent) != 1 {
				t.Fatalf("sent %d requests, want 1", len(sock.sent))
			}
			msgs, perr := parseMessages(sock.sent[0])
			if perr != nil || len(msgs) != 1 {
				t.Fatalf("parse sent request: %v", perr)
			}
			m := msgs[0]
			if m.Cmd != nbdCmdReconfigure {
				t.Fatalf("cmd = %d, want reconfigure", m.Cmd)
			}
			if _, ok := attrByType(m.Attrs, nbdAttrSockets); ok {
				t.Fatal("a timeouts-only reconfigure must not carry a socket")
			}
			if idx, _ := mustAttr(t, m, nbdAttrIndex).U32(); idx != 3 {
				t.Fatalf("index = %d, want 3", idx)
			}
		})
	}
}

func mustAttr(t *testing.T, m message, typ uint16) rawAttr {
	t.Helper()
	a, ok := attrByType(m.Attrs, typ)
	if !ok {
		t.Fatalf("message lacks attribute %d", typ)
	}
	return a
}

func TestWatcherNext(t *testing.T) {
	const family = 0x1d
	linkDead := func(idx uint32) []byte {
		return marshalMessage(family, 0, 0, 0, nbdCmdLinkDead, nbdGenlVersion, []attr{u32Attr(nbdAttrIndex, idx)})
	}
	overflow := &os.PathError{Op: "read", Path: "netlink:nbd", Err: unix.ENOBUFS}

	type result struct {
		idx uint32
		err error
	}
	for _, tc := range []struct {
		name   string
		script []readStep
		want   []result
	}{
		{
			name:   "delivers dead links and skips noise",
			script: []readStep{{data: []byte{1, 2, 3}}, {data: marshalMessage(0x1e, 0, 0, 0, nbdCmdLinkDead, nbdGenlVersion, nil)}, {data: linkDead(4)}},
			want:   []result{{idx: 4}},
		},
		{
			name:   "overflow is reported and the watch carries on",
			script: []readStep{{err: overflow}, {data: linkDead(2)}},
			want:   []result{{err: ErrNotificationsDropped}, {idx: 2}},
		},
		{
			name:   "other errors end the watch",
			script: []readStep{{err: os.ErrClosed}},
			want:   []result{{err: os.ErrClosed}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &Watcher{f: &scriptedSocket{script: tc.script}, family: familyInfo{id: family}}
			for i, want := range tc.want {
				idx, err := w.Next()
				if !errors.Is(err, want.err) || idx != want.idx {
					t.Fatalf("call %d: Next = (%d, %v), want (%d, %v)", i, idx, err, want.idx, want.err)
				}
			}
			if _, err := w.Next(); err == nil || errors.Is(err, ErrNotificationsDropped) {
				t.Fatalf("an exhausted socket should end the watch, got %v", err)
			}
		})
	}
}
