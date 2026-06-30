package gossip

import (
	"bytes"
	"net"
	"testing"

	"github.com/hyperized/silo/internal/membership"
)

// extSubsystem builds a Subsystem wired with a sync extension whose local
// state is the given bytes — used to drive the oversized-extension paths.
func extSubsystem(t *testing.T, self string, state []byte) *Subsystem {
	t.Helper()
	m, err := membership.New(self, self+":1", self+":2")
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	s, err := New(m, Options{
		Addr:      self + ":1",
		ServerTLS: dummyTLS(),
		ClientTLS: dummyTLS(),
		Extension: &fakeExt{state: state},
	}, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// drainPipe returns the client end of a net.Pipe whose server end is read
// once and then closed — enough for syncWith's deferred Close to unblock
// when the write never happens (the body exceeds the cap before any bytes
// reach the wire).
func drainPipe() net.Conn {
	server, client := net.Pipe()
	go func() {
		defer server.Close()
		_, _ = readMessage(server)
	}()
	return client
}

// When the sync extension (the namespace snapshot) exceeds the per-message
// cap, the initiator's writeMessage fails before anything reaches the wire.
// That used to be swallowed; it must now be counted and the extension size
// recorded so the cap can be watched approaching.
func TestSyncWith_OversizedExtensionIsCountedNotSwallowed(t *testing.T) {
	huge := bytes.Repeat([]byte("x"), MaxMessageBytes+1)
	s := extSubsystem(t, "self", huge)
	s.dialer = &fakeDialer{dial: func(string) (net.Conn, error) { return drainPipe(), nil }}

	if s.syncWith("peer:1") {
		t.Fatal("syncWith must fail when the extension exceeds the per-message cap")
	}
	if got := s.syncSendFailures.Load(); got != 1 {
		t.Errorf("syncSendFailures = %d, want 1", got)
	}
	if got := s.lastExtBytes.Load(); got != int64(len(huge)) {
		t.Errorf("lastExtBytes = %d, want %d", got, len(huge))
	}
}

// The responder side has the same exposure: replying to a peer's SyncReq
// with an oversized extension must be counted, not discarded.
func TestHandleConn_OversizedSyncReplyIsCounted(t *testing.T) {
	huge := bytes.Repeat([]byte("x"), MaxMessageBytes+1)
	b := extSubsystem(t, "b", huge)

	server, client := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() { b.handleConn(server); close(done) }()

	a := newTestSubsystem(t, "a", "a:1", nil, discardLogger())
	req := a.selfEnvelope(KindSyncReq, "")
	req.MembershipView = a.viewSnapshot()
	if err := writeMessage(client, req); err != nil {
		t.Fatalf("write SyncReq: %v", err)
	}
	<-done // handleConn returns after attempting (and failing) the reply

	if got := b.syncSendFailures.Load(); got != 1 {
		t.Errorf("syncSendFailures = %d, want 1", got)
	}
}

// A small extension still sends cleanly: no failure is counted and the
// extension-size gauge reflects the payload that went out.
func TestSyncWith_SmallExtensionSucceedsNoFailure(t *testing.T) {
	s := extSubsystem(t, "self", []byte("small"))
	s.dialer = &fakeDialer{dial: func(string) (net.Conn, error) {
		return pipeServer(t, func(req *Message) *Message {
			if req.Kind != KindSyncReq {
				t.Errorf("server saw %s, want sync-req", req.Kind)
			}
			return &Message{Kind: KindSyncResp, SenderID: "peer", SenderIncarn: 1}
		}), nil
	}}
	if !s.syncWith("peer:1") {
		t.Fatal("syncWith should succeed for a small extension")
	}
	if got := s.syncSendFailures.Load(); got != 0 {
		t.Errorf("syncSendFailures = %d, want 0", got)
	}
	if got := s.lastExtBytes.Load(); got != int64(len("small")) {
		t.Errorf("lastExtBytes = %d, want %d", got, len("small"))
	}
}

// CollectMetrics surfaces both new series.
func TestCollectMetrics_IncludesSyncFailureSeries(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.syncSendFailures.Store(3)
	s.lastExtBytes.Store(12345)

	got := map[string]float64{}
	for _, m := range s.CollectMetrics() {
		got[m.Name] = m.Value
	}
	if got["sync_send_failures_total"] != 3 {
		t.Errorf("sync_send_failures_total = %v, want 3", got["sync_send_failures_total"])
	}
	if got["sync_extension_bytes"] != 12345 {
		t.Errorf("sync_extension_bytes = %v, want 12345", got["sync_extension_bytes"])
	}
}
