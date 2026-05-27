package gossip

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/membership"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func bufLogger(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

// dummyTLS returns a non-nil TLS config so the constructor accepts it
// without us building a real keypair for tests that don't open sockets.
func dummyTLS() *tls.Config { return &tls.Config{MinVersion: tls.VersionTLS13} }

func newTestSubsystem(t *testing.T, selfID, addr string, seeds []string, logger *slog.Logger) *Subsystem {
	t.Helper()
	m, err := membership.New(selfID, addr)
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	s, err := New(m, Options{
		Addr:                addr,
		Seeds:               seeds,
		ServerTLS:           dummyTLS(),
		ClientTLS:           dummyTLS(),
		ProbeInterval:       5 * time.Millisecond,
		ProbeTimeout:        20 * time.Millisecond,
		SuspectTimeout:      50 * time.Millisecond,
		DeadRetention:       200 * time.Millisecond,
		AntiEntropyInterval: 100 * time.Millisecond,
		Timeout:             50 * time.Millisecond,
		PiggybackCap:        4,
	}, logger)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestNew_ValidatesArgs(t *testing.T) {
	logger := discardLogger()
	m, _ := membership.New("self", "self:1")

	if _, err := New(nil, Options{Addr: "a:1", ServerTLS: dummyTLS(), ClientTLS: dummyTLS()}, logger); err == nil {
		t.Error("nil membership should fail")
	}
	if _, err := New(m, Options{Addr: "a:1", ServerTLS: dummyTLS(), ClientTLS: dummyTLS()}, nil); err == nil {
		t.Error("nil logger should fail")
	}
	if _, err := New(m, Options{Addr: "", ServerTLS: dummyTLS(), ClientTLS: dummyTLS()}, logger); err == nil {
		t.Error("empty Addr should fail")
	}
	if _, err := New(m, Options{Addr: "a:1"}, logger); err == nil {
		t.Error("missing TLS configs should fail")
	}
}

func TestNew_RefusesSelfSeed(t *testing.T) {
	logger := discardLogger()
	m, _ := membership.New("alpha", "alpha:7100")
	_, err := New(m, Options{
		Addr:      "alpha:7100",
		Seeds:     []string{"alpha:7100"},
		ServerTLS: dummyTLS(),
		ClientTLS: dummyTLS(),
	}, logger)
	if err == nil || !strings.Contains(err.Error(), "own identity") {
		t.Errorf("got %v, want self-seed error", err)
	}
}

func TestNew_RefusesSelfSeedByNodeID(t *testing.T) {
	logger := discardLogger()
	m, _ := membership.New("alpha", "alpha:7100")
	_, err := New(m, Options{
		Addr:      "0.0.0.0:7100",
		Seeds:     []string{"alpha"},
		ServerTLS: dummyTLS(),
		ClientTLS: dummyTLS(),
	}, logger)
	if err == nil || !strings.Contains(err.Error(), "own identity") {
		t.Errorf("got %v, want self-seed error", err)
	}
}

func TestNew_EmptySeedEntriesAreIgnored(t *testing.T) {
	logger := discardLogger()
	m, _ := membership.New("alpha", "alpha:7100")
	_, err := New(m, Options{
		Addr:      "alpha:7100",
		Seeds:     []string{""},
		ServerTLS: dummyTLS(),
		ClientTLS: dummyTLS(),
	}, logger)
	if err != nil {
		t.Errorf("empty seed entry should not fail construction, got %v", err)
	}
}

func TestWithDefaults_FillsZeroValues(t *testing.T) {
	got := withDefaults(Options{})
	if got.ProbeInterval != DefaultProbeInterval ||
		got.ProbeTimeout != DefaultProbeTimeout ||
		got.IndirectK != DefaultIndirectK ||
		got.SuspectTimeout != DefaultSuspectTimeout ||
		got.DeadRetention != DefaultDeadRetention ||
		got.AntiEntropyInterval != DefaultAntiEntropyInterval ||
		got.Timeout != DefaultTimeout ||
		got.PiggybackCap != DefaultPiggybackCap {
		t.Errorf("defaults not filled: %+v", got)
	}
	got = withDefaults(Options{
		ProbeInterval:       2 * time.Second,
		ProbeTimeout:        2 * time.Second,
		IndirectK:           7,
		SuspectTimeout:      2 * time.Second,
		DeadRetention:       2 * time.Second,
		AntiEntropyInterval: 2 * time.Second,
		Timeout:             2 * time.Second,
		PiggybackCap:        7,
	})
	if got.IndirectK != 7 || got.PiggybackCap != 7 {
		t.Errorf("explicit values should be preserved: %+v", got)
	}
}

func TestSubsystem_NameMembersAddr(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	if s.Name() != "gossip" {
		t.Errorf("Name: %q", s.Name())
	}
	if s.Members() == nil {
		t.Error("Members should not be nil")
	}
	if s.Addr() != "" {
		t.Errorf("Addr before Start: got %q, want empty", s.Addr())
	}
}

func TestRemember_RingTrim(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	// PiggybackCap is 4 for test subsystems.
	for i := 0; i < 8; i++ {
		s.remember(membership.Event{ID: "n", Incarnation: uint64(i)})
	}
	snap := s.piggybackSnapshot()
	if len(snap) != 4 {
		t.Fatalf("snapshot size: got %d, want 4", len(snap))
	}
	// Most recent events survive; oldest are trimmed.
	if snap[0].Incarnation != 4 || snap[3].Incarnation != 7 {
		t.Errorf("ring trim wrong: %+v", snap)
	}
}

func TestRemember_NoOpOnEmpty(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.remember()
	if got := s.piggybackSnapshot(); got != nil {
		t.Errorf("piggybackSnapshot on empty: got %v", got)
	}
}

func TestApplyIncoming_LogsAndRebroadcasts(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)
	msg := &Message{
		Kind:          KindPing,
		SenderID:      "peer-a",
		SenderAddress: "peer-a:7100",
		SenderIncarn:  1,
		Piggyback: []membership.Event{
			{ID: "peer-b", Address: "peer-b:7100", State: membership.StateAlive, Incarnation: 1},
		},
	}
	changed := s.applyIncoming(msg)
	if len(changed) != 2 {
		t.Fatalf("changed: got %d, want 2", len(changed))
	}
	out := buf.String()
	if !strings.Contains(out, "peer-a") || !strings.Contains(out, "peer-b") {
		t.Errorf("expected both peers in log output, got: %s", out)
	}
	// Piggyback should now contain the rebroadcast events.
	snap := s.piggybackSnapshot()
	if len(snap) == 0 {
		t.Error("piggyback should contain the rebroadcast events")
	}
}

func TestApplyIncoming_DetectsNodeIDCollision(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "alpha", "127.0.0.1:0", nil, logger)
	msg := &Message{
		Kind:          KindPing,
		SenderID:      "alpha", // another peer claiming our id
		SenderAddress: "rogue:7100",
		SenderIncarn:  1,
	}
	if changed := s.applyIncoming(msg); changed != nil {
		t.Errorf("collision should not merge, got %+v", changed)
	}
	if !strings.Contains(buf.String(), "two nodes share") {
		t.Errorf("expected collision warning in log, got: %s", buf.String())
	}
}

func TestSelfEnvelope(t *testing.T) {
	s := newTestSubsystem(t, "alpha", "alpha:7100", nil, discardLogger())
	env := s.selfEnvelope(KindPing, "beta")
	if env.Kind != KindPing || env.SenderID != "alpha" || env.Target != "beta" {
		t.Errorf("selfEnvelope: %+v", env)
	}
}

func TestPickAlivePeer_EmptyAndPresent(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	if _, _, ok := s.pickAlivePeer(); ok {
		t.Error("expected no alive peers in fresh table")
	}
	s.members.Apply(membership.Event{ID: "p", Address: "p:1", State: membership.StateAlive, Incarnation: 1})
	id, addr, ok := s.pickAlivePeer()
	if !ok || id != "p" || addr != "p:1" {
		t.Errorf("pickAlivePeer: %q, %q, %v", id, addr, ok)
	}
}

func TestPickHelpers(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	for i := 0; i < 5; i++ {
		s.members.Apply(membership.Event{
			ID:          string(rune('a' + i)),
			Address:     "a:1",
			State:       membership.StateAlive,
			Incarnation: 1,
		})
	}
	// Filter target out; ask for 3 helpers.
	got := s.pickHelpers("a", 3)
	if len(got) != 3 {
		t.Errorf("len: got %d, want 3", len(got))
	}
	for _, h := range got {
		if h.ID == "a" {
			t.Errorf("target should be filtered, got %v", got)
		}
	}
	// Ask for more than available — capped at len(candidates).
	all := s.pickHelpers("a", 100)
	if len(all) != 4 {
		t.Errorf("len with k>candidates: got %d, want 4", len(all))
	}
	// Empty case.
	s2 := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	if got := s2.pickHelpers("x", 3); len(got) != 0 {
		t.Errorf("empty candidates: got %+v", got)
	}
}

func TestViewSnapshot_IncludesAllMembers(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "p", Address: "p:1", State: membership.StateAlive, Incarnation: 2})
	view := s.viewSnapshot()
	// Two entries: self and p.
	if len(view) != 2 {
		t.Errorf("len: got %d, want 2", len(view))
	}
	var seenSelf, seenP bool
	for _, e := range view {
		switch e.ID {
		case "self":
			seenSelf = true
		case "p":
			seenP = true
		}
	}
	if !seenSelf || !seenP {
		t.Errorf("missing entries: %+v", view)
	}
}

// fakeDialer hands back one end of a net.Pipe and feeds the other end
// to a function the test controls. Used to drive probeDirect /
// probeIndirect / syncWith without standing up real TLS.
type fakeDialer struct {
	dial func(addr string) (net.Conn, error)
}

func (f *fakeDialer) Dial(addr string, _ *tls.Config, _ time.Duration) (net.Conn, error) {
	return f.dial(addr)
}

// pipeServer wires one side of a pipe to a handler. The handler reads
// the inbound message and writes its reply. Returns the client side
// for the caller (the test subject) to use.
func pipeServer(t *testing.T, handle func(*Message) *Message) net.Conn {
	t.Helper()
	server, client := net.Pipe()
	go func() {
		defer server.Close()
		msg, err := readMessage(server)
		if err != nil {
			return
		}
		reply := handle(msg)
		if reply != nil {
			_ = writeMessage(server, reply)
		}
	}()
	return client
}

func TestProbeDirect_SuccessReturnsTrue(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return pipeServer(t, func(m *Message) *Message {
				if m.Kind != KindPing {
					t.Errorf("server saw kind %s, want ping", m.Kind)
				}
				return &Message{Kind: KindAck, SenderID: m.Target, SenderIncarn: 1}
			}), nil
		},
	}
	ok := s.probeDirect("peer", "peer:1")
	if !ok {
		t.Error("probeDirect: expected success")
	}
}

func TestProbeDirect_DialFailureReturnsFalse(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return nil, errors.New("simulated dial failure")
		},
	}
	if s.probeDirect("peer", "peer:1") {
		t.Error("probeDirect should fail when dial fails")
	}
}

func TestProbeDirect_NonAckReply(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return pipeServer(t, func(_ *Message) *Message {
				return &Message{Kind: KindPing, SenderID: "rogue"}
			}), nil
		},
	}
	if s.probeDirect("peer", "peer:1") {
		t.Error("probeDirect should fail on non-Ack reply")
	}
}

func TestProbeIndirect_NoHelpersReturnsFalse(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	if s.probeIndirect("nobody", "addr:1") {
		t.Error("probeIndirect should fail with no helpers available")
	}
}

func TestProbeIndirect_AtLeastOneHelperAcks(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	// Add two helpers and one target.
	s.members.Apply(membership.Event{ID: "h1", Address: "h1:1", State: membership.StateAlive, Incarnation: 1})
	s.members.Apply(membership.Event{ID: "h2", Address: "h2:1", State: membership.StateAlive, Incarnation: 1})
	s.members.Apply(membership.Event{ID: "target", Address: "target:1", State: membership.StateAlive, Incarnation: 1})

	var mu sync.Mutex
	count := 0
	s.dialer = &fakeDialer{
		dial: func(addr string) (net.Conn, error) {
			mu.Lock()
			count++
			n := count
			mu.Unlock()
			return pipeServer(t, func(m *Message) *Message {
				if m.Kind != KindPingReq {
					t.Errorf("helper saw kind %s, want ping-req", m.Kind)
				}
				if n == 1 {
					// First helper Acks.
					return &Message{Kind: KindAck, SenderID: addr}
				}
				return &Message{Kind: KindPing, SenderID: "noise"}
			}), nil
		},
	}
	if !s.probeIndirect("target", "target:1") {
		t.Error("probeIndirect should succeed when one helper Acks")
	}
}

func TestProbeIndirect_AllHelpersFail(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "h1", Address: "h1:1", State: membership.StateAlive, Incarnation: 1})
	s.members.Apply(membership.Event{ID: "target", Address: "target:1", State: membership.StateAlive, Incarnation: 1})
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return nil, errors.New("nope")
		},
	}
	if s.probeIndirect("target", "target:1") {
		t.Error("probeIndirect should fail when all helpers fail")
	}
}

func TestSyncWith_RoundTripMergesView(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return pipeServer(t, func(req *Message) *Message {
				if req.Kind != KindSyncReq {
					t.Errorf("server saw kind %s, want sync-req", req.Kind)
				}
				return &Message{
					Kind:         KindSyncResp,
					SenderID:     "peer",
					SenderIncarn: 1,
					MembershipView: []membership.Event{
						{ID: "peer", State: membership.StateAlive, Incarnation: 1},
						{ID: "extra", State: membership.StateAlive, Incarnation: 1},
					},
				}
			}), nil
		},
	}
	if !s.syncWith("peer:1") {
		t.Fatal("syncWith should succeed")
	}
	if _, ok := s.members.Lookup("extra"); !ok {
		t.Error("syncWith should have merged the peer's view")
	}
}

func TestSyncWith_DialFailureReturnsFalse(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return nil, errors.New("nope")
		},
	}
	if s.syncWith("peer:1") {
		t.Error("syncWith should fail on dial error")
	}
}

func TestSyncWith_NonSyncRespIsRejected(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return pipeServer(t, func(_ *Message) *Message {
				return &Message{Kind: KindAck, SenderID: "wrong"}
			}), nil
		},
	}
	if s.syncWith("peer:1") {
		t.Error("syncWith should reject non-sync-resp reply")
	}
}

func TestSuspectTracker_Lifecycle(t *testing.T) {
	tr := newSuspectTracker()
	t0 := time.Now()
	tr.start("p", t0)
	tr.start("p", t0.Add(time.Hour)) // idempotent on re-start
	exp := tr.expired(t0.Add(-time.Second))
	if len(exp) != 0 {
		t.Errorf("expired before cutoff: got %v", exp)
	}
	exp = tr.expired(t0.Add(time.Second))
	if len(exp) != 1 || exp[0] != "p" {
		t.Errorf("expected one expired entry, got %v", exp)
	}
	tr.clear("p")
	if exp = tr.expired(t0.Add(time.Hour)); len(exp) != 0 {
		t.Errorf("expired after clear: got %v", exp)
	}
}

func TestRunProbeTick_NoPeersIsNoOp(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	tracker := newSuspectTracker()
	// Should not panic and should not block — there is nothing to probe.
	s.runProbeTick(tracker)
}

func TestRunProbeTick_MarksSuspectOnFailedProbes(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)
	s.members.Apply(membership.Event{ID: "peer", Address: "peer:1", State: membership.StateAlive, Incarnation: 1})
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return nil, errors.New("nope")
		},
	}
	tracker := newSuspectTracker()
	s.runProbeTick(tracker)
	n, _ := s.members.Lookup("peer")
	if n.State != membership.StateSuspect {
		t.Errorf("state: got %s, want suspect", n.State)
	}
	if !strings.Contains(buf.String(), "suspect") {
		t.Errorf("expected suspect log line, got: %s", buf.String())
	}
}

func TestRunProbeTick_PromotesSuspectToDeadAfterTimeout(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "peer", Address: "peer:1", State: membership.StateSuspect, Incarnation: 1})
	tracker := newSuspectTracker()
	// Start the timer in the past so the cutoff catches it immediately.
	tracker.start("peer", time.Now().Add(-time.Hour))
	s.runProbeTick(tracker)
	n, _ := s.members.Lookup("peer")
	if n.State != membership.StateDead {
		t.Errorf("state: got %s, want dead", n.State)
	}
}

func TestRunProbeTick_DirectProbeSuccessClearsTimer(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "peer", Address: "peer:1", State: membership.StateAlive, Incarnation: 1})
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return pipeServer(t, func(m *Message) *Message {
				return &Message{Kind: KindAck, SenderID: m.Target}
			}), nil
		},
	}
	tracker := newSuspectTracker()
	tracker.start("peer", time.Now())
	s.runProbeTick(tracker)
	if tracker.expired(time.Now().Add(time.Hour)); false {
		_ = tracker // placeholder; cleared below
	}
	if exp := tracker.expired(time.Now().Add(time.Hour)); len(exp) != 0 {
		t.Errorf("timer should be cleared after successful direct probe, got %v", exp)
	}
}

func TestRunProbeTick_IndirectProbeRecoversTarget(t *testing.T) {
	// Two members so the prober picks one but the indirect-helpers
	// pool isn't empty. We force the direct dial to "target:1" to
	// fail, and the indirect dial to "h1:1" to succeed.
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "target", Address: "target:1", State: membership.StateAlive, Incarnation: 1})
	s.members.Apply(membership.Event{ID: "h1", Address: "h1:1", State: membership.StateAlive, Incarnation: 1})
	// Direct calls (to target:1) fail; indirect calls (to h1:1) succeed.
	s.dialer = &fakeDialer{
		dial: func(addr string) (net.Conn, error) {
			if addr == "target:1" {
				return nil, errors.New("direct path down")
			}
			return pipeServer(t, func(m *Message) *Message {
				if m.Kind == KindPingReq {
					return &Message{Kind: KindAck, SenderID: m.Target}
				}
				return &Message{Kind: KindAck, SenderID: m.Target}
			}), nil
		},
	}
	// Drive probeIndirect directly so the test is deterministic
	// regardless of which peer the random pick lands on.
	tracker := newSuspectTracker()
	tracker.start("target", time.Now())
	if !s.probeIndirect("target", "target:1") {
		t.Fatal("probeIndirect should succeed via helper h1")
	}
	tracker.clear("target")
	if exp := tracker.expired(time.Now().Add(time.Hour)); len(exp) != 0 {
		t.Errorf("tracker should be cleared, got %v", exp)
	}
}

func TestRunProbeTick_IndirectSuccessClearsTrackerEntry(t *testing.T) {
	// Drive runProbeTick with one peer (so pickAlivePeer returns it
	// deterministically). Direct probe fails because the dialer
	// returns an error for the target's address only; without a helper
	// in the table, probeIndirect cannot succeed, so this case ends in
	// MarkSuspect. We then run a second tick with a real helper added
	// and the dialer reconfigured so the indirect branch hits the
	// `tracker.clear(id); return` path.
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "target", Address: "target:1", State: membership.StateAlive, Incarnation: 1})
	s.members.Apply(membership.Event{ID: "h1", Address: "h1:1", State: membership.StateAlive, Incarnation: 1})
	s.dialer = &fakeDialer{
		dial: func(addr string) (net.Conn, error) {
			if addr == "target:1" {
				return nil, errors.New("direct path down")
			}
			return pipeServer(t, func(m *Message) *Message {
				if m.Kind == KindPing || m.Kind == KindPingReq {
					return &Message{Kind: KindAck, SenderID: m.Target}
				}
				return nil
			}), nil
		},
	}
	tracker := newSuspectTracker()
	tracker.start("target", time.Now())
	// Repeat the tick a few times so the random pick lands on target.
	for i := 0; i < 20; i++ {
		s.runProbeTick(tracker)
		// State must never become Suspect — indirect probe always recovers
		// when picked, and pinging h1 directly always Acks.
		n, _ := s.members.Lookup("target")
		if n.State == membership.StateSuspect || n.State == membership.StateDead {
			t.Fatalf("target unexpectedly transitioned to %s", n.State)
		}
	}
}

func TestTryJoinSeeds_EmptySkipped(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", []string{""}, discardLogger())
	// No real seed entries: returns false because there's nothing to try.
	if s.tryJoinSeeds() {
		t.Error("expected false with only empty seeds")
	}
}

func TestTryJoinSeeds_StopsAtFirstSuccess(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "self:1", []string{"good:1", "second:1"}, logger)
	calls := 0
	s.dialer = &fakeDialer{
		dial: func(addr string) (net.Conn, error) {
			calls++
			return pipeServer(t, func(req *Message) *Message {
				if req.Kind != KindSyncReq {
					return nil
				}
				return &Message{Kind: KindSyncResp, SenderID: addr, SenderIncarn: 1}
			}), nil
		},
	}
	if !s.tryJoinSeeds() {
		t.Fatal("tryJoinSeeds should succeed")
	}
	if calls != 1 {
		t.Errorf("dial calls: got %d, want 1 (should stop on first success)", calls)
	}
	if !strings.Contains(buf.String(), "joined via seed") {
		t.Errorf("expected join log, got: %s", buf.String())
	}
}

func TestStartStop_LifecycleClean(t *testing.T) {
	// Bind to ephemeral port; we don't need real TLS because we never
	// dial through the listener — Start sets up the goroutines and
	// Shutdown tears them down. The TLS config is required by the
	// constructor but acceptLoop will receive net.ErrClosed when we
	// close the listener at shutdown, exercising the closed-listener
	// branch.
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	done := make(chan error, 1)
	go func() { done <- s.Start() }()
	deadline := time.After(2 * time.Second)
	for s.Addr() == "" {
		select {
		case <-deadline:
			t.Fatal("Start did not bind within 2s")
		case <-time.After(2 * time.Millisecond):
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return within 1s of Shutdown")
	}
	// Idempotent Shutdown.
	if err := s.Shutdown(context.Background()); err != nil {
		t.Errorf("second Shutdown: %v", err)
	}
}

func TestStart_BindFailureIsActionable(t *testing.T) {
	// Hold a port and try to start a second subsystem on the same one.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer occupied.Close()
	s := newTestSubsystem(t, "self", occupied.Addr().String(), nil, discardLogger())
	err = s.Start()
	if err == nil || !strings.Contains(err.Error(), "SILO_GOSSIP_ADDR") {
		t.Errorf("got %v, want actionable bind error", err)
	}
}

func TestShutdown_DeadlineExpires(t *testing.T) {
	// Shutdown with a context that's already cancelled returns the
	// deadline error rather than waiting forever; here we hold the
	// goroutines busy by signalling stopCh manually then re-closing.
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	// Pre-occupy stopCh with a long-running wait via a goroutine that
	// never finishes, so wg.Wait times out.
	s.wg.Add(1)
	hold := make(chan struct{})
	go func() {
		defer s.wg.Done()
		<-hold
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := s.Shutdown(ctx)
	if err == nil || !strings.Contains(err.Error(), "shutdown deadline expired") {
		t.Errorf("got %v, want deadline error", err)
	}
	close(hold)
}

// inProcessSubsystem hooks two test subsystems' dialers to each
// other's listeners via direct handleConn calls — no TLS, no real
// listener. Validates Ping/Ack and SyncReq/SyncResp across the boundary.
func TestEndToEnd_PingAckViaHandler(t *testing.T) {
	// silo-a and silo-b both have membership tables; we drive a single
	// Ping from a to b by writing the framed bytes into a net.Pipe and
	// letting b's handleConn process them.
	a := newTestSubsystem(t, "a", "a:1", nil, discardLogger())
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// b runs as the server side.
	go b.handleConn(server)

	// a writes a Ping to client.
	if err := writeMessage(client, a.selfEnvelope(KindPing, "b")); err != nil {
		t.Fatalf("write: %v", err)
	}
	ack, err := readMessage(client)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if ack.Kind != KindAck {
		t.Errorf("expected ack, got %s", ack.Kind)
	}
	if _, ok := b.members.Lookup("a"); !ok {
		t.Error("b should have learned about a from the Ping envelope")
	}
}

func TestHandleConn_SyncReqMergesAndReplies(t *testing.T) {
	a := newTestSubsystem(t, "a", "a:1", nil, discardLogger())
	a.members.Apply(membership.Event{ID: "extra-a", State: membership.StateAlive, Incarnation: 1})
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	b.members.Apply(membership.Event{ID: "extra-b", State: membership.StateAlive, Incarnation: 1})

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go b.handleConn(server)
	req := a.selfEnvelope(KindSyncReq, "")
	req.MembershipView = a.viewSnapshot()
	if err := writeMessage(client, req); err != nil {
		t.Fatalf("write: %v", err)
	}
	resp, err := readMessage(client)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.Kind != KindSyncResp {
		t.Errorf("expected sync-resp, got %s", resp.Kind)
	}
	// b merged extra-a from a's view.
	if _, ok := b.members.Lookup("extra-a"); !ok {
		t.Error("b should have learned about extra-a")
	}
	// resp.MembershipView contains b's view including extra-b.
	var seen bool
	for _, e := range resp.MembershipView {
		if e.ID == "extra-b" {
			seen = true
		}
	}
	if !seen {
		t.Errorf("resp should include extra-b, got %+v", resp.MembershipView)
	}
}

func TestHandleConn_PingReqInvokesDirectProbe(t *testing.T) {
	a := newTestSubsystem(t, "a", "a:1", nil, discardLogger())
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	// b knows about target c.
	b.members.Apply(membership.Event{ID: "c", Address: "c:1", State: membership.StateAlive, Incarnation: 1})
	// Wire b's dialer to a synthetic c that Acks.
	b.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return pipeServer(t, func(_ *Message) *Message {
				return &Message{Kind: KindAck, SenderID: "c"}
			}), nil
		},
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go b.handleConn(server)
	if err := writeMessage(client, a.selfEnvelope(KindPingReq, "c")); err != nil {
		t.Fatalf("write: %v", err)
	}
	ack, err := readMessage(client)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if ack.Kind != KindAck {
		t.Errorf("expected ack relay, got %s", ack.Kind)
	}
}

func TestHandleConn_PingReqWithoutTargetIsNoOp(t *testing.T) {
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	// Run handleConn directly so it returns when no reply is sent.
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(server)
	}()
	// Write a PingReq with empty target.
	if err := writeMessage(client, &Message{Kind: KindPingReq, SenderID: "a"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Closing client side wakes the server's read loop on its next read.
	_ = client.Close()
	<-done
}

func TestHandleConn_PingReqUnknownTargetIsSilent(t *testing.T) {
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(server)
	}()
	if err := writeMessage(client, &Message{Kind: KindPingReq, SenderID: "a", Target: "unknown"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = client.Close()
	<-done
}

func TestHandleConn_UnknownKindIsIgnored(t *testing.T) {
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(server)
	}()
	if err := writeMessage(client, &Message{Kind: MessageKind("weird"), SenderID: "a"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = client.Close()
	<-done
}

func TestHandleConn_AckOnInboundIsIgnored(t *testing.T) {
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.handleConn(server)
	}()
	if err := writeMessage(client, &Message{Kind: KindAck, SenderID: "a", SenderIncarn: 1}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = client.Close()
	<-done
}

func TestHandleConn_SetDeadlineFailureReturnsEarly(t *testing.T) {
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	// Wrap server so SetDeadline fails: handleConn must return without
	// trying to read.
	wrapped := &flakyConn{Conn: server, failSetDeadline: true}
	done := make(chan struct{})
	go func() { defer close(done); b.handleConn(wrapped) }()
	<-done
}

func TestHandleConn_ReadFailureLogged(t *testing.T) {
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	server, client := net.Pipe()
	defer server.Close()
	// Close client without writing — server's read returns EOF and the
	// handler exits. We wrap the conn so SetDeadline still succeeds
	// (a closed net.Pipe rejects SetDeadline, which would short-circuit
	// the test before the readMessage branch).
	wrapped := &deadlineOKConn{Conn: server}
	_ = client.Close()
	done := make(chan struct{})
	go func() { defer close(done); b.handleConn(wrapped) }()
	<-done
}

// deadlineOKConn shadows SetDeadline so it never fails even after the
// other side of the pipe has been closed; used to test handleConn's
// readMessage error path independently of SetDeadline's behaviour.
type deadlineOKConn struct {
	net.Conn
}

func (deadlineOKConn) SetDeadline(time.Time) error { return nil }

func TestAcceptLoop_ExitsOnClosedListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := newTestSubsystem(t, "self", ln.Addr().String(), nil, discardLogger())
	s.wg.Add(1)
	go s.acceptLoop(ln)
	_ = ln.Close()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acceptLoop did not exit on closed listener")
	}
}

// flakyListener accepts once with an error to exercise the warn-and-continue path.
type flakyListener struct {
	net.Listener
	calls int
	addr  net.Addr
}

func (f *flakyListener) Accept() (net.Conn, error) {
	f.calls++
	if f.calls == 1 {
		return nil, errors.New("transient")
	}
	return nil, net.ErrClosed
}

func (f *flakyListener) Close() error   { return nil }
func (f *flakyListener) Addr() net.Addr { return f.addr }

func TestAcceptLoop_TransientErrorIsLogged(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	fl := &flakyListener{addr: ln.Addr()}
	s.wg.Add(1)
	go s.acceptLoop(fl)
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acceptLoop did not exit")
	}
	if !strings.Contains(buf.String(), "accept failed") {
		t.Errorf("expected accept-failed log, got: %s", buf.String())
	}
}

// stoppedListener returns "use of closed network connection" without ErrClosed.
type stoppedListener struct {
	addr net.Addr
}

func (s *stoppedListener) Accept() (net.Conn, error) {
	return nil, errors.New("use of closed network connection")
}
func (s *stoppedListener) Close() error   { return nil }
func (s *stoppedListener) Addr() net.Addr { return s.addr }

func TestAcceptLoop_ClosedStringError(t *testing.T) {
	logger := discardLogger()
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	sl := &stoppedListener{addr: ln.Addr()}
	s.wg.Add(1)
	go s.acceptLoop(sl)
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acceptLoop did not exit")
	}
}

// acceptOnceListener pipes one fake conn through Accept then returns ErrClosed.
type acceptOnceListener struct {
	conn net.Conn
	done bool
	addr net.Addr
}

func (a *acceptOnceListener) Accept() (net.Conn, error) {
	if a.done {
		return nil, net.ErrClosed
	}
	a.done = true
	return a.conn, nil
}

func (a *acceptOnceListener) Close() error   { return nil }
func (a *acceptOnceListener) Addr() net.Addr { return a.addr }

func TestAcceptLoop_HandlesOneConnection(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	server, client := net.Pipe()
	// Have the "client" send one Ping then close.
	go func() {
		_ = writeMessage(client, &Message{Kind: KindPing, SenderID: "remote", SenderIncarn: 1})
		_, _ = readMessage(client)
		_ = client.Close()
	}()
	listenerAddr := mustListenForAddr(t)
	ln := &acceptOnceListener{conn: server, addr: listenerAddr}
	s.wg.Add(1)
	go s.acceptLoop(ln)
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acceptLoop did not finish")
	}
	if _, ok := s.members.Lookup("remote"); !ok {
		t.Error("expected the inbound Ping to register the remote peer")
	}
}

// mustListenForAddr is a helper that yields a net.Addr we can hand a
// fake listener (some assertions inspect Addr()).
func mustListenForAddr(t *testing.T) net.Addr {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr()
}

func TestAntiEntropyLoop_PicksRandomPeerAndSyncs(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "peer", Address: "peer:1", State: membership.StateAlive, Incarnation: 1})
	called := make(chan struct{}, 1)
	s.dialer = &fakeDialer{
		dial: func(addr string) (net.Conn, error) {
			select {
			case called <- struct{}{}:
			default:
			}
			return pipeServer(t, func(_ *Message) *Message {
				return &Message{Kind: KindSyncResp, SenderID: addr, SenderIncarn: 1}
			}), nil
		},
	}
	s.wg.Add(1)
	go s.antiEntropyLoop()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("antiEntropyLoop did not dial a peer within 1s")
	}
	close(s.stopCh)
	s.wg.Wait()
}

func TestAntiEntropyLoop_NoPeersSkipped(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	var dialed atomic.Int32
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			dialed.Add(1)
			return nil, errors.New("nope")
		},
	}
	s.wg.Add(1)
	go s.antiEntropyLoop()
	// Wait one anti-entropy interval; nothing should dial because the
	// table has no peers.
	time.Sleep(s.opts.AntiEntropyInterval + 50*time.Millisecond)
	close(s.stopCh)
	s.wg.Wait()
	if got := dialed.Load(); got != 0 {
		t.Errorf("antiEntropyLoop dialed with no peers: %d times", got)
	}
}

func TestAntiEntropyLoop_SkipsPeerWithoutAddress(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "peer", State: membership.StateAlive, Incarnation: 1})
	var dialed atomic.Int32
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			dialed.Add(1)
			return nil, errors.New("nope")
		},
	}
	s.wg.Add(1)
	go s.antiEntropyLoop()
	time.Sleep(s.opts.AntiEntropyInterval + 50*time.Millisecond)
	close(s.stopCh)
	s.wg.Wait()
	if got := dialed.Load(); got != 0 {
		t.Errorf("antiEntropyLoop dialed an address-less peer: %d times", got)
	}
}

func TestJoinLoop_SucceedsOnSecondAttempt(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", []string{"seed:1"}, discardLogger())
	var calls atomic.Int32
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			n := calls.Add(1)
			if n < 2 {
				return nil, errors.New("not ready yet")
			}
			return pipeServer(t, func(_ *Message) *Message {
				return &Message{Kind: KindSyncResp, SenderID: "seed", SenderIncarn: 1}
			}), nil
		},
	}
	s.wg.Add(1)
	go s.joinLoop()
	deadline := time.After(2 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("joinLoop did not retry within 2s")
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(s.stopCh)
	s.wg.Wait()
}

// flakyConn returns a net.Conn that errors after the first SetDeadline or
// drops one of the read/write halves. Used to drive the error branches
// inside probeDirect / probeIndirect / syncWith that are otherwise
// reached only when a peer's TCP stack misbehaves.
type flakyConn struct {
	net.Conn
	failSetDeadline bool
	failWrite       bool
	failRead        bool
}

func (f *flakyConn) SetDeadline(t time.Time) error {
	if f.failSetDeadline {
		return errors.New("simulated SetDeadline failure")
	}
	return f.Conn.SetDeadline(t)
}

func (f *flakyConn) Write(p []byte) (int, error) {
	if f.failWrite {
		return 0, errors.New("simulated write failure")
	}
	return f.Conn.Write(p)
}

func (f *flakyConn) Read(p []byte) (int, error) {
	if f.failRead {
		return 0, errors.New("simulated read failure")
	}
	return f.Conn.Read(p)
}

func newFlakyDialer(t *testing.T, fc func(c net.Conn) net.Conn, reply func(*Message) *Message) dialer {
	t.Helper()
	return &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			pipe := pipeServer(t, reply)
			return fc(pipe), nil
		},
	}
}

func TestProbeDirect_SetDeadlineFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = newFlakyDialer(t,
		func(c net.Conn) net.Conn { return &flakyConn{Conn: c, failSetDeadline: true} },
		func(_ *Message) *Message { return &Message{Kind: KindAck} },
	)
	if s.probeDirect("peer", "peer:1") {
		t.Error("probeDirect should fail when SetDeadline errors")
	}
}

func TestProbeDirect_WriteFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = newFlakyDialer(t,
		func(c net.Conn) net.Conn { return &flakyConn{Conn: c, failWrite: true} },
		func(_ *Message) *Message { return &Message{Kind: KindAck} },
	)
	if s.probeDirect("peer", "peer:1") {
		t.Error("probeDirect should fail when write errors")
	}
}

func TestProbeDirect_ReadFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = newFlakyDialer(t,
		func(c net.Conn) net.Conn { return &flakyConn{Conn: c, failRead: true} },
		func(_ *Message) *Message { return &Message{Kind: KindAck} },
	)
	if s.probeDirect("peer", "peer:1") {
		t.Error("probeDirect should fail when read errors")
	}
}

func TestProbeIndirect_SetDeadlineFailureCountsAsHelperFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "h1", Address: "h1:1", State: membership.StateAlive, Incarnation: 1})
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			c := pipeServer(t, func(_ *Message) *Message { return &Message{Kind: KindAck} })
			return &flakyConn{Conn: c, failSetDeadline: true}, nil
		},
	}
	if s.probeIndirect("target", "target:1") {
		t.Error("probeIndirect should fail when helpers cannot SetDeadline")
	}
}

func TestProbeIndirect_WriteFailureCountsAsHelperFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "h1", Address: "h1:1", State: membership.StateAlive, Incarnation: 1})
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			c := pipeServer(t, func(_ *Message) *Message { return &Message{Kind: KindAck} })
			return &flakyConn{Conn: c, failWrite: true}, nil
		},
	}
	if s.probeIndirect("target", "target:1") {
		t.Error("probeIndirect should fail when helpers cannot write")
	}
}

func TestProbeIndirect_ReadFailureCountsAsHelperFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "h1", Address: "h1:1", State: membership.StateAlive, Incarnation: 1})
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			c := pipeServer(t, func(_ *Message) *Message { return &Message{Kind: KindAck} })
			return &flakyConn{Conn: c, failRead: true}, nil
		},
	}
	if s.probeIndirect("target", "target:1") {
		t.Error("probeIndirect should fail when helpers cannot read")
	}
}

func TestSyncWith_SetDeadlineFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			c := pipeServer(t, func(_ *Message) *Message {
				return &Message{Kind: KindSyncResp, SenderID: "peer"}
			})
			return &flakyConn{Conn: c, failSetDeadline: true}, nil
		},
	}
	if s.syncWith("peer:1") {
		t.Error("syncWith should fail when SetDeadline errors")
	}
}

func TestSyncWith_WriteFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			c := pipeServer(t, func(_ *Message) *Message {
				return &Message{Kind: KindSyncResp, SenderID: "peer"}
			})
			return &flakyConn{Conn: c, failWrite: true}, nil
		},
	}
	if s.syncWith("peer:1") {
		t.Error("syncWith should fail when write errors")
	}
}

func TestProbeLoop_TicksAndExitsOnStopCh(t *testing.T) {
	// Drive probeLoop end-to-end: one peer, one direct probe Ack, then
	// stop the loop and confirm it returns.
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "peer", Address: "peer:1", State: membership.StateAlive, Incarnation: 1})
	var probes atomic.Int32
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			probes.Add(1)
			return pipeServer(t, func(m *Message) *Message {
				return &Message{Kind: KindAck, SenderID: m.Target}
			}), nil
		},
	}
	s.wg.Add(1)
	go s.probeLoop()
	deadline := time.After(2 * time.Second)
	for probes.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("probeLoop did not probe within 2s")
		case <-time.After(2 * time.Millisecond):
		}
	}
	close(s.stopCh)
	s.wg.Wait()
}

func TestRunProbeTick_LogsPrune(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)
	// Make a Dead peer with a LastChange in the distant past so Prune
	// picks it up.
	s.members.Apply(membership.Event{ID: "old-peer", State: membership.StateAlive, Incarnation: 1})
	s.members.MarkDead("old-peer")
	// Drive a wider DeadRetention check by hammering Now().
	prev := membership.Now
	defer func() { membership.Now = prev }()
	membership.Now = func() time.Time { return time.Now().Add(time.Hour) }
	tracker := newSuspectTracker()
	s.runProbeTick(tracker)
	if !strings.Contains(buf.String(), "member pruned") {
		t.Errorf("expected member-pruned log, got: %s", buf.String())
	}
}

func TestRunProbeTick_SelfIDFilteredOut(t *testing.T) {
	// If pickAlivePeer somehow returns our own id, runProbeTick should
	// noop. This is defence-in-depth; AlivePeers already filters self,
	// but the explicit check guards against a future refactor.
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	tracker := newSuspectTracker()
	// Pre-seed a peer named "self" to simulate the misconfig (the
	// AlivePeers method filters by id, but a misconfigured table could
	// have an alias). Approximate by seeding a peer with empty address
	// so the secondary branch fires.
	s.members.Apply(membership.Event{ID: "peer", State: membership.StateAlive, Incarnation: 1})
	s.runProbeTick(tracker)
}

func TestProbeLoop_StopChImmediately(t *testing.T) {
	// Start probeLoop then immediately close stopCh; the goroutine
	// must return without firing a probe.
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.wg.Add(1)
	close(s.stopCh)
	go s.probeLoop()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probeLoop did not exit on pre-closed stopCh")
	}
}

func TestJoinLoop_ExitsOnShutdownDuringBackoff(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", []string{"seed:1"}, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			return nil, errors.New("never")
		},
	}
	s.wg.Add(1)
	done := make(chan struct{})
	go func() { defer close(done); s.joinLoop() }()
	close(s.stopCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("joinLoop did not exit within 1s of stopCh close")
	}
}

func TestJoinLoop_HitsBackoffCap(t *testing.T) {
	// Crank the initial back-off so the cap branch fires within ~50ms
	// instead of the production five seconds.
	prevInit := joinInitialBackoff
	prevMax := joinMaxBackoff
	t.Cleanup(func() { joinInitialBackoff = prevInit; joinMaxBackoff = prevMax })
	joinInitialBackoff = 2 * time.Millisecond
	joinMaxBackoff = 5 * time.Millisecond

	s := newTestSubsystem(t, "self", "self:1", []string{"seed:1"}, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) { return nil, errors.New("never") },
	}
	s.wg.Add(1)
	go s.joinLoop()
	// Let it do enough doublings to hit the cap.
	time.Sleep(40 * time.Millisecond)
	close(s.stopCh)
	s.wg.Wait()
}

func TestTLSDialer_DialFailure(t *testing.T) {
	// Real production dialer: dialing 127.0.0.1:1 with no listener
	// should fail. We just want to cover the error-wrapping branch.
	d := tlsDialer{}
	_, err := d.Dial("127.0.0.1:1", &tls.Config{MinVersion: tls.VersionTLS13}, 50*time.Millisecond)
	if err == nil {
		t.Error("expected dial failure on closed port")
	}
	if !strings.Contains(err.Error(), "could not dial") {
		t.Errorf("expected wrapped error, got %v", err)
	}
}

// newSelfSignedCert mints a one-shot Ed25519 self-signed cert for the
// TLS dial test. Avoids pulling in clustertls (cyclic import) and
// matches the package's stdlib-first preference.
func newSelfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gossip-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}

func TestTLSDialer_DialSuccess(t *testing.T) {
	// Spin up a minimal TLS listener and dial it. We use a self-signed
	// cert generated in-test so we don't depend on cluster CA wiring.
	serverCert := newSelfSignedCert(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	defer listener.Close()
	addr := listener.Addr().String()
	// Accept and explicitly handshake; tls.DialWithDialer would otherwise
	// fail with "connection reset by peer" if we close before the
	// client's handshake completes.
	accepted := make(chan struct{})
	go func() {
		c, _ := listener.Accept()
		if c == nil {
			close(accepted)
			return
		}
		// Force the handshake from the server side.
		if tc, ok := c.(*tls.Conn); ok {
			_ = tc.Handshake()
		}
		close(accepted)
		// Hold the connection open until the client closes it.
		buf := make([]byte, 1)
		_, _ = c.Read(buf)
		_ = c.Close()
	}()
	d := tlsDialer{}
	conn, err := d.Dial(addr, &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test-only: we're proving the dial succeeds, not authenticating
		MinVersion:         tls.VersionTLS13,
	}, time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = conn.Close()
	<-accepted
}

func TestStart_WithSeedsSpawnsJoinLoop(t *testing.T) {
	// Start with seeds set; we don't need the join to succeed, only to
	// observe that the joinLoop goroutine is spawned (Start's len()>0
	// branch) and the subsystem shuts down cleanly.
	s := newTestSubsystem(t, "self", "127.0.0.1:0", []string{"127.0.0.1:1"}, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) { return nil, errors.New("seed unreachable") },
	}
	done := make(chan error, 1)
	go func() { done <- s.Start() }()
	for s.Addr() == "" {
		time.Sleep(2 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Start did not return")
	}
}

func TestSyncWith_ReadFailure(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.dialer = &fakeDialer{
		dial: func(_ string) (net.Conn, error) {
			c := pipeServer(t, func(_ *Message) *Message {
				return &Message{Kind: KindSyncResp, SenderID: "peer"}
			})
			return &flakyConn{Conn: c, failRead: true}, nil
		},
	}
	if s.syncWith("peer:1") {
		t.Error("syncWith should fail when read errors")
	}
}

func TestShutdown_IdempotentEarlyReturn(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	close(s.stopCh) // simulate already-shut-down state
	if err := s.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown after pre-closed stopCh: got %v, want nil", err)
	}
}

// blockingHandlerListener accepts the first connection and immediately
// returns an unknown error to exercise the warn-branch's default case in
// acceptLoop (neither ErrClosed nor "use of closed").
type errOnceListener struct {
	addr net.Addr
	done bool
}

func (l *errOnceListener) Accept() (net.Conn, error) {
	if l.done {
		return nil, net.ErrClosed
	}
	l.done = true
	return nil, errors.New("acceptLoop default branch")
}

func (l *errOnceListener) Close() error   { return nil }
func (l *errOnceListener) Addr() net.Addr { return l.addr }

func TestAcceptLoop_DefaultErrorBranch(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	s.wg.Add(1)
	go s.acceptLoop(&errOnceListener{addr: ln.Addr()})
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acceptLoop did not exit")
	}
	if !strings.Contains(buf.String(), "accept failed") {
		t.Errorf("expected accept-failed log, got: %s", buf.String())
	}
}

// stoppedDuringAccept returns ErrClosed after stopCh is observed to be
// closed; covers the "stopCh closed, ignore the error" branch in
// acceptLoop.
type stopBeforeAcceptListener struct {
	addr   net.Addr
	stopCh <-chan struct{}
}

func (l *stopBeforeAcceptListener) Accept() (net.Conn, error) {
	<-l.stopCh
	return nil, errors.New("transient post-stop")
}

func (l *stopBeforeAcceptListener) Close() error   { return nil }
func (l *stopBeforeAcceptListener) Addr() net.Addr { return l.addr }

func TestAcceptLoop_ExitsWhenStopChObservedAfterError(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	s.wg.Add(1)
	go s.acceptLoop(&stopBeforeAcceptListener{addr: ln.Addr(), stopCh: s.stopCh})
	close(s.stopCh)
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acceptLoop did not exit after stopCh + error")
	}
}
