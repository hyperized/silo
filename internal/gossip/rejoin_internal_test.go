package gossip

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/membership"
)

// lockedBuf lets a test read log output while a loop goroutine is still
// writing to it without tripping the race detector.
type lockedBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func lockedLogger() (*slog.Logger, *lockedBuf) {
	b := &lockedBuf{}
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})), b
}

func shrinkJoinBackoff(t *testing.T, initial, maxWait time.Duration) {
	t.Helper()
	prevInit, prevMax := joinInitialBackoff, joinMaxBackoff
	t.Cleanup(func() { joinInitialBackoff, joinMaxBackoff = prevInit, prevMax })
	joinInitialBackoff, joinMaxBackoff = initial, maxWait
}

// seedServer answers a SyncReq the way a real peer does: if the
// requester's view holds the seed as anything worse than Alive, the seed
// refutes by bumping its incarnation past that view before replying.
func seedServer(t *testing.T, id, addr string, incarn *atomic.Uint64) net.Conn {
	t.Helper()
	return pipeServer(t, func(req *Message) *Message {
		for _, ev := range req.MembershipView {
			if ev.ID == id && ev.State != membership.StateAlive && ev.Incarnation >= incarn.Load() {
				incarn.Store(ev.Incarnation + 1)
			}
		}
		return &Message{Kind: KindSyncResp, SenderID: id, SenderAddress: addr, SenderIncarn: incarn.Load()}
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// The scenario from the field: a node joins, loses every peer, keeps
// retrying its seed while the seed is unreachable, gets back in once the
// seed answers, and then goes quiet instead of dialling forever.
func TestJoinLoop_RejoinsAfterIsolationThenGoesQuiet(t *testing.T) {
	shrinkJoinBackoff(t, time.Millisecond, 2*time.Millisecond)
	logger, buf := lockedLogger()
	s := newTestSubsystem(t, "self", "self:1", []string{"seed:1"}, logger)
	var incarn atomic.Uint64
	incarn.Store(1)
	var down atomic.Bool
	var dials atomic.Int64
	s.dialer = &fakeDialer{dial: func(_ string) (net.Conn, error) {
		dials.Add(1)
		if down.Load() {
			return nil, errors.New("seed unreachable")
		}
		return seedServer(t, "seed", "seed:1", &incarn), nil
	}}

	s.wg.Add(1)
	go s.joinLoop()
	t.Cleanup(func() {
		close(s.stopCh)
		s.wg.Wait()
	})

	waitFor(t, "startup join", func() bool { return len(s.members.AlivePeers()) == 1 && !s.isolated.Load() })
	if got := s.rejoinAttempts.Load(); got != 0 {
		t.Errorf("startup join counted as %d rejoin attempts, want 0", got)
	}

	down.Store(true)
	s.members.MarkDead("seed")
	waitFor(t, "several failed rejoin rounds", func() bool { return s.rejoinAttempts.Load() >= 3 })
	if !s.isolated.Load() {
		t.Error("isolated should be set while no peer is alive")
	}

	down.Store(false)
	waitFor(t, "rejoin", func() bool { return len(s.members.AlivePeers()) == 1 && !s.isolated.Load() })
	if n, _ := s.members.Lookup("seed"); n.Incarnation < 2 {
		t.Errorf("seed incarnation %d: rejoin should have needed a refutation", n.Incarnation)
	}

	settled := dials.Load()
	time.Sleep(20 * time.Millisecond)
	if got := dials.Load(); got != settled {
		t.Errorf("loop kept dialling after rejoin: %d -> %d", settled, got)
	}

	out := buf.String()
	if n := strings.Count(out, "retrying seeds until one answers"); n != 1 {
		t.Errorf("isolation warning logged %d times, want once per episode:\n%s", n, out)
	}
	for _, want := range []string{"gossip joined via seed", "gossip rejoined via seed", "SILO_SEEDS"} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
}

func TestJoinStep_MinPeersSkipsAlreadyAliveSeeds(t *testing.T) {
	shrinkJoinBackoff(t, 2*time.Millisecond, 5*time.Millisecond)
	s := newTestSubsystem(t, "self", "self:1", []string{"", "a:1", "b:1"}, discardLogger())
	s.opts.MinPeers = 2
	s.members.Apply(membership.Event{ID: "a", Address: "a:1", State: membership.StateAlive, Incarnation: 1})

	var bUp atomic.Bool
	var incarn atomic.Uint64
	var mu sync.Mutex
	var dialed []string
	s.dialer = &fakeDialer{dial: func(addr string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, addr)
		mu.Unlock()
		if !bUp.Load() {
			return nil, errors.New("down")
		}
		return seedServer(t, "b", "b:1", &incarn), nil
	}}

	st := &joinState{backoff: joinInitialBackoff}
	if got := s.joinStep(st); got != 2*time.Millisecond {
		t.Errorf("first failed round waited %s, want 2ms", got)
	}
	if !s.isolated.Load() {
		t.Error("one alive peer with MinPeers=2 should count as isolated")
	}
	bUp.Store(true)
	if got := s.joinStep(st); got != joinInitialBackoff {
		t.Errorf("successful round waited %s, want the initial backoff", got)
	}
	if got := s.joinStep(st); got != s.opts.ProbeInterval {
		t.Errorf("full replica set waited %s, want ProbeInterval", got)
	}
	if s.isolated.Load() {
		t.Error("isolated should clear once MinPeers are alive")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, addr := range dialed {
		if addr != "b:1" {
			t.Errorf("dialled %q; only the missing seed b:1 should be tried", addr)
		}
	}
}

func TestJoinStep_BackoffDoublesToCapAndWarnsOnce(t *testing.T) {
	shrinkJoinBackoff(t, 2*time.Millisecond, 5*time.Millisecond)
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "self:1", []string{"seed:1"}, logger)
	s.dialer = &fakeDialer{dial: func(_ string) (net.Conn, error) { return nil, errors.New("down") }}

	st := &joinState{backoff: joinInitialBackoff, joined: true}
	for i, want := range []time.Duration{2, 4, 5, 5} {
		if got := s.joinStep(st); got != want*time.Millisecond {
			t.Errorf("round %d waited %s, want %dms", i, got, want)
		}
	}
	if got := s.rejoinAttempts.Load(); got != 4 {
		t.Errorf("rejoinAttempts = %d, want 4", got)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Errorf("got %d warnings, want 1", n)
	}

	// A peer reappearing ends the episode: backoff and warning reset so
	// the next outage is reported again.
	s.members.Apply(membership.Event{ID: "p", Address: "p:1", State: membership.StateAlive, Incarnation: 1})
	s.joinStep(st)
	if st.warned || st.backoff != joinInitialBackoff {
		t.Errorf("episode state not reset: %+v", st)
	}
}

func TestStart_WithoutSeedsSaysRejoinIsImpossible(t *testing.T) {
	logger, buf := lockedLogger()
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)
	done := make(chan error, 1)
	go func() { done <- s.Start() }()
	waitFor(t, "listener bind", func() bool { return s.Addr() != "" })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	<-done
	if !strings.Contains(buf.String(), "Set SILO_SEEDS") {
		t.Errorf("expected a no-seeds hint, got: %s", buf.String())
	}
}

func TestWithDefaults_MinPeers(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int
		want int
	}{
		{"zero means one", 0, 1},
		{"negative means one", -3, 1},
		{"explicit kept", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := withDefaults(Options{MinPeers: tc.in}).MinPeers; got != tc.want {
				t.Errorf("MinPeers = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestForget_DropsOnlyNamedIDs(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	s.remember(
		membership.Event{ID: "a", Incarnation: 1},
		membership.Event{ID: "b", Incarnation: 1},
		membership.Event{ID: "a", Incarnation: 2},
	)
	s.forget()
	if got := len(s.piggybackSnapshot()); got != 3 {
		t.Fatalf("forget() with no ids changed the ring: %d events", got)
	}
	s.forget("a")
	snap := s.piggybackSnapshot()
	if len(snap) != 1 || snap[0].ID != "b" {
		t.Errorf("after forget(a): %+v", snap)
	}
}

func TestRunProbeTick_PruneForgetsRememberedEvents(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	s.members.Apply(membership.Event{ID: "gone", State: membership.StateAlive, Incarnation: 1})
	ev, _ := s.members.MarkDead("gone")
	s.remember(membership.Event{ID: "gone", State: membership.StateAlive, Incarnation: 1}, ev,
		membership.Event{ID: "other", State: membership.StateAlive, Incarnation: 1})

	prev := membership.Now
	t.Cleanup(func() { membership.Now = prev })
	membership.Now = func() time.Time { return time.Now().Add(time.Hour) }
	s.runProbeTick(newSuspectTracker())

	if _, ok := s.members.Lookup("gone"); ok {
		t.Fatal("gone should have been pruned")
	}
	for _, e := range s.piggybackSnapshot() {
		if e.ID == "gone" {
			t.Errorf("ring still carries %+v after prune", e)
		}
	}
}

func TestPiggybackSnapshot_ExpiresOldEvents(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	base := time.Unix(1_000_000, 0)
	prev := timeNow
	t.Cleanup(func() { timeNow = prev })
	timeNow = func() time.Time { return base }

	s.remember(
		membership.Event{ID: "stale", At: base.Add(-s.opts.DeadRetention - time.Second)},
		membership.Event{ID: "fresh", At: base.Add(-time.Millisecond)},
		membership.Event{ID: "unstamped"},
	)
	snap := s.piggybackSnapshot()
	ids := make([]string, 0, len(snap))
	for _, e := range snap {
		ids = append(ids, e.ID)
	}
	if strings.Join(ids, ",") != "fresh,unstamped" {
		t.Errorf("snapshot ids = %v, want [fresh unstamped]", ids)
	}

	timeNow = func() time.Time { return base.Add(time.Hour) }
	if snap := s.piggybackSnapshot(); len(snap) != 1 || snap[0].ID != "unstamped" {
		t.Errorf("after an hour: %+v, want only the unstamped event", snap)
	}
}

func TestCollectMetrics_RejoinSeries(t *testing.T) {
	s := newTestSubsystem(t, "self", "self:1", nil, discardLogger())
	read := func() map[string]float64 {
		got := map[string]float64{}
		for _, m := range s.CollectMetrics() {
			got[m.Name] = m.Value
		}
		return got
	}
	if got := read(); got["isolated"] != 0 || got["rejoin_attempts_total"] != 0 {
		t.Errorf("fresh subsystem: %v", got)
	}
	s.isolated.Store(true)
	s.rejoinAttempts.Store(4)
	if got := read(); got["isolated"] != 1 || got["rejoin_attempts_total"] != 4 {
		t.Errorf("isolated subsystem: isolated=%v rejoin_attempts_total=%v", got["isolated"], got["rejoin_attempts_total"])
	}
}
