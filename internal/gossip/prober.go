package gossip

import (
	"sync"
	"time"

	"github.com/hyperized/silo/internal/membership"
)

// suspectTracker tracks when each Suspect entry began so the prober
// can promote it to Dead after SuspectTimeout. We keep this separate
// from the membership table because the table doesn't know about
// SuspectTimeout — it just stores the state machine. The tracker is
// internal to the prober goroutine plus the Apply callback, so it
// needs its own mutex.
type suspectTracker struct {
	mu     sync.Mutex
	timers map[string]time.Time
}

func newSuspectTracker() *suspectTracker {
	return &suspectTracker{timers: make(map[string]time.Time)}
}

func (s *suspectTracker) start(id string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.timers[id]; ok {
		return
	}
	s.timers[id] = now
}

func (s *suspectTracker) clear(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.timers, id)
}

// expired returns ids whose Suspect timer started before cutoff.
func (s *suspectTracker) expired(cutoff time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for id, t := range s.timers {
		if t.Before(cutoff) {
			out = append(out, id)
		}
	}
	return out
}

// probeLoop is the SWIM probe driver. One tick: pick one Alive peer
// (skipping self), Ping it directly, and on no Ack within ProbeTimeout
// fan out to k Alive helpers asking them to PingReq the target. If
// neither path succeeds, mark Suspect; promote to Dead after
// SuspectTimeout elapses without a refutation.
func (s *Subsystem) probeLoop() {
	defer s.wg.Done()
	tracker := newSuspectTracker()
	ticker := time.NewTicker(s.opts.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.runProbeTick(tracker)
		}
	}
}

// runProbeTick does one round of probe + suspect-expiry. Pulled out so
// the unit test can drive a tick at a time without sleeping.
func (s *Subsystem) runProbeTick(tracker *suspectTracker) {
	// Promote Suspect entries whose timer has elapsed before probing
	// — otherwise a single probe could re-suspect them and reset the
	// timer indefinitely.
	cutoff := time.Now().Add(-s.opts.SuspectTimeout)
	for _, id := range tracker.expired(cutoff) {
		if ev, ok := s.members.MarkDead(id); ok {
			s.logger.Info("gossip state change", "node", id, "state", ev.State.String(), "incarnation", ev.Incarnation)
			s.remember(ev)
		}
		tracker.clear(id)
	}
	// Prune Dead entries that have outlived the retention window.
	for _, id := range s.members.Prune(s.opts.DeadRetention) {
		s.logger.Info("gossip member pruned", "node", id)
	}
	id, addr, ok := s.pickAlivePeer()
	if !ok {
		return
	}
	if id == s.members.SelfID() || addr == "" {
		return
	}
	if s.probeDirect(id, addr) {
		// Successful probe: clear any stale Suspect timer for this id.
		tracker.clear(id)
		return
	}
	if s.probeIndirect(id, addr) {
		tracker.clear(id)
		return
	}
	if ev, marked := s.members.MarkSuspect(id); marked {
		s.logger.Info("gossip state change", "node", id, "state", ev.State.String(), "incarnation", ev.Incarnation)
		s.remember(ev)
		tracker.start(id, time.Now())
	}
}

// probeDirect Pings target directly and returns true on a successful
// Ack. Failures are absorbed silently — the SWIM contract is that the
// caller falls through to indirect probes and ultimately to suspicion.
func (s *Subsystem) probeDirect(target, addr string) bool {
	conn, err := s.dialer.Dial(addr, s.opts.ClientTLS, s.opts.ProbeTimeout)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	if err := applyTimeouts(conn, s.opts.ProbeTimeout); err != nil {
		return false
	}
	if err := writeMessage(conn, s.selfEnvelope(KindPing, target)); err != nil {
		return false
	}
	resp, err := readMessage(conn)
	if err != nil {
		return false
	}
	if resp.Kind != KindAck {
		return false
	}
	s.applyIncoming(resp)
	return true
}

// probeIndirect picks up to IndirectK helpers and asks each to ping
// target on our behalf. Any one Ack from a helper counts as success.
// Helpers themselves do the dial; we just collect their replies.
func (s *Subsystem) probeIndirect(target, _ string) bool {
	helpers := s.pickHelpers(target, s.opts.IndirectK)
	if len(helpers) == 0 {
		return false
	}
	type result struct {
		ok  bool
		msg *Message
	}
	results := make(chan result, len(helpers))
	for _, h := range helpers {
		go func(helper membership.Node) {
			conn, err := s.dialer.Dial(helper.Address, s.opts.ClientTLS, s.opts.ProbeTimeout)
			if err != nil {
				results <- result{}
				return
			}
			defer func() { _ = conn.Close() }()
			if err := applyTimeouts(conn, s.opts.ProbeTimeout+s.opts.Timeout); err != nil {
				results <- result{}
				return
			}
			env := s.selfEnvelope(KindPingReq, target)
			if err := writeMessage(conn, env); err != nil {
				results <- result{}
				return
			}
			resp, err := readMessage(conn)
			if err != nil {
				results <- result{}
				return
			}
			results <- result{ok: resp.Kind == KindAck, msg: resp}
		}(h)
	}
	var success bool
	for range helpers {
		r := <-results
		if r.ok && r.msg != nil {
			s.applyIncoming(r.msg)
			success = true
		}
	}
	return success
}
