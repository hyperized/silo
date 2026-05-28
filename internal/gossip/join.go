package gossip

import (
	"math/rand/v2"
	"time"
)

// joinInitialBackoff is the first delay between failed seed attempts.
// Pulled out as a var so unit tests can shrink the back-off and exercise
// the cap branch without sleeping for the production five seconds.
var (
	joinInitialBackoff = 250 * time.Millisecond
	joinMaxBackoff     = 5 * time.Second
)

// joinLoop drives the initial seed join. We try each configured seed
// in turn until one Acks; if none do, we back off and retry. A
// successful join is "we exchanged at least one SyncReq/SyncResp pair
// with a seed" — after that, anti-entropy and probing keep us
// converging on the rest of the cluster.
func (s *Subsystem) joinLoop() {
	defer s.wg.Done()
	// Initial best-effort attempt: most clusters succeed on the very
	// first try (the seed is healthy). The back-off path covers the
	// slow-cold-start case (docker-compose, where silo-a is still
	// minting its CA when silo-b boots).
	backoff := joinInitialBackoff
	for {
		if s.tryJoinSeeds() {
			return
		}
		select {
		case <-s.stopCh:
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > joinMaxBackoff {
				backoff = joinMaxBackoff
			}
		}
	}
}

// tryJoinSeeds returns true once any one seed has answered with a
// SyncResp. Failed seeds are logged at debug; otherwise compose logs
// would shout one warning per seed per retry, which is noise.
func (s *Subsystem) tryJoinSeeds() bool {
	for _, addr := range s.opts.Seeds {
		if addr == "" {
			continue
		}
		if s.syncWith(addr) {
			s.logger.Info("gossip joined via seed", "seed", addr)
			return true
		}
		s.logger.Debug("gossip seed not yet reachable", "seed", addr)
	}
	return false
}

// antiEntropyLoop runs the slow background reconciliation: every
// AntiEntropyInterval, pick one random known peer and exchange full
// membership views. Anti-entropy is the safety net for events that
// fell off the piggyback bus (the ring is bounded; very old events
// are forgotten).
func (s *Subsystem) antiEntropyLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opts.AntiEntropyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			peers := s.members.AlivePeers()
			if len(peers) == 0 {
				continue
			}
			peer := peers[rand.IntN(len(peers))] //nolint:gosec // anti-entropy peer pick, not cryptographic
			if peer.Address == "" {
				continue
			}
			s.syncWith(peer.Address)
		}
	}
}

// syncWith opens one SyncReq/SyncResp round-trip against addr. The
// request carries our full view; the response carries the peer's.
// Returns true on a successful exchange.
func (s *Subsystem) syncWith(addr string) bool {
	conn, err := s.dialer.Dial(addr, s.opts.ClientTLS, s.opts.Timeout)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	if err := applyTimeouts(conn, s.opts.Timeout); err != nil {
		return false
	}
	req := s.selfEnvelope(KindSyncReq, "")
	req.MembershipView = s.viewSnapshot()
	req.Extension = s.extLocalState()
	if err := writeMessage(conn, req); err != nil {
		return false
	}
	resp, err := readMessage(conn)
	if err != nil {
		return false
	}
	if resp.Kind != KindSyncResp {
		return false
	}
	s.extMergeRemote(resp.Extension)
	// applyIncoming logs and re-broadcasts state changes; we want full
	// MembershipView contents to flow through it so each newly-learned
	// peer surfaces as a single "alive" log line on first encounter.
	// Stuff the membership view into Piggyback for the merge so the
	// existing logging path catches every entry.
	resp.Piggyback = append(resp.Piggyback, resp.MembershipView...)
	s.applyIncoming(resp)
	return true
}
