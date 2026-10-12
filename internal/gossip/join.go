package gossip

import (
	"math/rand/v2"
	"time"

	"github.com/hyperized/silo/internal/membership"
)

// joinInitialBackoff is the first delay between failed seed rounds and
// joinMaxBackoff caps the doubling. Vars rather than consts so unit
// tests can shrink them instead of sleeping for real seconds.
var (
	joinInitialBackoff = 250 * time.Millisecond
	joinMaxBackoff     = 5 * time.Second
)

// joinState is the join loop's private bookkeeping. Only the loop
// goroutine touches it, so it needs no lock.
type joinState struct {
	backoff time.Duration
	// joined flips once any seed has answered. Before that the node is
	// booting and its seed dials are not counted as rejoin attempts.
	joined bool
	// warned is true once the current isolated episode has been logged
	// at Warn, so a long outage produces one line instead of one per
	// round.
	warned bool
}

// joinLoop keeps this node connected to the cluster for the life of the
// subsystem. Startup and recovery share it: whenever the node sees fewer
// than MinPeers Alive peers it dials its seeds, backing off while none
// answer. Success is judged by re-reading the membership table, not by
// a seed answering. A peer that still holds us Dead ignores our Alive
// claim until our incarnation is bumped past its view, so one answered
// sync does not by itself mean we are back.
func (s *Subsystem) joinLoop() {
	defer s.wg.Done()
	st := &joinState{backoff: joinInitialBackoff}
	for {
		wait := s.joinStep(st)
		select {
		case <-s.stopCh:
			return
		case <-time.After(wait):
		}
	}
}

// joinStep runs one evaluation of the join loop and returns how long to
// wait before the next.
func (s *Subsystem) joinStep(st *joinState) time.Duration {
	alive := s.members.AlivePeers()
	if len(alive) >= s.opts.MinPeers {
		s.isolated.Store(false)
		st.warned = false
		st.backoff = joinInitialBackoff
		return s.opts.ProbeInterval
	}
	s.isolated.Store(true)
	if s.tryJoinSeeds(alive, st.joined) {
		st.joined = true
		st.backoff = joinInitialBackoff
		return st.backoff
	}
	if !st.warned {
		st.warned = true
		s.logger.Warn("gossip has fewer alive peers than it needs; retrying seeds until one answers. Check that SILO_SEEDS lists the other nodes' gossip addresses and that their gossip ports are reachable from this node",
			"alive_peers", len(alive), "min_peers", s.opts.MinPeers, "seeds", s.opts.Seeds)
	}
	wait := st.backoff
	st.backoff = min(st.backoff*2, joinMaxBackoff)
	return wait
}

// tryJoinSeeds dials each seed once and returns true as soon as one
// answers with a SyncResp. Seeds already in alive are skipped, so a node
// that is only short of a full replica set does not keep hammering the
// peers it already has. rejoin marks the dials as recovery rather than
// startup. Failed seeds log at Debug; the caller owns the one Warn per
// episode.
func (s *Subsystem) tryJoinSeeds(alive []membership.Node, rejoin bool) bool {
	have := make(map[string]struct{}, len(alive))
	for _, n := range alive {
		have[n.Address] = struct{}{}
	}
	for _, addr := range s.opts.Seeds {
		if _, ok := have[addr]; ok || addr == "" {
			continue
		}
		if rejoin {
			s.rejoinAttempts.Add(1)
		}
		if s.syncWith(addr) {
			msg := "gossip joined via seed"
			if rejoin {
				msg = "gossip rejoined via seed"
			}
			s.logger.Info(msg, "seed", addr, "alive_peers", len(s.members.AlivePeers()))
			return true
		}
		s.logger.Debug("gossip seed did not answer; will retry", "seed", addr)
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
		s.syncSendFailures.Add(1)
		s.logger.Warn("gossip: could not send the anti-entropy sync request; this peer will not converge with our state (if the extension exceeds the per-message cap, our whole namespace is stranded)", "peer", addr, "error", err)
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
	s.lastSync.Store(timeNow().UnixNano())
	return true
}
