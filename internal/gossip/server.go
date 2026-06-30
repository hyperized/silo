package gossip

import (
	"errors"
	"net"
	"strings"

	"github.com/hyperized/silo/internal/membership"
)

// acceptLoop services inbound gossip connections. Every connection is
// one message (per the wire-protocol design); we hand each off to a
// short-lived goroutine so a slow peer can't stall the accept side.
func (s *Subsystem) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			// A closed listener is the expected shutdown path; other
			// errors surface to the operator log so transient network
			// faults are observable.
			if errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case <-s.stopCh:
				return
			default:
			}
			// Ignore "use of closed network connection"-shaped strings
			// that some Go versions return without ErrClosed.
			if strings.Contains(err.Error(), "use of closed") {
				return
			}
			s.logger.Warn("gossip accept failed", "err", err)
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() { _ = c.Close() }()
			s.handleConn(c)
		}(conn)
	}
}

// handleConn dispatches one inbound gossip message and writes the
// appropriate reply. Per-connection deadlines bound the handler so a
// hostile or stuck peer cannot tie up a goroutine forever.
func (s *Subsystem) handleConn(c net.Conn) {
	if err := applyTimeouts(c, s.opts.Timeout); err != nil {
		return
	}
	msg, err := readMessage(c)
	if err != nil {
		s.logger.Debug("gossip read failed", "remote", c.RemoteAddr().String(), "err", err)
		return
	}
	s.applyIncoming(msg)
	switch msg.Kind {
	case KindPing:
		_ = writeMessage(c, s.selfEnvelope(KindAck, msg.SenderID))
	case KindPingReq:
		// Helper path: try to ping the named target, reply Ack only on
		// success. Failure leaves the requester to fall through to its
		// own suspicion timer.
		if msg.Target == "" {
			return
		}
		if target, ok := s.members.Lookup(msg.Target); ok && target.Address != "" {
			if s.probeDirect(msg.Target, target.Address) {
				_ = writeMessage(c, s.selfEnvelope(KindAck, msg.SenderID))
			}
		}
	case KindSyncReq:
		// Anti-entropy: reply with our full view so the initiator
		// reconciles. We also merge the initiator's MembershipView
		// (carried in the SyncReq) before computing our reply, so the
		// reply already reflects what the initiator told us. The
		// applyIncoming call above already handled msg.Piggyback +
		// msg.SenderID; here we additionally apply the larger
		// MembershipView so every newly-learned peer surfaces as a
		// single state-change log line.
		if len(msg.MembershipView) > 0 {
			extra := &Message{Piggyback: msg.MembershipView}
			s.applyIncoming(extra)
		}
		// Merge the initiator's extension state before computing our reply
		// so the reply already reflects it, letting the initiator converge
		// in this single round-trip.
		s.extMergeRemote(msg.Extension)
		reply := s.selfEnvelope(KindSyncResp, msg.SenderID)
		reply.MembershipView = s.viewSnapshot()
		reply.Extension = s.extLocalState()
		if err := writeMessage(c, reply); err != nil {
			s.syncSendFailures.Add(1)
			s.logger.Warn("gossip: could not send the anti-entropy sync reply; the initiator will not converge with our state (if the extension exceeds the per-message cap, our whole namespace is stranded)", "peer", msg.SenderID, "error", err)
		}
	case KindAck, KindSyncResp:
		// Stray ack or sync-resp on an inbound connection — these are
		// only valid as replies to our own outbound dials. Ignore but
		// don't disconnect; piggyback was still useful.
	default:
		s.logger.Debug("gossip unknown message kind", "kind", string(msg.Kind))
	}
}

// viewSnapshot turns the membership table into a slice of Events. Used
// by SyncResp and the initial join handshake.
func (s *Subsystem) viewSnapshot() []membership.Event {
	members := s.members.Members()
	out := make([]membership.Event, 0, len(members))
	for _, n := range members {
		out = append(out, membership.Event{
			ID:            n.ID,
			Address:       n.Address,
			DataAddress:   n.DataAddress,
			State:         n.State,
			Incarnation:   n.Incarnation,
			At:            n.LastChange,
			CapacityBytes: n.CapacityBytes,
			UsedBytes:     n.UsedBytes,
			Pressured:     n.Pressured,
		})
	}
	return out
}
