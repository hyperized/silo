package gossip

import (
	"net"
	"testing"

	"github.com/hyperized/silo/internal/membership"
)

// deliver runs one framed message from 'from' through to's inbound handler and
// returns the reply, the same path a real probe or sync takes minus TLS.
func deliver(t *testing.T, to *Subsystem, msg *Message) *Message {
	t.Helper()
	server, client := net.Pipe()
	defer client.Close()
	go func() {
		defer server.Close()
		to.handleConn(server)
	}()
	if err := writeMessage(client, msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	reply, err := readMessage(client)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return reply
}

// TestCapacity_ProbeBeforeSyncStillPropagates is the regression for peers
// showing zero capacity forever. The owner's capacity advert bumps its
// incarnation; the next probe carried only that incarnation, so the peer
// recorded the new incarnation with no figures and then ignored the sync
// snapshot that did carry them, because it arrived at an equal incarnation.
func TestCapacity_ProbeBeforeSyncStillPropagates(t *testing.T) {
	a := newTestSubsystem(t, "a", "a:1", nil, discardLogger())
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	a.members.SetSelfCapacity(1000, 250, true)

	deliver(t, b, a.selfEnvelope(KindPing, "b"))
	req := a.selfEnvelope(KindSyncReq, "b")
	req.MembershipView = a.viewSnapshot()
	deliver(t, b, req)

	got, ok := b.members.Lookup("a")
	if !ok {
		t.Fatal("b does not know a")
	}
	if got.CapacityBytes != 1000 || got.UsedBytes != 250 || !got.Pressured {
		t.Errorf("b's view of a: capacity=%d used=%d pressured=%v, want 1000/250/true", got.CapacityBytes, got.UsedBytes, got.Pressured)
	}
}

// TestCapacity_ProbeAloneCarriesAdvert checks the sender envelope itself
// carries the figures, so one probe is enough without waiting for a sync.
func TestCapacity_ProbeAloneCarriesAdvert(t *testing.T) {
	a := newTestSubsystem(t, "a", "a:1", nil, discardLogger())
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	b.members.SetSelfCapacity(2000, 500, false)

	ack := deliver(t, b, a.selfEnvelope(KindPing, "b"))
	a.applyIncoming(ack)

	got, _ := a.members.Lookup("b")
	if got.CapacityBytes != 2000 || got.UsedBytes != 500 {
		t.Errorf("a's view of b after one ack: capacity=%d used=%d, want 2000/500", got.CapacityBytes, got.UsedBytes)
	}
}

// TestCapacity_RebroadcastCarriesAdvert makes sure a node relaying news about a
// peer passes the figures on, so a third node learns them second-hand.
func TestCapacity_RebroadcastCarriesAdvert(t *testing.T) {
	b := newTestSubsystem(t, "b", "b:1", nil, discardLogger())
	b.applyIncoming(&Message{Kind: KindPing, Piggyback: []membership.Event{
		{ID: "a", Address: "a:1", State: membership.StateAlive, Incarnation: 3, CapacityBytes: 1000, UsedBytes: 100, Pressured: true},
	}})
	for _, ev := range b.piggybackSnapshot() {
		if ev.ID != "a" {
			continue
		}
		if ev.CapacityBytes != 1000 || ev.UsedBytes != 100 || !ev.Pressured {
			t.Errorf("rebroadcast event lost the advert: %+v", ev)
		}
		return
	}
	t.Fatal("no rebroadcast event for a")
}

func TestHealthAccessors(t *testing.T) {
	s := newTestSubsystem(t, "a", "a:1", nil, discardLogger())
	if s.Isolated() || s.RejoinAttempts() != 0 {
		t.Fatalf("fresh subsystem: isolated=%v attempts=%d", s.Isolated(), s.RejoinAttempts())
	}
	s.isolated.Store(true)
	s.rejoinAttempts.Add(3)
	if !s.Isolated() || s.RejoinAttempts() != 3 {
		t.Errorf("after setting: isolated=%v attempts=%d, want true/3", s.Isolated(), s.RejoinAttempts())
	}
}
