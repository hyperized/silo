package gossip

import (
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/clusterproto"
	"github.com/hyperized/silo/internal/membership"
)

func metricValue(s *Subsystem, name string) (float64, bool) {
	for _, m := range s.CollectMetrics() {
		if m.Name == name {
			return m.Value, true
		}
	}
	return 0, false
}

func TestSelfEnvelope_StampsProtocol(t *testing.T) {
	s := newTestSubsystem(t, "alpha", "alpha:7100", nil, discardLogger())
	env := s.selfEnvelope(KindPing, "beta")
	if env.SenderProto != clusterproto.Protocol {
		t.Errorf("SenderProto = %d, want %d", env.SenderProto, clusterproto.Protocol)
	}
}

func TestApplyIncoming_CompatiblePeerMerges(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	msg := &Message{
		Kind:         KindPing,
		SenderID:     "peer-a",
		SenderProto:  clusterproto.Protocol,
		SenderIncarn: 1,
	}
	if changed := s.applyIncoming(msg); len(changed) != 1 {
		t.Fatalf("a compatible peer should merge, changed = %d", len(changed))
	}
	if v, _ := metricValue(s, "incompatible_messages_total"); v != 0 {
		t.Errorf("incompatible_messages_total = %v, want 0", v)
	}
	if v, _ := metricValue(s, "newer_protocol_messages_total"); v != 0 {
		t.Errorf("newer_protocol_messages_total = %v, want 0", v)
	}
}

func TestApplyIncoming_UnversionedPeerTreatedAsV1(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	// A pre-handshake build sends no sender_proto (0); it must still merge.
	msg := &Message{Kind: KindPing, SenderID: "legacy", SenderIncarn: 1}
	if changed := s.applyIncoming(msg); len(changed) != 1 {
		t.Fatalf("an unversioned peer should merge as v1, changed = %d", len(changed))
	}
}

func TestApplyIncoming_NewerPeerMergesAndFlags(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)
	msg := &Message{
		Kind:         KindPing,
		SenderID:     "ahead",
		SenderProto:  clusterproto.Protocol + 1,
		SenderIncarn: 1,
	}
	// A newer peer is still merged (best-effort forward compatibility)…
	if changed := s.applyIncoming(msg); len(changed) != 1 {
		t.Fatalf("a newer peer should still merge, changed = %d", len(changed))
	}
	// …but flagged so the operator upgrades this node.
	if v, _ := metricValue(s, "newer_protocol_messages_total"); v != 1 {
		t.Errorf("newer_protocol_messages_total = %v, want 1", v)
	}
	if !strings.Contains(buf.String(), "newer protocol") {
		t.Errorf("expected a newer-protocol warning, got: %s", buf.String())
	}
}

func TestApplyIncoming_TooOldPeerIsFenced(t *testing.T) {
	logger, buf := bufLogger(t)
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, logger)

	// Force the fenced classification (unreachable from real input until the
	// first MinCompatible bump).
	prev := classifyProto
	classifyProto = func(uint32) clusterproto.Compatibility { return clusterproto.PeerTooOld }
	t.Cleanup(func() { classifyProto = prev })

	msg := &Message{
		Kind:         KindPing,
		SenderID:     "ancient",
		SenderProto:  1,
		SenderIncarn: 1,
		Piggyback: []membership.Event{
			{ID: "third-party", Address: "tp:7100", State: membership.StateAlive, Incarnation: 1},
		},
	}
	if changed := s.applyIncoming(msg); changed != nil {
		t.Fatalf("a too-old peer should be fenced (no merge), got %+v", changed)
	}
	// Neither the sender nor its piggyback should have entered the table.
	if _, ok := s.members.Lookup("ancient"); ok {
		t.Error("the fenced sender must not be merged")
	}
	if _, ok := s.members.Lookup("third-party"); ok {
		t.Error("a fenced peer's piggyback must not be merged")
	}
	if v, _ := metricValue(s, "incompatible_messages_total"); v != 1 {
		t.Errorf("incompatible_messages_total = %v, want 1", v)
	}
	if !strings.Contains(buf.String(), "unsupported protocol") {
		t.Errorf("expected an unsupported-protocol error, got: %s", buf.String())
	}
}

func TestCollectMetrics_ReportsProtocolVersion(t *testing.T) {
	s := newTestSubsystem(t, "self", "127.0.0.1:0", nil, discardLogger())
	v, ok := metricValue(s, "protocol_version")
	if !ok || v != float64(clusterproto.Protocol) {
		t.Errorf("protocol_version = %v (found %v), want %d", v, ok, clusterproto.Protocol)
	}
}
