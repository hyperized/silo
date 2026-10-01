package namespace

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/hlc"
)

func ts(node string, wall int64) hlc.Timestamp { return hlc.Timestamp{Wall: wall, Node: node} }

func TestObservePeerClocks(t *testing.T) {
	const self = "me"
	// Mutation timestamps from other nodes, some far in the future. None of
	// them may feed the observer: only the sender's send time does.
	inodes := []wireInode{{
		ID: "d", Type: Dir,
		ACLTS: ts("peer-b", 9_000),
		Adds: []crdt.ElementTags[Entry]{{
			Elem: Entry{Name: "f", Inode: "x"},
			Tags: []hlc.Timestamp{ts("peer-c", 99_999)},
		}},
	}}

	for _, tc := range []struct {
		name     string
		from     string
		sentAt   int64
		wantCall bool
	}{
		{name: "peer send time is reported", from: "peer-a", sentAt: 1_234, wantCall: true},
		{name: "no sender (pre-upgrade peer)", from: "", sentAt: 1_234},
		{name: "no send time (pre-upgrade peer)", from: "peer-a", sentAt: 0},
		{name: "own snapshot echoed back", from: self, sentAt: 1_234},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotNode string
			var gotWall int64
			calls := 0
			n := New(hlc.New(self), WithPeerClockObserver(func(node string, wall int64) {
				gotNode, gotWall = node, wall
				calls++
			}))

			n.observePeerClocks(wireNamespace{From: tc.from, SentAt: tc.sentAt, Inodes: inodes})

			if !tc.wantCall {
				if calls != 0 {
					t.Errorf("observer called %d times, want 0", calls)
				}
				return
			}
			if calls != 1 || gotNode != tc.from || gotWall != tc.sentAt {
				t.Errorf("observed calls=%d (%s, %d), want 1 (%s, %d)", calls, gotNode, gotWall, tc.from, tc.sentAt)
			}
		})
	}
}

func TestObservePeerClocks_GuardsNilObserverAndNilClock(t *testing.T) {
	w := wireNamespace{From: "peer", SentAt: 1}

	// No observer registered: nothing to do, must not panic.
	New(hlc.New("me")).observePeerClocks(w)

	// Observer set but the namespace has no clock (only Merge sources are
	// clock-less, and those never reach here, but the guard keeps it safe).
	called := false
	n := New(nil, WithPeerClockObserver(func(string, int64) { called = true }))
	n.observePeerClocks(w)
	if called {
		t.Error("observer fired despite a nil clock")
	}
}

func decodeWire(t *testing.T, b []byte) wireNamespace {
	t.Helper()
	var w wireNamespace
	if err := json.Unmarshal(b, &w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return w
}

func TestGossipSnapshot_StampsSenderAndSendTime(t *testing.T) {
	prev := nsTimeNow
	t.Cleanup(func() { nsTimeNow = prev })
	sent := time.Unix(1_700_000_000, 42)
	nsTimeNow = func() time.Time { return sent }

	n := New(hlc.New("me"))
	if _, err := n.Mkdir("/d"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	gossip, err := n.GossipSnapshot()
	if err != nil {
		t.Fatalf("GossipSnapshot: %v", err)
	}
	if w := decodeWire(t, gossip); w.From != "me" || w.SentAt != sent.UnixNano() {
		t.Errorf("gossip snapshot from=%q sent_at=%d, want me/%d", w.From, w.SentAt, sent.UnixNano())
	}

	// The persisted form stays free of wall-clock noise.
	persisted, err := n.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if w := decodeWire(t, persisted); w.From != "" || w.SentAt != 0 {
		t.Errorf("persisted snapshot from=%q sent_at=%d, want both empty", w.From, w.SentAt)
	}
}

func TestGossipSnapshot_ClocklessNamespaceHasNoSender(t *testing.T) {
	b, err := New(nil).GossipSnapshot()
	if err != nil {
		t.Fatalf("GossipSnapshot: %v", err)
	}
	if w := decodeWire(t, b); w.From != "" || w.SentAt != 0 {
		t.Errorf("clock-less snapshot from=%q sent_at=%d, want both empty", w.From, w.SentAt)
	}
}
