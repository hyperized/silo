package namespace_test

import (
	"testing"
	"time"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

type observed struct {
	calls int
	node  string
	wall  int64
}

func (o *observed) observe(node string, wall int64) {
	o.calls++
	o.node, o.wall = node, wall
}

// An idle cluster must not look skewed. The peer's only write happened an hour
// ago; the skew reading has to follow the gossip round, not that write. Before
// the fix the observer got the write's timestamp, so the gauge fell by one
// second per second of idle time.
func TestNamespace_MergeBytesReportsSendTimeNotLastWrite(t *testing.T) {
	lastWrite := time.Now().Add(-time.Hour)
	peer := namespace.New(hlc.New("peer", hlc.WithNow(func() time.Time { return lastWrite })))
	if _, err := peer.Mkdir("/d"); err != nil {
		t.Fatalf("peer Mkdir: %v", err)
	}

	var o observed
	n := namespace.New(hlc.New("me"), namespace.WithPeerClockObserver(o.observe))
	for round := range 2 {
		before := time.Now()
		state, err := peer.GossipSnapshot()
		if err != nil {
			t.Fatalf("GossipSnapshot: %v", err)
		}
		if err := n.MergeBytes(state); err != nil {
			t.Fatalf("MergeBytes: %v", err)
		}
		after := time.Now()
		if o.calls != round+1 || o.node != "peer" {
			t.Fatalf("round %d: observer calls=%d node=%q, want %d/peer", round, o.calls, o.node, round+1)
		}
		if o.wall < before.UnixNano() || o.wall > after.UnixNano() {
			t.Errorf("round %d: observed wall %v outside the gossip round [%v, %v]; last write was %v",
				round, time.Unix(0, o.wall), before, after, lastWrite)
		}
	}
}

func TestNamespace_MergeBytesPersistedSnapshotNotObserved(t *testing.T) {
	peer := namespace.New(hlc.New("peer"))
	if _, err := peer.Mkdir("/d"); err != nil {
		t.Fatalf("peer Mkdir: %v", err)
	}
	state, err := peer.Snapshot() // persisted/backup form carries no send time
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var o observed
	n := namespace.New(hlc.New("me"), namespace.WithPeerClockObserver(o.observe))
	if err := n.MergeBytes(state); err != nil {
		t.Fatalf("MergeBytes: %v", err)
	}
	if o.calls != 0 {
		t.Errorf("observer called %d times for a snapshot without a send time", o.calls)
	}
	if entries, err := n.List("/"); err != nil || len(entries) != 1 {
		t.Errorf("merge did not apply: entries=%v err=%v", entries, err)
	}
}

func TestNamespace_MergeBytesWithoutObserver(t *testing.T) {
	peer := namespace.New(hlc.New("peer"))
	if _, err := peer.Mkdir("/d"); err != nil {
		t.Fatalf("peer Mkdir: %v", err)
	}
	state, err := peer.GossipSnapshot()
	if err != nil {
		t.Fatalf("GossipSnapshot: %v", err)
	}
	// No observer registered: MergeBytes still converges, just silently.
	n := namespace.New(hlc.New("me"))
	if err := n.MergeBytes(state); err != nil {
		t.Fatalf("MergeBytes: %v", err)
	}
	if entries, err := n.List("/"); err != nil || len(entries) != 1 {
		t.Errorf("merge did not apply: entries=%v err=%v", entries, err)
	}
}
