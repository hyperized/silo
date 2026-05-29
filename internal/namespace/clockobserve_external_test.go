package namespace_test

import (
	"testing"
	"time"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

func TestNamespace_MergeBytesReportsPeerClock(t *testing.T) {
	// The peer stamps its mutations from a clock pinned to a known instant,
	// so the wall the observer reports is predictable.
	const peerWall = 12_345
	peerClock := hlc.New("peer", hlc.WithNow(func() time.Time { return time.Unix(0, peerWall) }))
	peer := namespace.New(peerClock)
	if _, err := peer.Mkdir("/d"); err != nil {
		t.Fatalf("peer Mkdir: %v", err)
	}
	state, err := peer.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	var gotNode string
	var gotWall int64
	calls := 0
	n := namespace.New(hlc.New("me"), namespace.WithPeerClockObserver(func(node string, wall int64) {
		gotNode, gotWall = node, wall
		calls++
	}))
	if err := n.MergeBytes(state); err != nil {
		t.Fatalf("MergeBytes: %v", err)
	}
	if calls != 1 || gotNode != "peer" || gotWall != peerWall {
		t.Errorf("observer saw calls=%d (%s, %d), want 1 (peer, %d)", calls, gotNode, gotWall, peerWall)
	}
}

func TestNamespace_MergeBytesWithoutObserver(t *testing.T) {
	peer := namespace.New(hlc.New("peer"))
	if _, err := peer.Mkdir("/d"); err != nil {
		t.Fatalf("peer Mkdir: %v", err)
	}
	state, err := peer.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
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
