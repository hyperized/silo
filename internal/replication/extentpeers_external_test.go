package replication_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	extentv1 "github.com/hyperized/silo/api/proto/silo/extent/v1"
	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/replication"
	"github.com/hyperized/silo/internal/transport"
)

// startExtentServer runs the ExtentMap service over insecure gRPC backed by a
// real in-memory extent store, returning its dial address.
func startExtentServer(t testing.TB, store *extentmap.Store) string {
	t.Helper()
	svc := transport.NewExtentService(store, discardLogger())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	extentv1.RegisterExtentMapServer(s, svc)
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() {
		s.Stop()
		_ = ln.Close()
	})
	return ln.Addr().String()
}

func ewall(w int64) hlc.Timestamp { return hlc.Timestamp{Wall: w} }

func TestExtentPeers_ApplyFetchStatRoundTrip(t *testing.T) {
	store := extentmap.New(discardLogger())
	addr := startExtentServer(t, store)
	peers := replication.NewExtentGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })
	ctx := context.Background()

	entries := []crdt.MapEntry[uint64, string]{
		{Key: 0, Value: "c0", TS: ewall(1)},
		{Key: 5, Value: "c5", TS: ewall(2)},
	}
	if err := peers.Apply(ctx, addr, "vol", entries, false); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	has, count, digest, err := peers.Stat(ctx, addr, "vol")
	if err != nil || !has || count != 2 {
		t.Fatalf("Stat = (%v,%d,%v), want (true,2,nil)", has, count, err)
	}
	// A held map has to come back fingerprinted, or the currency check that
	// relies on it silently degrades to comparing counts.
	if len(digest) == 0 {
		t.Error("Stat returned no digest for a map the peer holds")
	}

	got, err := peers.Fetch(ctx, addr, "vol")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(got) != 2 || got[0].Value != "c0" || got[1].Value != "c5" || got[1].TS.Wall != 2 {
		t.Errorf("fetched entries wrong: %+v", got)
	}

	// Ensure (no entries) creates an empty map that Stat reports as present.
	if err := peers.Apply(ctx, addr, "empty", nil, true); err != nil {
		t.Fatalf("Apply ensure: %v", err)
	}
	if has, count, _, _ := peers.Stat(ctx, addr, "empty"); !has || count != 0 {
		t.Errorf("ensured map Stat = (has=%v,count=%d), want (true,0)", has, count)
	}

	// A large map streams back across multiple frames.
	big := make([]crdt.MapEntry[uint64, string], 2500)
	for i := range big {
		big[i] = crdt.MapEntry[uint64, string]{Key: uint64(i), Value: "c", TS: ewall(int64(i + 1))}
	}
	if err := peers.Apply(ctx, addr, "big", big, false); err != nil {
		t.Fatalf("Apply big: %v", err)
	}
	gotBig, err := peers.Fetch(ctx, addr, "big")
	if err != nil {
		t.Fatalf("Fetch big: %v", err)
	}
	if len(gotBig) != 2500 {
		t.Errorf("fetched %d entries, want 2500", len(gotBig))
	}

	// Delete removes a map; Stat then reports it absent. Deleting again is a no-op.
	if err := peers.Delete(ctx, addr, "vol"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if has, _, _, _ := peers.Stat(ctx, addr, "vol"); has {
		t.Error("Stat should report a deleted map as absent")
	}
	if err := peers.Delete(ctx, addr, "never-existed"); err != nil {
		t.Errorf("Delete of an unknown map should be a no-op, got %v", err)
	}
}

func TestExtentPeers_ServerRejectsEmptyVolume(t *testing.T) {
	addr := startExtentServer(t, extentmap.New(discardLogger()))
	peers := replication.NewExtentGRPCPeers(insecure.NewCredentials(), discardLogger())
	t.Cleanup(func() { _ = peers.Close() })
	ctx := context.Background()

	if err := peers.Apply(ctx, addr, "", nil, false); err == nil {
		t.Error("Apply with an empty volume id should error")
	}
	if _, err := peers.Fetch(ctx, addr, ""); err == nil {
		t.Error("Fetch with an empty volume id should error")
	}
	if _, _, _, err := peers.Stat(ctx, addr, ""); err == nil {
		t.Error("Stat with an empty volume id should error")
	}
	if err := peers.Delete(ctx, addr, ""); err == nil {
		t.Error("Delete with an empty volume id should error")
	}
}
