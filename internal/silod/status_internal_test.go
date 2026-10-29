package silod

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clusterstatus"
	"github.com/hyperized/silo/internal/gossip"
	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/nbd"
	"github.com/hyperized/silo/internal/replication"
)

type noStore struct{}

func (noStore) List(context.Context) ([]string, error) { return nil, nil }

// TestSubsystemStatusOptions wires the real gossip, chunk GC and NBD
// subsystems into a builder and checks each one's fact reaches the snapshot,
// while nil and fake subsystems contribute nothing.
func TestSubsystemStatusOptions(t *testing.T) {
	members, err := membership.New("node-a", "10.0.0.1:7100", "10.0.0.1:7000")
	if err != nil {
		t.Fatal(err)
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
	g, err := gossip.New(members, gossip.Options{Addr: "127.0.0.1:0", ServerTLS: tlsCfg, ClientTLS: tlsCfg}, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	gc := replication.NewChunkGC(nil, nil, nil, "node-a", time.Hour, time.Hour, true, discardLogger())
	nbdSubsys := newNBDSub("127.0.0.1:0", nbd.NewServer(&volumeBackend{}, discardLogger()), discardLogger())

	opts := subsystemStatusOptions(&gossipSub{srv: g}, gc, nbdSubsys, nil, newFakeSubsystem("fake", nil, nil, false))
	if len(opts) != 3 {
		t.Fatalf("got %d options, want one each from gossip, GC and NBD", len(opts))
	}
	resp := clusterstatus.New(members, noStore{}, t.TempDir(), "node-a", "v", discardLogger(), opts...).Build(context.Background())
	if h := resp.GetHealth(); h.GetIsolated() || h.GetRejoinAttempts() != 0 {
		t.Errorf("health from a fresh gossip subsystem = %+v", h)
	}
	if !resp.GetGc().GetEnabled() {
		t.Error("GC enabled flag did not reach the snapshot")
	}
	if got := nbdSubsys.AttachedExports(); len(got) != 0 {
		t.Errorf("a fresh NBD subsystem reports attached exports %v", got)
	}
}
