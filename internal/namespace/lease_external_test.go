package namespace_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

func TestNamespace_LeaseAcquireRenewRelease(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	// A fresh volume is vacant.
	if l, err := ns.Lease("/vol"); err != nil || l.Holder != "" {
		t.Fatalf("initial lease = (%+v,%v), want vacant", l, err)
	}

	clk++
	acq, err := ns.AcquireLease("/vol", "writer-1")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if acq.Holder != "writer-1" || acq.At.IsZero() {
		t.Errorf("acquired lease = %+v, want holder writer-1 with a timestamp", acq)
	}

	// Renewing advances the acquisition HLC (the fencing token) without
	// changing the holder.
	clk++
	ren, err := ns.RenewLease("/vol", "writer-1")
	if err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	if !acq.At.Before(ren.At) {
		t.Errorf("renew did not advance the fence token: acquire=%s renew=%s", acq.At, ren.At)
	}

	// Release leaves it vacant.
	clk++
	if err := ns.ReleaseLease("/vol", "writer-1"); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}
	if l, _ := ns.Lease("/vol"); l.Holder != "" {
		t.Errorf("after release holder = %q, want vacant", l.Holder)
	}

	// Renewing a now-vacant lease fails and names it as vacant.
	if _, err := ns.RenewLease("/vol", "writer-1"); err == nil || !strings.Contains(err.Error(), "vacant") {
		t.Errorf("renew of a vacant lease = %v, want a 'vacant' error", err)
	}
}

func TestNamespace_LeaseStealFencesOlderHolder(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	clk++
	if _, err := ns.AcquireLease("/vol", "writer-1"); err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	clk++
	if _, err := ns.AcquireLease("/vol", "writer-2"); err != nil { // steal
		t.Fatalf("acquire 2: %v", err)
	}

	if l, _ := ns.Lease("/vol"); l.Holder != "writer-2" {
		t.Errorf("after steal holder = %q, want writer-2", l.Holder)
	}
	// The fenced holder can neither renew nor release.
	if _, err := ns.RenewLease("/vol", "writer-1"); !errors.Is(err, namespace.ErrLeaseHeld) {
		t.Errorf("fenced renew err = %v, want ErrLeaseHeld", err)
	}
	if err := ns.ReleaseLease("/vol", "writer-1"); !errors.Is(err, namespace.ErrLeaseHeld) {
		t.Errorf("fenced release err = %v, want ErrLeaseHeld", err)
	}
}

func TestNamespace_WriteExtentFencesStaleHolder(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	// Writing without holding the lease is fenced, even on a vacant volume.
	if err := ns.WriteExtent("/vol", 0, "c", "writer-1"); !errors.Is(err, namespace.ErrLeaseHeld) {
		t.Fatalf("write without a lease = %v, want ErrLeaseHeld", err)
	}

	clk++
	if _, err := ns.AcquireLease("/vol", "writer-1"); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 0, "c0", "writer-1"); err != nil {
		t.Fatalf("holder write should succeed: %v", err)
	}

	// A second writer steals the lease; the original is now fenced.
	clk++
	if _, err := ns.AcquireLease("/vol", "writer-2"); err != nil {
		t.Fatalf("steal: %v", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 0, "c-stale", "writer-1"); !errors.Is(err, namespace.ErrLeaseHeld) {
		t.Errorf("fenced write = %v, want ErrLeaseHeld", err)
	}
	clk++
	if err := ns.WriteExtent("/vol", 0, "c-new", "writer-2"); err != nil {
		t.Errorf("new holder write should succeed: %v", err)
	}
	if got, _ := ns.Extents("/vol"); got[0] != "c-new" {
		t.Errorf("extent 0 = %q, want c-new (the fenced write must not have applied)", got[0])
	}
}

func TestNamespace_LeaseOpsRejectWrongTarget(t *testing.T) {
	var clk int64 = 1
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.Touch("/file"); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	cases := []struct {
		name string
		do   func() error
		want string
	}{
		{"acquire on a file", func() error { _, err := ns.AcquireLease("/file", "w"); return err }, "not a volume"},
		{"renew on a missing path", func() error { _, err := ns.RenewLease("/nope", "w"); return err }, "does not exist"},
		{"release on root", func() error { return ns.ReleaseLease("/", "w") }, "not a volume"},
		{"read lease of a file", func() error { _, err := ns.Lease("/file"); return err }, "not a volume"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.do(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestNamespace_LeasePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ns.json")
	first, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := first.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if _, err := first.AcquireLease("/vol", "writer-1"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}

	second, err := namespace.Open(hlc.New("a"), path, discardLogger())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if l, _ := second.Lease("/vol"); l.Holder != "writer-1" || l.At.IsZero() {
		t.Errorf("persisted lease = %+v, want writer-1 with a timestamp", l)
	}
}

func TestNamespace_LeaseConvergesAcrossReplicas(t *testing.T) {
	var clkA, clkB int64 = 10, 100 // b's clock runs ahead so its claim wins
	a := nsAt("a", &clkA)
	clkA++
	if _, err := a.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	clkA++
	if _, err := a.AcquireLease("/vol", "on-a"); err != nil {
		t.Fatalf("a acquire: %v", err)
	}

	state, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	b := nsAt("b", &clkB)
	if err := b.MergeBytes(state); err != nil {
		t.Fatalf("b merge: %v", err)
	}
	clkB++
	if _, err := b.AcquireLease("/vol", "on-b"); err != nil { // steal with a newer HLC
		t.Fatalf("b acquire: %v", err)
	}

	bState, err := b.Snapshot()
	if err != nil {
		t.Fatalf("b snapshot: %v", err)
	}
	if err := a.MergeBytes(bState); err != nil {
		t.Fatalf("a merge: %v", err)
	}
	if l, _ := a.Lease("/vol"); l.Holder != "on-b" {
		t.Errorf("converged holder = %q, want on-b (newer HLC wins)", l.Holder)
	}
}

func TestNamespace_ReleaseLeaseAtIsCompareAndRelease(t *testing.T) {
	var clk int64 = 100
	ns := nsAt("a", &clk)
	clk++
	if _, err := ns.CreateVolume("/vol", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	clk++
	first, err := ns.AcquireLease("/vol", "node-1")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	// The same holder re-acquires — a reconnected client's fresh claim.
	clk++
	second, err := ns.AcquireLease("/vol", "node-1")
	if err != nil {
		t.Fatalf("re-AcquireLease: %v", err)
	}
	if second.At.Compare(first.At) <= 0 {
		t.Fatalf("re-acquisition should carry a newer stamp: %v then %v", first.At, second.At)
	}

	// The old connection's teardown must not vacate the live claim.
	clk++
	if err := ns.ReleaseLeaseAt("/vol", "node-1", first.At); err != nil {
		t.Fatalf("stale ReleaseLeaseAt: %v", err)
	}
	if l, _ := ns.Lease("/vol"); l.Holder != "node-1" || l.At.Compare(second.At) != 0 {
		t.Fatalf("lease after stale release = %+v, want the live claim untouched", l)
	}

	// A mismatched holder is likewise a no-op.
	clk++
	if err := ns.ReleaseLeaseAt("/vol", "node-2", second.At); err != nil {
		t.Fatalf("wrong-holder ReleaseLeaseAt: %v", err)
	}
	if l, _ := ns.Lease("/vol"); l.Holder != "node-1" {
		t.Fatalf("lease after wrong-holder release = %+v, want node-1", l)
	}

	// The exact acquisition releases cleanly.
	clk++
	if err := ns.ReleaseLeaseAt("/vol", "node-1", second.At); err != nil {
		t.Fatalf("matching ReleaseLeaseAt: %v", err)
	}
	if l, _ := ns.Lease("/vol"); l.Holder != "" {
		t.Fatalf("lease after matching release = %+v, want vacant", l)
	}

	// Missing volumes still surface an error.
	if err := ns.ReleaseLeaseAt("/missing", "node-1", second.At); err == nil {
		t.Fatal("ReleaseLeaseAt on a missing volume should error")
	}
}
