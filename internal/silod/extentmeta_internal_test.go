package silod

import (
	"context"
	"errors"
	"testing"

	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
)

// fakeEVM is a fake extentVolumeMeta (the gossiped-namespace surface).
type fakeEVM struct {
	extentSize    int64
	extentSizeErr error
	leaseHolder   string
	leaseErr      error
}

func (f *fakeEVM) ExtentSize(string) (int64, error)     { return f.extentSize, f.extentSizeErr }
func (f *fakeEVM) VolumeInodeID(string) (string, error) { return "inode-vol", nil }
func (f *fakeEVM) Lease(string) (namespace.Lease, error) {
	return namespace.Lease{Holder: f.leaseHolder}, f.leaseErr
}

// fakeEC is a fake extentCoord.
type fakeEC struct {
	lookupID  string
	lookupOK  bool
	applyErr  error
	warmErr   error
	applied   []appliedDelta
	warmCalls int
}

type appliedDelta struct {
	vol string
	idx []uint64
	ids []string
}

func (f *fakeEC) Lookup(_ string, _ uint64) (string, bool) { return f.lookupID, f.lookupOK }
func (f *fakeEC) ApplyDelta(_ context.Context, vol string, idx []uint64, ids []string, _ hlc.Timestamp) error {
	f.applied = append(f.applied, appliedDelta{vol: vol, idx: idx, ids: ids})
	return f.applyErr
}
func (f *fakeEC) Warm(context.Context, string) error { f.warmCalls++; return f.warmErr }

func newExtentMeta(ns extentVolumeMeta, coord extentCoord) *extentMetadata {
	return &extentMetadata{ctx: context.Background(), ns: ns, coord: coord, clock: hlc.New("node"), export: "/vol", volumeID: "inode-vol"}
}

func TestExtentMetadata_ExtentSize(t *testing.T) {
	m := newExtentMeta(&fakeEVM{extentSize: 4096}, &fakeEC{})
	if sz, err := m.ExtentSize("/vol"); err != nil || sz != 4096 {
		t.Errorf("ExtentSize = (%d,%v), want (4096,nil)", sz, err)
	}
	m2 := newExtentMeta(&fakeEVM{extentSizeErr: errBoom}, &fakeEC{})
	if _, err := m2.ExtentSize("/vol"); !errors.Is(err, errBoom) {
		t.Errorf("ExtentSize err = %v, want errBoom", err)
	}
}

func TestExtentMetadata_ExtentReadsFromCoordinator(t *testing.T) {
	m := newExtentMeta(&fakeEVM{}, &fakeEC{lookupID: "c7", lookupOK: true})
	if id, ok, err := m.Extent("/vol", 7); err != nil || !ok || id != "c7" {
		t.Errorf("Extent = (%q,%v,%v), want (c7,true,nil)", id, ok, err)
	}
	mu := newExtentMeta(&fakeEVM{}, &fakeEC{})
	if id, ok, err := mu.Extent("/vol", 9); err != nil || ok || id != "" {
		t.Errorf("unmapped Extent = (%q,%v,%v), want (\"\",false,nil)", id, ok, err)
	}
}

func TestExtentMetadata_WriteFencedAndReplicated(t *testing.T) {
	coord := &fakeEC{}
	m := newExtentMeta(&fakeEVM{leaseHolder: "node"}, coord)

	if err := m.WriteExtent("/vol", 0, "c0", "node"); err != nil {
		t.Fatalf("WriteExtent: %v", err)
	}
	if err := m.WriteExtents("/vol", []uint64{1, 2}, []string{"c1", "c2"}, "node"); err != nil {
		t.Fatalf("WriteExtents: %v", err)
	}
	if len(coord.applied) != 2 || len(coord.applied[1].idx) != 2 {
		t.Fatalf("expected two ApplyDelta calls, second a batch of 2: %+v", coord.applied)
	}
}

func TestExtentMetadata_WriteFencedOutByLease(t *testing.T) {
	// The current holder is someone else: writes are refused with ErrLeaseHeld
	// and never reach the coordinator.
	coord := &fakeEC{}
	m := newExtentMeta(&fakeEVM{leaseHolder: "other"}, coord)
	if err := m.WriteExtent("/vol", 0, "c0", "node"); !errors.Is(err, namespace.ErrLeaseHeld) {
		t.Errorf("WriteExtent err = %v, want ErrLeaseHeld", err)
	}
	if err := m.WriteExtents("/vol", []uint64{0}, []string{"c0"}, "node"); !errors.Is(err, namespace.ErrLeaseHeld) {
		t.Errorf("WriteExtents err = %v, want ErrLeaseHeld", err)
	}
	if len(coord.applied) != 0 {
		t.Error("a fenced write must not replicate")
	}
}

func TestExtentMetadata_WriteLeaseLookupError(t *testing.T) {
	m := newExtentMeta(&fakeEVM{leaseErr: errBoom}, &fakeEC{})
	if err := m.WriteExtent("/vol", 0, "c0", "node"); !errors.Is(err, errBoom) {
		t.Errorf("WriteExtent err = %v, want errBoom", err)
	}
}

func TestExtentMetadata_ApplyDeltaErrorSurfaces(t *testing.T) {
	m := newExtentMeta(&fakeEVM{leaseHolder: "node"}, &fakeEC{applyErr: errBoom})
	if err := m.WriteExtent("/vol", 0, "c0", "node"); !errors.Is(err, errBoom) {
		t.Errorf("WriteExtent err = %v, want errBoom", err)
	}
}

func TestVolumeBackend_MetadataSelectsPath(t *testing.T) {
	// Flag off -> legacy namespace path (returns the ns itself).
	nsLegacy := &fakeNS{}
	bOff := newVolumeBackend(nsLegacy, &fakeCoord{}, "node-a", discardLogger(), &fakeEC{}, hlc.New("node-a"), false)
	meta, err := bOff.metadata(context.Background(), "/vol")
	if err != nil {
		t.Fatalf("metadata (off): %v", err)
	}
	if _, isExtent := meta.(*extentMetadata); isExtent {
		t.Error("flag off should use the legacy namespace path")
	}

	// Flag on -> warms and returns the replica-set adapter.
	coord := &fakeEC{}
	bOn := newVolumeBackend(&fakeNS{}, &fakeCoord{}, "node-a", discardLogger(), coord, hlc.New("node-a"), true)
	meta, err = bOn.metadata(context.Background(), "/vol")
	if err != nil {
		t.Fatalf("metadata (on): %v", err)
	}
	if _, isExtent := meta.(*extentMetadata); !isExtent {
		t.Error("flag on should use the extent-replication adapter")
	}
	if coord.warmCalls != 1 {
		t.Errorf("metadata should warm the map once, got %d", coord.warmCalls)
	}
}

func TestVolumeBackend_MetadataErrors(t *testing.T) {
	// VolumeInodeID failure.
	bID := newVolumeBackend(&fakeNS{extentErr: errBoom}, &fakeCoord{}, "n", discardLogger(), &fakeEC{}, hlc.New("n"), true)
	if _, err := bID.metadata(context.Background(), "/vol"); err == nil {
		t.Error("metadata should fail when the volume id cannot be resolved")
	}
	// Warm failure.
	bWarm := newVolumeBackend(&fakeNS{}, &fakeCoord{}, "n", discardLogger(), &fakeEC{warmErr: errBoom}, hlc.New("n"), true)
	if _, err := bWarm.metadata(context.Background(), "/vol"); err == nil {
		t.Error("metadata should fail when warming the map fails")
	}
}
