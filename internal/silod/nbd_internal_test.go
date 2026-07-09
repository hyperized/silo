package silod

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/namespace"
	"github.com/hyperized/silo/internal/nbd"
)

var errBoom = errors.New("boom")

type fakeCoord struct {
	readData []byte
	readErr  error
	writeErr error
	wrote    map[string][]byte
}

func (c *fakeCoord) Write(_ context.Context, id string, data []byte) (chunkstore.Info, error) {
	if c.writeErr != nil {
		return chunkstore.Info{}, c.writeErr
	}
	if c.wrote == nil {
		c.wrote = map[string][]byte{}
	}
	c.wrote[id] = data
	return chunkstore.Info{}, nil
}

func (c *fakeCoord) Read(_ context.Context, _ string) ([]byte, chunkstore.Info, error) {
	return c.readData, chunkstore.Info{}, c.readErr
}
func (c *fakeCoord) Delete(context.Context, string) error { return nil }
func (c *fakeCoord) Stat(context.Context, string) (chunkstore.Info, error) {
	return chunkstore.Info{}, nil
}

type fakeNS struct {
	size        int64
	sizeErr     error
	extentErr   error
	acquireErr  error
	releaseErr  error
	released    bool
	leaseHolder string
}

func (n *fakeNS) ExtentSize(string) (int64, error) {
	if n.extentErr != nil {
		return 0, n.extentErr
	}
	return 4096, nil
}
func (n *fakeNS) Extent(string, uint64) (string, bool, error)           { return "", false, nil }
func (n *fakeNS) WriteExtent(string, uint64, string, string) error      { return nil }
func (n *fakeNS) WriteExtents(string, []uint64, []string, string) error { return nil }
func (n *fakeNS) Size(string) (int64, error)                            { return n.size, n.sizeErr }

func (n *fakeNS) AcquireLease(string, string) (namespace.Lease, error) {
	return namespace.Lease{}, n.acquireErr
}

func (n *fakeNS) ReleaseLeaseAt(string, string, hlc.Timestamp) error {
	n.released = true
	return n.releaseErr
}

func (n *fakeNS) VolumeInodeID(string) (string, error) {
	if n.extentErr != nil {
		return "", n.extentErr
	}
	return "inode-vol", nil
}

func (n *fakeNS) Lease(string) (namespace.Lease, error) {
	return namespace.Lease{Holder: n.leaseHolder}, nil
}

func TestCoordChunks(t *testing.T) {
	coord := &fakeCoord{readData: []byte("hi")}
	c := coordChunks{coord: coord}

	if got, err := c.GetChunk(context.Background(), "id"); err != nil || string(got) != "hi" {
		t.Errorf("GetChunk = (%q,%v), want (hi,nil)", got, err)
	}
	if err := c.PutChunk(context.Background(), "id", []byte("bye")); err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if string(coord.wrote["id"]) != "bye" {
		t.Errorf("PutChunk stored %q, want bye", coord.wrote["id"])
	}

	bad := coordChunks{coord: &fakeCoord{readErr: errBoom, writeErr: errBoom}}
	if _, err := bad.GetChunk(context.Background(), "id"); !errors.Is(err, errBoom) {
		t.Errorf("GetChunk err = %v, want errBoom", err)
	}
	if err := bad.PutChunk(context.Background(), "id", nil); !errors.Is(err, errBoom) {
		t.Errorf("PutChunk err = %v, want errBoom", err)
	}
}

func TestVolumeBackend_OpenSuccess(t *testing.T) {
	ns := &fakeNS{size: 2048}
	b := newVolumeBackend(ns, &fakeCoord{}, "node-a", discardLogger(), nil, nil, false)
	dev, release, err := b.Open(context.Background(), "/vol")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if dev.Size() != 2048 {
		t.Errorf("device size = %d, want 2048", dev.Size())
	}
	release()
	if !ns.released {
		t.Error("release did not drop the lease")
	}
}

func TestVolumeBackend_OpenErrors(t *testing.T) {
	cases := []struct {
		name string
		ns   *fakeNS
		want string
	}{
		{"size error", &fakeNS{sizeErr: errBoom}, "cannot serve"},
		{"no size", &fakeNS{size: 0}, "no size"},
		{"acquire fails", &fakeNS{size: 1024, acquireErr: errBoom}, "could not acquire the lease"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newVolumeBackend(tc.ns, &fakeCoord{}, "node-a", discardLogger(), nil, nil, false)
			if _, _, err := b.Open(context.Background(), "/vol"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Open err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestVolumeBackend_OpenVolumeFailureReleasesLease(t *testing.T) {
	// Size + AcquireLease succeed, but volume.Open then fails resolving the
	// extent size; the backend must release the lease it just took.
	ns := &fakeNS{size: 1024, extentErr: errBoom}
	b := newVolumeBackend(ns, &fakeCoord{}, "node-a", discardLogger(), nil, nil, false)
	if _, _, err := b.Open(context.Background(), "/vol"); err == nil {
		t.Fatal("Open should fail when the volume cannot be opened")
	}
	if !ns.released {
		t.Error("a failed Open must release the lease it acquired")
	}
}

func TestVolumeBackend_ReleaseErrorIsLogged(t *testing.T) {
	// A release error is logged, not propagated; just exercise the branch.
	ns := &fakeNS{size: 1024, releaseErr: errBoom}
	b := newVolumeBackend(ns, &fakeCoord{}, "node-a", discardLogger(), nil, nil, false)
	_, release, err := b.Open(context.Background(), "/vol")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	release() // must not panic; the error is logged
	if !ns.released {
		t.Error("release was not attempted")
	}
}

func TestNBDSub_BindError(t *testing.T) {
	sub := newNBDSub("not-an-address", nbd.NewServer(&volumeBackend{}, discardLogger()), discardLogger())
	if sub.boundAddr() != "" {
		t.Errorf("boundAddr before Start = %q, want empty", sub.boundAddr())
	}
	if err := sub.Start(); err == nil {
		t.Error("Start should fail on a malformed address")
	}
}

func TestNBDSub_Lifecycle(t *testing.T) {
	sub := newNBDSub("127.0.0.1:0", nbd.NewServer(&volumeBackend{}, discardLogger()), discardLogger())
	done := make(chan error, 1)
	go func() { done <- sub.Start() }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && sub.boundAddr() == "" {
		time.Sleep(2 * time.Millisecond)
	}
	if sub.boundAddr() == "" {
		t.Fatal("nbd subsystem did not bind within 2s")
	}
	if err := sub.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned %v, want nil after Shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Shutdown")
	}
}

func TestNBDSub_ShutdownBeforeStartIsNoop(t *testing.T) {
	sub := newNBDSub("127.0.0.1:0", nbd.NewServer(&volumeBackend{}, discardLogger()), discardLogger())
	// Shutdown before Start: Start must then be a no-op rather than serving a
	// listener nobody will close.
	if err := sub.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Start: %v", err)
	}
	if err := sub.Start(); err != nil {
		t.Errorf("Start after Shutdown = %v, want nil no-op", err)
	}
}
