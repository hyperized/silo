package csi

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
)

// fakeNSClient records the namespace RPCs the backend issues and returns canned
// errors so idempotency can be exercised.
type fakeNSClient struct {
	mkdirs      []string
	createPath  string
	snapSrc     string
	snapDst     string
	removed     string
	createErr   error
	snapErr     error
	removeErr   error
	mkdirErr    error
	mkdirErrFor string // return mkdirErr only for this path
}

func (f *fakeNSClient) Mkdir(_ context.Context, in *namespacev1.MkdirRequest, _ ...grpc.CallOption) (*namespacev1.MkdirResponse, error) {
	f.mkdirs = append(f.mkdirs, in.GetPath())
	if f.mkdirErr != nil && (f.mkdirErrFor == "" || f.mkdirErrFor == in.GetPath()) {
		return nil, f.mkdirErr
	}
	return &namespacev1.MkdirResponse{}, nil
}

func (f *fakeNSClient) CreateVolume(_ context.Context, in *namespacev1.CreateVolumeRequest, _ ...grpc.CallOption) (*namespacev1.CreateVolumeResponse, error) {
	f.createPath = in.GetPath()
	return &namespacev1.CreateVolumeResponse{Inode: "inode-x"}, f.createErr
}

func (f *fakeNSClient) SnapshotVolume(_ context.Context, in *namespacev1.SnapshotVolumeRequest, _ ...grpc.CallOption) (*namespacev1.SnapshotVolumeResponse, error) {
	f.snapSrc, f.snapDst = in.GetSourcePath(), in.GetDestPath()
	return &namespacev1.SnapshotVolumeResponse{Inode: "inode-x"}, f.snapErr
}

func (f *fakeNSClient) Remove(_ context.Context, in *namespacev1.RemoveRequest, _ ...grpc.CallOption) (*namespacev1.RemoveResponse, error) {
	f.removed = in.GetPath()
	return &namespacev1.RemoveResponse{}, f.removeErr
}

func TestNamespaceBackend_CreateVolume(t *testing.T) {
	ctx := context.Background()
	fake := &fakeNSClient{}
	b := NewNamespaceBackend(fake)

	id, err := b.CreateVolume(ctx, "pvc-1", 10<<20, 4<<20)
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if id != "/csi/volumes/pvc-1" || fake.createPath != "/csi/volumes/pvc-1" {
		t.Errorf("id/path = (%q, %q), want /csi/volumes/pvc-1", id, fake.createPath)
	}
	// The /csi tree is ensured mkdir -p style.
	if len(fake.mkdirs) != 2 || fake.mkdirs[0] != "/csi" || fake.mkdirs[1] != "/csi/volumes" {
		t.Errorf("mkdirs = %v, want [/csi /csi/volumes]", fake.mkdirs)
	}
}

func TestNamespaceBackend_CreateVolumeIdempotent(t *testing.T) {
	ctx := context.Background()
	// An already-existing volume (AlreadyExists) is success and returns the id.
	fake := &fakeNSClient{createErr: status.Error(codes.AlreadyExists, "exists"), mkdirErr: status.Error(codes.AlreadyExists, "exists")}
	id, err := NewNamespaceBackend(fake).CreateVolume(ctx, "pvc-1", 1<<20, 0)
	if err != nil || id != "/csi/volumes/pvc-1" {
		t.Fatalf("idempotent create = (%q, %v), want the id and no error", id, err)
	}

	// A genuine error propagates.
	boom := &fakeNSClient{createErr: status.Error(codes.Internal, "disk full")}
	if _, err := NewNamespaceBackend(boom).CreateVolume(ctx, "pvc-1", 1<<20, 0); status.Code(err) != codes.Internal {
		t.Errorf("create error = %v, want Internal", status.Code(err))
	}
}

func TestNamespaceBackend_CloneAndSnapshot(t *testing.T) {
	ctx := context.Background()

	clone := &fakeNSClient{}
	id, err := NewNamespaceBackend(clone).CloneVolume(ctx, "/csi/snapshots/snap-1", "pvc-restore")
	if err != nil || id != "/csi/volumes/pvc-restore" {
		t.Fatalf("CloneVolume = (%q, %v)", id, err)
	}
	if clone.snapSrc != "/csi/snapshots/snap-1" || clone.snapDst != "/csi/volumes/pvc-restore" {
		t.Errorf("clone snapshot args = (%q, %q)", clone.snapSrc, clone.snapDst)
	}

	snap := &fakeNSClient{}
	sid, err := NewNamespaceBackend(snap).CreateSnapshot(ctx, "/csi/volumes/pvc-1", "snap-1")
	if err != nil || sid != "/csi/snapshots/snap-1" {
		t.Fatalf("CreateSnapshot = (%q, %v)", sid, err)
	}
	if snap.snapSrc != "/csi/volumes/pvc-1" || snap.snapDst != "/csi/snapshots/snap-1" {
		t.Errorf("snapshot args = (%q, %q)", snap.snapSrc, snap.snapDst)
	}
	if len(snap.mkdirs) != 2 || snap.mkdirs[1] != "/csi/snapshots" {
		t.Errorf("snapshot mkdirs = %v, want the /csi/snapshots tree", snap.mkdirs)
	}
}

func TestNamespaceBackend_CloneAndSnapshotIdempotent(t *testing.T) {
	ctx := context.Background()

	// An already-existing destination (AlreadyExists) is success.
	clone := &fakeNSClient{snapErr: status.Error(codes.AlreadyExists, "exists")}
	if id, err := NewNamespaceBackend(clone).CloneVolume(ctx, "/csi/snapshots/s", "pvc-1"); err != nil || id != "/csi/volumes/pvc-1" {
		t.Errorf("idempotent clone = (%q, %v)", id, err)
	}
	snap := &fakeNSClient{snapErr: status.Error(codes.AlreadyExists, "exists")}
	if id, err := NewNamespaceBackend(snap).CreateSnapshot(ctx, "/csi/volumes/pvc-1", "s"); err != nil || id != "/csi/snapshots/s" {
		t.Errorf("idempotent snapshot = (%q, %v)", id, err)
	}

	// A genuine snapshot error propagates.
	boom := &fakeNSClient{snapErr: status.Error(codes.Internal, "boom")}
	if _, err := NewNamespaceBackend(boom).CloneVolume(ctx, "/x", "pvc-1"); status.Code(err) != codes.Internal {
		t.Errorf("clone error = %v, want Internal", status.Code(err))
	}
	if _, err := NewNamespaceBackend(boom).CreateSnapshot(ctx, "/x", "s"); status.Code(err) != codes.Internal {
		t.Errorf("snapshot error = %v, want Internal", status.Code(err))
	}

	// Bad names are rejected before any RPC.
	b := NewNamespaceBackend(&fakeNSClient{})
	if _, err := b.CloneVolume(ctx, "/x", "a/b"); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CloneVolume bad name = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := b.CreateSnapshot(ctx, "/x", ".."); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateSnapshot bad name = %v, want InvalidArgument", status.Code(err))
	}

	// A mkdir failure aborts a clone and a snapshot.
	mk := &fakeNSClient{mkdirErr: errors.New("unreachable"), mkdirErrFor: "/csi"}
	if _, err := NewNamespaceBackend(mk).CloneVolume(ctx, "/x", "pvc-1"); err == nil {
		t.Error("clone should abort on mkdir failure")
	}
	mk2 := &fakeNSClient{mkdirErr: errors.New("unreachable"), mkdirErrFor: "/csi"}
	if _, err := NewNamespaceBackend(mk2).CreateSnapshot(ctx, "/x", "s"); err == nil {
		t.Error("snapshot should abort on mkdir failure")
	}
}

func TestNamespaceBackend_DeleteIdempotent(t *testing.T) {
	ctx := context.Background()

	del := &fakeNSClient{}
	if err := NewNamespaceBackend(del).DeleteVolume(ctx, "/csi/volumes/pvc-1"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if del.removed != "/csi/volumes/pvc-1" {
		t.Errorf("removed = %q", del.removed)
	}

	// A missing volume (NotFound) deletes cleanly.
	gone := &fakeNSClient{removeErr: status.Error(codes.NotFound, "gone")}
	if err := NewNamespaceBackend(gone).DeleteSnapshot(ctx, "/csi/snapshots/snap-1"); err != nil {
		t.Errorf("deleting a missing snapshot should succeed, got %v", err)
	}

	// Another error propagates.
	boom := &fakeNSClient{removeErr: status.Error(codes.Internal, "boom")}
	if err := NewNamespaceBackend(boom).DeleteVolume(ctx, "/csi/volumes/pvc-1"); status.Code(err) != codes.Internal {
		t.Errorf("delete error = %v, want Internal", status.Code(err))
	}
	if err := NewNamespaceBackend(boom).DeleteVolume(ctx, ""); status.Code(err) != codes.InvalidArgument {
		t.Errorf("delete empty id = %v, want InvalidArgument", status.Code(err))
	}
}

func TestNamespaceBackend_RejectsBadNames(t *testing.T) {
	ctx := context.Background()
	b := NewNamespaceBackend(&fakeNSClient{})
	for _, name := range []string{"", "a/b", "..", "."} {
		if _, err := b.CreateVolume(ctx, name, 1<<20, 0); status.Code(err) != codes.InvalidArgument {
			t.Errorf("CreateVolume(%q) = %v, want InvalidArgument", name, status.Code(err))
		}
	}
}

func TestNamespaceBackend_EnsureDirErrorPropagates(t *testing.T) {
	ctx := context.Background()
	// A non-AlreadyExists mkdir error on the first segment aborts the create.
	fake := &fakeNSClient{mkdirErr: errors.New("namespace unreachable"), mkdirErrFor: "/csi"}
	if _, err := NewNamespaceBackend(fake).CreateVolume(ctx, "pvc-1", 1<<20, 0); err == nil {
		t.Error("a mkdir failure should abort the create")
	}
}
