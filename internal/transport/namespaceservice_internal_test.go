package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/namespace"
)

type fakeNamespaceOps struct {
	id            string
	err           error
	entries       []namespace.ResolvedEntry
	ids           []string    // returned by Manifest
	appended      [][2]string // records (path, chunkID) passed to AppendChunk
	volExtentSize int64       // records the extent size passed to CreateVolume
	snapSrc       string      // records the source path passed to SnapshotVolume
	snapDst       string      // records the dest path passed to SnapshotVolume
	volInodeID    string      // returned by VolumeInodeID ("" => not a volume)
	volInodeErr   error       // error returned by VolumeInodeID
	removed       []string    // records the paths passed to Remove
	removeErr     error       // error returned by Remove, falling back to err when nil
}

func (f *fakeNamespaceOps) Mkdir(string) (string, error) { return f.id, f.err }
func (f *fakeNamespaceOps) Touch(string) (string, error) { return f.id, f.err }
func (f *fakeNamespaceOps) Remove(path string) error {
	f.removed = append(f.removed, path)
	if f.removeErr != nil {
		return f.removeErr
	}
	return f.err
}
func (f *fakeNamespaceOps) VolumeInodeID(string) (string, error) { return f.volInodeID, f.volInodeErr }
func (f *fakeNamespaceOps) List(string) ([]namespace.ResolvedEntry, error) {
	return f.entries, f.err
}

func (f *fakeNamespaceOps) AppendChunk(path, chunkID string) error {
	f.appended = append(f.appended, [2]string{path, chunkID})
	return f.err
}

func (f *fakeNamespaceOps) Manifest(string) ([]string, error) { return f.ids, f.err }

func (f *fakeNamespaceOps) CreateVolume(_ string, extentSize int64, _ ...namespace.VolumeOption) (string, error) {
	f.volExtentSize = extentSize
	return f.id, f.err
}

func (f *fakeNamespaceOps) SnapshotVolume(src, dst string) (string, error) {
	f.snapSrc, f.snapDst = src, dst
	return f.id, f.err
}

func nsService(ops NamespaceOps) *NamespaceService {
	return NewNamespaceService(ops, discardNSLogger())
}

func discardNSLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type fakeExtentDeleter struct {
	deleted []string
	err     error
}

func (f *fakeExtentDeleter) DeleteMap(_ context.Context, volumeID string) error {
	f.deleted = append(f.deleted, volumeID)
	return f.err
}

func TestNamespaceService_RemoveDeletesExtentMap(t *testing.T) {
	ctx := context.Background()

	// A volume: its inode id is captured before removal and its map deleted.
	del := &fakeExtentDeleter{}
	ops := &fakeNamespaceOps{volInodeID: "inode-vol-1"}
	svc := NewNamespaceService(ops, discardNSLogger(), WithExtentDeleter(del))
	if _, err := svc.Remove(ctx, &namespacev1.RemoveRequest{Path: "/csi/volumes/v"}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if len(ops.removed) != 1 || ops.removed[0] != "/csi/volumes/v" {
		t.Errorf("namespace Remove not called as expected: %v", ops.removed)
	}
	if len(del.deleted) != 1 || del.deleted[0] != "inode-vol-1" {
		t.Errorf("extent map not deleted for the resolved inode: %v", del.deleted)
	}

	// A non-volume path (VolumeInodeID errors): removed, but no extent delete.
	del2 := &fakeExtentDeleter{}
	ops2 := &fakeNamespaceOps{volInodeErr: namespace.ErrNotVolume}
	svc2 := NewNamespaceService(ops2, discardNSLogger(), WithExtentDeleter(del2))
	if _, err := svc2.Remove(ctx, &namespacev1.RemoveRequest{Path: "/csi/volumes"}); err != nil {
		t.Fatalf("Remove dir: %v", err)
	}
	if len(del2.deleted) != 0 {
		t.Errorf("a non-volume removal should not delete an extent map: %v", del2.deleted)
	}

	// A DeleteMap failure is logged, not returned: removal still succeeds.
	del3 := &fakeExtentDeleter{err: errors.New("replica down")}
	svc3 := NewNamespaceService(&fakeNamespaceOps{volInodeID: "inode-vol-3"}, discardNSLogger(), WithExtentDeleter(del3))
	if _, err := svc3.Remove(ctx, &namespacev1.RemoveRequest{Path: "/csi/volumes/v3"}); err != nil {
		t.Errorf("a best-effort extent-map delete failure must not fail Remove: %v", err)
	}

	// A namespace Remove failure maps through and skips the extent delete.
	del4 := &fakeExtentDeleter{}
	svc4 := NewNamespaceService(&fakeNamespaceOps{volInodeID: "inode-vol-4", err: namespace.ErrNotExist}, discardNSLogger(), WithExtentDeleter(del4))
	if _, err := svc4.Remove(ctx, &namespacev1.RemoveRequest{Path: "/csi/volumes/gone"}); status.Code(err) != codes.NotFound {
		t.Errorf("Remove error code = %v, want NotFound", status.Code(err))
	}
	if len(del4.deleted) != 0 {
		t.Errorf("a failed namespace Remove should not delete the extent map: %v", del4.deleted)
	}
}

func TestNamespaceService_HappyPaths(t *testing.T) {
	svc := nsService(&fakeNamespaceOps{id: "inode-1", entries: []namespace.ResolvedEntry{
		{Name: "dir", Inode: "inode-1", Type: namespace.Dir},
		{Name: "file.conflict-x", Inode: "inode-2", Type: namespace.File, Conflict: true},
	}})
	ctx := context.Background()

	if resp, err := svc.Mkdir(ctx, &namespacev1.MkdirRequest{Path: "/d"}); err != nil || resp.GetInode() != "inode-1" {
		t.Fatalf("Mkdir = %v, %v", resp, err)
	}
	if resp, err := svc.Touch(ctx, &namespacev1.TouchRequest{Path: "/f"}); err != nil || resp.GetInode() != "inode-1" {
		t.Fatalf("Touch = %v, %v", resp, err)
	}
	if _, err := svc.Remove(ctx, &namespacev1.RemoveRequest{Path: "/d"}); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	resp, err := svc.List(ctx, &namespacev1.ListRequest{Path: "/"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.GetEntries()) != 2 {
		t.Fatalf("List entries = %d, want 2", len(resp.GetEntries()))
	}
	if resp.Entries[0].GetType() != namespacev1.EntryType_ENTRY_TYPE_DIR {
		t.Errorf("entry 0 type = %v, want DIR", resp.Entries[0].GetType())
	}
	if resp.Entries[1].GetType() != namespacev1.EntryType_ENTRY_TYPE_FILE || !resp.Entries[1].GetConflict() {
		t.Errorf("entry 1 = %+v, want file+conflict", resp.Entries[1])
	}
}

func TestNamespaceService_AppendChunkAndManifest(t *testing.T) {
	ctx := context.Background()

	// A valid id is stored verbatim and reaches the namespace.
	ops := &fakeNamespaceOps{}
	svc := nsService(ops)
	if _, err := svc.AppendChunk(ctx, &namespacev1.AppendChunkRequest{Path: "/f", ChunkId: "w-1-0-0"}); err != nil {
		t.Fatalf("AppendChunk: %v", err)
	}
	if len(ops.appended) != 1 || ops.appended[0] != [2]string{"/f", "w-1-0-0"} {
		t.Fatalf("AppendChunk recorded %v, want one (/f, w-1-0-0)", ops.appended)
	}

	// A store-invalid id is rejected at the boundary and never reaches the
	// namespace, so a bad client cannot poison the manifest.
	if _, err := svc.AppendChunk(ctx, &namespacev1.AppendChunkRequest{Path: "/f", ChunkId: "bad/slash"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("AppendChunk bad id code = %v, want InvalidArgument", status.Code(err))
	}
	if len(ops.appended) != 1 {
		t.Errorf("a rejected id still reached the namespace: %v", ops.appended)
	}

	// A namespace error on a valid id maps through to a gRPC status.
	bad := nsService(&fakeNamespaceOps{err: namespace.ErrNotExist})
	if _, err := bad.AppendChunk(ctx, &namespacev1.AppendChunkRequest{Path: "/missing", ChunkId: "w-1-0-0"}); status.Code(err) != codes.NotFound {
		t.Errorf("AppendChunk on missing path code = %v, want NotFound", status.Code(err))
	}

	// Manifest returns the ids in order, and maps errors.
	readSvc := nsService(&fakeNamespaceOps{ids: []string{"c0", "c1", "c2"}})
	resp, err := readSvc.Manifest(ctx, &namespacev1.ManifestRequest{Path: "/f"})
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if got := resp.GetChunkIds(); len(got) != 3 || got[0] != "c0" || got[2] != "c2" {
		t.Errorf("Manifest ids = %v, want [c0 c1 c2]", got)
	}
	if _, err := bad.Manifest(ctx, &namespacev1.ManifestRequest{Path: "/missing"}); status.Code(err) != codes.NotFound {
		t.Errorf("Manifest on missing path code = %v, want NotFound", status.Code(err))
	}
}

func TestNamespaceService_CreateVolume(t *testing.T) {
	ctx := context.Background()
	ops := &fakeNamespaceOps{id: "inode-vol"}
	svc := nsService(ops)

	resp, err := svc.CreateVolume(ctx, &namespacev1.CreateVolumeRequest{Path: "/vol", SizeBytes: 1 << 20, ExtentSizeBytes: 8192})
	if err != nil || resp.GetInode() != "inode-vol" {
		t.Fatalf("CreateVolume = (%v, %v), want inode-vol", resp, err)
	}
	if ops.volExtentSize != 8192 {
		t.Errorf("extent size passed = %d, want 8192", ops.volExtentSize)
	}

	bad := nsService(&fakeNamespaceOps{err: namespace.ErrExists})
	if _, err := bad.CreateVolume(ctx, &namespacev1.CreateVolumeRequest{Path: "/vol"}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateVolume over existing = %v, want AlreadyExists", status.Code(err))
	}
}

func TestNamespaceService_SnapshotVolume(t *testing.T) {
	ctx := context.Background()
	ops := &fakeNamespaceOps{id: "inode-snap"}
	svc := nsService(ops)

	resp, err := svc.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: "/vol", DestPath: "/snap"})
	if err != nil || resp.GetInode() != "inode-snap" {
		t.Fatalf("SnapshotVolume = (%v, %v), want inode-snap", resp, err)
	}
	if ops.snapSrc != "/vol" || ops.snapDst != "/snap" {
		t.Errorf("paths passed = (%q, %q), want (/vol, /snap)", ops.snapSrc, ops.snapDst)
	}

	// Snapshotting a non-volume source surfaces FailedPrecondition.
	bad := nsService(&fakeNamespaceOps{err: namespace.ErrNotVolume})
	if _, err := bad.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: "/file", DestPath: "/snap"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("SnapshotVolume of a non-volume = %v, want FailedPrecondition", status.Code(err))
	}
}

type fakeExtentSnapshotter struct {
	calls [][2]string // records (srcVolumeID, dstVolumeID)
	err   error
}

func (f *fakeExtentSnapshotter) SnapshotMap(_ context.Context, src, dst string) error {
	f.calls = append(f.calls, [2]string{src, dst})
	return f.err
}

func TestNamespaceService_SnapshotVolumeClonesExtentMap(t *testing.T) {
	ctx := context.Background()

	// Snapshotter wired and the clone succeeds: the source id is resolved before
	// the snapshot, the out-of-band map is cloned src->dst, and the snapshot is
	// reported with no rollback.
	ops := &fakeNamespaceOps{id: "inode-snap", volInodeID: "inode-src"}
	snap := &fakeExtentSnapshotter{}
	del := &fakeExtentDeleter{}
	svc := NewNamespaceService(ops, discardNSLogger(), WithExtentDeleter(del), WithExtentSnapshotter(snap))
	resp, err := svc.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: "/vol", DestPath: "/snap"})
	if err != nil || resp.GetInode() != "inode-snap" {
		t.Fatalf("SnapshotVolume = (%v, %v), want inode-snap", resp, err)
	}
	if len(snap.calls) != 1 || snap.calls[0] != [2]string{"inode-src", "inode-snap"} {
		t.Errorf("SnapshotMap calls = %v, want one (inode-src, inode-snap)", snap.calls)
	}
	if len(ops.removed) != 0 || len(del.deleted) != 0 {
		t.Errorf("a successful snapshot must not roll back: removed=%v deleted=%v", ops.removed, del.deleted)
	}
}

func TestNamespaceService_SnapshotVolumeRollsBackOnCloneFailure(t *testing.T) {
	ctx := context.Background()

	// The clone cannot replicate: the snapshot is rolled back (namespace entry
	// removed, partial map deleted) and the call fails with Unavailable, so a
	// silently-empty snapshot is never reported ready.
	ops := &fakeNamespaceOps{id: "inode-snap", volInodeID: "inode-src"}
	snap := &fakeExtentSnapshotter{err: errors.New("quorum not reached")}
	del := &fakeExtentDeleter{}
	svc := NewNamespaceService(ops, discardNSLogger(), WithExtentDeleter(del), WithExtentSnapshotter(snap))
	if _, err := svc.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: "/vol", DestPath: "/snap"}); status.Code(err) != codes.Unavailable {
		t.Errorf("clone failure code = %v, want Unavailable", status.Code(err))
	}
	if len(ops.removed) != 1 || ops.removed[0] != "/snap" {
		t.Errorf("rollback should remove the snapshot path, got %v", ops.removed)
	}
	if len(del.deleted) != 1 || del.deleted[0] != "inode-snap" {
		t.Errorf("rollback should delete the partial extent map, got %v", del.deleted)
	}
}

func TestNamespaceService_SnapshotVolumeRollsBackWhenSourceUnresolved(t *testing.T) {
	ctx := context.Background()

	// The namespace accepted the snapshot but the source volume id could not be
	// resolved (VolumeInodeID errors): the snapshot would be silently empty, so
	// it is rolled back and fails Internal without ever calling the snapshotter.
	ops := &fakeNamespaceOps{id: "inode-snap", volInodeErr: errors.New("gone")}
	snap := &fakeExtentSnapshotter{}
	del := &fakeExtentDeleter{}
	svc := NewNamespaceService(ops, discardNSLogger(), WithExtentDeleter(del), WithExtentSnapshotter(snap))
	if _, err := svc.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: "/vol", DestPath: "/snap"}); status.Code(err) != codes.Internal {
		t.Errorf("unresolved source code = %v, want Internal", status.Code(err))
	}
	if len(snap.calls) != 0 {
		t.Errorf("the snapshotter must not be called when the source is unresolved: %v", snap.calls)
	}
	if len(ops.removed) != 1 || len(del.deleted) != 1 {
		t.Errorf("the empty snapshot should be rolled back: removed=%v deleted=%v", ops.removed, del.deleted)
	}
}

func TestNamespaceService_SnapshotRollbackToleratesFailures(t *testing.T) {
	ctx := context.Background()

	// Rollback is best-effort: even when the namespace removal AND the partial-map
	// delete both fail, the original clone error is still surfaced (Unavailable).
	ops := &fakeNamespaceOps{id: "inode-snap", volInodeID: "inode-src", removeErr: errors.New("remove boom")}
	snap := &fakeExtentSnapshotter{err: errors.New("quorum not reached")}
	del := &fakeExtentDeleter{err: errors.New("delete boom")}
	svc := NewNamespaceService(ops, discardNSLogger(), WithExtentDeleter(del), WithExtentSnapshotter(snap))
	if _, err := svc.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: "/vol", DestPath: "/snap"}); status.Code(err) != codes.Unavailable {
		t.Errorf("clone failure code = %v, want Unavailable even when rollback fails", status.Code(err))
	}
	if len(ops.removed) != 1 || len(del.deleted) != 1 {
		t.Errorf("rollback should still attempt both steps: removed=%v deleted=%v", ops.removed, del.deleted)
	}
}

func TestNamespaceService_SnapshotRollbackWithoutDeleter(t *testing.T) {
	ctx := context.Background()

	// A snapshotter wired without an extent deleter: a clone failure still rolls
	// the namespace entry back and fails, and the missing deleter is skipped
	// (the reaper reclaims any partial map instead).
	ops := &fakeNamespaceOps{id: "inode-snap", volInodeID: "inode-src"}
	snap := &fakeExtentSnapshotter{err: errors.New("quorum not reached")}
	svc := NewNamespaceService(ops, discardNSLogger(), WithExtentSnapshotter(snap))
	if _, err := svc.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: "/vol", DestPath: "/snap"}); status.Code(err) != codes.Unavailable {
		t.Errorf("clone failure code = %v, want Unavailable", status.Code(err))
	}
	if len(ops.removed) != 1 || ops.removed[0] != "/snap" {
		t.Errorf("rollback should remove the snapshot path even without a deleter, got %v", ops.removed)
	}
}

func TestNamespaceService_ListReportsVolumeType(t *testing.T) {
	svc := nsService(&fakeNamespaceOps{entries: []namespace.ResolvedEntry{
		{Name: "vol", Inode: "inode-1", Type: namespace.Volume},
	}})
	resp, err := svc.List(context.Background(), &namespacev1.ListRequest{Path: "/"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := resp.Entries[0].GetType(); got != namespacev1.EntryType_ENTRY_TYPE_VOLUME {
		t.Errorf("entry type = %v, want VOLUME", got)
	}
}

func TestNamespaceService_ErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"exists", namespace.ErrExists, codes.AlreadyExists},
		{"not exist", namespace.ErrNotExist, codes.NotFound},
		{"invalid path", namespace.ErrInvalidPath, codes.InvalidArgument},
		{"not dir", namespace.ErrNotDir, codes.InvalidArgument},
		{"not volume", namespace.ErrNotVolume, codes.FailedPrecondition},
		{"other", errors.New("disk on fire"), codes.Internal},
	}
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := nsService(&fakeNamespaceOps{err: tc.err})
			// Each RPC funnels through mapNamespaceError; Mkdir suffices to
			// exercise the mapping, the others share it.
			_, err := svc.Mkdir(ctx, &namespacev1.MkdirRequest{Path: "/x"})
			if status.Code(err) != tc.want {
				t.Errorf("got %v, want %v", status.Code(err), tc.want)
			}
		})
	}

	// Confirm the other RPCs also surface the mapped code, not just Mkdir.
	svc := nsService(&fakeNamespaceOps{err: namespace.ErrNotExist})
	if _, err := svc.Touch(ctx, &namespacev1.TouchRequest{Path: "/x"}); status.Code(err) != codes.NotFound {
		t.Errorf("Touch code = %v, want NotFound", status.Code(err))
	}
	if _, err := svc.Remove(ctx, &namespacev1.RemoveRequest{Path: "/x"}); status.Code(err) != codes.NotFound {
		t.Errorf("Remove code = %v, want NotFound", status.Code(err))
	}
	if _, err := svc.List(ctx, &namespacev1.ListRequest{Path: "/x"}); status.Code(err) != codes.NotFound {
		t.Errorf("List code = %v, want NotFound", status.Code(err))
	}
}
