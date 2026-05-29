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
}

func (f *fakeNamespaceOps) Mkdir(string) (string, error) { return f.id, f.err }
func (f *fakeNamespaceOps) Touch(string) (string, error) { return f.id, f.err }
func (f *fakeNamespaceOps) Remove(string) error          { return f.err }
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
	return NewNamespaceService(ops, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
