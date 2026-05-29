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
	id      string
	err     error
	entries []namespace.ResolvedEntry
}

func (f *fakeNamespaceOps) Mkdir(string) (string, error) { return f.id, f.err }
func (f *fakeNamespaceOps) Touch(string) (string, error) { return f.id, f.err }
func (f *fakeNamespaceOps) Remove(string) error          { return f.err }
func (f *fakeNamespaceOps) List(string) ([]namespace.ResolvedEntry, error) {
	return f.entries, f.err
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
