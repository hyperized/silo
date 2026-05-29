package csi

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
)

// NamespaceClient is the slice of the silod namespace gRPC API the backend
// needs. *namespacev1.NamespaceStoreClient satisfies it; tests supply a fake.
type NamespaceClient interface {
	Mkdir(ctx context.Context, in *namespacev1.MkdirRequest, opts ...grpc.CallOption) (*namespacev1.MkdirResponse, error)
	CreateVolume(ctx context.Context, in *namespacev1.CreateVolumeRequest, opts ...grpc.CallOption) (*namespacev1.CreateVolumeResponse, error)
	SnapshotVolume(ctx context.Context, in *namespacev1.SnapshotVolumeRequest, opts ...grpc.CallOption) (*namespacev1.SnapshotVolumeResponse, error)
	Remove(ctx context.Context, in *namespacev1.RemoveRequest, opts ...grpc.CallOption) (*namespacev1.RemoveResponse, error)
}

// NamespaceBackend implements VolumeStore against a silod namespace replica. It
// maps each CSI name onto a stable path under /csi (so the path doubles as the
// CSI id) and absorbs the at-least-once retries CSI makes: an already-created
// volume reports success, an already-deleted one too. Because every operation
// is a namespace mutation, it converges to the whole cluster over gossip — the
// controller can run against any node.
type NamespaceBackend struct {
	client NamespaceClient
}

// NewNamespaceBackend wires the backend onto a namespace gRPC client.
func NewNamespaceBackend(client NamespaceClient) *NamespaceBackend {
	return &NamespaceBackend{client: client}
}

// CreateVolume creates /csi/volumes/<name>, returning that path as the id. An
// existing volume of the same name is treated as success (idempotent create).
func (b *NamespaceBackend) CreateVolume(ctx context.Context, name string, sizeBytes, extentSize int64) (string, error) {
	id, err := b.volumePath(name)
	if err != nil {
		return "", err
	}
	if err := b.ensureDir(ctx, volumesDir); err != nil {
		return "", err
	}
	_, err = b.client.CreateVolume(ctx, &namespacev1.CreateVolumeRequest{Path: id, SizeBytes: sizeBytes, ExtentSizeBytes: extentSize})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return "", err
	}
	return id, nil
}

// CloneVolume creates /csi/volumes/<name> as a copy-on-write copy of sourceID
// (an existing volume or snapshot path). Idempotent on the destination name.
func (b *NamespaceBackend) CloneVolume(ctx context.Context, sourceID, name string) (string, error) {
	id, err := b.volumePath(name)
	if err != nil {
		return "", err
	}
	if err := b.ensureDir(ctx, volumesDir); err != nil {
		return "", err
	}
	if _, err := b.client.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: sourceID, DestPath: id}); err != nil && status.Code(err) != codes.AlreadyExists {
		return "", err
	}
	return id, nil
}

// DeleteVolume removes the volume at id; a missing volume is success.
func (b *NamespaceBackend) DeleteVolume(ctx context.Context, id string) error {
	return b.remove(ctx, id)
}

// CreateSnapshot freezes sourceVolumeID into /csi/snapshots/<name>, returning
// that path as the snapshot id. Idempotent on the snapshot name.
func (b *NamespaceBackend) CreateSnapshot(ctx context.Context, sourceVolumeID, name string) (string, error) {
	id, err := join(snapshotsDir, name)
	if err != nil {
		return "", err
	}
	if err := b.ensureDir(ctx, snapshotsDir); err != nil {
		return "", err
	}
	if _, err := b.client.SnapshotVolume(ctx, &namespacev1.SnapshotVolumeRequest{SourcePath: sourceVolumeID, DestPath: id}); err != nil && status.Code(err) != codes.AlreadyExists {
		return "", err
	}
	return id, nil
}

// DeleteSnapshot removes the snapshot at id; a missing snapshot is success.
func (b *NamespaceBackend) DeleteSnapshot(ctx context.Context, id string) error {
	return b.remove(ctx, id)
}

// remove deletes a namespace path, swallowing NotFound so deletes are idempotent.
func (b *NamespaceBackend) remove(ctx context.Context, path string) error {
	if path == "" {
		return status.Error(codes.InvalidArgument, "an id is required")
	}
	if _, err := b.client.Remove(ctx, &namespacev1.RemoveRequest{Path: path}); err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	return nil
}

// volumePath maps a CSI volume name to its namespace path under /csi/volumes.
func (b *NamespaceBackend) volumePath(name string) (string, error) { return join(volumesDir, name) }

// ensureDir creates dir and every ancestor, treating an already-existing
// component as success — the silo equivalent of `mkdir -p` for the /csi tree.
func (b *NamespaceBackend) ensureDir(ctx context.Context, dir string) error {
	path := ""
	for _, seg := range strings.Split(strings.Trim(dir, "/"), "/") {
		path += "/" + seg
		if _, err := b.client.Mkdir(ctx, &namespacev1.MkdirRequest{Path: path}); err != nil && status.Code(err) != codes.AlreadyExists {
			return err
		}
	}
	return nil
}

// join builds the namespace path dir/name, rejecting a name that would escape
// its directory (CSI names are flat ids like "pvc-<uuid>").
func join(dir, name string) (string, error) {
	if name == "" || strings.ContainsAny(name, "/") || name == "." || name == ".." {
		return "", status.Errorf(codes.InvalidArgument, "%q is not a valid name; CSI names are flat identifiers without slashes", name)
	}
	return fmt.Sprintf("%s/%s", dir, name), nil
}

// Compile-time check that NamespaceBackend satisfies the controller's store.
var _ VolumeStore = (*NamespaceBackend)(nil)
