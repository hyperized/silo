package transport

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/namespace"
)

// NamespaceOps is the slice of the namespace the gRPC service drives.
// *namespace.Namespace satisfies it.
type NamespaceOps interface {
	Mkdir(path string) (string, error)
	Touch(path string) (string, error)
	Remove(path string) error
	List(path string) ([]namespace.ResolvedEntry, error)
	AppendChunk(path, chunkID string) error
	Manifest(path string) ([]string, error)
	CreateVolume(path string, extentSize int64, opts ...namespace.VolumeOption) (string, error)
	SnapshotVolume(srcPath, dstPath string) (string, error)
	VolumeInodeID(path string) (string, error)
}

// ExtentDeleter removes a volume's extent map from its replica set.
// *replication.ExtentCoordinator satisfies it via DeleteMap. When wired (see
// WithExtentDeleter), Remove deletes a volume's map synchronously as the volume
// is removed — the prompt half of the delete path; the extent reaper is the
// backstop. Without it, Remove touches only the directory tree and the reaper
// alone reclaims the map.
type ExtentDeleter interface {
	DeleteMap(ctx context.Context, volumeID string) error
}

// NamespaceService exposes the node's namespace replica over gRPC. Writes
// mutate the local replica and converge to peers over gossip; reads resolve
// the local replica's current view.
type NamespaceService struct {
	namespacev1.UnimplementedNamespaceStoreServer

	ns            NamespaceOps
	extentDeleter ExtentDeleter
	logger        *slog.Logger
}

// NamespaceOption configures a NamespaceService at construction.
type NamespaceOption func(*NamespaceService)

// WithExtentDeleter makes Remove also delete a removed volume's extent map from
// its replica set, so a deleted volume's map does not outlive it.
func WithExtentDeleter(d ExtentDeleter) NamespaceOption {
	return func(s *NamespaceService) { s.extentDeleter = d }
}

// NewNamespaceService wires a namespace replica onto the gRPC surface.
func NewNamespaceService(ns NamespaceOps, logger *slog.Logger, opts ...NamespaceOption) *NamespaceService {
	s := &NamespaceService{ns: ns, logger: logger}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Mkdir creates a directory at the requested path.
func (s *NamespaceService) Mkdir(_ context.Context, req *namespacev1.MkdirRequest) (*namespacev1.MkdirResponse, error) {
	id, err := s.ns.Mkdir(req.GetPath())
	if err != nil {
		return nil, mapNamespaceError(err)
	}
	return &namespacev1.MkdirResponse{Inode: id}, nil
}

// Touch creates an empty file at the requested path.
func (s *NamespaceService) Touch(_ context.Context, req *namespacev1.TouchRequest) (*namespacev1.TouchResponse, error) {
	id, err := s.ns.Touch(req.GetPath())
	if err != nil {
		return nil, mapNamespaceError(err)
	}
	return &namespacev1.TouchResponse{Inode: id}, nil
}

// Remove deletes the entry at the requested path. When an extent deleter is
// wired and the path is a volume, its extent map is also deleted from the
// replica set — captured before removal (afterwards the path no longer
// resolves) and best-effort afterwards (a failure is logged, not returned: the
// volume is logically gone and the reaper reclaims any leftover map).
func (s *NamespaceService) Remove(ctx context.Context, req *namespacev1.RemoveRequest) (*namespacev1.RemoveResponse, error) {
	var volumeID string
	if s.extentDeleter != nil {
		if id, err := s.ns.VolumeInodeID(req.GetPath()); err == nil {
			volumeID = id // a non-volume (dir, or missing) yields no id and is skipped
		}
	}
	if err := s.ns.Remove(req.GetPath()); err != nil {
		return nil, mapNamespaceError(err)
	}
	if volumeID != "" {
		if err := s.extentDeleter.DeleteMap(ctx, volumeID); err != nil {
			s.logger.Warn("could not delete a volume's extent map on removal; the reaper will reclaim it", "volume", volumeID, "error", err)
		}
	}
	return &namespacev1.RemoveResponse{}, nil
}

// List returns the conflict-resolved children of the directory at path.
func (s *NamespaceService) List(_ context.Context, req *namespacev1.ListRequest) (*namespacev1.ListResponse, error) {
	entries, err := s.ns.List(req.GetPath())
	if err != nil {
		return nil, mapNamespaceError(err)
	}
	resp := &namespacev1.ListResponse{Entries: make([]*namespacev1.Entry, 0, len(entries))}
	for _, e := range entries {
		resp.Entries = append(resp.Entries, &namespacev1.Entry{
			Name:     e.Name,
			Inode:    e.Inode,
			Type:     protoEntryType(e.Type),
			Conflict: e.Conflict,
		})
	}
	return resp, nil
}

// AppendChunk records a chunk id in the file's manifest. The chunk id is
// validated against the chunk store's allowlist before it is stored, so a
// misbehaving client cannot poison the manifest with an id the store would
// reject (or one that could traverse the data directory).
func (s *NamespaceService) AppendChunk(_ context.Context, req *namespacev1.AppendChunkRequest) (*namespacev1.AppendChunkResponse, error) {
	if err := chunkstore.ValidateID(req.GetChunkId()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "the chunk id %q is not valid (%v); append the id silod returned from a chunk put", req.GetChunkId(), err)
	}
	if err := s.ns.AppendChunk(req.GetPath(), req.GetChunkId()); err != nil {
		return nil, mapNamespaceError(err)
	}
	return &namespacev1.AppendChunkResponse{}, nil
}

// Manifest returns the file's chunk ids in append (HLC) order.
func (s *NamespaceService) Manifest(_ context.Context, req *namespacev1.ManifestRequest) (*namespacev1.ManifestResponse, error) {
	ids, err := s.ns.Manifest(req.GetPath())
	if err != nil {
		return nil, mapNamespaceError(err)
	}
	return &namespacev1.ManifestResponse{ChunkIds: ids}, nil
}

// CreateVolume creates a block volume at the requested path with the given
// device and extent sizes (a zero extent size uses the server default).
func (s *NamespaceService) CreateVolume(_ context.Context, req *namespacev1.CreateVolumeRequest) (*namespacev1.CreateVolumeResponse, error) {
	id, err := s.ns.CreateVolume(req.GetPath(), req.GetExtentSizeBytes(), namespace.WithSize(req.GetSizeBytes()))
	if err != nil {
		return nil, mapNamespaceError(err)
	}
	return &namespacev1.CreateVolumeResponse{Inode: id}, nil
}

// SnapshotVolume freezes the source volume's extent map into a new volume at
// the destination path — a point-in-time, copy-on-write copy.
func (s *NamespaceService) SnapshotVolume(_ context.Context, req *namespacev1.SnapshotVolumeRequest) (*namespacev1.SnapshotVolumeResponse, error) {
	id, err := s.ns.SnapshotVolume(req.GetSourcePath(), req.GetDestPath())
	if err != nil {
		return nil, mapNamespaceError(err)
	}
	return &namespacev1.SnapshotVolumeResponse{Inode: id}, nil
}

func protoEntryType(t namespace.InodeType) namespacev1.EntryType {
	switch t {
	case namespace.Dir:
		return namespacev1.EntryType_ENTRY_TYPE_DIR
	case namespace.Volume:
		return namespacev1.EntryType_ENTRY_TYPE_VOLUME
	default:
		return namespacev1.EntryType_ENTRY_TYPE_FILE
	}
}

// mapNamespaceError translates a namespace sentinel into a gRPC status,
// keeping the actionable message text for the operator.
func mapNamespaceError(err error) error {
	switch {
	case errors.Is(err, namespace.ErrExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, namespace.ErrNotExist):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, namespace.ErrInvalidPath), errors.Is(err, namespace.ErrNotDir):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, namespace.ErrNotVolume):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// Compile-time check that NamespaceService satisfies the generated server.
var _ namespacev1.NamespaceStoreServer = (*NamespaceService)(nil)
