package csi

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
)

// VolumeStore is the slice of silo the controller drives. The namespaceBackend
// adapter (over a silod namespace gRPC client) satisfies it. Every method is
// idempotent, mirroring CSI's at-least-once delivery: creating a volume that
// already exists returns its id, deleting one that is already gone succeeds.
type VolumeStore interface {
	// CreateVolume provisions a block volume named name of sizeBytes, with an
	// optional copy-on-write extent size (0 = silod default), and returns its
	// opaque volume id.
	CreateVolume(ctx context.Context, name string, sizeBytes, extentSize int64) (volumeID string, err error)
	// CloneVolume provisions a new volume named name whose contents are a
	// copy-on-write copy of an existing volume or snapshot (sourceID). It backs
	// both "create from snapshot" and "clone volume" content sources.
	CloneVolume(ctx context.Context, sourceID, name string) (volumeID string, err error)
	// DeleteVolume removes the volume; a missing volume is not an error.
	DeleteVolume(ctx context.Context, volumeID string) error
	// CreateSnapshot freezes sourceVolumeID into a snapshot named name and
	// returns its opaque snapshot id.
	CreateSnapshot(ctx context.Context, sourceVolumeID, name string) (snapshotID string, err error)
	// DeleteSnapshot removes the snapshot; a missing snapshot is not an error.
	DeleteSnapshot(ctx context.Context, snapshotID string) error
}

// ControllerService answers the CSI Controller RPCs — the cluster-wide volume
// lifecycle (provision, delete, snapshot, clone). It runs once per cluster
// (alongside the external-provisioner/attacher/snapshotter sidecars), not on
// every node. It owns no state: each call is a translation into a VolumeStore
// operation against the CRDT namespace.
type ControllerService struct {
	store VolumeStore
	now   func() time.Time
}

// ControllerOption configures a ControllerService.
type ControllerOption func(*ControllerService)

// WithClock overrides the source of snapshot creation timestamps (tests pin it
// for determinism).
func WithClock(now func() time.Time) ControllerOption {
	return func(s *ControllerService) { s.now = now }
}

// NewControllerService builds the Controller service over store.
func NewControllerService(store VolumeStore, opts ...ControllerOption) *ControllerService {
	s := &ControllerService{store: store, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// CreateVolume provisions a block volume. It is the entry point the external-
// provisioner calls for every PersistentVolumeClaim. With a volume_content_source
// it instead clones an existing snapshot or volume (silo snapshots and clones
// are the same copy-on-write freeze of the extent map).
func (s *ControllerService) CreateVolume(ctx context.Context, req *csiv1.CreateVolumeRequest) (*csiv1.CreateVolumeResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "a volume name is required; the external-provisioner supplies one derived from the PVC")
	}
	if err := validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	size, err := capacityBytes(req.GetCapacityRange())
	if err != nil {
		return nil, err
	}
	extent, err := parseByteSize(req.GetParameters()["chunk-size"])
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chunk-size parameter %q (%v); use a byte count optionally suffixed Ki, Mi, Gi, or Ti", req.GetParameters()["chunk-size"], err)
	}

	var id string
	switch src := req.GetVolumeContentSource(); {
	case src.GetSnapshot() != nil:
		id, err = s.store.CloneVolume(ctx, src.GetSnapshot().GetSnapshotId(), name)
	case src.GetVolume() != nil:
		id, err = s.store.CloneVolume(ctx, src.GetVolume().GetVolumeId(), name)
	default:
		id, err = s.store.CreateVolume(ctx, name, size, extent)
	}
	if err != nil {
		return nil, err
	}

	return &csiv1.CreateVolumeResponse{Volume: &csiv1.Volume{
		VolumeId:      id,
		CapacityBytes: size,
		VolumeContext: map[string]string{
			contextPath:      id,
			contextSizeBytes: strconv.FormatInt(size, 10),
		},
		ContentSource: req.GetVolumeContentSource(),
	}}, nil
}

// DeleteVolume removes a volume. CSI requires it to be idempotent, so a volume
// that is already gone is a success.
func (s *ControllerService) DeleteVolume(ctx context.Context, req *csiv1.DeleteVolumeRequest) (*csiv1.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "a volume id is required")
	}
	if err := s.store.DeleteVolume(ctx, req.GetVolumeId()); err != nil {
		return nil, err
	}
	return &csiv1.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume marks a volume as attachable to a node. silo has no
// controller-side attach step — a node attaches by taking the volume's lease
// over NBD, and the lease itself fences any stale holder — so this just hands
// the node the volume's path to attach. The capability exists so the external-
// attacher creates VolumeAttachment objects, giving Kubernetes a clean
// attach/detach ordering signal.
func (s *ControllerService) ControllerPublishVolume(_ context.Context, req *csiv1.ControllerPublishVolumeRequest) (*csiv1.ControllerPublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "both a volume id and a node id are required to publish")
	}
	if c := req.GetVolumeCapability(); c != nil {
		if err := validateVolumeCapabilities([]*csiv1.VolumeCapability{c}); err != nil {
			return nil, err
		}
	}
	return &csiv1.ControllerPublishVolumeResponse{PublishContext: map[string]string{contextPath: req.GetVolumeId()}}, nil
}

// ControllerUnpublishVolume is the detach counterpart. The lease is released
// when the node unpublishes, so there is nothing to do here.
func (s *ControllerService) ControllerUnpublishVolume(_ context.Context, req *csiv1.ControllerUnpublishVolumeRequest) (*csiv1.ControllerUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "a volume id is required")
	}
	return &csiv1.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities confirms whether the driver can satisfy the
// requested access modes for a volume. Unsupported capabilities are reported by
// an empty Confirmed with an explanatory Message, per the CSI contract.
func (s *ControllerService) ValidateVolumeCapabilities(_ context.Context, req *csiv1.ValidateVolumeCapabilitiesRequest) (*csiv1.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "a volume id is required")
	}
	if err := validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csiv1.ValidateVolumeCapabilitiesResponse{Message: status.Convert(err).Message()}, nil
	}
	return &csiv1.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csiv1.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: req.GetVolumeCapabilities()},
	}, nil
}

// ControllerGetCapabilities advertises the Controller RPCs silo implements.
func (s *ControllerService) ControllerGetCapabilities(_ context.Context, _ *csiv1.ControllerGetCapabilitiesRequest) (*csiv1.ControllerGetCapabilitiesResponse, error) {
	rpcs := []csiv1.ControllerServiceCapability_RPC_Type{
		csiv1.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csiv1.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csiv1.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csiv1.ControllerServiceCapability_RPC_CLONE_VOLUME,
	}
	caps := make([]*csiv1.ControllerServiceCapability, 0, len(rpcs))
	for _, t := range rpcs {
		caps = append(caps, &csiv1.ControllerServiceCapability{
			Type: &csiv1.ControllerServiceCapability_Rpc{Rpc: &csiv1.ControllerServiceCapability_RPC{Type: t}},
		})
	}
	return &csiv1.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

// CreateSnapshot freezes a volume into a point-in-time, copy-on-write snapshot.
// silo snapshots are synchronous (the extent map is frozen immediately), so the
// snapshot is ready to use the moment this returns.
func (s *ControllerService) CreateSnapshot(ctx context.Context, req *csiv1.CreateSnapshotRequest) (*csiv1.CreateSnapshotResponse, error) {
	if req.GetSourceVolumeId() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "both a source volume id and a snapshot name are required")
	}
	id, err := s.store.CreateSnapshot(ctx, req.GetSourceVolumeId(), req.GetName())
	if err != nil {
		return nil, err
	}
	return &csiv1.CreateSnapshotResponse{Snapshot: &csiv1.Snapshot{
		SnapshotId:     id,
		SourceVolumeId: req.GetSourceVolumeId(),
		CreationTime:   timestamppb.New(s.now()),
		ReadyToUse:     true,
	}}, nil
}

// DeleteSnapshot removes a snapshot, idempotently.
func (s *ControllerService) DeleteSnapshot(ctx context.Context, req *csiv1.DeleteSnapshotRequest) (*csiv1.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "a snapshot id is required")
	}
	if err := s.store.DeleteSnapshot(ctx, req.GetSnapshotId()); err != nil {
		return nil, err
	}
	return &csiv1.DeleteSnapshotResponse{}, nil
}

// The Controller RPCs below are not part of silo's surface yet. Each returns
// Unimplemented — ControllerGetCapabilities does not advertise them, so sidecars
// never call them, but a direct call gets a clear answer rather than a crash.

// ListVolumes is not implemented; browse volumes with `siloctl ns ls`.
func (s *ControllerService) ListVolumes(_ context.Context, _ *csiv1.ListVolumesRequest) (*csiv1.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi does not list volumes; browse them with `siloctl ns ls /csi/volumes`")
}

// GetCapacity is not implemented; check capacity with `siloctl status`.
func (s *ControllerService) GetCapacity(_ context.Context, _ *csiv1.GetCapacityRequest) (*csiv1.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi does not report capacity; check cluster capacity with `siloctl status`")
}

// ListSnapshots is not implemented; browse snapshots with `siloctl ns ls`.
func (s *ControllerService) ListSnapshots(_ context.Context, _ *csiv1.ListSnapshotsRequest) (*csiv1.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi does not list snapshots; browse them with `siloctl ns ls /csi/snapshots`")
}

// ControllerExpandVolume is not implemented; volumes cannot grow in place yet.
func (s *ControllerService) ControllerExpandVolume(_ context.Context, _ *csiv1.ControllerExpandVolumeRequest) (*csiv1.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi cannot expand volumes yet; create a larger volume and copy the data")
}

// ControllerGetVolume is not implemented.
func (s *ControllerService) ControllerGetVolume(_ context.Context, _ *csiv1.ControllerGetVolumeRequest) (*csiv1.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi does not implement ControllerGetVolume")
}

// ControllerModifyVolume is not implemented.
func (s *ControllerService) ControllerModifyVolume(_ context.Context, _ *csiv1.ControllerModifyVolumeRequest) (*csiv1.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi does not implement ControllerModifyVolume")
}

// Volume-context keys the controller stamps onto a created volume; the node
// service reads them back when it attaches the volume.
const (
	contextPath      = "silo.path"
	contextSizeBytes = "silo.sizeBytes"
)

// capacityBytes resolves the requested volume size from a CSI CapacityRange,
// preferring the required minimum and falling back to the upper limit.
func capacityBytes(r *csiv1.CapacityRange) (int64, error) {
	switch {
	case r.GetRequiredBytes() > 0:
		return r.GetRequiredBytes(), nil
	case r.GetLimitBytes() > 0:
		return r.GetLimitBytes(), nil
	default:
		return 0, status.Error(codes.InvalidArgument, "a volume size is required; set spec.resources.requests.storage on the PersistentVolumeClaim")
	}
}

// validateVolumeCapabilities rejects access modes silo cannot honour. A silo
// block volume is single-writer (a fenced lease), so any multi-node writer mode
// is refused — that is exactly ReadWriteOnce semantics.
func validateVolumeCapabilities(caps []*csiv1.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "at least one volume capability is required")
	}
	for _, c := range caps {
		mode := c.GetAccessMode().GetMode()
		if !accessModeSupported(mode) {
			return status.Errorf(codes.InvalidArgument, "silo block volumes are single-writer (ReadWriteOnce); access mode %s is not supported", mode)
		}
	}
	return nil
}

// accessModeSupported reports whether a CSI access mode fits silo's single-
// writer block volume: any single-node mode is fine; multi-node modes are not.
func accessModeSupported(m csiv1.VolumeCapability_AccessMode_Mode) bool {
	switch m {
	case csiv1.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csiv1.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csiv1.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		csiv1.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
		return true
	default:
		return false
	}
}

// parseByteSize parses a byte count with an optional binary unit suffix — both
// the Kubernetes-style Ki/Mi/Gi/Ti and the bare K/M/G/T are powers of 1024. An
// empty string means "unset" and returns 0 so the caller can apply its default.
func parseByteSize(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	u := strings.TrimSpace(s)
	if n := len(u); n > 0 && (u[n-1] == 'i' || u[n-1] == 'I') {
		u = u[:n-1]
	}
	mult := int64(1)
	if n := len(u); n > 0 {
		switch u[n-1] {
		case 'K', 'k':
			mult, u = 1<<10, u[:n-1]
		case 'M', 'm':
			mult, u = 1<<20, u[:n-1]
		case 'G', 'g':
			mult, u = 1<<30, u[:n-1]
		case 'T', 't':
			mult, u = 1<<40, u[:n-1]
		}
	}
	val, err := strconv.ParseInt(strings.TrimSpace(u), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a byte count")
	}
	if val < 0 {
		return 0, fmt.Errorf("must not be negative")
	}
	res := val * mult
	if mult != 0 && res/mult != val {
		return 0, fmt.Errorf("overflows a 64-bit byte count")
	}
	return res, nil
}

// Compile-time check that ControllerService satisfies the generated server.
var _ csiv1.ControllerServer = (*ControllerService)(nil)
