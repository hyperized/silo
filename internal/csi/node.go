package csi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
)

// defaultFilesystem is used for a mount-type volume whose capability does not
// name one — ext4 is the safe, universally-available default.
const defaultFilesystem = "ext4"

// VolumeAttacher makes a silo volume present on the local host as a block
// device and tears it down. The real implementation drives the kernel NBD
// client against the node's own silod; both calls are idempotent so CSI's
// at-least-once retries are safe.
type VolumeAttacher interface {
	// Attach exposes the volume at volumePath (the namespace path, which is also
	// its NBD export name) as a local device and returns the device path. A
	// volume already attached returns its existing device.
	Attach(ctx context.Context, volumePath string) (device string, err error)
	// Detach removes the local block device for volumePath. A volume that is
	// not attached is not an error.
	Detach(ctx context.Context, volumePath string) error
}

// VolumeMounter performs the filesystem operations that make an attached device
// usable by a pod. A real implementation shells out to the host's mkfs/mount.
type VolumeMounter interface {
	// Mount makes device available at target. For a block volume it bind-mounts
	// the device node at target; for a filesystem volume it formats device with
	// fsType (only if it has no filesystem yet) and mounts it with flags. A
	// read-only request mounts read-only.
	Mount(ctx context.Context, device, target, fsType string, flags []string, block, readOnly bool) error
	// Unmount unmounts target; a target that is not mounted is not an error.
	Unmount(ctx context.Context, target string) error
}

// NodeService answers the CSI Node RPCs — the per-node half of the driver,
// running as a DaemonSet pod beside silod on every node. It attaches a volume
// over NBD (which takes the volume's lease and fences any prior holder) and
// mounts it into the pod. It does not implement staging: the attach is cheap
// and idempotent, so it happens at publish time.
type NodeService struct {
	attacher VolumeAttacher
	mounter  VolumeMounter
	nodeID   string
}

// NewNodeService builds the Node service. nodeID is reported to Kubernetes and
// keys VolumeAttachment objects to this node, so it must match the node's name.
func NewNodeService(nodeID string, attacher VolumeAttacher, mounter VolumeMounter) *NodeService {
	return &NodeService{attacher: attacher, mounter: mounter, nodeID: nodeID}
}

// NodePublishVolume attaches the volume and mounts it at the pod's target path.
// The volume id is the silo namespace path, which is also the NBD export name.
func (s *NodeService) NodePublishVolume(ctx context.Context, req *csiv1.NodePublishVolumeRequest) (*csiv1.NodePublishVolumeResponse, error) {
	volPath := req.GetVolumeId()
	target := req.GetTargetPath()
	capability := req.GetVolumeCapability()
	if volPath == "" || target == "" || capability == nil {
		return nil, status.Error(codes.InvalidArgument, "a volume id, a target path, and a volume capability are all required")
	}

	block := capability.GetBlock() != nil
	fsType := defaultFilesystem
	var flags []string
	if m := capability.GetMount(); m != nil {
		if m.GetFsType() != "" {
			fsType = m.GetFsType()
		}
		flags = m.GetMountFlags()
	}

	device, err := s.attacher.Attach(ctx, volPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not attach volume %q over NBD (%v); check that silod is running on this node and serving its NBD address", volPath, err)
	}
	if err := s.mounter.Mount(ctx, device, target, fsType, flags, block, req.GetReadonly()); err != nil {
		return nil, status.Errorf(codes.Internal, "attached volume %q as %s but could not mount it at %q (%v)", volPath, device, target, err)
	}
	return &csiv1.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the volume from the pod's target path and
// detaches it. Both steps are idempotent so a repeated teardown succeeds.
func (s *NodeService) NodeUnpublishVolume(ctx context.Context, req *csiv1.NodeUnpublishVolumeRequest) (*csiv1.NodeUnpublishVolumeResponse, error) {
	volPath := req.GetVolumeId()
	target := req.GetTargetPath()
	if volPath == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "both a volume id and a target path are required")
	}
	if err := s.mounter.Unmount(ctx, target); err != nil {
		return nil, status.Errorf(codes.Internal, "could not unmount volume %q at %q (%v)", volPath, target, err)
	}
	if err := s.attacher.Detach(ctx, volPath); err != nil {
		return nil, status.Errorf(codes.Internal, "unmounted volume %q but could not detach it (%v)", volPath, err)
	}
	return &csiv1.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetCapabilities reports that the node plugin needs no staging step and
// no optional features — publish does the whole attach-and-mount.
func (s *NodeService) NodeGetCapabilities(_ context.Context, _ *csiv1.NodeGetCapabilitiesRequest) (*csiv1.NodeGetCapabilitiesResponse, error) {
	return &csiv1.NodeGetCapabilitiesResponse{}, nil
}

// NodeGetInfo identifies this node to Kubernetes. silo derives placement from
// the chunk-id hash, not CSI topology, so no accessible topology is reported.
func (s *NodeService) NodeGetInfo(_ context.Context, _ *csiv1.NodeGetInfoRequest) (*csiv1.NodeGetInfoResponse, error) {
	return &csiv1.NodeGetInfoResponse{NodeId: s.nodeID}, nil
}

// The Node RPCs below are not part of silo's surface. NodeGetCapabilities does
// not advertise them, so the kubelet never calls them; a direct call gets a
// clear answer.

// NodeStageVolume is not implemented; the attach happens at publish time.
func (s *NodeService) NodeStageVolume(_ context.Context, _ *csiv1.NodeStageVolumeRequest) (*csiv1.NodeStageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi does not stage volumes; the attach happens at NodePublishVolume")
}

// NodeUnstageVolume is not implemented; see NodeStageVolume.
func (s *NodeService) NodeUnstageVolume(_ context.Context, _ *csiv1.NodeUnstageVolumeRequest) (*csiv1.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi does not stage volumes; the detach happens at NodeUnpublishVolume")
}

// NodeGetVolumeStats is not implemented yet.
func (s *NodeService) NodeGetVolumeStats(_ context.Context, _ *csiv1.NodeGetVolumeStatsRequest) (*csiv1.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi does not report per-volume stats yet")
}

// NodeExpandVolume is not implemented; volumes cannot grow in place yet.
func (s *NodeService) NodeExpandVolume(_ context.Context, _ *csiv1.NodeExpandVolumeRequest) (*csiv1.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi cannot expand volumes yet")
}

// Compile-time check that NodeService satisfies the generated server.
var _ csiv1.NodeServer = (*NodeService)(nil)
