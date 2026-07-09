package csi

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
	"github.com/hyperized/silo/internal/nbdclient"
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

// healthSource reports an attachment's condition; the NBD attacher implements
// it, and NodeGetVolumeStats surfaces it to the kubelet as a VolumeCondition.
type healthSource interface {
	Health(volumePath string) (AttachmentHealth, bool)
}

// NodeService answers the CSI Node RPCs — the per-node half of the driver,
// running as a DaemonSet pod beside silod on every node. It attaches a volume
// over NBD (which takes the volume's lease and fences any prior holder) and
// mounts it into the pod. It does not implement staging: the attach is cheap
// and idempotent, so it happens at publish time.
type NodeService struct {
	attacher VolumeAttacher
	mounter  VolumeMounter
	health   healthSource
	nodeID   string
}

// NewNodeService builds the Node service. nodeID is reported to Kubernetes and
// keys VolumeAttachment objects to this node, so it must match the node's name.
// An attacher that also reports health powers NodeGetVolumeStats' volume
// condition.
func NewNodeService(nodeID string, attacher VolumeAttacher, mounter VolumeMounter) *NodeService {
	s := &NodeService{attacher: attacher, mounter: mounter, nodeID: nodeID}
	if h, ok := attacher.(healthSource); ok {
		s.health = h
	}
	return s
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

// NodeGetCapabilities reports that the node plugin needs no staging step —
// publish does the whole attach-and-mount — but does serve volume stats with
// a volume condition, so the kubelet can see an attachment that lost silod.
func (s *NodeService) NodeGetCapabilities(_ context.Context, _ *csiv1.NodeGetCapabilitiesRequest) (*csiv1.NodeGetCapabilitiesResponse, error) {
	rpc := func(t csiv1.NodeServiceCapability_RPC_Type) *csiv1.NodeServiceCapability {
		return &csiv1.NodeServiceCapability{
			Type: &csiv1.NodeServiceCapability_Rpc{Rpc: &csiv1.NodeServiceCapability_RPC{Type: t}},
		}
	}
	return &csiv1.NodeGetCapabilitiesResponse{
		Capabilities: []*csiv1.NodeServiceCapability{
			rpc(csiv1.NodeServiceCapability_RPC_GET_VOLUME_STATS),
			rpc(csiv1.NodeServiceCapability_RPC_VOLUME_CONDITION),
		},
	}, nil
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

// NodeGetVolumeStats reports the volume's filesystem usage and — the part the
// kubelet acts on — its condition: whether the attachment is connected to
// silod or waiting out a reconnect.
func (s *NodeService) NodeGetVolumeStats(_ context.Context, req *csiv1.NodeGetVolumeStatsRequest) (*csiv1.NodeGetVolumeStatsResponse, error) {
	volPath := req.GetVolumeId()
	target := req.GetVolumePath()
	if volPath == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "both a volume id and a volume path are required")
	}
	if s.health == nil {
		return nil, status.Error(codes.Unimplemented, "this node plugin's attacher does not track attachment health")
	}
	health, ok := s.health.Health(volPath)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "volume %q is not attached on this node", volPath)
	}
	resp := &csiv1.NodeGetVolumeStatsResponse{VolumeCondition: volumeCondition(health)}
	if usage, err := statfsUsage(target); err == nil {
		resp.Usage = usage
	}
	return resp, nil
}

// volumeCondition translates an attachment's state into the CSI condition the
// kubelet publishes as events on the pod's PVC.
func volumeCondition(health AttachmentHealth) *csiv1.VolumeCondition {
	if health.State == nbdclient.StateHealthy {
		return &csiv1.VolumeCondition{Abnormal: false, Message: fmt.Sprintf("connected to silod via %s", health.Device)}
	}
	return &csiv1.VolumeCondition{
		Abnormal: true,
		Message:  fmt.Sprintf("the connection to silod is down; I/O on %s is paused while it reconnects — check silod on this node if this persists", health.Device),
	}
}

// NodeExpandVolume is not implemented; volumes cannot grow in place yet.
func (s *NodeService) NodeExpandVolume(_ context.Context, _ *csiv1.NodeExpandVolumeRequest) (*csiv1.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "silo-csi cannot expand volumes yet")
}

// Compile-time check that NodeService satisfies the generated server.
var _ csiv1.NodeServer = (*NodeService)(nil)
