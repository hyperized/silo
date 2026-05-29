package csi_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
	"github.com/hyperized/silo/internal/csi"
)

type fakeAttacher struct {
	attached  string
	detached  string
	device    string
	attachErr error
	detachErr error
}

func (f *fakeAttacher) Attach(_ context.Context, volumePath string) (string, error) {
	f.attached = volumePath
	if f.attachErr != nil {
		return "", f.attachErr
	}
	if f.device == "" {
		return "/dev/nbd0", nil
	}
	return f.device, nil
}

func (f *fakeAttacher) Detach(_ context.Context, volumePath string) error {
	f.detached = volumePath
	return f.detachErr
}

type fakeMounter struct {
	device, target, fsType string
	flags                  []string
	block, readOnly        bool
	unmounted              string
	mountErr               error
	unmountErr             error
}

func (f *fakeMounter) Mount(_ context.Context, device, target, fsType string, flags []string, block, readOnly bool) error {
	f.device, f.target, f.fsType, f.flags, f.block, f.readOnly = device, target, fsType, flags, block, readOnly
	return f.mountErr
}

func (f *fakeMounter) Unmount(_ context.Context, target string) error {
	f.unmounted = target
	return f.unmountErr
}

func mountCap(fsType string) *csiv1.VolumeCapability {
	return &csiv1.VolumeCapability{
		AccessType: &csiv1.VolumeCapability_Mount{Mount: &csiv1.VolumeCapability_MountVolume{FsType: fsType, MountFlags: []string{"noatime"}}},
		AccessMode: &csiv1.VolumeCapability_AccessMode{Mode: csiv1.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func blockCap() *csiv1.VolumeCapability {
	return &csiv1.VolumeCapability{
		AccessType: &csiv1.VolumeCapability_Block{Block: &csiv1.VolumeCapability_BlockVolume{}},
		AccessMode: &csiv1.VolumeCapability_AccessMode{Mode: csiv1.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func TestNodePublishVolume_Mount(t *testing.T) {
	ctx := context.Background()
	att := &fakeAttacher{device: "/dev/nbd3"}
	mnt := &fakeMounter{}
	svc := csi.NewNodeService("node-a", att, mnt)

	_, err := svc.NodePublishVolume(ctx, &csiv1.NodePublishVolumeRequest{
		VolumeId: "/csi/volumes/pvc-1", TargetPath: "/var/lib/kubelet/pods/p/vol", VolumeCapability: mountCap("xfs"),
	})
	if err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	if att.attached != "/csi/volumes/pvc-1" {
		t.Errorf("attached %q", att.attached)
	}
	if mnt.device != "/dev/nbd3" || mnt.target != "/var/lib/kubelet/pods/p/vol" || mnt.fsType != "xfs" || mnt.block {
		t.Errorf("mount = %+v, want device /dev/nbd3 xfs non-block", mnt)
	}
	if len(mnt.flags) != 1 || mnt.flags[0] != "noatime" {
		t.Errorf("flags = %v, want [noatime]", mnt.flags)
	}
}

func TestNodePublishVolume_BlockAndDefaultFS(t *testing.T) {
	ctx := context.Background()

	// Block volume: mounter is told block=true.
	att, mnt := &fakeAttacher{}, &fakeMounter{}
	if _, err := csi.NewNodeService("n", att, mnt).NodePublishVolume(ctx, &csiv1.NodePublishVolumeRequest{
		VolumeId: "/csi/volumes/b", TargetPath: "/t", VolumeCapability: blockCap(),
	}); err != nil {
		t.Fatalf("publish block: %v", err)
	}
	if !mnt.block {
		t.Error("block volume should set block=true")
	}

	// Mount volume with no fs type defaults to ext4.
	att2, mnt2 := &fakeAttacher{}, &fakeMounter{}
	if _, err := csi.NewNodeService("n", att2, mnt2).NodePublishVolume(ctx, &csiv1.NodePublishVolumeRequest{
		VolumeId: "/csi/volumes/m", TargetPath: "/t", VolumeCapability: mountCap(""),
	}); err != nil {
		t.Fatalf("publish default fs: %v", err)
	}
	if mnt2.fsType != "ext4" {
		t.Errorf("default fs = %q, want ext4", mnt2.fsType)
	}
}

func TestNodePublishVolume_Errors(t *testing.T) {
	ctx := context.Background()
	svc := csi.NewNodeService("n", &fakeAttacher{}, &fakeMounter{})

	bad := []*csiv1.NodePublishVolumeRequest{
		{TargetPath: "/t", VolumeCapability: blockCap()}, // no id
		{VolumeId: "/v", VolumeCapability: blockCap()},   // no target
		{VolumeId: "/v", TargetPath: "/t"},               // no capability
	}
	for i, req := range bad {
		if _, err := svc.NodePublishVolume(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("bad req %d = %v, want InvalidArgument", i, status.Code(err))
		}
	}

	// Attach failure and mount failure both surface as Internal.
	attBoom := csi.NewNodeService("n", &fakeAttacher{attachErr: errors.New("no silod")}, &fakeMounter{})
	if _, err := attBoom.NodePublishVolume(ctx, &csiv1.NodePublishVolumeRequest{VolumeId: "/v", TargetPath: "/t", VolumeCapability: blockCap()}); status.Code(err) != codes.Internal {
		t.Errorf("attach failure = %v, want Internal", status.Code(err))
	}
	mntBoom := csi.NewNodeService("n", &fakeAttacher{}, &fakeMounter{mountErr: errors.New("mkfs failed")})
	if _, err := mntBoom.NodePublishVolume(ctx, &csiv1.NodePublishVolumeRequest{VolumeId: "/v", TargetPath: "/t", VolumeCapability: blockCap()}); status.Code(err) != codes.Internal {
		t.Errorf("mount failure = %v, want Internal", status.Code(err))
	}
}

func TestNodeUnpublishVolume(t *testing.T) {
	ctx := context.Background()
	att, mnt := &fakeAttacher{}, &fakeMounter{}
	svc := csi.NewNodeService("n", att, mnt)

	if _, err := svc.NodeUnpublishVolume(ctx, &csiv1.NodeUnpublishVolumeRequest{VolumeId: "/csi/volumes/pvc-1", TargetPath: "/t"}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if mnt.unmounted != "/t" || att.detached != "/csi/volumes/pvc-1" {
		t.Errorf("unmounted %q, detached %q", mnt.unmounted, att.detached)
	}

	if _, err := svc.NodeUnpublishVolume(ctx, &csiv1.NodeUnpublishVolumeRequest{TargetPath: "/t"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("missing id = %v, want InvalidArgument", status.Code(err))
	}

	// Unmount failure and detach failure surface as Internal.
	umBoom := csi.NewNodeService("n", &fakeAttacher{}, &fakeMounter{unmountErr: errors.New("busy")})
	if _, err := umBoom.NodeUnpublishVolume(ctx, &csiv1.NodeUnpublishVolumeRequest{VolumeId: "/v", TargetPath: "/t"}); status.Code(err) != codes.Internal {
		t.Errorf("unmount failure = %v, want Internal", status.Code(err))
	}
	deBoom := csi.NewNodeService("n", &fakeAttacher{detachErr: errors.New("busy")}, &fakeMounter{})
	if _, err := deBoom.NodeUnpublishVolume(ctx, &csiv1.NodeUnpublishVolumeRequest{VolumeId: "/v", TargetPath: "/t"}); status.Code(err) != codes.Internal {
		t.Errorf("detach failure = %v, want Internal", status.Code(err))
	}
}

func TestNodeInfoCapabilitiesAndUnimplemented(t *testing.T) {
	ctx := context.Background()
	svc := csi.NewNodeService("node-7", &fakeAttacher{}, &fakeMounter{})

	info, err := svc.NodeGetInfo(ctx, &csiv1.NodeGetInfoRequest{})
	if err != nil || info.GetNodeId() != "node-7" {
		t.Errorf("NodeGetInfo = (%v, %v), want node-7", info, err)
	}
	caps, err := svc.NodeGetCapabilities(ctx, &csiv1.NodeGetCapabilitiesRequest{})
	if err != nil || len(caps.GetCapabilities()) != 0 {
		t.Errorf("NodeGetCapabilities = (%v, %v), want none", caps, err)
	}

	checks := []func() error{
		func() error { _, err := svc.NodeStageVolume(ctx, &csiv1.NodeStageVolumeRequest{}); return err },
		func() error { _, err := svc.NodeUnstageVolume(ctx, &csiv1.NodeUnstageVolumeRequest{}); return err },
		func() error { _, err := svc.NodeGetVolumeStats(ctx, &csiv1.NodeGetVolumeStatsRequest{}); return err },
		func() error { _, err := svc.NodeExpandVolume(ctx, &csiv1.NodeExpandVolumeRequest{}); return err },
	}
	for i, check := range checks {
		if status.Code(check()) != codes.Unimplemented {
			t.Errorf("node check %d: want Unimplemented", i)
		}
	}
}
