package csi_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
	"github.com/hyperized/silo/internal/csi"
)

// fakeStore records the VolumeStore calls the controller makes and returns
// canned ids/errors.
type fakeStore struct {
	createName            string
	createSize            int64
	createExtent          int64
	cloneSource           string
	cloneName             string
	snapSource, snapName  string
	deletedVol, deletedSn string

	id  string
	err error
}

func (f *fakeStore) CreateVolume(_ context.Context, name string, size, extent int64) (string, error) {
	f.createName, f.createSize, f.createExtent = name, size, extent
	return f.id, f.err
}

func (f *fakeStore) CloneVolume(_ context.Context, source, name string) (string, error) {
	f.cloneSource, f.cloneName = source, name
	return f.id, f.err
}

func (f *fakeStore) DeleteVolume(_ context.Context, id string) error { f.deletedVol = id; return f.err }

func (f *fakeStore) CreateSnapshot(_ context.Context, source, name string) (string, error) {
	f.snapSource, f.snapName = source, name
	return f.id, f.err
}

func (f *fakeStore) DeleteSnapshot(_ context.Context, id string) error {
	f.deletedSn = id
	return f.err
}

func rwoCaps() []*csiv1.VolumeCapability {
	return []*csiv1.VolumeCapability{{
		AccessType: &csiv1.VolumeCapability_Block{Block: &csiv1.VolumeCapability_BlockVolume{}},
		AccessMode: &csiv1.VolumeCapability_AccessMode{Mode: csiv1.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}}
}

func TestControllerCreateVolume(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{id: "/csi/volumes/pvc-1"}
	ctrl := csi.NewControllerService(store)

	resp, err := ctrl.CreateVolume(ctx, &csiv1.CreateVolumeRequest{
		Name:               "pvc-1",
		CapacityRange:      &csiv1.CapacityRange{RequiredBytes: 10 << 20},
		VolumeCapabilities: rwoCaps(),
		Parameters:         map[string]string{"chunk-size": "4Mi"},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if store.createName != "pvc-1" || store.createSize != 10<<20 || store.createExtent != 4<<20 {
		t.Errorf("store got (%q, %d, %d), want (pvc-1, 10Mi, 4Mi)", store.createName, store.createSize, store.createExtent)
	}
	v := resp.GetVolume()
	if v.GetVolumeId() != "/csi/volumes/pvc-1" || v.GetCapacityBytes() != 10<<20 {
		t.Errorf("volume = %+v, want id /csi/volumes/pvc-1 cap 10Mi", v)
	}
	if v.GetVolumeContext()["silo.path"] != "/csi/volumes/pvc-1" || v.GetVolumeContext()["silo.sizeBytes"] != "10485760" {
		t.Errorf("volume context = %v, want path+size", v.GetVolumeContext())
	}
}

func TestControllerCreateVolume_FromSnapshotAndVolume(t *testing.T) {
	ctx := context.Background()

	snap := &fakeStore{id: "/csi/volumes/pvc-restore"}
	ctrl := csi.NewControllerService(snap)
	_, err := ctrl.CreateVolume(ctx, &csiv1.CreateVolumeRequest{
		Name: "pvc-restore", CapacityRange: &csiv1.CapacityRange{RequiredBytes: 1 << 20}, VolumeCapabilities: rwoCaps(),
		VolumeContentSource: &csiv1.VolumeContentSource{Type: &csiv1.VolumeContentSource_Snapshot{Snapshot: &csiv1.VolumeContentSource_SnapshotSource{SnapshotId: "/csi/snapshots/snap-1"}}},
	})
	if err != nil {
		t.Fatalf("CreateVolume from snapshot: %v", err)
	}
	if snap.cloneSource != "/csi/snapshots/snap-1" || snap.cloneName != "pvc-restore" {
		t.Errorf("clone got (%q, %q), want snapshot source", snap.cloneSource, snap.cloneName)
	}

	clone := &fakeStore{id: "/csi/volumes/pvc-clone"}
	ctrl = csi.NewControllerService(clone)
	if _, err := ctrl.CreateVolume(ctx, &csiv1.CreateVolumeRequest{
		Name: "pvc-clone", CapacityRange: &csiv1.CapacityRange{RequiredBytes: 1 << 20}, VolumeCapabilities: rwoCaps(),
		VolumeContentSource: &csiv1.VolumeContentSource{Type: &csiv1.VolumeContentSource_Volume{Volume: &csiv1.VolumeContentSource_VolumeSource{VolumeId: "/csi/volumes/pvc-src"}}},
	}); err != nil {
		t.Fatalf("CreateVolume from volume: %v", err)
	}
	if clone.cloneSource != "/csi/volumes/pvc-src" {
		t.Errorf("clone source = %q, want pvc-src", clone.cloneSource)
	}
}

func TestControllerCreateVolume_Errors(t *testing.T) {
	ctx := context.Background()
	ctrl := csi.NewControllerService(&fakeStore{})

	cases := []struct {
		name string
		req  *csiv1.CreateVolumeRequest
		code codes.Code
	}{
		{"no name", &csiv1.CreateVolumeRequest{CapacityRange: &csiv1.CapacityRange{RequiredBytes: 1}, VolumeCapabilities: rwoCaps()}, codes.InvalidArgument},
		{"no caps", &csiv1.CreateVolumeRequest{Name: "v", CapacityRange: &csiv1.CapacityRange{RequiredBytes: 1}}, codes.InvalidArgument},
		{"no size", &csiv1.CreateVolumeRequest{Name: "v", VolumeCapabilities: rwoCaps()}, codes.InvalidArgument},
		{"bad chunk-size", &csiv1.CreateVolumeRequest{Name: "v", CapacityRange: &csiv1.CapacityRange{RequiredBytes: 1}, VolumeCapabilities: rwoCaps(), Parameters: map[string]string{"chunk-size": "nope"}}, codes.InvalidArgument},
		{"multi-node writer", &csiv1.CreateVolumeRequest{Name: "v", CapacityRange: &csiv1.CapacityRange{RequiredBytes: 1}, VolumeCapabilities: []*csiv1.VolumeCapability{{AccessMode: &csiv1.VolumeCapability_AccessMode{Mode: csiv1.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}}}}, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ctrl.CreateVolume(ctx, tc.req); status.Code(err) != tc.code {
				t.Errorf("code = %v, want %v", status.Code(err), tc.code)
			}
		})
	}

	// A store error propagates.
	boom := csi.NewControllerService(&fakeStore{err: errors.New("silod down")})
	if _, err := boom.CreateVolume(ctx, &csiv1.CreateVolumeRequest{Name: "v", CapacityRange: &csiv1.CapacityRange{RequiredBytes: 1}, VolumeCapabilities: rwoCaps()}); err == nil {
		t.Error("a store error should propagate")
	}
}

func TestControllerDeleteVolumeAndSnapshot(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	ctrl := csi.NewControllerService(store)

	if _, err := ctrl.DeleteVolume(ctx, &csiv1.DeleteVolumeRequest{VolumeId: "/csi/volumes/pvc-1"}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if store.deletedVol != "/csi/volumes/pvc-1" {
		t.Errorf("deleted volume = %q", store.deletedVol)
	}
	if _, err := ctrl.DeleteVolume(ctx, &csiv1.DeleteVolumeRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("DeleteVolume without id = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := ctrl.DeleteSnapshot(ctx, &csiv1.DeleteSnapshotRequest{SnapshotId: "/csi/snapshots/snap-1"}); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if store.deletedSn != "/csi/snapshots/snap-1" {
		t.Errorf("deleted snapshot = %q", store.deletedSn)
	}
	if _, err := ctrl.DeleteSnapshot(ctx, &csiv1.DeleteSnapshotRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("DeleteSnapshot without id = %v, want InvalidArgument", status.Code(err))
	}
}

func TestControllerStoreErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	boom := csi.NewControllerService(&fakeStore{err: errors.New("silod down")})

	if _, err := boom.DeleteVolume(ctx, &csiv1.DeleteVolumeRequest{VolumeId: "/csi/volumes/pvc-1"}); err == nil {
		t.Error("DeleteVolume should propagate a store error")
	}
	if _, err := boom.DeleteSnapshot(ctx, &csiv1.DeleteSnapshotRequest{SnapshotId: "/csi/snapshots/s"}); err == nil {
		t.Error("DeleteSnapshot should propagate a store error")
	}
	if _, err := boom.CreateSnapshot(ctx, &csiv1.CreateSnapshotRequest{SourceVolumeId: "/v", Name: "s"}); err == nil {
		t.Error("CreateSnapshot should propagate a store error")
	}
}

func TestControllerCreateSnapshot(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{id: "/csi/snapshots/snap-1"}
	ctrl := csi.NewControllerService(store, csi.WithClock(func() time.Time { return at }))

	resp, err := ctrl.CreateSnapshot(ctx, &csiv1.CreateSnapshotRequest{SourceVolumeId: "/csi/volumes/pvc-1", Name: "snap-1"})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	snap := resp.GetSnapshot()
	if snap.GetSnapshotId() != "/csi/snapshots/snap-1" || snap.GetSourceVolumeId() != "/csi/volumes/pvc-1" || !snap.GetReadyToUse() {
		t.Errorf("snapshot = %+v, want ready snap-1 from pvc-1", snap)
	}
	if !snap.GetCreationTime().AsTime().Equal(at) {
		t.Errorf("creation time = %v, want %v", snap.GetCreationTime().AsTime(), at)
	}
	if store.snapSource != "/csi/volumes/pvc-1" || store.snapName != "snap-1" {
		t.Errorf("store got (%q, %q)", store.snapSource, store.snapName)
	}

	if _, err := ctrl.CreateSnapshot(ctx, &csiv1.CreateSnapshotRequest{Name: "snap-1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateSnapshot without source = %v, want InvalidArgument", status.Code(err))
	}
}

func TestControllerPublishUnpublishAndValidate(t *testing.T) {
	ctx := context.Background()
	ctrl := csi.NewControllerService(&fakeStore{})

	pub, err := ctrl.ControllerPublishVolume(ctx, &csiv1.ControllerPublishVolumeRequest{VolumeId: "/csi/volumes/pvc-1", NodeId: "node-a", VolumeCapability: rwoCaps()[0]})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	if pub.GetPublishContext()["silo.path"] != "/csi/volumes/pvc-1" {
		t.Errorf("publish context = %v, want the volume path", pub.GetPublishContext())
	}
	if _, err := ctrl.ControllerPublishVolume(ctx, &csiv1.ControllerPublishVolumeRequest{VolumeId: "/csi/volumes/pvc-1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("publish without node = %v, want InvalidArgument", status.Code(err))
	}
	// Publishing with an unsupported access mode is rejected.
	if _, err := ctrl.ControllerPublishVolume(ctx, &csiv1.ControllerPublishVolumeRequest{
		VolumeId: "/csi/volumes/pvc-1", NodeId: "node-a",
		VolumeCapability: &csiv1.VolumeCapability{AccessMode: &csiv1.VolumeCapability_AccessMode{Mode: csiv1.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("publish multi-node = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := ctrl.ControllerUnpublishVolume(ctx, &csiv1.ControllerUnpublishVolumeRequest{VolumeId: "/csi/volumes/pvc-1"}); err != nil {
		t.Fatalf("ControllerUnpublishVolume: %v", err)
	}
	if _, err := ctrl.ControllerUnpublishVolume(ctx, &csiv1.ControllerUnpublishVolumeRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unpublish without id = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := ctrl.ValidateVolumeCapabilities(ctx, &csiv1.ValidateVolumeCapabilitiesRequest{VolumeCapabilities: rwoCaps()}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("validate without id = %v, want InvalidArgument", status.Code(err))
	}

	// ValidateVolumeCapabilities confirms RWO and rejects multi-node.
	ok, err := ctrl.ValidateVolumeCapabilities(ctx, &csiv1.ValidateVolumeCapabilitiesRequest{VolumeId: "/csi/volumes/pvc-1", VolumeCapabilities: rwoCaps()})
	if err != nil || ok.GetConfirmed() == nil {
		t.Errorf("validate RWO = (%v, %v), want confirmed", ok, err)
	}
	bad, err := ctrl.ValidateVolumeCapabilities(ctx, &csiv1.ValidateVolumeCapabilitiesRequest{VolumeId: "/csi/volumes/pvc-1", VolumeCapabilities: []*csiv1.VolumeCapability{{AccessMode: &csiv1.VolumeCapability_AccessMode{Mode: csiv1.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}}}})
	if err != nil || bad.GetConfirmed() != nil || bad.GetMessage() == "" {
		t.Errorf("validate multi-node = (%v, %v), want unconfirmed with a message", bad, err)
	}
}

func TestControllerGetCapabilities(t *testing.T) {
	resp, err := csi.NewControllerService(&fakeStore{}).ControllerGetCapabilities(context.Background(), &csiv1.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities: %v", err)
	}
	got := map[csiv1.ControllerServiceCapability_RPC_Type]bool{}
	for _, c := range resp.GetCapabilities() {
		got[c.GetRpc().GetType()] = true
	}
	for _, want := range []csiv1.ControllerServiceCapability_RPC_Type{
		csiv1.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csiv1.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csiv1.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csiv1.ControllerServiceCapability_RPC_CLONE_VOLUME,
	} {
		if !got[want] {
			t.Errorf("missing advertised capability %s", want)
		}
	}
}

func TestControllerUnimplemented(t *testing.T) {
	ctx := context.Background()
	ctrl := csi.NewControllerService(&fakeStore{})
	checks := []func() error{
		func() error { _, err := ctrl.ListVolumes(ctx, &csiv1.ListVolumesRequest{}); return err },
		func() error { _, err := ctrl.GetCapacity(ctx, &csiv1.GetCapacityRequest{}); return err },
		func() error { _, err := ctrl.ListSnapshots(ctx, &csiv1.ListSnapshotsRequest{}); return err },
		func() error {
			_, err := ctrl.ControllerExpandVolume(ctx, &csiv1.ControllerExpandVolumeRequest{})
			return err
		},
		func() error { _, err := ctrl.ControllerGetVolume(ctx, &csiv1.ControllerGetVolumeRequest{}); return err },
		func() error {
			_, err := ctrl.ControllerModifyVolume(ctx, &csiv1.ControllerModifyVolumeRequest{})
			return err
		},
	}
	for i, check := range checks {
		if status.Code(check()) != codes.Unimplemented {
			t.Errorf("check %d: want Unimplemented", i)
		}
	}
}
