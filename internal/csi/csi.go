// Package csi implements silo's Container Storage Interface driver, silo-csi.
// It is a thin translation layer: the CSI gRPC calls Kubernetes makes (create
// a volume, attach it to a node, mount it into a pod) become operations on
// silo's existing block-volume surface (the namespace CreateVolume/
// SnapshotVolume RPCs and the node's NBD server). The driver holds no state of
// its own — every volume lives in the CRDT namespace, so any replica of the
// driver sees the same volumes.
//
// The package is split by CSI service: IdentityService (who am I), ControllerService
// (provision/snapshot), and NodeService (attach/mount). A VolumeStore adapter
// (namespaceBackend) maps CSI's opaque names onto silo namespace paths.
package csi

// DriverName is the CSI plugin name silo registers under. Kubernetes keys a
// StorageClass's `provisioner`, the CSIDriver object, and the per-node socket
// directory on this exact string, so it is part of the driver's external
// contract and must not change between releases without a migration.
const DriverName = "csi.silo.hyperized.net"

// Namespace layout for CSI-managed objects. CSI hands the driver opaque names
// (PVC-derived, e.g. "pvc-<uuid>"); the driver maps each into a stable
// namespace path under these directories and uses that path as the CSI id, so
// an id is self-describing and later calls need no lookup table.
const (
	volumesDir   = "/csi/volumes"
	snapshotsDir = "/csi/snapshots"
)
