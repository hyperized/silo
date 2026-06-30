# Running silo on Kubernetes

This guide takes you from zero to a pod writing to a silo-backed
`PersistentVolumeClaim`, then covers snapshots, mutual TLS, and the things that
actually go wrong on day one.

If you just want the happy path, jump to [Install](#install).

**Jump to:** [What gets deployed](#what-gets-deployed) · [Prerequisites](#prerequisites) · [Install](#install) · [Provision a volume](#provision-a-volume) · [Snapshots](#snapshots) · [Mutual TLS](#mutual-tls-to-silod) · [Troubleshooting](#troubleshooting) · [Uninstall](#uninstall)

---

## What gets deployed

The [`silo-csi` chart](../deploy/helm/silo-csi) installs the CSI driver only —
the glue between Kubernetes and your silo cluster:

| Object | Runs | Purpose |
|---|---|---|
| `CSIDriver/csi.silo.hyperized.net` | — | Registers the driver with Kubernetes |
| `Deployment …-controller` | once | `silo-csi` (controller) + external-provisioner, -attacher, -snapshotter sidecars. Provisions/snapshots volumes via silod. |
| `DaemonSet …-node` | every node | `silo-csi` (node) + node-driver-registrar. Attaches volumes over NBD and mounts them. Runs **privileged**, `hostNetwork`. |
| RBAC + ServiceAccounts | — | Permissions the sidecars need |
| `StorageClass/silo` | — | Optional default class |

It does **not** deploy `silod`. The driver connects to a silo cluster you run
(`silod.address`). See [Prerequisites](#prerequisites).

How the pieces fit together:

```
   kubectl apply PVC                                   your pods
          │                                                │
          ▼                                                ▼  ReadWriteOnce mount
   ┌──────────────┐   CSI gRPC    ┌───────────────────────────────────┐
   │  silo-csi    │──────────────▶│  silo-csi node plugin (DaemonSet)  │
   │  controller  │               │  attaches over NBD, mkfs, mounts   │
   └──────┬───────┘               └────────────────┬──────────────────┘
          │ create/snapshot volume                 │ NBD to node-local silod
          ▼                                         ▼
   ┌───────────────────────────────────────────────────────────────────┐
   │            Cluster of identical silod nodes (gossip + CRDT)         │
   │   chunk store · consistent-hash placement · N-way replication      │
   └───────────────────────────────────────────────────────────────────┘
```

- **Provision:** the controller turns a PVC into a silo volume (an extent map).
- **Attach:** the node plugin opens the volume over NBD from the silod on that
  node, which **takes the volume's lease and fences any prior holder**, so a pod
  rescheduled onto a new node steals its disk cleanly.
- **Heal:** lose a node and its chunks re-replicate onto survivors in the
  background; the namespace converges over gossip.

---

## Prerequisites

1. **A silo cluster reachable from Kubernetes.**
   - Each node's `silod` must have its **NBD server enabled** (`SILO_NBD_ADDR`,
     e.g. `0.0.0.0:10809`) so the node plugin can attach volumes.
   - The node plugin reaches the **node-local** silod over `127.0.0.1:10809`
     (it runs with `hostNetwork`). The simplest topology is a `silod` DaemonSet
     with `hostNetwork`, one per node, co-located with the silo-csi node plugin.
   - The controller reaches silod's gRPC API at `silod.address` (a `Service`,
     `host:port`).
   - A packaged `silod` Helm chart is [roadmapped](known-gaps.md). Until then,
     deploy `silod` from its [container image](../Dockerfile) using the
     [configuration reference](operations.md#deployment-paths) (the Kubernetes
     path), or use the `make up` docker-compose cluster for evaluation.

2. **The `nbd` kernel module on every node** that will mount volumes:

   ```sh
   modprobe nbd nbds_max=64        # make it persistent via /etc/modules-load.d/
   ```

3. **Images your cluster can pull.** Build and push them:

   ```sh
   make images IMAGE_REGISTRY=registry.example.com/silo IMAGE_TAG=v0.10.0
   docker push registry.example.com/silo/silo-csi:v0.10.0
   ```

   The `silo-csi` image bundles `nbd-client`, `mkfs.ext4`/`mkfs.xfs`, and
   `mount`/`blkid` so the node plugin needs nothing extra on the host but the
   kernel module.

---

## Install

```sh
helm install silo-csi deploy/helm/silo-csi \
  --namespace silo-system --create-namespace \
  --set image.repository=registry.example.com/silo/silo-csi \
  --set image.tag=v0.10.0 \
  --set silod.address=silo.silo-system.svc.cluster.local:7000 \
  --set silod.nbdAddress=127.0.0.1:10809
```

Verify the driver registered on every node:

```sh
kubectl get csidrivers csi.silo.hyperized.net
kubectl -n silo-system get pods          # controller + one node pod per node
kubectl get csinodes -o wide             # the driver should be listed per node
```

### Key Helm values

| Value | Default | Notes |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/hyperized/silo-csi` / chart appVersion | Point at your registry |
| `silod.address` | `silo:7000` | silod gRPC `Service` the controller dials |
| `silod.nbdAddress` | `127.0.0.1:10809` | node-local silod NBD endpoint |
| `silod.tls.secretName` | `""` | Secret with `ca.crt`/`client.crt`/`client.key` for mTLS to silod |
| `node.hostNetwork` | `true` | lets the node plugin reach node-local silod and host nbd devices |
| `node.kubeletDir` | `/var/lib/kubelet` | override for k3s/microk8s/k0s |
| `storageClass.create` | `true` | install the `silo` StorageClass |
| `controller.replicas` | `1` | controller is stateless; scale for availability |

Full set: [`values.yaml`](../deploy/helm/silo-csi/values.yaml).

---

## Provision a volume

```sh
kubectl apply -f deploy/helm/silo-csi/examples/pvc.yaml
kubectl get pvc data        # Bound once the consuming pod is scheduled
kubectl exec silo-demo -- cat /data/hello
```

The StorageClass uses `volumeBindingMode: WaitForFirstConsumer`, so the PVC
binds when its pod is scheduled — matching `ReadWriteOnce` semantics (one fenced
writer at a time).

### StorageClass parameters

```yaml
parameters:
  chunk-size: "4Mi"          # copy-on-write extent size — honoured today
  replicas: "3"              # reserved (richer placement deferred)
  region-affinity: ""        # reserved
  snapshot-retention: "24h"  # reserved
```

Only `chunk-size` changes behaviour today (it sets the volume's extent size).
The others are accepted and recorded so manifests written now keep working when
placement and retention ship.

---

## Snapshots

silo snapshots are instant and copy-on-write (freeze the extent map; immutable
chunks are shared until either side is written). Under extent replication (the
default) a volume's extent map lives on its replica set, so a snapshot clones
that map onto the snapshot's own replica set as part of the operation: the
source's replica set must be reachable, and if the clone can't replicate the
snapshot is rolled back and the request fails (gRPC `Unavailable`) rather than
returning a silently-empty snapshot — retry once the replica set is healthy. To
use snapshots through Kubernetes you need the cluster-wide snapshot machinery
(installed once per cluster, not by this chart):

1. Install the snapshot CRDs and controller from
   [kubernetes-csi/external-snapshotter](https://github.com/kubernetes-csi/external-snapshotter)
   if your distribution doesn't already ship them.

2. Create a `VolumeSnapshotClass` for the driver:

   ```yaml
   apiVersion: snapshot.storage.k8s.io/v1
   kind: VolumeSnapshotClass
   metadata:
     name: silo
   driver: csi.silo.hyperized.net
   deletionPolicy: Delete
   ```

3. Snapshot a PVC, then restore it into a new PVC:

   ```yaml
   apiVersion: snapshot.storage.k8s.io/v1
   kind: VolumeSnapshot
   metadata:
     name: data-snap
   spec:
     volumeSnapshotClassName: silo
     source:
       persistentVolumeClaimName: data
   ---
   apiVersion: v1
   kind: PersistentVolumeClaim
   metadata:
     name: data-restored
   spec:
     accessModes: ["ReadWriteOnce"]
     storageClassName: silo
     resources:
       requests:
         storage: 10Gi
     dataSource:
       name: data-snap
       kind: VolumeSnapshot
       apiGroup: snapshot.storage.k8s.io
   ```

You can also clone a PVC directly (`dataSource` of `kind: PersistentVolumeClaim`).

---

## Mutual TLS to silod

A production silo cluster requires mTLS on its gRPC API. Give the controller
client credentials issued by the cluster CA (see
[operations.md → credentials](operations.md#claiming-operator-credentials)):

```sh
kubectl -n silo-system create secret generic silo-client \
  --from-file=ca.crt --from-file=client.crt --from-file=client.key

helm upgrade silo-csi deploy/helm/silo-csi --reuse-values \
  --set silod.tls.secretName=silo-client
```

With no secret set, the controller connects insecurely — fine for an
evaluation cluster, not for production.

---

## Troubleshooting

**Pod stuck in `ContainerCreating`, events say "failed to attach/mount".**
Check the node plugin logs:

```sh
kubectl -n silo-system logs -l app.kubernetes.io/component=node -c silo-csi
```

- `could not attach volume … over NBD` → the node-local silod isn't reachable or
  has no NBD server. Confirm `SILO_NBD_ADDR` is set on silod and that
  `silod.nbdAddress` resolves from the node plugin (with `hostNetwork: true`,
  `127.0.0.1:10809` reaches the silod on the same node).
- `no free /dev/nbd device` → the `nbd` module isn't loaded or `nbds_max` is too
  low. `modprobe nbd nbds_max=64`.
- `mkfs`/`mount` errors → the image is missing block tooling. Use the provided
  `Dockerfile.csi` (it bundles e2fsprogs/xfsprogs/util-linux).

**PVC stuck `Pending`.** Look at the controller:

```sh
kubectl -n silo-system logs deploy/silo-csi-controller -c silo-csi
kubectl -n silo-system logs deploy/silo-csi-controller -c csi-provisioner
```

A common cause is the controller can't reach silod (`silod.address` wrong, or
mTLS required but no `silod.tls.secretName`).

**Driver not listed in `kubectl get csinodes`.** The node-driver-registrar
couldn't reach the kubelet plugin dir — check `node.kubeletDir` matches your
distribution (`/var/lib/k0s/kubelet`, `/var/snap/microk8s/common/var/lib/kubelet`,
etc.).

**A volume won't attach on a new node after the old node died.** This is
expected to *just work*: attaching takes the volume's lease and fences the old
holder (last-writer-wins on the HLC). If it doesn't, the old node's silod may
still hold a live lease — confirm it's actually down, or check the controller
logs for the lease takeover.

---

## Uninstall

```sh
helm uninstall silo-csi -n silo-system
```

Existing `PersistentVolume`s and their data in silo are not removed by
uninstalling the driver; delete the PVCs first if you want the volumes reclaimed
(with `reclaimPolicy: Delete`).
