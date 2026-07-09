# silo-csi Helm chart

Installs the silo CSI driver — the `silo-csi` controller and per-node plugin —
so Kubernetes workloads can use silo block volumes through ordinary
PersistentVolumeClaims.

## What it deploys

| Object | Purpose |
|---|---|
| `CSIDriver` | Registers `csi.silo.hyperized.net` with Kubernetes |
| `Deployment …-controller` | `silo-csi` (controller mode) + external-provisioner/attacher/snapshotter sidecars |
| `DaemonSet …-node` | `silo-csi` (node mode) + node-driver-registrar, one pod per node |
| `StorageClass` | Default `silo` class (optional) |
| RBAC | ServiceAccounts + ClusterRole/Binding for the sidecars |

## Prerequisites

- A running silo cluster reachable from the cluster (set `silod.address`).
- Each node runs a silod with its NBD server enabled, reachable from the node
  plugin (default `127.0.0.1:10809` via `hostNetwork`).
- The `nbd` kernel module on every node (`modprobe nbd`). The node plugin
  drives the kernel directly — no `nbd-client` on the host is needed, and
  devices are picked by the kernel, so `nbds_max` needs no tuning.

## Behaviour during silod restarts

Attached volumes survive silod restarts (rolling upgrades, crashes): the node
plugin watches every attachment and reconnects it the moment silod is back,
while the kernel holds the volume's I/O. Workloads see a short pause, not an
error. `silod.nbdReconnectTimeout` (default `5m`) bounds how long I/O waits
before it fails, and `silod.nbdRequestTimeout` (default `2m`) bounds a single
hung request — keep it set; it is also what lets a node with in-use volumes
shut down instead of hanging on an unanswerable write. The plugin remembers
its attachments on disk, so its own restarts (upgrades of this chart) never
orphan a mounted volume.

Each node plugin serves Prometheus metrics and `/healthz` on
`node.metricsAddress` (default `:7090`): attached volumes, volumes currently
reconnecting, and completed reconnects — alert on reconnects that no rollout
explains. Volume health also reaches the kubelet through the CSI volume
condition.

## Install

```sh
helm install silo-csi deploy/helm/silo-csi \
  --namespace silo-system --create-namespace \
  --set silod.address=silo:7000
```

For mutual TLS to silod, create a Secret with `ca.crt`, `client.crt`,
`client.key` and pass `--set silod.tls.secretName=<secret>`.

## Use

```sh
kubectl apply -f examples/pvc.yaml
kubectl get pvc data        # Bound once the consuming pod is scheduled
```

## StorageClass parameters

| Parameter | Honoured | Meaning |
|---|---|---|
| `chunk-size` | yes | Copy-on-write extent size (default `256Ki`; raise for large-streaming, lower for small-random workloads) |
| `replicas` | reserved | Per-volume replication factor (placement lands in a later milestone) |
| `region-affinity` | reserved | Region placement hint |
| `snapshot-retention` | reserved | Automatic snapshot retention |

See `values.yaml` for the full set of options.
