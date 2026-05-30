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
- The `nbd` kernel module on every node: `modprobe nbd nbds_max=64`, and
  `nbd-client` available in the node plugin image.

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
| `chunk-size` | yes | Copy-on-write extent size (e.g. `4Mi`) |
| `replicas` | reserved | Per-volume replication factor (placement lands in a later milestone) |
| `region-affinity` | reserved | Region placement hint |
| `snapshot-retention` | reserved | Automatic snapshot retention |

See `values.yaml` for the full set of options.
