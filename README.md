# silo

**Distributed block storage for Kubernetes that you can actually operate.**

silo is a symmetric, partition-tolerant storage system in Go. One binary per
node, no metadata tier, no quorum to lose, no rebalance ceremony. It gives your
pods `ReadWriteOnce` volumes through an ordinary `PersistentVolumeClaim` — and
heals itself through network partitions without paging you.

> **Status:** active development, through **M7 (CSI driver)**. The data plane
> (encrypted replicated chunks, gossip membership, CRDT namespace, writer/reader
> SDKs, block volumes over NBD) and the Kubernetes CSI driver are in place. See
> [PLAN.md](PLAN.md) for the full roadmap.

---

## Why silo

If you have run Ceph/Rook in anger, you know the failure mode: a quorum of mons
goes unhealthy, a rebalance saturates your network at 3am, and recovering means
understanding placement groups. silo is built around the opposite bet.

| You want | silo does |
|---|---|
| **One thing to deploy** | A single binary, `silod`. Every node is identical — no monitor/metadata/OSD roles to size and babysit. |
| **No quorum to lose** | Membership is SWIM gossip; the namespace is a CRDT. A partition can't take the cluster down — both sides keep serving and **converge automatically** when the network heals. No Raft, no "2 of 3 mons up." |
| **Joins/leaves that aren't events** | A node joins by pointing at one peer and leaves by dying. Re-replication is a paced background task, not a stop-the-world rebalance. |
| **Storage you can reason about** | A volume is an extent map of immutable, AES-GCM-encrypted 4 MiB chunks. Snapshots are free (freeze the map). A single-writer **lease is fenced**, so a split brain can't corrupt a volume. |
| **No client to install** | Volumes attach over **NBD**, which ships in the mainline Linux kernel. The CSI node plugin needs the `nbd` module, nothing exotic. |

silo deliberately trades a few things for this simplicity: volumes are
`ReadWriteOnce` (single fenced writer), the filesystem surface is close-to-open
coherent (NFS-style), and there's no erasure coding yet. If you need RWX today or
EC, silo isn't there yet — see [Out of scope](PLAN.md#10-out-of-scope-v1).

---

## How it works in 30 seconds

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

- **Provision:** the controller turns a PVC into a silo volume (an extent map)
  via silod's namespace API.
- **Attach:** the node plugin opens the volume over NBD from the silod on that
  node, which **takes the volume's lease and fences any prior holder** — so a
  pod rescheduled to a new node steals the device cleanly.
- **Heal:** lose a node and its chunks re-replicate onto survivors in the
  background; the namespace converges over gossip.

---

## Try it in 5 minutes (local)

No Kubernetes required. This boots a real 3-node cluster with Prometheus and
Grafana, then creates and snapshots a volume.

```sh
git clone https://github.com/hyperized/silo && cd silo

make up        # build + boot 3 silod nodes (+ Prometheus on :9090, Grafana on :3030)
make build     # compile ./bin/{silod,siloctl,silo-csi}
```

`make up` prints a one-time, single-use join command in **silo-a's logs**. Grab
it and claim your operator credentials (this writes the cluster CA + your client
cert to `~/.config/silo/` and remembers the server):

```sh
docker logs silo-a 2>&1 | grep -A8 'bootstrap token'   # copy the `siloctl auth init …` line

./bin/siloctl auth init \
  --token <TOKEN> \
  --server 127.0.0.1:7001 \
  --server-fingerprint <FINGERPRINT>
```

Now drive the cluster — every command authenticates over mTLS automatically:

```sh
./bin/siloctl volume create /db --size 10G        # create a 10 GiB block volume
./bin/siloctl volume snapshot /db /db-backup      # instant copy-on-write snapshot
./bin/siloctl ns ls /                             # list the namespace
./bin/siloctl auth status                         # show your cluster credentials
```

Watch it heal: `docker kill silo-b`, then `make status` and re-list — the cluster
stays available and re-replicates in the background. `make down` tears it all down.

> Block-mounting a volume locally needs silod's NBD server enabled
> (`SILO_NBD_ADDR`) plus `nbd-client` on the host. On Kubernetes the CSI node
> plugin does this for you — see below.

---

## Use it on Kubernetes

The [`silo-csi` Helm chart](deploy/helm/silo-csi) installs the driver: a
controller `Deployment` (with the standard external-provisioner/attacher/
snapshotter sidecars) and a node `DaemonSet`. Full guide:
**[docs/kubernetes.md](docs/kubernetes.md)**.

**Prerequisites:** a running silo cluster reachable from Kubernetes, and the
`nbd` kernel module on each node (`modprobe nbd nbds_max=64`). Today you bring
your own silod deployment (the [`silod` container image](Dockerfile) plus the
[env reference](docs/operations.md); a silod Helm chart is on the M9 roadmap).
For evaluation, the `make up` stack above is a perfectly good backing cluster.

```sh
# 1. Build the images and push them where your cluster can pull from.
make images IMAGE_REGISTRY=registry.example.com/silo IMAGE_TAG=v0.7.0
docker push registry.example.com/silo/silo-csi:v0.7.0

# 2. Install the driver, pointed at your silo cluster.
helm install silo-csi deploy/helm/silo-csi \
  --namespace silo-system --create-namespace \
  --set image.repository=registry.example.com/silo/silo-csi \
  --set image.tag=v0.7.0 \
  --set silod.address=silo.silo-system.svc.cluster.local:7000

# 3. Provision a volume and use it.
kubectl apply -f deploy/helm/silo-csi/examples/pvc.yaml
kubectl get pvc data        # -> Bound once the consuming pod is scheduled
```

That `pvc.yaml` is an ordinary PVC against the `silo` StorageClass plus a pod
that writes to it — the no-brainer path your app teams already know:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: silo
  resources:
    requests:
      storage: 10Gi
```

Snapshots are CSI-native (`VolumeSnapshot`), and a snapshot can seed a new PVC
(clone/restore). The full StorageClass parameters, snapshot workflow, and
troubleshooting (the `nbd` module, lease takeover, sidecar RBAC) are in
[docs/kubernetes.md](docs/kubernetes.md).

---

## What works today

| Capability | Status |
|---|---|
| Encrypted, replicated chunk store (AES-GCM, N=3, consistent-hash placement) | ✅ |
| Gossip membership (SWIM) + mTLS, token-based operator join | ✅ |
| CRDT namespace — partition-tolerant `mkdir`/`touch`/`ls`/`rm`, conflict surfacing | ✅ |
| Writer/reader SDKs, writer-owned chunks (no metadata round-trip on writes) | ✅ |
| Block volumes: extent map, fenced single-writer lease, NBD server, COW snapshots | ✅ |
| **Kubernetes CSI driver** — provision, attach, mount, snapshot, clone; Helm chart | ✅ |
| FUSE (RWX) filesystem surface | ⏳ M8 |
| Observability/ops (`siloctl status`, drain, rebalance, nvme-tcp) | ⏳ M9 |
| Production hardening (cert rotation, KMS, rolling upgrades, backup) | ⏳ M10 |

---

## Documentation

- **[docs/kubernetes.md](docs/kubernetes.md)** — install and operate silo-csi on Kubernetes (Helm values, StorageClass, snapshots, troubleshooting)
- **[docs/operations.md](docs/operations.md)** — operator guide: configuration reference, deployment topologies, mTLS/credentials, NBD, troubleshooting
- **[PLAN.md](PLAN.md)** — full design, decisions, milestones, and scope
- **[docs/known-gaps.md](docs/known-gaps.md)** — what's not finished yet and why (deferred work, kernel-bound seams)
- **[deploy/helm/silo-csi/README.md](deploy/helm/silo-csi/README.md)** — chart reference
- **[.env.example](.env.example)** — annotated `silod` configuration

---

## Building from source

```sh
make build              # ./bin/{silod,siloctl,silo-csi}
make images             # silo/silod and silo/silo-csi container images
make test               # unit tests
make test-integration   # end-to-end against a real silod (build-tag 'integration')
make check              # fmt + vet + lint + test
```

Requirements: Go 1.25+, and Docker for the local cluster and image builds.

## Design principles

- **One binary, symmetric nodes** — operational simplicity is the whole point.
- **Recoverable by default** — no protocol step requires a human to break a tie.
- **Errors are instructions** — every error tells you what to do next, not just what broke.
- **Stdlib-first** — external dependencies require justification (the FUSE protocol and the CSI bindings are built/vendored in-tree, not pulled as libraries).

See [PLAN.md §1](PLAN.md#1-design-principles) for the full set.

## License

See the repository for license details.
