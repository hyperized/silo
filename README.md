# silo

**Distributed block storage for Kubernetes that you can actually operate.**

silo is a symmetric, partition-tolerant storage system in Go. One binary per
node, no metadata tier, no quorum to lose, no rebalance ceremony. It gives your
pods `ReadWriteOnce` volumes through an ordinary `PersistentVolumeClaim` — and
heals itself through network partitions without paging you.

**Jump to:** [Why silo](#why-silo) · [How it works](#how-it-works-in-30-seconds) · [Try locally](#try-it-in-5-minutes-local) · [Deploy](#deploy) · [Kubernetes](#use-it-on-kubernetes) · [vs Ceph](#performance-trade-offs-vs-ceph) · [What works today](#what-works-today) · [Docs](#documentation) · [Build from source](#building-from-source)

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
EC, silo isn't there yet — see [docs/known-gaps.md](docs/known-gaps.md).

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

## Deploy

Same binary everywhere — pick a path by how much you need to survive a node loss:

| Path | Nodes | Survives node loss? | For |
|---|---|---|---|
| **Standalone** | 1 | No — one copy, [back it up](docs/operations.md#backups) | edge, a single host, dev, a small store |
| **Cluster** | 3+ | Yes — replication factor N | self-managed hosts / VMs / Compose with HA |
| **Kubernetes** | 3+ | Yes | pods consuming silo as a CSI StorageClass |

Adding nodes (same CA, same encryption key) turns Standalone into Cluster with no
data migration. Standalone in one line:

```sh
SILO_ENCRYPTION_KEY_SOURCE=file SILO_ENCRYPTION_KEY_PATH=/etc/silo/key \
SILO_DATA_DIR=/var/lib/silo SILO_NBD_ADDR=0.0.0.0:10809 ./bin/silod
```

Full recipes (CA seeding, advertise addresses, KMS, backups) for all three:
**[docs/operations.md → Deployment paths](docs/operations.md#deployment-paths)**.

---

## Use it on Kubernetes

The [`silo-csi` Helm chart](deploy/helm/silo-csi) installs the driver: a
controller `Deployment` (with the standard external-provisioner/attacher/
snapshotter sidecars) and a node `DaemonSet`. Full guide:
**[docs/kubernetes.md](docs/kubernetes.md)**.

**Prerequisites:** a running silo cluster reachable from Kubernetes, and the
`nbd` kernel module on each node (`modprobe nbd nbds_max=64`). Today you bring
your own silod deployment (the [`silod` container image](Dockerfile) plus the
[Kubernetes deployment path](docs/operations.md#3-kubernetes); a packaged silod
Helm chart is [roadmapped](docs/known-gaps.md)). For evaluation, the `make up`
stack above is a perfectly good backing cluster.

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

## Performance trade-offs vs Ceph

silo's writes are slower than Ceph's at default settings, and we don't pretend
otherwise. The trade is deliberate.

**What silo does on every write.** When a client writes a chunk over NBD:

1. silod encrypts the chunk and hands it to the placement layer.
2. Two or three nodes (your choice via `SILO_REPLICATION`) receive a copy.
3. Each of those nodes writes the chunk to disk **and forces it to physical
   storage** before answering "done".
4. silod tells the client OK only after a quorum has done so.

Once silod says the write succeeded, the data is on real disk on multiple
machines. No background work to drain, no in-flight state to recover.

**What Ceph does instead.** Ceph acknowledges from a journal — the data is on a
fast journal device but hasn't reached its final home yet, and the journal
drains in the background. That is why default-tuned Ceph wins on small
synchronous writes: most of the cost is paid later.

**Where you'll feel it.** Workloads dominated by small synchronous writes —
database transaction logs, `fsync`-heavy filesystems, NBD-backed swap — will be
noticeably faster on Ceph. Bulk sequential I/O is much closer, because the
per-write overhead spreads across more bytes.

**Why silo doesn't trade durability for latency this way.**

- **Failure recovery stays simple.** If a silod dies mid-write, the write
  either landed on a quorum of disks (it happened) or it didn't (it failed).
  There is no third "the journal said OK, the data isn't at rest yet" state to
  reason about during a node loss.
- **Nothing to size or tune.** No journal device to provision, no
  fast-pool/slow-pool tiering, no separate write-ahead log to manage. The
  number you benchmark on day one is the number production sees.

**Knobs you do get.**

- `SILO_REPLICATION=2` lands writes on two disks instead of three. Still
  real-disk durable on every ACK, just fewer copies before silod says OK.
- The CSI StorageClass `chunk-size` parameter sets extent size per volume.
  Larger extents amortize per-chunk overhead for sequential workloads; smaller
  extents reduce read amplification for small random reads.

If you optimise for transaction-log latency, Ceph at defaults will likely be
faster. If you'd rather know exactly what state your data is in after a node
failure, silo is the trade.

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
| FUSE (RWX) filesystem surface — from-scratch protocol library, close-to-open | ✅ |
| Observability/ops — Prometheus metrics, **Grafana overview dashboard** (health markers + per-subject graphs), `siloctl status`, drain, capacity rebalance | ✅ |
| Security hardening — cloud KMS keys (AWS/GCP/Azure), cert auto-rotation, CRL revocation, scoped capability tokens | ✅ |
| Durability/upgrades — encrypted backup to S3/GCS/Azure, rolling-upgrade protocol handshake | ✅ |
| nvme-tcp block transport | ⏳ v1.1 (kernel-bound) |

---

### Block volumes

A *volume* is a virtual disk backed by silo. Internally it is an inode whose data is mapped extent-by-extent (offset → chunk) with copy-on-write, guarded by a single-writer lease so two hosts can't scribble over each other. Create one with:

```sh
siloctl volume create --size 10G --extent-size 64K /my-volume
```

You attach a volume to a host as a block device over **NBD** (Network Block Device — a Linux kernel protocol that exposes a remote disk as a local `/dev/nbdX`). Each silod can serve its volumes over NBD when `SILO_NBD_ADDR` is set; the volume's path is the NBD export name.

Two end-to-end demos prove the block surface — they differ in *which* NBD client mounts the volume, because that client is the part worth verifying independently:

```sh
make nbd-demo      # privileged Linux container: nbd-client attaches the volume,
                   # mkfs.ext4 + mount, write/read, detach/re-attach to show the
                   # data persists. Needs a Linux host with the `nbd` kernel
                   # module (the macOS Docker VM does not ship it).

make nbd-demo-vm   # boots a throwaway aarch64 Linux guest under QEMU with the
                   # volume attached as its virtio disk over NBD, so QEMU's own
                   # NBD client is the verifier. Runs fully on macOS via the
                   # Hypervisor framework — no host NBD kernel module needed.
                   # Requires `qemu` (brew install qemu) and Docker.
```

## Documentation

- **[docs/kubernetes.md](docs/kubernetes.md)** — install and operate silo-csi on Kubernetes (Helm values, StorageClass, snapshots, troubleshooting)
- **[docs/operations.md](docs/operations.md)** — operator guide: configuration reference, deployment topologies, mTLS/credentials, NBD, troubleshooting
- **[docs/runbook.md](docs/runbook.md)** — production readiness checklist, golden-signal alerts, and failure-recovery playbooks
- **[docs/threat-model.md](docs/threat-model.md)** — what silo defends against, how, and the current security edges
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

## License

See the repository for license details.
