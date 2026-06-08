# silo

**Distributed block storage for Kubernetes that you can actually operate.**

silo is a symmetric storage system in Go. One binary per node, no metadata tier,
no quorum to lose, no rebalance ceremony. It gives your pods `ReadWriteOnce`
volumes through an ordinary `PersistentVolumeClaim` — and heals itself through
network partitions without paging you.

**Jump to:** [Design principles](#design-principles) · [What you get](#what-you-get) · [Try locally](#try-it-in-5-minutes-local) · [Deploy](#deploy) · [Kubernetes](#use-it-on-kubernetes) · [Performance](#performance) · [What works today](#what-works-today) · [Docs](#documentation)

---

## Design principles

silo's bet is that storage you can operate beats storage with every feature: one
component, deployed the same way everywhere, with as few decisions as possible
between you and a working volume. Five choices follow, and a feature that would
violate one doesn't ship.

- **One binary, symmetric nodes.** Every node runs the same `silod` and can do
  every job — no monitor/metadata/OSD roles to size, place, and babysit. You
  deploy one thing and scale it by adding more of the same.
- **No quorum to lose.** Membership is SWIM gossip and the namespace is a CRDT, so
  a partition keeps serving on both sides and **converges automatically** when the
  network heals — no Raft, no "2 of 3 monitors up." A node joins by pointing at one
  peer and leaves by dying; re-replication is a paced background task, not a
  stop-the-world rebalance.
- **Recoverable by default.** No protocol step needs a human to break a tie. A
  volume is an extent map of immutable, encrypted chunks under a **fenced**
  single-writer lease, so a split brain can't corrupt it and snapshots are free.
- **Errors are instructions.** Every error tells you what to do next, not just
  what broke.
- **Stdlib-first.** External dependencies require justification — the FUSE protocol
  and CSI bindings are built in-tree, and volumes attach over **NBD** from the
  mainline kernel, so there's no client to install and little to audit.

silo trades a few things for this: volumes are `ReadWriteOnce`, the FUSE surface
is close-to-open coherent (NFS-style), and there's no erasure coding yet. If you
need RWX or EC today, silo isn't there — see [docs/known-gaps.md](docs/known-gaps.md).
How this compares to Ceph, in operational surface and write latency, is in
[docs/performance.md](docs/performance.md).

---

## What you get

- **Block volumes for Kubernetes.** An ordinary `PersistentVolumeClaim` against
  the `silo` StorageClass becomes a `ReadWriteOnce` device in your pod. CSI-native
  snapshots and clones; a fenced lease so a rescheduled pod steals its disk
  cleanly instead of corrupting it.
- **Self-healing replication.** Every chunk lives on N nodes. Lose one and its
  chunks re-replicate onto survivors in the background — no rebalance to babysit.
- **Encrypted at rest, always.** Chunks are AES-GCM encrypted with per-chunk keys;
  cluster traffic is mTLS. Keys come from a file or a cloud KMS (AWS/GCP/Azure).
- **Partition-tolerant namespace.** `mkdir`/`touch`/`ls`/`rm` keep working on both
  sides of a split and converge afterward, with conflicts surfaced rather than
  silently dropped.
- **A shared filesystem too.** A from-scratch FUSE surface gives RWX,
  close-to-open coherent access where you need many readers.
- **Operability built in.** Prometheus metrics, a ready-made Grafana overview
  dashboard (health markers over per-subject graphs), and a `siloctl` CLI for
  status, draining, and capacity rebalancing.

---

## Try it in 5 minutes (local)

No Kubernetes required. This boots a real 3-node cluster with Prometheus and
Grafana, then creates and snapshots a volume.

```sh
git clone https://github.com/hyperized/silo && cd silo

make up        # build + boot 3 silod nodes (+ Prometheus on :9090, Grafana on :3030)
make build     # compile ./bin/{silod,siloctl,silo-csi}
```

`make up` scrapes silo-a's bootstrap token from the container logs and prints a
paste-ready `siloctl auth init` command. Run it to claim your operator
credentials (this writes the cluster CA + your client cert to `~/.config/silo/`
and remembers the server):

```sh
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

> Block-mounting a volume locally needs silod's NBD server (`SILO_NBD_ADDR`) plus
> `nbd-client` on the host. On Kubernetes the CSI node plugin does this for you.

---

## Deploy

The same binary runs everywhere; what changes is *where* it runs. There are two
shapes, and you can start in one and grow into the other with no data migration.

- **Co-located (small scale).** Run silod on machines you already have — your
  Kubernetes control-plane or worker nodes, or a couple of existing hosts. In
  Kubernetes it runs as a **DaemonSet** on node-local disk; outside Kubernetes
  it's the **standalone binary** under systemd. No dedicated storage hardware.
  This is the right starting point and is production-capable across 3+ nodes.
- **Dedicated (larger scale).** Run silod on its own set of machines sized for
  storage and point your clients at them. Same binary, same wiring — you've just
  moved the disks off the compute nodes so storage and compute scale
  independently.

| Shape | Nodes | Survives node loss? | For |
|---|---|---|---|
| **Standalone** | 1 | No — one copy, [back it up](docs/operations.md#backups) | edge, a single host, dev |
| **Co-located cluster** | 3+ | Yes — replication factor N | silod alongside your existing nodes |
| **Dedicated cluster** | 3+ | Yes | a storage fleet serving many clients |

Standalone is one line; adding nodes (same CA, same encryption key) grows it into
either cluster shape:

```sh
SILO_ENCRYPTION_KEY_SOURCE=file SILO_ENCRYPTION_KEY_PATH=/etc/silo/key \
SILO_DATA_DIR=/var/lib/silo SILO_NBD_ADDR=0.0.0.0:10809 ./bin/silod
```

Full recipes — co-located DaemonSet, dedicated fleet, CA seeding, advertise
addresses, KMS, backups — are in
**[docs/operations.md → Deployment paths](docs/operations.md#deployment-paths)**.

---

## Use it on Kubernetes

The [`silo-csi` Helm chart](deploy/helm/silo-csi) installs the driver — a
controller `Deployment` (with the standard external-provisioner/attacher/
snapshotter sidecars) and a node `DaemonSet`. You point it at a running silo
cluster, whether that's co-located on your nodes or a dedicated fleet.
**Prerequisite:** the `nbd` kernel module on each node (`modprobe nbd nbds_max=64`).

```sh
helm install silo-csi deploy/helm/silo-csi \
  --namespace silo-system --create-namespace \
  --set silod.address=silo.silo-system.svc.cluster.local:7000

kubectl apply -f deploy/helm/silo-csi/examples/pvc.yaml
```

From there it's the path your app teams already know — an ordinary PVC against the
`silo` StorageClass:

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
(clone/restore). Image building, StorageClass parameters, the snapshot workflow,
and troubleshooting are in **[docs/kubernetes.md](docs/kubernetes.md)**.

---

## Performance

silo's writes are slower than Ceph's at default settings, and the trade is
deliberate: **every write is flushed to real disk on multiple nodes before silod
says OK** — no journal to drain, no in-flight state to recover after a node loss.
The number you benchmark on day one is the number production sees.

That costs you on small synchronous writes (transaction logs, `fsync`-heavy
filesystems); bulk sequential I/O is much closer. `SILO_REPLICATION=2` and the
per-volume `chunk-size` are the knobs. The full explanation — what each system
does on a write and why — is in **[docs/performance.md](docs/performance.md)**.

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
| Observability/ops — Prometheus metrics, **Grafana overview dashboard**, `siloctl status`, drain, capacity rebalance | ✅ |
| Security hardening — cloud KMS keys (AWS/GCP/Azure), cert auto-rotation, CRL revocation, scoped capability tokens | ✅ |
| Durability/upgrades — encrypted backup to S3/GCS/Azure, rolling-upgrade protocol handshake | ✅ |
| nvme-tcp block transport | ⏳ v1.1 (kernel-bound) |

---

## Documentation

- **[docs/kubernetes.md](docs/kubernetes.md)** — install and operate silo-csi on Kubernetes (Helm values, StorageClass, snapshots, troubleshooting)
- **[docs/operations.md](docs/operations.md)** — operator guide: configuration, deployment paths (co-located and dedicated), mTLS/credentials, NBD, troubleshooting
- **[docs/performance.md](docs/performance.md)** — the trade vs Ceph: operational surface, the write path, and the measured baseline
- **[docs/runbook.md](docs/runbook.md)** — production readiness checklist, golden-signal alerts, failure-recovery playbooks
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

## License

See the repository for license details.
