# Design

silo's bet is that storage you can operate beats storage with every feature. One
component, deployed the same way everywhere, with as few decisions as possible
between you and a working volume.

Five choices follow from that bet. A feature that would violate one doesn't ship.

## Principles

**One binary, symmetric nodes.** Every node runs the same `silod` and can do every
job. There are no monitor, metadata, or OSD roles to size, place, and babysit. You
deploy one thing and scale it by adding more of the same.

**No quorum to lose.** Membership is SWIM gossip and the namespace is a CRDT, so a
partition keeps serving on both sides and converges when the network heals. No
Raft, no "2 of 3 monitors up". A node joins by pointing at one peer and leaves by
dying. Re-replication is a paced background task rather than a stop-the-world
rebalance.

**Recoverable by default.** No protocol step needs a human to break a tie. A volume
is an extent map of immutable, encrypted chunks under a fenced single-writer lease,
so a split brain cannot corrupt it and snapshots cost nothing.

**Errors are instructions.** Every error says what to do next, not just what broke.

**Stdlib-first.** External dependencies need justification. The FUSE protocol and
CSI bindings are built in-tree, and volumes attach over NBD from the mainline
kernel, so there is no client to install and little to audit.

## How a volume is stored

A silo volume is not a file. It is a map from extent index to chunk id, and the
chunks are immutable encrypted blobs that live independently of any volume.

Reading at some offset means finding the extent that offset falls in, looking that
extent up in the map, and fetching the chunk it names. Left to right below: the
volume's logical extents (256 KiB each by default), the extent map, and the chunk
store, where every chunk is immutable, AES-GCM encrypted, and present on all N
replicas.

```mermaid
flowchart LR
    e0["extent 0"] --> k0["0 → node-a-0b8c…-0-17"] --> c0["chunk node-a-0b8c…-0-17"]
    e1["extent 1"] --> k1["1 → node-a-0b8c…-0-04"] --> c1["chunk node-a-0b8c…-0-04"]
    e2["extent 2"] --> k2["no entry"] --> z["reads as zeroes"]
    e3["extent 3"] --> k3["3 → node-b-9c14…-0-88"] --> c3["chunk node-b-9c14…-0-88"]
    e4["extent 4"] --> k4["4 → unmapped"] --> z
```

Four consequences follow from that shape, and most of silo's behaviour is one of
them:

**Volumes are sparse.** Extent 2 has no entry, so it was never written and reads
as zeroes. Extent 4 was written once and later trimmed, and reads as zeroes too.
A 10 GiB volume with 40 MiB written costs 40 MiB.

**Snapshots are free.** A snapshot copies the map, not the data. Both maps point
at the same immutable chunks, and neither can disturb the other.

**Overwrites do not overwrite.** Writing to extent 1 mints a new chunk, replicates
it, and rebinds the map entry. The chunk it replaced is still on disk, now
referenced by nothing. Reclaiming those is the chunk garbage collector's job, and
it is the reason silo has one at all. See
[operations.md](operations.md#configuration) for `SILO_CHUNK_GC_ENABLE`.

**Freed space comes back through trim.** When a filesystem frees blocks it sends
a discard to the device, and the NBD server hands it to the volume. Each extent
the discard covers completely gets an entry with an empty chunk id, like
extent 4 above. That entry goes through the same path as a write: the lease
fences it, it carries an HLC timestamp, and it is replicated to the volume's
replica set. So it beats every older binding of that extent on every replica,
including one that arrives late. From then on the extent reads as zeroes and the
chunk it used to name is referenced by nothing, which the chunk GC reclaims
once the chunk is older than its grace period. The entry is kept rather than
deleted because a missing key carries no timestamp, and a replica still holding
the old binding would merge it straight back. An extent the discard only partly
covers keeps its chunk: zeroing part of it would mean writing a new chunk, and
that frees nothing. A later write to a trimmed extent binds it again in the
usual way.

**Replicas of a map must agree before anything is reclaimed.** The extent write
path acknowledges once a majority of a volume's replicas have the delta, so the
replica that missed out keeps a copy that looks healthy while lacking bindings
its peers have. Reads resolve against the local map, and reclamation decides what
is unreferenced from it, so a copy that has quietly drifted is both a source of
zero-filled reads and a way to delete live data. The extent scrubber reconciles
divergent copies in both directions, and the chunk GC waits for that to finish
rather than sweeping on a view it cannot trust.

The GC has to tell a replica that stays behind from one that is a few
milliseconds behind because a write is still on its way to it. While a volume
is being written or trimmed, two healthy replicas' digests differ for that long
after every delta, and a GC that skipped on any mismatch would skip most sweeps
on a busy cluster. So it re-probes a mismatched replica for a second or two
first. If the copies still differ, it fetches the peer's map and checks the one
thing deletion depends on: the local map must hold every chunk binding the peer
has that is older than half the grace window. A newer binding names a chunk at
most about that old, since chunk ids are never reused and a binding is recorded
right after its chunk is written, and the grace window already keeps the sweep
away from chunks that young. A missed trim only keeps garbage around. A short
lag therefore cannot cause a wrong delete and the GC sweeps through it. A
missing binding older than the horizon means the replica really is behind, and
the GC waits for the extent scrubber, which repairs that kind of divergence
every interval.

**A damaged local map is rebuilt from every replica, never from one.** Each node
saves its maps under `<DATA_DIR>/extents/` as a snapshot plus an append-only
log: a write appends a checksummed record to the log and fsyncs it, and the
snapshot is rewritten (write, fsync, rename, directory fsync) only when the log
has grown. A crash leaves the map as of the last completed write, because a
record cut short at the end of the log was never acknowledged. A replica only
counts toward a write's majority once its copy is on disk. If a map still fails
to load (a disk fault, or a file written before this was in place), the node
moves it to `<file>.corrupt` and will not serve the volume until it has
merged the copy of every replica. The merge keeps the newest binding of each
extent, so the result is at least as new as anything this node wrote. Rebuilding
from whichever replica answers first could hand back an older copy, which then
reads as a clean but earlier version of the volume, and the chunk GC may already
have reclaimed what that older copy points at.

**A chunk id names a writer, not a volume.** The id is the writing node, a random
per-session identifier, and a counter. Chunks from one volume are scattered across
many such sets, and a set outlives the session that created it. This is why
reclamation has to be mark-and-sweep over the live reference set rather than a
per-volume delete.

## Capabilities

An ordinary `PersistentVolumeClaim` against the `silo` StorageClass becomes a
`ReadWriteOnce` device in your pod, with CSI-native snapshots and clones. The
fenced lease means a rescheduled pod takes over its disk instead of corrupting it,
and attachments survive a silod restart: volumes pause and reconnect rather than
returning I/O errors.

Every chunk lives on N nodes. Lose one and its chunks re-replicate onto the
survivors in the background, with nothing to babysit. Chunks are AES-GCM encrypted
with per-chunk keys and cluster traffic is mTLS; keys come from a file or from a
cloud KMS on AWS, GCP, or Azure.

The namespace keeps working on both sides of a split. `mkdir`, `touch`, `ls`, and
`rm` all converge afterward, and conflicts surface instead of being silently
dropped. Where you need many readers, a from-scratch FUSE surface gives you RWX
with close-to-open coherence.

Operationally you get Prometheus metrics, a Grafana overview dashboard built
around health markers rather than per-subject graphs, and a `siloctl` CLI for
status, draining, and capacity rebalancing.

## Trade-offs

Volumes are `ReadWriteOnce`. The FUSE surface is close-to-open coherent, in the
NFS sense, not POSIX-coherent. There is no erasure coding. If you need RWX or EC
today, silo isn't there yet; [known-gaps.md](known-gaps.md) is the honest list.

Writes are slower than Ceph's at default settings, deliberately: every write is
flushed to real disk on multiple nodes before silod says OK. There is no journal
to drain and no in-flight state to recover after a node loss, so the number you
benchmark on day one is the number production sees. You feel that cost on small
synchronous writes, much less on bulk sequential I/O. See
[performance.md](performance.md) for what each system actually does on a write.

## Status

| Capability | Status |
|---|---|
| Encrypted, replicated chunk store (AES-GCM, N=3, consistent-hash placement) | ✅ |
| Gossip membership (SWIM) + mTLS, token-based operator join | ✅ |
| CRDT namespace: partition-tolerant `mkdir`/`touch`/`ls`/`rm`, conflict surfacing | ✅ |
| Writer/reader SDKs, writer-owned chunks (no metadata round-trip on writes) | ✅ |
| Block volumes: extent map, fenced single-writer lease, NBD server, COW snapshots | ✅ |
| Kubernetes CSI driver: provision, attach, mount, snapshot, clone; Helm chart | ✅ |
| Restart-resilient attachments: volumes pause and reconnect across silod rollouts | ✅ |
| Chunk garbage collection: mark-and-sweep reclamation of copy-on-write orphans | ✅ |
| Discard (TRIM): extents a filesystem frees are unmapped and their chunks reclaimed | ✅ |
| FUSE (RWX) filesystem surface: from-scratch protocol library, close-to-open | ✅ |
| Observability: Prometheus metrics, Grafana dashboard, `siloctl` status/drain/rebalance | ✅ |
| Security: cloud KMS keys (AWS/GCP/Azure), cert auto-rotation, CRL revocation, scoped tokens | ✅ |
| Durability: encrypted backup to S3/GCS/Azure, rolling-upgrade protocol handshake | ✅ |
| nvme-tcp block transport | ⏳ v1.1 (kernel-bound) |
