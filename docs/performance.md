# The trade vs Ceph

silo trades against Ceph on two axes: a smaller operational surface in exchange
for fewer features, and on-disk durability on every ACK in exchange for some write
latency. This page explains both so you can decide whether the trade fits.

**Jump to:** [Operational surface](#operational-surface) ·
[What silo does on every write](#what-silo-does-on-every-write) ·
[What Ceph does instead](#what-ceph-does-instead) ·
[Where you'll feel it](#where-youll-feel-it) ·
[Why silo makes this trade](#why-silo-doesnt-trade-durability-for-latency) ·
[Knobs you get](#knobs-you-do-get) · [Measured baseline](#measured-baseline)

---

## Operational surface

Running replicated storage well usually means running a lot of moving parts. A
typical Ceph/Rook install has several daemon types to place and scale (monitors
that must hold quorum, managers, metadata servers, an OSD per disk), a
placement-group model you size up front and reshape as you grow, and an operator
whose custom resources span dozens of fields before a single volume exists. It is
proven, and most of that surface area is something you have to
understand *before* you can run it safely.

silo makes a smaller bet: one component (`silod`), deployed the same way
everywhere, with as few decisions as possible between you and a working volume.
You give up features for that: `ReadWriteOnce` volumes, close-to-open FUSE
coherence, no erasure coding yet (see
[What works today](../README.md#what-works-today) and
[known-gaps.md](known-gaps.md)). On small synchronous writes you also give up
some latency, which the rest of this page covers.

---

## What silo does on every write

When a client writes a chunk over NBD:

1. silod encrypts the chunk and hands it to the placement layer.
2. Two or three nodes (your choice via `SILO_REPLICATION`) receive a copy.
3. Each of those nodes writes the chunk to disk **and forces it to physical
   storage** before answering "done".
4. silod tells the client OK only after a majority of that chunk's replicas have
   done so.

Once silod says the write succeeded, the data is on real disk on multiple
machines. There is no background work to drain and no in-flight state to recover.

This majority is **per chunk**: it is the chunk's own replica set acknowledging,
not a cluster-wide consensus quorum. It is unrelated to the "no quorum to lose"
property of membership: there is no Raft/monitor quorum that, once lost, takes the
cluster down. Losing nodes here never blocks the cluster; it only leaves some
chunks under-replicated until the scrubber heals them onto survivors.

## What Ceph does instead

Ceph acknowledges from a journal: the data is on a fast journal device but
hasn't reached its final home yet, and the journal drains in the background. That
is why default-tuned Ceph wins on small synchronous writes: most of the cost is
paid later.

## Where you'll feel it

Workloads dominated by small synchronous writes (database transaction logs,
`fsync`-heavy filesystems, NBD-backed swap) will be noticeably faster on Ceph.
Bulk sequential I/O is much closer, because the per-write overhead spreads across
more bytes.

## Why silo doesn't trade durability for latency

- **Failure recovery stays simple.** If a silod dies mid-write, the write either
  landed on a majority of the chunk's replicas (it happened) or it didn't (it
  failed). There is no
  third "the journal said OK, the data isn't at rest yet" state to reason about
  during a node loss.
- **Nothing to size or tune.** No journal device to provision, no
  fast-pool/slow-pool tiering, no separate write-ahead log to manage. The number
  you benchmark on day one is the number production sees.

## Knobs you do get

- `SILO_REPLICATION=2` lands writes on two disks instead of three. Still
  real-disk durable on every ACK, with fewer copies before silod says OK.
- The CSI StorageClass `chunk-size` parameter sets extent size per volume. Larger
  extents amortize per-chunk overhead for sequential workloads; smaller extents
  reduce read amplification for small random reads.

If you optimise for transaction-log latency, Ceph at defaults will likely be
faster. If you'd rather know exactly what state your data is in after a node
failure, silo is the trade.

## Measured baseline

The hot paths are benchmarked in-tree (`make bench`): chunk encrypt/decrypt
(`internal/crypto`), the file chunk store (`internal/chunkstore`), and the
consistent-hash placement locator (`internal/placement`). The placement locator
is flat from 3 to 200 nodes (~3 µs, one allocation per lookup), so cluster size
is not a write-path cost. Cluster-level and end-to-end throughput benchmarks are
not yet automated; see [known-gaps.md](known-gaps.md#performance-measured-floor-deferred-follow-ups).
