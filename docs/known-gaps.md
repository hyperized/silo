# Known gaps & deferred work

A running, honest list of what is *not* finished, why, and what it would take to
close. Kept current as milestones land. For the milestone roadmap see
[PLAN.md](../PLAN.md).

## Needs validation on a real Linux host

These are implemented and unit-tested for everything that doesn't touch the
kernel, but their kernel-facing seam has not been exercised — this repo's CI/dev
environment has no `/dev/fuse`, no mount privileges, and no kernel nvme-tcp.

- **FUSE mount path** (`pkg/fuse/conn_linux.go`, `internal/silofuse/grpcbackend.go`).
  The protocol library and `SiloFS` are tested over an in-memory `Conn`; the
  `open("/dev/fuse") + mount(2)` layer and the live-silod backend need a real
  mount. To close: run `silo-fuse <dir>` against a `make up` cluster on a host
  with the `fuse` module and `CAP_SYS_ADMIN`, then run the fstest suite.

## M8 — FUSE: opcodes not yet implemented

The library covers the **core (F1)** opcodes. Deferred (FUSE track F2/F3):

- `RENAME`, `SYMLINK`, `READLINK`, `LINK`, `FSYNC`, `STATFS`, `INTERRUPT`,
  extended attributes (`GETXATTR`/`SETXATTR`/`LISTXATTR`/`REMOVEXATTR`).
- `SiloFS` therefore has no rename (the plan lists rename as best-effort).
- Performance hardening (F3): worker-pool dispatch (requests are served
  sequentially today), zero-copy/splice, readahead/writeback caching modes,
  `FUSE_PASSTHROUGH`.
- fstest conformance suite (gated on a real mount, above).

## M9 — deferred

- **Automated restore.** Backup *export* ships (encrypted chunks + namespace to
  local/S3/GCS/Azure, `SILO_BACKUP_TARGET`); restoring is a manual procedure
  today (recreate the data dir from the export, start with the same cluster
  key). A `siloctl restore` command is future work.
- **nvme-tcp target.** An alternative block transport to NBD. Deferred to a
  dedicated effort on a host with a kernel nvme-tcp initiator — it is the plan's
  largest kernel-bound piece and is **v1.1-scoped** (`PLAN.md` decision #6). The
  pure NVMe/TCP PDU codec could be built and unit-tested first (as the FUSE
  protocol layer was), with the kernel round-trip validated on a real host.
- **Data-key cache hit-rate metric.** silod currently unwraps each chunk's data
  key on demand; there is no DEK cache to measure. Closing this is two steps:
  add a wrapped-DEK cache in `internal/crypto`, then expose
  `silo_crypto_datakey_cache_{hits,misses}_total`. (The *cluster*-key source is
  done — static/file/AWS-KMS/GCP-KMS/Azure-KV; this is the per-chunk DEK cache.)
- **Replication "queue depth" metric.** The plan lists it; today the equivalent
  health signal is `silo_replication_shortfall_chunks` (under-replicated count).
  A true in-flight re-replication queue gauge would require instrumenting the
  scrubber's work queue.

## M10 — deferred

- **Retroactive migration / GC off a pressured node.** Disk-pressure steering
  (`SILO_DISK_PRESSURE_STEERING`, default on) stops *new* chunks from landing on
  a near-full node and heals around it, bounded so a quorum of natural replicas
  is always kept (reads stay correct). What it does **not** do is move or delete
  the chunks already on the node — silo has no chunk-migration/GC primitive at
  all (the scrubber only adds replicas, never removes), so a pressured node drains
  only as old chunks are deleted and new data lands elsewhere, not actively. A
  safe "shed" loop (copy-verify-delete a node's excess chunks) would relieve a
  hot node faster and would also make capacity rebalancing retroactive rather
  than new-data-only; it is roadmapped. Today, for faster relief, add capacity or
  [drain](operations.md#draining-a-node). See
  [operations.md](operations.md#disk-high-watermarks-diskpressure).
- **DiskPressure in the status RPC.** Peers' DiskPressure conditions are gossiped
  and counted (`silo_rebalancer_pressured_nodes`), but `siloctl status` shows
  per-node capacity without the condition flag — surfacing it needs a status
  proto field (same follow-up as the protocol-version-in-status gap).
- **CRL hot reload.** Certificate revocation is enforced (`SILO_TLS_CRL`,
  `siloctl ca revoke`), but silod reads the CRL once at startup, so a freshly
  revoked cert takes effect on the next restart rather than instantly. A watcher
  that re-reads the CRL file (or pulls it over gossip) and swaps the in-memory
  set live is future work. Until then, revoke + distribute + restart.
- **Background cert-rotation loop.** Node certs auto-rotate on restart within
  their final ~4 months, which covers clusters that do rolling upgrades; a
  never-restarting node still needs a timer-driven re-mint. Roadmapped.
- **Per-resource token scopes + token revocation.** Capability tokens
  (`SILO_REQUIRE_TOKENS`, `siloctl auth mint-token`) scope by *operation class*
  (`chunk:write`), not by resource — there is no "write only under /tenant-a" or
  "attach only volume X" yet, and no RBAC mapped to namespace ACLs. Tokens also
  cannot be revoked individually before expiry (rely on short TTLs, or rotate the
  CA). Per-resource scoping and a token denylist are roadmapped (M10/U5).
- **Protocol version in the status RPC.** The rolling-upgrade handshake exposes
  each node's protocol via `silo_gossip_protocol_version` (and fences too-old
  peers), but `siloctl status` reports only the build semver, not the wire
  protocol. Surfacing protocol + per-peer compatibility through the status RPC
  would let an operator watch an upgrade converge without scraping `/metrics`.

## Performance — measured floor, deferred follow-ups

The data plane has two benchmark layers now:

- `make bench` — in-process micro-benchmarks: AES-GCM encrypt/decrypt, the
  full-fsync chunk `Put`/`Get`, the placement locator, the gRPC peer
  Store/Fetch path against a real ChunkService over insecure loopback (the
  remote-fetch hot path without the mTLS + network cost), and the volume
  block-I/O layer over a real encrypted chunk store (the NBD per-extent
  cost: copy-on-write + AES-GCM seal + fsync per extent for writes;
  GetChunk + AES-GCM open per extent for reads).
- `make bench-cluster` — end-to-end benchmarks against a real 3-node silod
  cluster spawned by the integration scaffold: quorum write fan-out, local +
  cross-node reads (`SILO_REPLICATION=1` to force every Get through
  `peers_grpc.Fetch`), the writer/reader SDK streaming path, namespace mkdir,
  and the cheap `Stat` floor. Tagged `integration`; build-tag gated like the
  rest of the integration suite.

Headline numbers on an Apple M2 Pro (3-node loopback cluster, repl=3 except
`CrossNode` which uses repl=1 to force every Get through `peers_grpc.Fetch`):

| Bench | 4 KiB | 64 KiB | 4 MiB |
|---|---|---|---|
| `ChunkPut` (quorum write + remote fsync) | 17 ms | 15 ms | 22 ms / 191 MB/s |
| `ChunkGet_Local` (local replica) | 128 µs | 239 µs / 274 MB/s | 5.0 ms / 842 MB/s |
| `ChunkGet_CrossNode` (forced peer Fetch) | 246 µs | 447 µs / 147 MB/s | 40 ms / 105 MB/s |
| `WriterSDK` (4 MiB stream, multi-chunk fan-out) | — | — | 25 ms / 166 MB/s |
| `ReaderSDK` (4 MiB stream, manifest replay) | — | — | 5.0 ms / 847 MB/s |
| `Stat` (metadata RPC) | 76 µs | — | — |
| `NamespaceMkdir` (CRDT entry) | 1.0 ms | — | — |

The fsync floor dominates ChunkPut at all sizes — there's no per-byte slope
on the small chunks because both fsync(file) and fsync(dir) happen unconditionally.
Cross-node reads are ~8× slower than local at 4 MiB on a single machine; on
real hardware where the network and disk are separate, the gap narrows.

Volume block-I/O (single-node, 64 KiB extent, same hardware):

| Bench | Per op | Throughput |
|---|---|---|
| `WriteAt_AlignedFullExtent` (64 KiB) | 5.9 ms | 11 MB/s |
| `WriteAt_SmallWriteHotExtent` (4 KiB into mapped extent) | 6.2 ms | 0.66 MB/s |
| `ReadAt_Sequential` (64 KiB) | 38 µs | 1.7 GB/s |
| `ReadAt_RandomSmall` (4 KiB random) | 40 µs | 104 MB/s |

Writes are fsync-bound — a small write inside an extent costs nearly the
same wall time as a full-extent aligned write because both seal exactly one
new chunk. The small/random read pays full extent-decrypt cost per 4 KiB
returned; that's the extent-amplification floor for read-heavy small-block
workloads (OLTP, FS metadata).

What's still open:

- **Per-volume write serialization is by design, and is the main write ceiling.**
  `volume.WriteAt` holds one `writeMu` across the whole read-modify-write, so
  disjoint extents of the same volume can't be written concurrently. This is
  inherent to the single-fenced-writer model; per-extent locking would lift it
  but adds complexity and isn't planned.
- **fsync-per-`Put`** (file + dir) is synchronous on the write path — correct for
  crash consistency. Batching multiple chunk writes behind one fsync would raise
  throughput at the cost of a wider durability window; not planned.
- **No perf-regression gate in CI yet.** Both bench targets run on demand; a
  comparison harness that fails CI on a regression past a threshold is the
  obvious next step but is not wired.

Floors raised so far:

- `crypto.EncryptChunk` now seals in place rather than copying the ciphertext
  into the envelope (+~34% encrypt throughput, allocations halved).
  `DecryptChunk` was already single-allocation.
- `peers_grpc.Fetch` now pre-sizes the reassembly buffer from the Info frame
  rather than `append`-ing per 64 KiB frame. Measured against the new in-process
  Fetch bench (4 MiB chunk): −48% bytes allocated, −18% latency, +23%
  throughput. Smaller chunks are unaffected (single frame).

## Reserved-but-not-yet-enforced

- **StorageClass parameters** `region-affinity` and `snapshot-retention` are
  accepted by `silo-csi` and recorded, but not yet enforced (placement by region
  and automatic snapshot pruning). `replicas` and `chunk-size` are honoured.
- **silod Kubernetes chart.** `silo-csi` ships a Helm chart; `silod` itself is
  operator-deployed from its container image today. A packaged `silod` chart is
  roadmapped (see [kubernetes.md](kubernetes.md)).
