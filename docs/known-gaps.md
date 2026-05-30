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

## Reserved-but-not-yet-enforced

- **StorageClass parameters** `region-affinity` and `snapshot-retention` are
  accepted by `silo-csi` and recorded, but not yet enforced (placement by region
  and automatic snapshot pruning). `replicas` and `chunk-size` are honoured.
- **silod Kubernetes chart.** `silo-csi` ships a Helm chart; `silod` itself is
  operator-deployed from its container image today. A packaged `silod` chart is
  roadmapped (see [kubernetes.md](kubernetes.md)).
