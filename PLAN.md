# Plan: silo — Cloud-Native Storage System

A symmetric, partition-tolerant distributed storage system in Go, designed for Kubernetes workloads on commodity hardware. Inspired by Erlang/BEAM cluster patterns: many independent actors, a tiny shared registry, gossip for membership, and "let it crash" recovery semantics.

## 1. Design principles

- **One binary, symmetric nodes.** Every node runs the same daemon (`silod`). No "metadata nodes" vs "data nodes" vs "monitor nodes." This is the operational simplicity dividend over Ceph/Rook.
- **Frictionless join/leave.** A node joins by pointing at one peer; it leaves by being drained or dying. Both are gossip events; no rebalance ceremony.
- **Recoverable by default.** No protocol step requires a human operator to break a tie. Partitions heal automatically via CRDT convergence; quorum loss is not a failure mode because we don't use Raft.
- **Frictionless on the write path.** After `open`, a writer is autonomous: it derives its own chunk IDs and placement, writes directly to data nodes, and only contacts the namespace at `open`/`close` and on periodic checkpoint.
- **Relaxed POSIX for the filesystem surface.** Close-to-open coherence (NFS-style), no byte-range locks, no atomic cross-directory rename. Strict semantics live on the block-volume surface, where a single writer makes them free.
- **Stdlib-first.** External dependencies require an explicit justification. The FUSE protocol is implemented from scratch on stdlib — no `bazil.org/fuse` or `hanwen/go-fuse`.
- **Approachable UX.** Both the CLI (`siloctl`) and the web UI (`silo-ui`) must be operable by someone with no prior knowledge of silo or distributed-storage internals. Plain language in command output and errors, jargon explained in-context, sensible defaults that work without configuration, and an onboarding/help flow that is the first thing a newcomer sees. This is a hard product requirement, not a nice-to-have — it's the reason silo exists in the first place.
- **Errors are instructions.** Every error message — in `silod` logs, `siloctl` output, UI alerts, and library returns — must tell the user what to do next, not just what went wrong. "SILO_ENCRYPTION_KEY is required" is forbidden; "SILO_ENCRYPTION_KEY is required when SILO_ENCRYPTION_KEY_SOURCE=static; generate one with `openssl rand -base64 32` and set it in your env" is required. Generic errors (`failed to open`, `invalid input`, `internal error`) are not allowed at any surface a human reads. This applies to error returns in Go code as well: every `fmt.Errorf` or `errors.New` should be readable as advice. Avoid jargon in user-facing names too: there is one operator-managed *encryption key*, not a "KEK" (the KEK/DEK split is an implementation detail).

## 2. Decisions

Locked in. These shape the implementation from M0 onward.

| # | Decision | Choice |
|---|---|---|
| 1 | Project name | **silo** (`silod` daemon, `siloctl` CLI, `silo-csi` driver) |
| 2 | Default replication factor | **N=3** (configurable per-volume) |
| 3 | Default chunk size | **4 MiB** (configurable per-inode) |
| 4 | Encryption at rest | **AES-GCM per-chunk from M1**, pluggable encryption-key source (env/file/KMS) |
| 5 | Bootstrap discovery | **Static seeds (env) + k8s SRV fallback** |
| 6 | Block exposure | **NBD in M6, nvme-tcp added in M9/M10** |
| 7 | FUSE library | **Written from scratch on stdlib** (parallel track from M3) |
| 8 | Web admin UI | **Full Vue 3 + TS + Tailwind v4 SPA**, parallel track from M2 |

## 3. Architecture

```
                    +---------------------------------------------+
                    |          Kubernetes (PV consumers)          |
                    +----------------+----------------------------+
                                     |
                  +------------------+------------------+
                  v                  v                  v
            +---------+        +---------+        +---------+
            | silo-   |        | silo-   |        | silo-   |
            | csi     |        | fuse    |        | s3-gw   |
            +----+----+        +----+----+        +----+----+
                 |                  |                  |
                 +-- writer SDK (gRPC) ----------------+
                                    |
                                    v
        +-----------------------------------------------------------+
        |             Cluster of identical silod nodes              |
        |                                                           |
        |  +--------+   +--------+   +--------+   +--------+        |
        |  | node 1 |<->| node 2 |<->| node 3 |<->| node N |        |
        |  +--------+   +--------+   +--------+   +--------+        |
        |  | SWIM   |   | SWIM   |   | SWIM   |   | SWIM   |        |
        |  | CRDT ns|   | CRDT ns|   | CRDT ns|   | CRDT ns|        |
        |  | chunk  |   | chunk  |   | chunk  |   | chunk  |        |
        |  | store  |   | store  |   | store  |   | store  |        |
        |  +--------+   +--------+   +--------+   +--------+        |
        +-----------------------------------------------------------+
                                    ^
                                    | (read-only mgmt API)
                                    |
                              +-----+------+
                              | silo-ui    |  Vue 3 SPA, served from silod
                              | (Vue 3 SPA)|
                              +------------+
```

### Locked-in design choices

| Layer | Choice | Reason |
|---|---|---|
| Cluster membership | SWIM gossip | Decentralized, partition-tolerant, BEAM-like |
| Namespace | CRDT (OR-Sets + HLC) | Always-available, recovers from any partition without operator action |
| Data placement | Consistent hash on chunk-id | No central allocator; everyone derives placement |
| Writer model | Writer-owned chunk-id space, autonomous after open | Zero metadata round-trips on data path |
| Multi-writer to one file | Per-writer chunks, logical concatenation, HLC ordering | No coordination on the hot path |
| Coherence | Close-to-open (NFS-style); no byte-range locks | Most workloads don't notice; massive complexity savings |
| First surface | Block volumes (RWO) via NBD | Simplest semantics, proves the data plane |
| Second surface | FUSE filesystem (RWX, relaxed POSIX) | Same chunks, thin metadata layer |
| Replication v1 | N-way synchronous (default N=3) | EC is a v2 optimization on the same chunk store |
| Backing store v1 | Sparse files on host filesystem | Easy dev, easy demo, runs anywhere; LVM in v1.1 |
| Transport | gRPC for control + data | One protocol, mTLS-ready |
| Block exposure | NBD (mainline Linux client) | Zero client install; nvme-tcp as optimization later |
| Encryption | AES-GCM per-chunk; per-chunk data keys wrapped by the silo encryption key | Authenticated; tamper-detection free |

### Key concepts

**Chunk.** Fixed-size (default 4 MiB) opaque blob, AES-GCM encrypted. Identified by a globally-unique `chunk_id` derived from `(writer_id, epoch, counter)`. Placed on N data nodes via consistent hash over the chunk_id. Immutable once written.

**Inode.** A logical object (file, directory, volume). Identified by `inode_id`. Holds:
- type (file / dir / volume)
- ACL (LWW register)
- writer manifest: OR-Set of `(writer_id, [chunk_id...], hlc...)`
- extent map (volumes only): offset -> chunk_id
- wrapped per-chunk data keys for chunks owned by this inode

**Writer.** A client process that has opened an inode for write. Identified by `writer_id` (random UUID). Owns its own chunk-id sequence space; cannot collide with other writers. Autonomous on the data path.

**HLC (Hybrid Logical Clock).** A monotonic timestamp combining wall-clock time, a logical counter, and a node-id tiebreaker. Used to order writes across writers without requiring synchronized clocks.

**Namespace.** Per-directory OR-Set of `(name, inode_id, claim_hlc, tombstone_hlc?)`. Replicated to all nodes by gossip. Convergent under any partition.

**Encryption key.** The cluster has one operator-managed encryption key (per the [[silo-ux-approachable]] principle, we don't burden the user with the cryptographic detail). Source is pluggable: `static` (key in `SILO_ENCRYPTION_KEY`, dev only), `file` (key at `SILO_ENCRYPTION_KEY_PATH`, single-node prod), KMS (future). Internally this acts as a key-encryption-key (KEK) that wraps per-chunk data keys (DEKs) — the DEKs live in the inode metadata, so chunks on disk are useless without both the inode and the cluster encryption key.

## 4. Iterative milestones — core track

Each milestone is a usable, demoable, shippable artifact.

### Progress at a glance

| Milestone | Status | Notes |
|---|---|---|
| M0  | done        | scaffold, `/healthz`, docker-compose, CI |
| M1  | done        | encrypted file-backed chunk store, gRPC, `siloctl` |
| M2  | done        | mTLS, token-based operator join, SWIM gossip; bootstrap UX paper-cuts fixed |
| M3  | done        | consistent-hash placement, quorum-replicated writes, replica-preferring reads, re-replication scrubber |
| M4  | done        | HLC + CRDT namespace (OR-Set dirs, LWW ACLs), gossip-propagated, conflict surfacing, tombstone GC |
| M5  | done        | writer-owned chunks (writer/reader SDKs, inode manifest, clock-skew monitor) |
| M6  | done        | block volume surface (extent-mapped inode, fenced single-writer lease, NBD server, COW snapshot) |
| M7  | done        | CSI driver (silo-csi: vendored CSI proto, Identity/Controller/Node, NBD attach + mount, Helm chart + StorageClass) |
| M8  | done*       | FUSE filesystem (stdlib FUSE protocol library + silo-backed mount); *mount path needs validation on a real /dev/fuse host |
| M9  | done*       | observability + ops: `siloctl status`, graceful drain, capacity-aware placement + rebalancing, full Prometheus metrics (capacity, replication shortfall, latency histograms, gossip/anti-entropy lag, HLC skew). *nvme-tcp deferred to a real-host effort (kernel-bound, v1.1-scoped); data-key-cache hit-rate metric pending the cache |
| M10 | in progress | production hardening: pluggable key provider (file fixed) + cloud KMS (AWS/GCP/Azure), node-cert auto-rotation, threat model, backup export to S3/GCS/Azure Blob, CRL-based revocation (`siloctl ca revoke` + `SILO_TLS_CRL`), rolling-upgrade protocol-version handshake (gossip fences too-old peers) done; signed CSI/FUSE tokens, runbook remaining |

The FUSE protocol track (§5) starts after M2 — eligible to begin now. The UI track (§6) is eligible since M0; U1 cluster-view work needs the M2 gossip data, which is now available.

### M0 — Foundation  [done]

- Repo scaffold, Go module (`github.com/hyperized/silo`), `Makefile`, `docker-compose.yml`, `.env.example`, `.gitignore`
- GitLab CI: lint (`golangci-lint`), unit tests, integration tests behind build tag, container build
- Protobuf definitions: `cluster.proto`, `chunk.proto`, `namespace.proto`, `writer.proto`
- Single binary `silod` that starts, reads config, exposes `/healthz` and `/metrics`
- Bootstrap: reads `SILO_SEEDS` env (comma-separated); if empty, attempts DNS SRV `_silo._tcp.<SILO_DOMAIN>`

**Demo:** `make up` boots a 3-node cluster locally; each node logs "alive."

### M1 — Local chunk engine (encrypted)  [done]

- File-backed chunk store on each node (one chunk = one file)
- Every chunk is AES-GCM encrypted with a per-chunk data key; the data key is wrapped by the cluster encryption key
- Encryption-key source pluggable; ships with `static` (dev, key from `SILO_ENCRYPTION_KEY`) and `file` (key file at `SILO_ENCRYPTION_KEY_PATH`)
- gRPC `Chunk.Put` / `Chunk.Get` / `Chunk.Delete` / `Chunk.Stat`
- Crash-consistent writes (write-to-tmp + `fsync` + rename)
- Internal + external + integration tests; 100% coverage target

**Demo:** `siloctl chunk put <id> <file>` / `get <id>` against one node; chunk file on disk is ciphertext, unreadable without the cluster encryption key.

### M2 — Cluster membership  [done]

- SWIM-style gossip (probe, indirect probe, suspect, dead) on stdlib `net`
- Each node advertises: address, capacity, used bytes, version, region/zone hint
- Anti-entropy: full member-list reconciliation every K seconds
- mTLS handshake for all node-to-node gRPC (cert material bootstrapped in M0)

**Demo:** kill/start nodes randomly; cluster view converges within seconds on every node.

### M3 — Distributed chunk placement + replication  [done]

- Consistent hash ring over live members (virtual nodes, derived from gossip)
- `Chunk.Put` to any node → that node coordinates a direct fan-out to the chunk's replicas, acking at majority quorum; stragglers heal in the background (refined from the original "forward to primary": chunks are immutable, so a coordinator fan-out is simpler and the ring still names a deterministic primary)
- Reads prefer local replica, fall back to ring
- Re-replication on member loss (eventual, paced) via scrubber goroutine — highest-priority *holder* pushes missing replicas
- Per-chunk data keys distributed alongside chunk replicas (the wrapped DEK travels inside the encrypted envelope)

**Demo:** kill a node holding replicas; `chunk get` still works; replicas re-form on a survivor.

### M4 — CRDT namespace  [done]

- Per-directory OR-Set of `(name, inode_id)` tagged with the claim HLC; removals are timestamped tombstones
- Per-inode metadata: type and ACL (LWW register). Writer manifest + wrapped per-chunk data keys are deferred to the writer work (M5), where they are actually populated
- HLC implementation (stdlib `time` + logical counter + node-id tiebreaker)
- Gossip-propagated state over the existing anti-entropy exchange (refined from "deltas": full-state CRDT merge, which is simpler and still converges)
- Conflict surface: `name.conflict-<hlc>` when concurrent creates collide
- Tombstone GC with configurable retention (`SILO_TOMBSTONE_RETENTION`, default 24h)

**Demo:** `siloctl ns mkdir/touch/ls/rm` across nodes; partition the cluster, mutate both sides, heal — converges with collisions surfaced cleanly.

### M5 — Writer-owned chunks  [done]

- Writer SDK (Go library): opens (or creates) an inode and streams it as chunks through an `io.WriteCloser`; chunk ids derive locally from a writer id (sanitised node id + random suffix) and a monotonic counter, so there is no metadata round-trip per write
- Reader SDK: reconstructs the byte stream from the inode manifest in HLC (append) order through an `io.ReadCloser`
- Chunks are written to any node and the existing replication coordinator fans out to the replicas (refined from "direct to primary, placement computed locally": reuses the proven quorum path and keeps the SDK free of cluster-membership state — the local-placement optimisation can land later)
- Manifest append is synchronous and chunk-first — the chunk is durable before its id is recorded — so a reader never sees an id it cannot fetch (refined from "async checkpoint")
- Per-chunk data keys stay server-side: the SDK sends plaintext and the chunk store seals each chunk with its wrapped key, so writers hold no key material (refined from the writer receiving a per-chunk data key)
- Clock skew monitor: each node compares peer-issued HLC timestamps (seen over anti-entropy) against its own clock and warns + counts an alert past `SILO_MAX_CLOCK_SKEW`; observation only, so write ordering is unchanged. Metrics are served by a dedicated `exporter` package (`silo_hlc_peer_clock_skew_seconds`, `silo_hlc_clock_skew_alerts_total`)

**Demo:** 10 pods append concurrently to one logical file; reads see all bytes; no central allocator was touched per write.

> **At M5 the system is usable end-to-end** as a programmable distributed store. Everything above is surfacing.

### M6 — Block volume surface  [done]

- "Volume" = special inode where chunks are extent-mapped (offset → chunk_id) instead of append-ordered
- Single-writer lease registered in the CRDT namespace (LWW with HLC; on conflict, newer holder wins, older fences off)
- Data nodes refuse writes from stale lease-holders (fencing, not just revocation)
- Local node runs an NBD server that translates kernel block ops to chunk reads/writes
- COW snapshot via chunk-list cloning (chunks are immutable; snapshot = freeze the extent map). `Namespace.SnapshotVolume` clones the source's extent-map CRDT into a new vacant volume inode that inherits the source's extent and device size; the two share immutable chunks and diverge cleanly on the next write to either side. Surfaced as the `SnapshotVolume` gRPC RPC and `siloctl volume snapshot <src> <dst>`. (Chunk reference-counting/GC across snapshots is deferred — nothing currently reclaims chunks, so shared chunks are safe; reclamation lands with the M9 ops work.)

**Demo:** `mkfs.ext4` + mount a 10GB volume on a Linux host; pull the underlying node's plug, volume reattaches elsewhere via lease takeover. `siloctl volume snapshot /vol /vol-backup` freezes a point-in-time copy that survives writes to the source.

### M7 — CSI driver  [done]

- `silo-csi` plugin wrapping M6: `CreateVolume`, `DeleteVolume`, `ControllerPublishVolume`, `NodePublishVolume`, snapshots. The CSI spec proto is vendored under `api/proto/csi/v1` and generated in-tree (no module dependency, per the stdlib-first principle). Identity/Controller/Node services live in `internal/csi`; the controller maps each CSI name onto a `/csi/{volumes,snapshots}` namespace path (the path doubles as the opaque CSI id) and provisions/snapshots/clones via the namespace `CreateVolume`/`SnapshotVolume` RPCs; the node attaches over NBD (taking the fenced lease) and formats/mounts via the host's nbd-client/mkfs/mount behind a tested command-runner seam. `cmd/silo-csi` runs controller, node, or both.
- Helm chart for cluster install (`deploy/helm/silo-csi`): CSIDriver, controller Deployment with the external-provisioner/attacher/snapshotter sidecars, node DaemonSet with the node-driver-registrar, RBAC, and an optional StorageClass. Validated with `helm lint`/`helm template`.
- StorageClass with parameters: `replicas`, `region-affinity`, `snapshot-retention`, `chunk-size` (chunk-size honoured now; the others are accepted and reserved for the M9 placement/ops work).

**Demo:** `kubectl apply -f deploy/helm/silo-csi/examples/pvc.yaml` → bound, mounted, used by a stateful pod.

### M8 — FUSE filesystem surface (integration)

This milestone integrates the FUSE protocol implementation from the parallel FUSE track (see §5) with the writer/reader SDKs.

- FUSE mount entrypoint: `silo-fuse mount <path>`
- Operations: `lookup`, `getattr`, `read`, `write` (via writer SDK), `mkdir`, `unlink`, `rename` (best-effort)
- Close-to-open coherence; no byte-range locks; `O_APPEND` via per-writer chunks
- Mountable from any node, including pods (with `/dev/fuse` privilege)
- The FUSE protocol library (parallel track) must reach v0.9 before this milestone can start

**Demo:** mount the cluster as `/mnt/silo` on 5 hosts simultaneously; concurrent writes, reads converge.

### M9 — Observability + ops

- Prometheus metrics: per-node capacity, gossip lag, replication queue depth, chunk read/write latency histograms, namespace anti-entropy lag, HLC skew, data-key cache hit rate
- Structured logs (`log/slog`)
- `siloctl status` — cluster health, per-node summary, replication shortfalls
- Capacity rebalancing on long-term skew (slow background mover)
- Graceful drain: `siloctl node drain` marks a node, re-replicates its chunks, then it's safe to remove
- nvme-tcp target as an alternative to NBD for block volumes

**Demo:** Grafana dashboard; chaos test (random node kills + network partitions for 1h) shows convergence and no data loss.

### M10 — Production hardening

- Cert lifecycle: automatic rotation, revocation, CA roll
- Auth: signed tokens for the CSI driver and FUSE clients
- KMS-backed encryption-key provider (AWS KMS, Vault, generic OIDC)
- Rolling upgrade protocol (version handshake, backward-compat one minor version)
- Backup: chunk-export + manifest-export to S3-compatible target
- README, ops runbook, troubleshooting guide, threat model

## 5. FUSE protocol track — parallel

Writing the FUSE protocol from scratch on stdlib is roughly 3 months of focused work. It can run in parallel with the core track because the FUSE protocol is independent of silo's data plane — it talks to the Linux `/dev/fuse` character device. We develop it against a noop backend (in-memory FS), then plug in the silo writer/reader SDK at M8.

### F0 — FUSE protocol scaffolding (starts after M2)  [eligible — not started]

- Protocol decoding/encoding for FUSE wire format (linux/fuse.h `fuse_in_header`, `fuse_out_header`, opcodes)
- Connection to `/dev/fuse` with `mount(2)` syscall wiring
- Skeleton dispatcher: receive request, dispatch by opcode, send response
- Stdlib-only: `syscall`, `os`, `unsafe` (carefully), no cgo

### F1 — Core opcodes

- `INIT`, `DESTROY`, `LOOKUP`, `FORGET`, `GETATTR`, `SETATTR`, `OPENDIR`, `READDIR`, `RELEASEDIR`, `OPEN`, `READ`, `WRITE`, `RELEASE`, `FLUSH`, `MKDIR`, `RMDIR`, `CREATE`, `UNLINK`
- In-memory FS backend for tests; full fstest suite passes

### F2 — Advanced opcodes

- `RENAME`, `SYMLINK`, `READLINK`, `LINK`, `FSYNC`, `STATFS`, `INTERRUPT`, extended attributes (`GETXATTR`, `SETXATTR`, `LISTXATTR`, `REMOVEXATTR`)
- Readahead, writeback caching modes
- Big-writes negotiation, splice support

### F3 — Performance + correctness hardening

- Worker-pool dispatch (one goroutine per concurrent request)
- Zero-copy paths where the kernel allows (`splice`, `FUSE_PASSTHROUGH` on 5.14+)
- Long-running fstest fuzzing
- Benchmarks vs `libfuse` reference implementation; document gaps

### F4 — silo integration (= M8 in core track)

- Hook the FUSE backend interface to the silo writer/reader SDK
- Inode/dentry cache backed by namespace CRDT subscriptions
- Negative-result caching for fast `ENOENT`

The FUSE library is published as a sub-package (`github.com/hyperized/silo/pkg/fuse`) — usable standalone for anyone who wants a pure-Go FUSE library, independent of silo.

## 6. UI track — parallel

`silo-ui` is a Vue 3 + Composition API + TypeScript + Tailwind v4 SPA. Served by `silod` itself from an embedded `fs.FS` (Go's `embed` package); no separate web-server deployment. Reads cluster state via a small read-mostly REST + WebSocket API on `silod`.

### U0 — Scaffolding (starts after M0)  [eligible — not started]

- Vue 3 + Vite + TS project setup in `ui/`
- Tailwind v4 with CSS-first config
- Component primitives (Button, Card, Table, Badge, Tabs)
- API client codegen from OpenAPI spec produced by `silod`
- E2E smoke test (Playwright)

### U1 — Cluster view (after M2 ships gossip)  [eligible — not started]

- Live cluster topology graph (node list + state colors)
- Per-node detail panel (capacity, version, uptime, gossip neighbors)
- WebSocket subscription to membership events

### U2 — Namespace browser (after M4 ships namespace)

- File-explorer UI: directory tree, mixed file/dir listing, breadcrumb nav
- File detail: size, type, ACL, writer manifest visualization
- Search by name / by inode_id
- Conflict-file UI: highlights `*.conflict-<hlc>` siblings, offers manual resolution

### U3 — Volume management (after M6/M7 ship volumes + CSI)

- Volume CRUD: create, expand, snapshot, restore, delete
- Per-volume health: replication state, last-write, attached host
- StorageClass viewer (read-only from k8s)

### U4 — Observability surface (after M9 ships metrics)

- Embed Grafana panels via iframe, or render minimal Prometheus charts in-app
- Alert summary (replication shortfalls, skewed clocks, partitioned nodes)
- "Cluster diagnose" wizard: walks an operator through common failure modes

### U5 — Polish + auth (after M10)

- SSO integration (OIDC), RBAC mapping to silo ACLs
- Theming, accessibility audit (WCAG 2.2 AA)
- Empty/error/loading states everywhere
- Onboarding flow for fresh clusters

## 7. Repository structure

```
silo/
+-- cmd/
|   +-- silod/                  # the main daemon
|   +-- siloctl/                # operator CLI
|   +-- silo-csi/               # CSI driver binary
|   +-- silo-fuse/              # FUSE mount helper
+-- internal/
|   +-- gossip/                 # SWIM
|   +-- hlc/                    # hybrid logical clock
|   +-- namespace/              # CRDT namespace
|   +-- chunkstore/             # local chunk backend (interface + file impl)
|   +-- crypto/                 # AES-GCM, key wrapping, key cache
|   +-- placement/              # consistent hash ring
|   +-- replication/            # primary + scrubber
|   +-- writer/                 # writer SDK + open/close protocol
|   +-- reader/                 # reader SDK + chunk merge
|   +-- volume/                 # block-volume layer (M6)
|   +-- nbd/                    # NBD server
|   +-- nvmetcp/                # nvme-tcp target (M9)
|   +-- transport/              # gRPC server + client wiring
|   +-- observability/          # metrics, logs, tracing
|   +-- webapi/                 # REST + WS API for the UI
+-- pkg/
|   +-- client/                 # public Go client (for CSI, third parties)
|   +-- fuse/                   # stdlib FUSE protocol library (standalone)
+-- api/
|   +-- proto/                  # *.proto + generated *.pb.go
|   +-- openapi/                # OpenAPI spec for the UI/admin API
+-- ui/                         # Vue 3 + TS + Tailwind v4 SPA
|   +-- src/
|   +-- public/
|   +-- package.json
|   +-- vite.config.ts
|   +-- tailwind.config.ts
|   +-- dist/                   # built; embedded into silod via go:embed
+-- deploy/
|   +-- helm/                   # k8s chart
|   +-- docker-compose.yml      # 3-node local cluster + grafana + prom + ui
|   +-- docker-compose-local.yml # single-binary dev iteration
+-- test/
|   +-- integration/            # build-tag `integration`
|   +-- e2e/                    # multi-node scenarios, partition simulation
|   +-- fstest/                 # filesystem conformance suite for FUSE
+-- docs/
|   +-- architecture.md
|   +-- operations.md
|   +-- protocol.md
|   +-- fuse-internals.md
+-- Makefile                    # `make` brings the stack up
+-- .env.example
+-- .gitlab-ci.yml
+-- README.md
```

Test layout: every package has both `pkg_internal_test.go` (`package pkg`) and `pkg_external_test.go` (`package pkg_test`). Integration tests under `test/integration` guarded by `//go:build integration`. Target coverage: 100% as baseline.

## 8. Local development experience

`make` is the entry point. Targets:

```
make                  # alias for `make up` — boots 3-node cluster + grafana + ui
make build            # compile all binaries
make build-ui         # vite build, output to ui/dist (embedded in silod)
make test             # unit tests (internal + external)
make test-integration # integration tests (real cluster, real disks)
make test-fstest      # filesystem conformance suite against silo-fuse
make lint             # golangci-lint + eslint for ui
make proto            # regenerate protobufs
make openapi          # regenerate OpenAPI spec + UI API client
make up               # docker compose up the local cluster
make down
make demo             # M-X specific demos as we go
```

`docker-compose.yml` boots:
- 3 × `silod` (different ports, different data dirs, UI accessible from any node)
- Prometheus + Grafana (preconfigured dashboard)
- A test client container with `siloctl` baked in
- A dev-mode UI hot-reload container (Vite dev server) — bypasses the embedded build

`docker-compose-local.yml` is the dev-loop variant: 1 node, fast restart, no observability stack.

Config follows 12-factor: all settings via env vars, `.env` file in dev, ConfigMap/Secret in k8s.

## 9. Scope assessment

**Core track:** ~5-6 months single-developer time to M10.

**FUSE protocol track (parallel):** ~3 months. Can start after M2 (week 3). Runs alongside core track until M8 integration. Either a second developer, or core-track developer alternates focus.

**UI track (parallel):** ~3-4 months. Can start after M0 (week 1). Either a second developer, or sequenced after M5 by the core developer. Front-loading mock-data screens reduces wall-clock time once real APIs arrive.

**Total wall-clock with three concurrent tracks:** ~6 months. **Sequential single-developer:** ~10-11 months.

### Hardest pieces, in order

1. **FUSE protocol from scratch** — full POSIX-ish semantics across ~40 opcodes, kernel quirks, version negotiation, race conditions in the dispatcher. Highest novelty risk.
2. **CRDT namespace with realistic anti-entropy** — easy to write naively, hard to make efficient at scale
3. **FUSE filesystem with sensible semantics** — every bug here is a data-loss bug
4. **Re-replication scheduler** — must avoid herd effects, network saturation, starvation
5. **CSI driver** — protocol is well-documented but full of mount/unmount race conditions

### Things that look hard but aren't

- SWIM gossip — well-understood, ~600 lines of Go
- Consistent hash — ~150 lines
- Chunk store on files — trivial
- gRPC plumbing — generated
- AES-GCM chunk encryption — `crypto/cipher` is the whole implementation

### Things to be paranoid about

- HLC clock skew detection — silently broken clocks corrupt write order; add active monitoring from M5
- Tombstone GC — too aggressive resurrects deleted files when a long-partitioned node returns; too conservative wastes namespace space
- Split-brain block volumes — lease must be **fenced** (data nodes refuse writes from stale lease-holders), not just revoked
- Data-key loss = data loss — wrapped per-chunk data keys live in the inode; inode metadata must be replicated as carefully as the chunks themselves
- FUSE protocol regressions silently corrupt files — fstest-style conformance suite must run on every commit

## 10. Out of scope (v1)

- Erasure coding (deferred to v2; same chunk store can support it)
- Object/S3 surface (deferred to v3)
- Multi-region replication / async DR
- Hot/cold tiering
- Quotas with hard limits (best-effort only)
- Atomic cross-directory rename
- POSIX byte-range locks (`fcntl`)
- Per-pod IO QoS
- KMS integration (basic file/env encryption-key only in v1; KMS in M10)
- nvme-tcp (in v1.1 via M9)
