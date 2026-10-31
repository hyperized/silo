# Operating silo

This is the operator reference for the `silod` daemon: how to configure it, the
deployment topologies, how credentials and encryption work, and how to diagnose
the common problems. For the Kubernetes CSI driver specifically, see
[kubernetes.md](kubernetes.md).

**Jump to:** [Configuration](#configuration) · [Deployment paths](#deployment-paths) · [Operator credentials](#claiming-operator-credentials) · [Capacity rebalancing](#capacity-rebalancing) · [Backups](#backups) · [Draining a node](#draining-a-node) · [Rolling upgrades](#rolling-upgrades) · [Block volumes (NBD)](#block-volumes-over-nbd) · [Health, metrics & profiling](#health-metrics--profiling) · [Troubleshooting](#troubleshooting)

---

## Configuration

`silod` is configured entirely through environment variables (12-factor). The
annotated [`.env.example`](../.env.example) is the canonical source; this table
is the quick reference.

On/off variables take `true` or `false` (`1`/`0`, `yes`/`no` and `on`/`off`
also work, in any case). Any other value stops silod at startup with an error
naming the variable, so a typo cannot quietly leave a feature on.

### Identity & networking

| Variable | Default | Purpose |
|---|---|---|
| `SILO_NODE_ID` | OS hostname | Stable node identifier; must survive restarts |
| `SILO_GRPC_ADDR` | `0.0.0.0:7000` | gRPC listener (operators + peers) |
| `SILO_GRPC_ADVERTISE` | loopback of `GRPC_ADDR` | gRPC address **operators** dial (returned in the Join response) |
| `SILO_GRPC_PEER_ADVERTISE` | loopback of `GRPC_ADDR` | gRPC address **peers** dial to replicate chunks |
| `SILO_BOOTSTRAP_ADDR` | `0.0.0.0:7001` | One-time join handshake (no client cert required) |
| `SILO_BOOTSTRAP_ADVERTISE` | loopback | Bootstrap address operators dial |
| `SILO_GOSSIP_ADDR` | `0.0.0.0:7100` | SWIM gossip listener |
| `SILO_GOSSIP_ADVERTISE` | `GOSSIP_ADDR` | Routable gossip address peers dial. **Set this** when binding `0.0.0.0` with >1 peer (e.g. the Pod IP) |
| `SILO_HTTP_ADDR` | `0.0.0.0:7080` | `/healthz`, `/metrics`, and `/debug/pprof/` when `SILO_PPROF` is set |
| `SILO_NBD_ADDR` | *(empty = off)* | NBD block-device server, e.g. `0.0.0.0:10809`. **Required to serve volumes.** |

### Cluster discovery

| Variable | Default | Purpose |
|---|---|---|
| `SILO_SEEDS` | *(empty)* | Comma-separated peer **gossip** addresses (port 7100), e.g. `silo-0:7100,silo-1:7100`. Used at startup and again whenever the node sees fewer than `SILO_REPLICATION`-1 alive peers. Without seeds a node that loses its peers cannot rejoin by itself |
| `SILO_DOMAIN` | *(empty)* | DNS SRV fallback: resolves `_silo._tcp.<domain>` when seeds are empty |

### Storage & replication

| Variable | Default | Purpose |
|---|---|---|
| `SILO_DATA_DIR` | `/var/lib/silo` | Where chunks are stored on this node |
| `SILO_CHUNK_SIZE` | `262144` (256 KiB) | Default chunk size (overridable per-inode/StorageClass). 256 KiB minimises copy-on-write amplification on small writes; raise it for large-sequential, capacity-heavy volumes |
| `SILO_REPLICATION` | `3` | Default replication factor |
| `SILO_MAX_CONCURRENT_WRITES` | `64` | Max peer replica sends in flight at once (across all writes, counting background stragglers). Caps grpc's replication send-buffer pool (≈ n·chunk-size); without it a write storm across many volumes grows the pool without limit and OOM-kills silod. `0` = unbounded. Raise it for more write parallelism if you have memory headroom |
| `SILO_SCRUB_INTERVAL` | internal default | Re-replication scrubber cadence (the local stack sets `5s` for visible healing; production paces slower). The scrubber heals only chunks the cluster still references; an orphan is left to the chunk GC, since copying one back from a peer that has not swept yet would undo the reclamation. |
| `SILO_TOMBSTONE_RETENTION` | `24h` | How long namespace tombstones are kept before GC |
| `SILO_MAX_CLOCK_SKEW` | `500ms` | Warn + count an alert when a peer's HLC exceeds this skew |
| `SILO_EXTENT_REPLICATION` | `true` | Serve each volume's extent map from its replica set (out of band, like chunks) instead of the gossiped namespace. Required for a volume to attach on any node, not just where it was created. Leave on. |
| `SILO_NBD_TRIM` | `true` | Act on discards from NBD clients: every extent a discard fully covers is unmapped, reads as zeroes, and its chunk becomes garbage for the chunk GC. Set it `false` to acknowledge discards without doing anything, which is what silo did before trim support. See [Rolling upgrades](#rolling-upgrades) for when that matters. |
| `SILO_EXTENT_REAP_AFTER` | `1h` | How long a deleted volume's extent map must sit untouched before the reaper reclaims orphaned replicas. This is the age guard that stops a freshly-created volume whose directory entry has not yet gossiped to a node from being mistaken for a deleted one. Keep it well above gossip convergence time. |
| `SILO_EXTENT_REAP_INTERVAL` | `15m` | Extent-map reaper sweep cadence (the GC backstop for the synchronous delete path). |
| `SILO_EXTENT_SCRUB_INTERVAL` | `1m` | Extent-map scrubber cadence: re-replicates an idle volume's extent map to its full replica set after a node loss (the metadata analog of `SILO_SCRUB_INTERVAL`, which heals chunks). The synchronous write-path fan-out only keeps replicas in step while writes flow; a volume written once and then left idle would otherwise stay under-replicated after a later node loss. |
| `SILO_CHUNK_GC_INTERVAL` | `10m` | Chunk garbage-collector sweep cadence. The GC reclaims chunks no live volume or file references anymore (whole-volume deletes and copy-on-write overwrite orphans) by mark-and-sweep over the cluster's live reference set. |
| `SILO_CHUNK_GC_GRACE` | `1h` | How old an unreferenced chunk must be before the GC reclaims it. This is the safety margin for the write-then-record gap (a chunk is stored just before its reference is recorded). Keep it well above write latency plus gossip/HLC skew. Half of it is also how far back a node's extent maps must match their replicas before it sweeps (see `SILO_CHUNK_GC_ENABLE`). |
| `SILO_CHUNK_GC_ENABLE` | `true` | Actually delete. On by default: copy-on-write overwrites orphan chunks continuously and no other mechanism reclaims them, so a store with reclamation off does not hold steady, it fills. Set it `false` for the reporting-only **dry run**, which computes the live set and reports `silo_chunkgc_orphan_chunks` without removing anything. Dry run is worth using to validate the computation against a particular store, but it is a commissioning step rather than a resting state. A sweep also waits while this node's extent maps are behind their replicas (`silo_chunkgc_diverged_maps`), since a keep set built from a map that missed a write omits chunks the cluster still references. Bindings written in the last half of `SILO_CHUNK_GC_GRACE` are allowed to be missing: they can only name chunks the grace window already protects, and it is what lets the GC sweep while writes and trims are in flight. |
| `SILO_CHUNK_GC_MAX_DELETES` | `100000` | Cap on reclamations per sweep. Only binds on a backlog: it keeps the first sweep after a long-running leak from turning into one unbounded burst of unlinks against the disk the serving path is using, draining across consecutive sweeps instead. `silo_chunkgc_deferred_chunks` reports what a sweep left behind. Negative = no cap. |

### Encryption (at rest)

Every chunk is AES-GCM encrypted with a per-chunk data key, wrapped by one
operator-managed cluster encryption key.

| Variable | Default | Purpose |
|---|---|---|
| `SILO_ENCRYPTION_KEY_SOURCE` | `static` | `static`, `file`, `aws-kms`, `gcp-kms`, or `azure-kv` |
| `SILO_ENCRYPTION_KEY` | *(none)* | base64 32-byte key; required when source is `static`. Generate: `openssl rand -base64 32` |
| `SILO_ENCRYPTION_KEY_PATH` | *(none)* | `file`: a raw 32-byte key (`openssl rand 32 > /etc/silo/key && chmod 0400`). KMS sources: the **KMS-wrapped** key blob. |
| `SILO_KMS_KEY_ID` | *(none)* | `aws-kms`: key ARN/id. `gcp-kms`: crypto-key resource name (`projects/…/cryptoKeys/…`). |
| `SILO_KMS_VAULT_URL` / `SILO_KMS_KEY_NAME` | *(none)* | `azure-kv`: Key Vault base URL + the RSA wrapping key's name. |

> Losing the encryption key means losing the data. The wrapped per-chunk keys
> live in the inode metadata and are useless without it. Back it up like a root
> credential.

**KMS (envelope encryption).** Generate a 32-byte cluster key, wrap it with your
cloud KMS, and store the *ciphertext* at `SILO_ENCRYPTION_KEY_PATH`. silod
decrypts it once at startup, so the plaintext key never touches disk.
Credentials come from each provider's standard chain (AWS env/role, GCP ADC,
Azure managed identity). Example (AWS):

```sh
openssl rand 32 > key.bin
aws kms encrypt --key-id <arn> --plaintext fileb://key.bin \
  --query CiphertextBlob --output text | base64 -d > /etc/silo/key.wrapped
export SILO_ENCRYPTION_KEY_SOURCE=aws-kms SILO_KMS_KEY_ID=<arn> \
       SILO_ENCRYPTION_KEY_PATH=/etc/silo/key.wrapped
```

GCP wraps with `gcloud kms encrypt`; Azure wraps the key with an RSA Key Vault
key. To rotate the cluster key, re-wrap it and restart.

### TLS (cluster-internal mTLS)

| Variable | Purpose |
|---|---|
| `SILO_TLS_CA_CERT` / `SILO_TLS_CA_KEY` | Cluster CA material. `silod` self-mints on first boot if absent. |
| `SILO_TLS_CA_SEED` | Set on the one node that mints the CA into shared storage |
| `SILO_TLS_NODE_CERT` / `SILO_TLS_NODE_KEY` | This node's server cert (issued from the CA) |
| `SILO_TLS_CRL` | Path to a CA-signed certificate revocation list. When set, silod rejects any mTLS peer whose cert serial is in it. Unset = no revocation checking. |
| `SILO_REQUIRE_TOKENS` | When `1`/`true`, client-cert callers must also present a scoped capability token (`SILO_TOKEN`). Cluster nodes are exempt. Default off (mTLS-only). |

Node certs are valid for one year and auto-rotate: a restart within their final
~4 months re-mints them. Rolling upgrades therefore keep identity fresh without
manual steps.

### Scoping client access with capability tokens

mTLS proves a caller is a cluster member; a **capability token** scopes what that
member may do. Enforcement is opt-in: set `SILO_REQUIRE_TOKENS=1` on silod, and
every caller presenting a *client* certificate must also carry a token granting
the operation. Cluster nodes (their certs carry a `spiffe://silo/node/` identity)
are exempt, so inter-node replication is unaffected.

Mint a token offline on a host that holds the CA key:

```sh
siloctl auth mint-token \
  --principal=csi@cluster \
  --cap=chunk:read --cap=chunk:write --cap=namespace:write \
  --ttl=24h > token.txt
```

Capabilities: `chunk:read`, `chunk:write`, `namespace:read`, `namespace:write`,
`status:read`, `node:admin`, or `*` for all (operator/admin tokens; use
sparingly). The token prints to stdout; the export hint goes to stderr, so
`> token.txt` captures only the token. Give it to the client via the environment:

```sh
export SILO_TOKEN=$(cat token.txt)   # siloctl, silo-csi, silo-fuse all read this
```

Tokens are signed by the cluster CA and self-expiring, so a leaked token stops
working at its TTL. Keep TTLs short for automation and re-mint. There is no
revocation list for tokens themselves; to cut one off sooner, rely on the
expiry, or rotate the CA (a far more disruptive step). Capabilities are
operation classes, not per-path/per-volume scopes (that is roadmapped).

### Revoking a compromised certificate

When a node or operator credential is compromised, revoke its cert so the
cluster stops trusting it without rotating the whole CA. Run on a host that
holds the CA key (`SILO_TLS_CA_KEY`):

```sh
# By cert file (siloctl reads the serial out of it)…
siloctl ca revoke --crl=/etc/silo/revoked.crl --cert=/path/to/leaked-node.crt
# …or by raw serial (hex, as openssl prints it):
siloctl ca revoke --crl=/etc/silo/revoked.crl --serial=1A:2B:3C

siloctl ca list-revoked --crl=/etc/silo/revoked.crl   # inspect the result
```

`revoke` extends the CRL in place (incrementing its sequence number), so prior
revocations are preserved. Then:

1. Distribute `revoked.crl` to every node (the same way you distribute the CA).
2. Set `SILO_TLS_CRL=/etc/silo/revoked.crl` on each node and restart silod.

silod verifies the CRL against the cluster CA at startup (a foreign-signed or
corrupt CRL is refused) and logs `certificate revocation list loaded` with the
count. A CRL past its `NextUpdate` still enforces but logs a staleness warning.
Re-run `siloctl ca revoke` (even with no new serials) to refresh it.

### Logging & diagnostics

| Variable | Default | Values |
|---|---|---|
| `SILO_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SILO_LOG_FORMAT` | `text` | `text`, `json` (use `json` in production) |
| `SILO_PPROF` | `false` | `true` mounts `net/http/pprof` at `/debug/pprof/` on `SILO_HTTP_ADDR`; see [Health, metrics & profiling](#health-metrics--profiling) |
| `SILO_UI_ENABLED` | `true` | Serves the read-only [web UI](#web-ui) at `/` and `GET /api/v1/status` on `SILO_HTTP_ADDR`. Set to `false` to turn both off |

---

## Deployment paths

silo runs the same single binary everywhere. What changes is *where* it runs and
how many you run. Two choices set the shape.

**Where the disks live: co-located or dedicated.**

- **Co-located (small scale).** Run silod on machines you already have: your
  Kubernetes control-plane or worker nodes, or a couple of existing hosts. Inside
  Kubernetes that's a DaemonSet using node-local disk; outside it's the standalone
  binary under systemd. There is no separate storage hardware to buy or manage.
  This is the right place to start, and it is fully production-capable with 3+
  nodes.
- **Dedicated (larger scale).** Run silod on its own set of machines sized for
  storage and point your clients (Kubernetes clusters or anything speaking the
  gRPC/NBD surface) at them. Same binary, same wiring; only the disks move off
  the compute nodes, so storage and compute scale independently.

**How many you run: durability.**

| Path | Nodes | Survives a node loss? | Use it for |
|---|---|---|---|
| **[Standalone](#1-standalone-single-node)** | 1 | **No** (single copy; back it up) | edge, a single host, dev, a small block-volume or object store |
| **[Cluster](#2-cluster-multi-node)** | 3+ | **Yes** (replication factor N) | co-located on your nodes, or a dedicated fleet |
| **[Kubernetes](#3-kubernetes)** | 3+ | **Yes** | pods consuming silo as a CSI StorageClass |

All paths are production-capable. The difference between Standalone and Cluster is
replication: a standalone node keeps **one** copy of each chunk (replication
clamps to the node count), so its durability comes from the disk and from
[backups](#backups), not from peers. Add nodes (same CA, same encryption key)
and it becomes a Cluster with no data migration, whether those nodes are
co-located or dedicated.

### 1. Standalone (single node)

One `silod`, one disk. No gossip, no seeds; it self-mints its cluster CA on first
boot. Good for a single block volume, an edge box, a CI artifact store, or SDK
work. Use it anywhere you want silo's encryption + volume/namespace surface
without running a fleet.

Fastest start (containerised, durable volume):

```sh
make up-local          # one silod in Docker; generates a dev key into deploy/.env
```

For a production single node, run the binary (or container) with a
**file-sourced** key, a durable data dir, and, because there is no replication,
a **backup target**:

```sh
# One-time: a 32-byte key on a 0400 file (NOT the static env source).
openssl rand 32 > /etc/silo/key && chmod 0400 /etc/silo/key

SILO_ENCRYPTION_KEY_SOURCE=file \
SILO_ENCRYPTION_KEY_PATH=/etc/silo/key \
SILO_DATA_DIR=/var/lib/silo \
SILO_NBD_ADDR=0.0.0.0:10809 \
SILO_BACKUP_TARGET=s3://my-bucket/silo \
  ./bin/silod
```

Then claim operator credentials against this node's bootstrap port the same way
the cluster path does ([below](#claiming-operator-credentials)). Caveats: a
single node is a single point of failure with no automatic failover. Keep
backups, and if you need HA, run a Cluster instead.

### 2. Cluster (multi-node)

Three or more `silod`s gossiping over mTLS, replicating every chunk to
`SILO_REPLICATION` nodes (default 3). The cluster stays available and re-forms
replicas when a node is lost. `deploy/docker-compose.yml` (`make up`) is the
reference wiring on one host; the same environment runs on separate hosts/VMs
under systemd or any container runtime.

The wiring that makes a multi-node cluster work:

- **One CA seed.** Exactly one node sets `SILO_TLS_CA_SEED=1` and mints the
  shared cluster CA into storage the others can read (a shared volume in Compose,
  or a pre-distributed CA in production; see [TLS](#tls-cluster-internal-mtls)).
  The rest wait for it. Every node trusts the same CA.
- **Routable advertise addresses.** Each node sets `SILO_GOSSIP_ADVERTISE` and
  `SILO_GRPC_PEER_ADVERTISE` to addresses peers can actually dial (a hostname or
  IP, not `0.0.0.0`).
- **Seeds point at gossip ports.** `SILO_SEEDS` lists peers' **gossip**
  addresses (7100 by default), not the data port. One reachable seed is enough
  to join; gossip discovers the rest. Seeds are not only for startup: a node
  that sees fewer than `SILO_REPLICATION`-1 alive peers retries its seeds with
  backoff until the missing peers are back, so list more than one.
- **Failure detection is deliberately slow.** A peer that stops answering is
  marked suspect after one failed probe, dead after 30 seconds without a
  refutation, and forgotten 10 minutes after that. A kubelet restart or a short
  network blip does not empty the membership table.
- **Shared encryption key.** Every node uses the same cluster encryption key
  (file or KMS source). The key belongs to the cluster, not to any one node.

Each node serves its own gRPC/NBD locally; any node can coordinate a write.

### 3. Kubernetes

**Co-located (the common case).** Run `silod` as a `hostNetwork` DaemonSet (one
per node) so the CSI node plugin reaches it at `127.0.0.1:10809`, fronted by a
`Service` for the controller's gRPC. Set `SILO_GOSSIP_ADVERTISE` to the Pod IP
(downward API), `SILO_NBD_ADDR` to enable block serving, and mount
`SILO_DATA_DIR` on durable node-local storage. The DaemonSet lands silod on
whichever nodes you select (control-plane nodes for a small cluster, or a
labelled subset of workers), using disk you already have.

**Dedicated.** For a larger or independently-scaled store, run the silod Cluster
(path 2) on its own machines and point the CSI controller at it via
`silod.address`; the node plugin dials that fleet instead of a node-local silod.
Storage capacity then grows by adding silod machines without touching the compute
nodes.

Either way the cluster wiring is the same as path 2; the
[`silo-csi` Helm chart](../deploy/helm/silo-csi) installs the driver that turns
volumes into a StorageClass. silod itself is operator-deployed today (a packaged
`silod` chart is roadmapped; see [known-gaps.md](known-gaps.md)). Full guide:
**[kubernetes.md](kubernetes.md)**.

---

## Claiming operator credentials

Every cluster runs under mutual TLS. On first boot `silod` mints its cluster CA
and prints a one-time, single-use join command (token + server fingerprint) to
its logs:

```sh
siloctl auth init \
  --token <TOKEN> \
  --server <bootstrap-host:7001> \
  --server-fingerprint <FINGERPRINT>
```

This writes the cluster CA, your client cert, and the matching key into
`~/.config/silo/` (or the platform config dir) and records the cluster's mTLS
gRPC address. Subsequent `siloctl` commands authenticate automatically; no
flags needed.

- Inspect your credentials: `siloctl auth status`
- Mint another token (e.g. for a teammate): restart `silod` with
  `SILO_PRINT_BOOTSTRAP_TOKEN=1`.
- Wipe cached credentials (e.g. before re-joining a freshly-bootstrapped
  cluster whose CA has rotated): `siloctl auth clean` (prompts; pass `--yes`
  to skip). `make down` runs this automatically.

The same `~/.config/silo/` material (`ca.crt`, `client.crt`, `client.key`) is
what you load into the CSI controller's `silod.tls.secretName` Secret for mTLS
in Kubernetes.

---

## Capacity rebalancing

Nodes advertise their backing-store capacity and usage over gossip. Placement is
**capacity-weighted**: a node with twice the disk is given twice the ring space
and therefore holds roughly twice the chunks. A cluster of mixed-size disks
keeps its used-fraction balanced instead of filling the smallest node first. When
capacity changes (a bigger node joins, or a disk is grown), the ring re-weights
so **new** chunks follow the new weighting. Rebalancing is prospective: the
scrubber adds replicas to match the ring but never deletes, so existing chunks
are not retroactively migrated (active migration/GC is roadmapped; see
[known-gaps.md](known-gaps.md)). A grown or fresh node fills via new writes, not
by pulling old data off its peers.

This is automatic and needs no operator action. Watch `silo_rebalancer_capacity_skew`
trend toward zero, and per-node fill via `siloctl status` (the CAPACITY column)
or `silo_storage_used_bytes / silo_storage_capacity_bytes`. A homogeneous cluster
(equal disks) is never reshuffled: equal weights reproduce the original ring
exactly.

### Disk high-watermarks (DiskPressure)

silo guards against a node filling to 100% with two watermarks on the data
filesystem, analogous to a kubelet node's DiskPressure condition and eviction
threshold. Both are fractions of the filesystem used, set via env:

| Variable | Default | Meaning |
|---|---|---|
| `SILO_DISK_PRESSURE_HIGH` | `0.85` | Enter the soft DiskPressure condition at/above this. |
| `SILO_DISK_PRESSURE_CLEAR` | `0.80` | Leave it only back at/below this (hysteresis, so it doesn't flap). |
| `SILO_DISK_PRESSURE_HARD` | `0.95` | Refuse new writes at/above this, before the filesystem hits ENOSPC. |
| `SILO_DISK_PRESSURE_STEERING` | `true` | Steer new chunks away from pressured nodes (below). Set `false` for the plain ring. |

- **Soft tier (DiskPressure condition + placement steering).** A node crossing
  the high watermark raises a condition it gossips to the cluster and exposes as
  `silo_rebalancer_disk_pressure` (1/0); any node also reports how many peers are
  pressured via `silo_rebalancer_pressured_nodes`. With steering on (the
  default), this condition **stops new chunks from landing on the node**: every
  path that resolves a chunk's replicas (writes, reads, deletes, and the
  scrubber) prefers non-pressured nodes for that chunk. A near-full node stops
  being chosen for new data, and the scrubber heals around it instead of
  retrying it forever. Steering is **bounded**: it keeps a quorum (`n/2+1`) of
  each chunk's natural ring replicas, so the steered set always overlaps the
  unsteered set by at least one node. That overlap keeps reads correct and lets
  the scrubber heal across pressure changes (no relocation of a chunk's whole
  replica set, so no read-availability gap). It steers **new** placement; it does
  not retroactively migrate or delete the chunks already on a pressured node
  (that needs active migration; see [known-gaps.md](known-gaps.md)). The node
  drains as old chunks are deleted and as new data lands elsewhere. The response
  to sustained pressure is still yours: add capacity or [drain](#draining-a-node).
  Steering does add some re-replication when a node first crosses the
  watermark (chunks it was a replica for gain a copy on a non-pressured node);
  the scrubber paces it. Disable with `SILO_DISK_PRESSURE_STEERING=false`
  to keep the plain capacity-weighted ring.
- **Hard tier (write fence).** At the hard watermark the node refuses new chunks
  with `RESOURCE_EXHAUSTED` (`ErrNoSpace`) before the disk is truly full. In the
  replication coordinator a refused replica only fails its own ack, so the write
  still completes on the other replicas (quorum) and the scrubber heals the chunk
  onto the node once it has room. Existing chunks are always still served. If
  *enough* nodes are hard-full that quorum can't be met, writes fail cluster-wide
  with an actionable error. That is the signal that the cluster is out of
  space and needs nodes added.

When you see DiskPressure, the response is the same as a full node below:
add capacity or drain.

## Backups

Set `SILO_BACKUP_TARGET` and every node periodically copies its data to object
storage (S3 or anything that speaks the S3 protocol, Google Cloud Storage, Azure
Blob, or a local directory). Backups are:

- **Incremental.** Each run uploads only the chunks the target does not have yet.
- **Encrypted chunks.** Chunks are copied exactly as they sit on disk, still
  encrypted with the cluster key. The bucket never sees your file contents, and
  the key is never written to it. Only chunks are encrypted: the namespace and
  extent objects are plaintext JSON, so anyone with access to the bucket can
  read file and volume paths, volume names and sizes, and lease holders. Volume
  ids also appear in object names, base64-encoded, which is not encryption.
  Treat bucket access as access to that metadata.
- **Checksummed.** The manifest records the size and SHA-256 of every object it
  lists, so damage and mix-ups are detected on verify and restore.
- **Restorable.** Each run ends by writing a *manifest*: a small JSON file that
  lists everything the run saved. A manifest is only written after every upload
  succeeded, so each manifest on the target is a complete restore point. A failed
  run leaves no manifest and the next run tries again.

`siloctl backup restore` rebuilds a data directory from any restore point; see
[Restore](#restore).

### Configuration

All settings are environment variables on `silod`.

| Variable | Default | Meaning |
|---|---|---|
| `SILO_BACKUP_TARGET` | empty | Where backups go. Empty turns backups off. Forms: `/local/dir` or `file:///dir`, `s3://bucket/prefix`, `gs://bucket/prefix`, `az://account/container/prefix`. A *prefix* is a folder-like path inside the bucket, so several systems can share one bucket. Credentials inside the URL are refused. |
| `SILO_BACKUP_INTERVAL` | `6h` | Time between runs, as a Go duration (`30m`, `1h`, `6h`). The first run happens one interval after silod starts, so a node stuck in a restart loop cannot hammer the bucket. |
| `SILO_BACKUP_RETAIN` | `7` | Number of backups (manifests) kept per node. `0` turns pruning off and keeps everything. |
| `SILO_BACKUP_RETAIN_FOR` | `168h` (7 days) | A backup younger than this is never pruned, whatever `SILO_BACKUP_RETAIN` says, so a burst of runs cannot push every good restore point out. Go duration. `0` turns the floor off. |
| `SILO_BACKUP_S3_ENDPOINT` | unset | Base URL of an S3-compatible service (MinIO, Ceph RGW, Backblaze B2, Hetzner Object Storage, Scaleway and so on), `http://` or `https://`, without credentials or a query string. Only valid with an `s3://` target. |
| `SILO_BACKUP_S3_REGION` | `us-east-1` with an endpoint | Region to sign requests for. Without an endpoint, the AWS SDK's usual lookup applies (`AWS_REGION`, `~/.aws/config`). |
| `SILO_BACKUP_S3_PATH_STYLE` | `true` with an endpoint, else `false` | Path-style addressing sends requests to `https://endpoint/bucket/key` instead of `https://bucket.endpoint/key`. Most self-hosted services need path-style, because they have no wildcard DNS name per bucket. |

Credentials come from each provider's standard chain. There are no silo-specific
credential variables.

| Backend | Where silod finds credentials |
|---|---|
| S3 and S3-compatible | The AWS SDK default chain: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`), an `AWS_PROFILE` from the shared config files, or an IAM role for service accounts (IRSA) or instance profile |
| Google Cloud Storage | Application Default Credentials |
| Azure Blob | `DefaultAzureCredential` |

silod never logs credentials. Status output and logs show only the target URL.

### What gets written

Every node writes into the same prefix. Chunks are shared, everything else is
filed under the node's id (`SILO_NODE_ID`) and a *stamp*: the run's start time in
UTC, such as `2026-10-28T12:00:00.000Z`.

```
<prefix>/
  chunks/<chunk-id>                              # encrypted chunk, as on disk
  runs/<node>/<stamp>.json                       # marker: this node has a run in progress
  namespace/<node>/<stamp>.json                  # file and volume tree snapshot (plaintext)
  extents/<node>/<stamp>/<volume-id>.json        # which chunk holds which block (plaintext)
  manifests/<node>/<stamp>.json                  # one per run: a restore point
  manifests/<node>/latest.json                   # copy of the newest manifest
```

- **Chunks** are shared by all nodes under the prefix. A chunk never changes once
  written, so one that is already present is skipped. Chunks are written with a
  "only if absent" condition (`If-None-Match` on S3), so two nodes uploading the
  same chunk cannot overwrite each other. A server that ignores conditional
  writes loses that protection: both nodes may write, the last copy wins, and
  the node whose copy was replaced fails checksum verification for that chunk.
  Use a server that supports conditional writes (AWS S3 and RustFS do).
- **The namespace snapshot** is the tree of files and volumes. It is replicated
  to every node, so any one node's snapshot describes the whole cluster.
- **Extent maps** are per volume. Each lists which chunk holds which block of
  that volume. This node's maps are saved; the volume id in the file name is
  base64url-encoded.
- **The manifest** records the silo version, node id, start and finish times, the
  names of the namespace and extent objects, the full list of chunk ids, and the
  counts and byte totals. Every namespace, extent and chunk entry carries the
  object's size and SHA-256.
- **Run markers** announce a run in progress. A node writes
  `runs/<node>/<stamp>.json` when a run starts, refreshes it every 5 minutes and
  deletes it when the run ends, whether it succeeded or not. See
  [Retention](#retention) for how other nodes use them.

Older silo versions wrote `namespace/<node>.json` and
`extents/<node>/<volume>.json` without a stamp. Pruning never touches those. Once
a new backup exists you can delete them by hand.

### How a run works

1. Write the run marker and start refreshing it.
2. Snapshot the namespace and this node's extent maps, and upload them.
3. List the chunk objects on the target once (one listing instead of one request
   per chunk).
4. Upload only the chunks the target is missing, four at a time. A chunk that
   another node uploaded first is downloaded and checksummed instead.
5. List the chunks again. If any chunk the manifest would refer to is gone or has
   a different size, the run fails without writing a manifest, rather than
   record a backup that cannot be restored.
6. Write the timestamped manifest, then `latest.json`, and delete the marker.

Chunks that silod's garbage collector deletes during a run are left out of that
run's manifest.

### Retention

After each successful scheduled run, and only when `SILO_BACKUP_RETAIN` is above
0, the node prunes. Runs started with `siloctl backup run` never prune; only the
schedule does.

1. Its own oldest timestamped manifests beyond the newest N, and only those older
   than `SILO_BACKUP_RETAIN_FOR`. `latest.json` is never deleted, and other
   nodes' manifests are never touched.
2. Its own namespace and extent objects that no kept manifest refers to.
3. Chunk objects that no manifest of any node under the prefix refers to.

Unreferenced objects are only deleted once they are older than a grace window of
`max(2 x SILO_BACKUP_INTERVAL, 24h)`. Another node sharing the prefix may be
halfway through a run: it has uploaded chunks but not yet written the manifest
that refers to them. If any manifest under the prefix cannot be read, no chunks
are deleted that round.

Chunks get a second guard. While another node's run marker is fresher than 1
hour, this node deletes no chunks, because that run may have decided to reuse a
chunk that looks unreferenced. Markers older than 1 hour belong to crashed runs
and are cleaned up. Retention is also skipped for a round when the target's
listing does not yet show the manifest that was just written, since a lagging
listing could hide manifests that keep chunks alive.

Pruning problems are logged and counted in `silo_backup_prune_failures_total`,
but they never mark a backup as failed.

The manifests of a node that was removed from the cluster stay, and so do the
chunks they refer to. Delete `manifests/<old-node>/` by hand once you no longer
need them; the chunks go on a later prune.

### Checking and running backups

`siloctl status` shows a Backup block: the target, the interval, the result and
age of the last run, and when the next run is due.

To back up right now:

```sh
siloctl backup run [--server host:port] [--wait 1h]
```

This asks a node to run a backup and waits for the result. It is refused when
backups are not configured on that node or a run is already going. If `--wait`
runs out, the backup keeps going on the node; `siloctl status` shows the outcome
later. You need operator credentials (`siloctl auth init`). With
`SILO_REQUIRE_TOKENS`, the token needs the `node:admin` capability.

### Inspecting a backup without a running cluster

These three commands read the bucket directly, so they work with no silod
running, for example from a laptop during a disaster. They take `--target` plus
`--s3-endpoint`, `--s3-region` and `--s3-path-style`, which default from the same
`SILO_BACKUP_*` variables. Credentials come from the environment (`AWS_*` for S3).

| Command | What it does |
|---|---|
| `siloctl backup list --target URL` | Table of backups per node: time, chunk count, size, silo version |
| `siloctl backup verify --target URL --node ID [--manifest latest\|STAMP] [--deep]` | Checks that every object the manifest refers to exists and has the recorded size. With `--deep` it downloads everything and checks each SHA-256, which catches damage that keeps the size. Exit status 1 when anything is wrong |
| `siloctl backup restore --target URL --node ID --data-dir DIR [--manifest latest\|STAMP] [--concurrency 4]` | Writes a data directory from a backup; see [Restore](#restore) |

### Examples

#### MinIO (self-hosted)

Create a bucket (for example `silo-backups`) and an access key that can read,
write and delete objects in it, using your MinIO's own console or tooling.

Give silod the endpoint and credentials. Path-style addressing and the
region already default correctly when an endpoint is set:

```sh
export SILO_BACKUP_TARGET=s3://silo-backups/prod
export SILO_BACKUP_S3_ENDPOINT=https://minio.example.com:9000
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

Use a dedicated user limited to that bucket rather than the root account.
Backups are only as safe as the bucket: see
[Protecting the bucket](#protecting-the-bucket).

#### AWS S3

Create the bucket, then let the silod runtime identity (an EC2 instance role,
EKS IRSA, an ECS task role, or a static IAM user, in that order of preference)
read, write and delete under the prefix. `s3:DeleteObject` is only needed for
pruning and for replacing run markers; `verify`, `restore` and `list` only read:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
    "Resource": "arn:aws:s3:::my-backups/silo/*"
  }, {
    "Effect": "Allow",
    "Action": ["s3:ListBucket"],
    "Resource": "arn:aws:s3:::my-backups",
    "Condition": {"StringLike": {"s3:prefix": ["silo/*"]}}
  }]
}
```

For `siloctl backup verify`, `restore` and `list`, use a separate read-only
credential with `s3:GetObject` and `s3:ListBucket` only. Then a leaked restore
credential cannot delete or rewrite backups.

Leave `SILO_BACKUP_S3_ENDPOINT` unset so the SDK uses AWS itself:

```sh
export SILO_BACKUP_TARGET=s3://my-backups/silo
export SILO_BACKUP_INTERVAL=1h
export AWS_REGION=eu-west-1
```

The chunks are already encrypted, so bucket-side encryption (SSE-S3 or SSE-KMS)
is an extra layer for them. It also covers the plaintext metadata objects, which
silo does not encrypt itself.

#### Backblaze B2, Hetzner Object Storage, Scaleway

Use the provider's S3 endpoint URL and region, with the access key pair the
provider issued:

```sh
export SILO_BACKUP_TARGET=s3://my-bucket/silo
export SILO_BACKUP_S3_ENDPOINT=https://<provider-s3-endpoint>
export SILO_BACKUP_S3_REGION=<provider-region>
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
```

#### Google Cloud Storage

Create the bucket (uniform bucket-level access is fine) and grant the silod
service account `roles/storage.objectAdmin` on it, since pruning deletes objects.
Credentials come from Application Default Credentials: workload identity on GKE,
a key file named by `GOOGLE_APPLICATION_CREDENTIALS`, or
`gcloud auth application-default login` on an operator host.

```sh
export SILO_BACKUP_TARGET=gs://my-backups/silo
```

#### Azure Blob

Create a storage account and a container. The URL is
`az://<account>/<container>/<prefix>`, and the account name maps to
`https://<account>.blob.core.windows.net`. Grant the silod identity
`Storage Blob Data Contributor` on the container. On AKS use a managed or
workload identity. Elsewhere set `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and
`AZURE_CLIENT_SECRET`.

```sh
export SILO_BACKUP_TARGET=az://mybackups/silo/prod
```

### Local stack

`make up` also starts an S3-compatible server (RustFS, compose services `s3` and
`s3-init`), and every node backs up to `s3://silo-backups/` on it every hour. Its
S3 API is on <http://localhost:9000> and its storage console on
<http://localhost:9001/rustfs/console/>; log in with `SILO_S3_ACCESS_KEY` and `SILO_S3_SECRET_KEY`
from `deploy/.env`. `make backup-demo` triggers a backup on silo-a (through its
operator gRPC port, 127.0.0.1:7900) and lists the result. The images of the
local stack are pinned by tag and digest, so a re-pushed tag cannot change what
runs, and `deploy/.env` is created with mode 600 because it holds the storage
credentials.

### Protecting the bucket

Anyone who can write to the bucket can delete or rewrite backups. The checksums
in a manifest catch damage and mix-ups, but they are not signed (see
[known gaps](known-gaps.md)). To limit what a compromised node or leaked
credential can do:

- Turn on **bucket versioning** and/or an immutability feature: S3 Object Lock,
  a GCS retention policy or object holds, or Azure immutable storage. Deleted or
  overwritten backups can then be recovered or cannot be changed at all.
- Give `silod` (which prunes) and the people who restore separate credentials,
  and keep the restore credential read-only.
- Remember that bucket access reveals the plaintext metadata described above.

### Cost and bucket lifecycle

Because unchanged chunks are skipped, a run costs one listing plus the new data,
and storage grows with your data rather than with the number of runs. Retention
keeps the number of manifests bounded. If you enable bucket versioning, deleted
objects stay as old versions; add a lifecycle rule to expire them, for example on
S3:

```json
{
  "Rules": [{
    "ID": "silo-old-versions",
    "Status": "Enabled",
    "Filter": {"Prefix": "silo/"},
    "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
  }]
}
```

### Monitoring

All metrics are prefixed `silo_backup_`:

| Metric | Meaning |
|---|---|
| `runs_total` | Runs attempted |
| `failures_total` | Runs that failed |
| `last_chunks` | Chunks in the last successful backup |
| `last_success_timestamp_seconds` | When the last run succeeded |
| `last_duration_seconds` | How long the last run took |
| `chunks_uploaded_total` | Chunks uploaded |
| `chunks_skipped_total` | Chunks skipped because the target already had them |
| `bytes_uploaded_total` | Bytes uploaded |
| `pruned_objects_total` | Objects deleted by retention |
| `prune_failures_total` | Retention deletions that failed |

Alert when backups have stopped succeeding:

```
time() - silo_backup_last_success_timestamp_seconds > 2 * <interval in seconds>
```

If `silo_backup_runs_total` is still zero well after one interval, check silod's
logs for `backup failed; the next scheduled run will retry`.

### Read size limits

siloctl and silod cap how much they read for each kind of object: manifests and
run markers 256 MiB, namespace and extent objects 1 GiB, chunks 64 MiB plus the
encryption overhead. An object over its cap is refused with an error that names
it. Without caps, anyone with write access to the bucket could plant a huge
object and exhaust the memory of silod or siloctl. If you hit a cap on a
legitimate backup, the object was probably not written by silo.

### Restore

`siloctl backup restore` writes `namespace.json`, `extents/` and every chunk file
into a directory, in exactly the layout silod loads at boot:

```sh
siloctl backup restore --target s3://silo-backups/prod --node silo-a \
  --data-dir /var/lib/silo-restored [--manifest latest|STAMP] [--concurrency 4]
```

- The directory must not exist or must be empty. siloctl never deletes or
  overwrites files.
- The namespace snapshot is checked to decode before it is written.
- Before writing anything, siloctl downloads every object and checks its size
  and SHA-256 against the manifest. A mismatch stops the restore, the damaged
  object is never written, and the error names it.
- It also checks that the target directory's disk has enough free space for the
  whole backup plus a margin. This check works on Linux; elsewhere siloctl prints
  a note and carries on.
- Chunk ids are not content hashes, but the manifest checksums cover the stored
  bytes. silod also verifies each chunk's encryption tag when it first reads it.
- Run `siloctl backup verify --deep` ahead of time for a restore you depend on.
- If a restore fails part-way, delete the directory and run it again.

The full step-by-step procedure, including TLS files and the encryption key, is
in the [runbook](runbook.md#restoring-from-a-backup).

> Keep the encryption key somewhere other than the bucket. A backup without the
> key is unreadable. KMS-wrapped key blobs are safe to keep next to the backups;
> raw static keys are not.

## Draining a node

To remove a node without risking data, drain it first:

```sh
siloctl node drain --server <node-grpc-addr>
```

This marks the node as having left the cluster and announces it over gossip.
Peers route new placement around it and the scrubber re-replicates the chunks it
held onto survivors, so every volume's replication factor is restored *before*
the node goes away. Drain is **not** shutdown: the node keeps running and serving
its chunks so survivors can rebuild from a quorum.

Watch the re-replication finish, then remove the node:

```sh
# shortfall falls back to zero once every chunk is back to full replication
curl -s http://<node>:7080/metrics | grep silo_replication_shortfall_chunks
```

When `silo_replication_shortfall_chunks` is zero across the cluster, stop and
remove the node. A drained node that is killed early is still safe as long as the
remaining replicas met quorum; drain only makes the transition orderly.

## Rolling upgrades

silod advertises a **cluster wire-protocol version** on every gossip message,
separate from the build's semver. Two builds with the same protocol interoperate.
That is what makes a rolling upgrade (replace nodes one at a time) safe: the new
build keeps speaking the old protocol until *every* node is upgraded, and only a
later release bumps the protocol.

Each node continuously classifies its peers:

- **Compatible**: same protocol window. Normal operation.
- **Newer peer**: a peer is ahead of this node. Its messages are still
  processed (the wire format tolerates unknown fields), but
  `silo_gossip_newer_protocol_messages_total` climbs and silod logs a warning.
  During an upgrade this is expected on the not-yet-upgraded nodes and should
  fall to a flat line once every node is on the new build.
- **Unsupported (too old)**: a peer below this build's minimum supported
  protocol. silod **fences** it: the message is dropped (neither the sender nor
  its gossip is merged) and `silo_gossip_incompatible_messages_total` climbs.
  This only happens when a release raises the minimum; it signals that a
  straggler is too far behind and must be upgraded or removed.

Each node's current protocol is exported as `silo_gossip_protocol_version`. To
verify an upgrade converged, scrape it across the fleet and confirm every node
reports the same value with `silo_gossip_incompatible_messages_total` flat at
zero:

```sh
curl -s http://<node>:7080/metrics | grep -E 'silo_gossip_(protocol_version|incompatible_messages_total|newer_protocol_messages_total)'
```

**Upgrading from a release without trim support.** An older silod reads an
extent that a newer node unmapped as a chunk with an empty id and fails the
read. That only bites when a volume is trimmed on an upgraded node and then
attached through a node that is not upgraded yet, which a drain during the
rollout can do. If that can happen, roll the new build out with
`SILO_NBD_TRIM=false`, then set it back to `true` once every node runs it.
Discards are dropped in the meantime, as they were before.

Upgrade order: drain is **not** required for an in-place binary swap (a restart
re-reads the data dir and rejoins), but for a node you are also moving or
reprovisioning, drain it first as above.

**Volumes attached through the CSI driver survive the restart.** The node
plugin watches every attachment and reconnects it as soon as silod is back.
The kernel queues the volume's I/O in the meantime, so workloads see a short
pause (a couple of seconds in practice) instead of an error. On shutdown,
silod first answers the block requests it has already accepted, so an
acknowledged write is always durable.

The pause is bounded by `SILO_CSI_NBD_RECONNECT_TIMEOUT` on the node plugin
(default `15m`, `silod.nbdReconnectTimeout` in the Helm chart). If silod stays
away longer than that, the volume's I/O fails and the pod sees errors. Keep
the window above your worst-case restart time. Volumes attached manually with
`nbd-client` do **not** reconnect; see
[Block volumes over NBD](#block-volumes-over-nbd).

**The node plugin itself can restart too.** The kernel keeps every device
attached while the plugin is away: the plugin's exit never disconnects a
device, and the reconnect window is kernel state that outlives the process.
A new plugin adopts the devices its predecessor recorded, reapplies its own
timeouts to them, reads from each one to see whether its connection is still
up, and keeps checking every 15 seconds afterwards (an adopted device has no
socket the plugin can watch, so a test read is the only reliable signal).
Nothing supervises a device between the old plugin stopping and the new one
adopting, so the window also has to cover that gap. Upgrade silod and the
node plugin as two separate rollouts and let silod settle before the plugin
rolls; when the plugin is replaced while silod is down, the window is all
that protects the volumes on that node.

**Rebooting a node with in-use volumes works, but drain it for a clean exit.**
A whole-node shutdown stops the workload pods, silod, and the node plugin in
no particular order. A write that is still in flight when silod dies has no
server to go to, and no supervisor is left to reconnect it. The request
timeout (`SILO_CSI_NBD_REQUEST_TIMEOUT`, default `2m`,
`silod.nbdRequestTimeout` in the chart) bounds a write that was already
sent, and the reconnect window bounds one issued after silod died: the kernel
fails the orphaned write, the unmount proceeds, and the reboot completes.
Expect the reboot to take about a minute longer than the reconnect window.
Verified on Talos with a 5-minute window: just over 6 minutes end to end, no
data loss up to the last fsync, and the volume re-attaches after boot.

**Never set the request timeout to 0 on Kubernetes nodes.** Without it the
kernel requeues the orphaned write forever and the shutdown hangs until
someone cuts the power. We have seen this take more than 20 minutes and end
in a BMC reset.

A drained node avoids the failed writes entirely:

- **Drain before maintenance** (`kubectl drain <node>`). Pods leave, their
  volumes unmount and detach, and nothing holds up the shutdown.
- **Kubelet graceful node shutdown.** Give workload pods a longer shutdown
  grace period than system pods, so their unmounts complete while silod is
  still running. On Talos this is `machine.kubelet.extraConfig` with
  `shutdownGracePeriod` and `shutdownGracePeriodCriticalPods`. Run silod with
  `priorityClassName: system-node-critical` so it stops last.

## Block volumes over NBD

A silo volume is an extent map over immutable chunks. To serve it as a block
device, enable the NBD server (`SILO_NBD_ADDR`). A client attaches with the
volume's path as the export name:

```sh
nbd-client <silod-host> 10809 /dev/nbd0 -name /db
mkfs.ext4 /dev/nbd0 && mount /dev/nbd0 /mnt/db     # first use only; chunks are immutable
```

> **A manual nbd-client attachment does not survive a silod restart.** The
> `-persist` flag looks like it should reconnect, but on modern kernels
> nbd-client configures the device and exits, leaving no process behind to
> reconnect. The device fails its I/O instead, and the filesystem on top
> usually shuts down until you unmount, re-attach, and fsck. On Kubernetes
> the CSI node plugin uses its own attach path, which reconnects
> automatically. A `siloctl volume attach` with the same behaviour for
> bare-metal hosts is roadmapped.

Attaching takes the volume's **single-writer lease** and fences any stale
holder, so moving a volume between hosts is safe: the old writer's writes are
refused at the data nodes. On Kubernetes the CSI node plugin handles all of
this.

Two end-to-end demos prove the block surface from a clean checkout; they differ
in *which* NBD client mounts the volume, because that client is the part worth
verifying independently:

```sh
make nbd-demo      # privileged Linux container: nbd-client attaches the volume,
                   # mkfs.ext4 + mount, write/read, detach/re-attach to show the
                   # data persists. Needs a Linux host with the `nbd` kernel
                   # module (the macOS Docker VM does not ship it).

make nbd-demo-vm   # boots a throwaway aarch64 Linux guest under QEMU with the
                   # volume attached as its virtio disk over NBD, so QEMU's own
                   # NBD client is the verifier. Runs fully on macOS via the
                   # Hypervisor framework; no host NBD kernel module needed.
                   # Requires `qemu` (brew install qemu) and Docker.
```

### Reclaiming space from volumes

silod acts on discards (`SILO_NBD_TRIM`, on by default). When a filesystem
frees blocks, the extents they fill are unmapped and the chunk GC reclaims
their chunks once they are older than `SILO_CHUNK_GC_GRACE`. The CSI node
plugin mounts ext4 and xfs volumes with `discard`, so space goes back as files
are deleted. To run a periodic `fstrim` instead, put `nodiscard` in the
StorageClass `mountOptions` (`storageClass.mountOptions` in the Helm chart).
An explicit `discard` or `nodiscard` there always wins. Raw block volumes get
no mount options; whatever uses the device has to issue its own discards.

Only whole extents are unmapped. A discard that covers part of an extent
leaves it mapped, so a filesystem that frees scattered 4 KiB blocks inside
256 KiB extents gets little back until whole extents are free.

**Volumes mounted before trim support need a one-off trim.** They were
mounted without `discard`, so the blocks their filesystems freed back then were
never reported. Remounting (restarting the pod) adds `discard` for future
frees but does not report the old ones. Run `fstrim` once against the mount:

```sh
kubectl exec <pod> -- fstrim -v <mount path>
```

`fstrim` needs `CAP_SYS_ADMIN`. If the workload pod lacks it, run it from a
privileged pod on the same node against the volume's mount point under
`/var/lib/kubelet/pods/<pod uid>/volumes/kubernetes.io~csi/<pv name>/mount`.

The "extents written" figure in `siloctl status` and the web UI counts mapped
extents, so it drops as soon as a trim lands. The chunk store's disk usage
follows after the next chunk GC sweep that finds the chunks past their grace
period.

---

## Web UI

![The status page during an incident: one disk at 84 %, one degraded volume, with capacity gauges and curves](images/ui-dashboard-light.png)

silod serves a read-only status page at `http://<node>:7080/`, on the same
listener as `/healthz` and `/metrics`. It shows:

- the cluster members as this node sees them: state, addresses, capacity,
  whether each one has advertised disk pressure, and this node's clock skew to
  each of them
- this node's health: the replication factor it runs with, how many members it
  sees alive, whether it is isolated, and its clock skew
- this node's local storage usage and its chunk GC
- the volumes: path, size, roughly how much is allocated, which writer is
  attached, which nodes hold the volume's extent map, whether the volume is
  attached on this node, and whether this node's copy of the map is damaged.
  The allocated figure comes from the node's own copy of each volume's extent
  map (the table of which regions hold data), so a node that does not keep a
  copy of a given volume's map shows 0 for it
- this node's backups: where they go, how often, whether one is running, and
  how the last run went (see [Backups](#backups))

The page refreshes itself by polling. Each node answers from its own view of the
cluster, so two nodes can disagree for a short while after a membership change.
Nothing in the UI can change state.

For scripts, `GET /api/v1/status` returns the data `siloctl status` prints, as
JSON, with two fields left empty: `storage.dataDir` and each volume's `inode`
are always `""` over HTTP, because an unauthenticated page has no use for them.
`siloctl status` still shows them. Field names are lowerCamelCase, node states
are names such as `NODE_STATE_ALIVE`, and 64-bit numbers (sizes, counts) are
strings, so convert them before doing arithmetic:

```sh
curl -s http://localhost:7080/api/v1/status | jq .
```

Every field below is always present. A peer's capacity arrives over gossip, so
a node that has only just joined can show `"0"` for its peers for a probe round
or two.

| Field | Meaning |
|---|---|
| `nodes[].capacityBytes`, `usedBytes` | The member's advertised backing-store size and usage; `"0"` until it has advertised them |
| `nodes[].pressured` | The member advertised disk pressure (it is above the soft high-watermark, see [Disk high-watermarks](#disk-high-watermarks-diskpressure)) |
| `nodes[].clockSkewMs` | This node's last reading of its clock skew to the member, in milliseconds. Positive means the member's clock is ahead. It includes one-way gossip latency. `"0"` for the responding node and for a member with no reading in the last five minutes |
| `health.replicationFactor` | `SILO_REPLICATION` on the responding node |
| `health.aliveNodes` | Members this node sees as alive, itself included |
| `health.isolated` | The node sees fewer alive peers than it needs and is retrying its seeds; same as `silo_gossip_isolated` |
| `health.rejoinAttempts` | Seed dials made to get back in after losing peers; same as `silo_gossip_rejoin_attempts_total` |
| `health.clockSkewMs` | The largest absolute skew to any member, in milliseconds; from the same readings as `silo_hlc_peer_clock_skew_seconds` |
| `health.clockSkewAlerts` | Times since start a peer ran ahead by more than `SILO_MAX_CLOCK_SKEW` (counted at most once a minute); same as `silo_hlc_clock_skew_alerts_total` |
| `health.startedUnix` | When the responding silod started |
| `gc.enabled` | `false` when `SILO_CHUNK_GC_ENABLE` is off: sweeps still count orphans but delete nothing |
| `gc.running` | A sweep is in progress |
| `gc.lastRunUnix`, `lastDurationMs` | When the last sweep finished and how long it took; `"0"` before the first sweep since start |
| `gc.lastOrphansDeleted`, `lastBytesFreed` | What the last sweep reclaimed (bytes as stored on disk) |
| `gc.lastError` | Empty when the last sweep completed, otherwise why it stopped or skipped; a skipped sweep's message starts with `skipped: ` |
| `gc.consecutiveSkips` | How many sweeps in a row have skipped; `"0"` once a sweep completes. A sweep that fails for another reason (a list error, shutdown) leaves it as it was |
| `volumes[].replicas` | Node ids that hold the volume's extent map, as this node's placement ring resolves them now |
| `volumes[].mapDamaged` | This node found its copy of the volume's extent map damaged and has not rebuilt it yet (see the [runbook](runbook.md#a-volume-will-not-attach-after-an-unclean-reboot-damaged-extent-map)) |
| `volumes[].attachedHere` | An NBD client has the volume open on this node |

`siloctl status` prints the same data: a `Health:` line with a warning for
isolation, under-replication and clock-skew alerts, `PRESSURE` and a `SKEW`
column in the node table, a `Chunk GC:` section, and `REPLICAS` and `NOTES`
columns in the volume table.

The endpoint reuses one answer for two seconds, so polling it faster than that
returns the same data. A request waits at most five seconds for silod to
collect its status, and when more than eight requests are already waiting it
answers 503 with `Retry-After: 1`. Either way, retry after a short pause.

Set `SILO_UI_ENABLED=false` to turn off both the page and the endpoint. `/` then
answers 404 with a JSON message that names the variable.

Like `/metrics`, the page and the endpoint are plain HTTP with no
authentication, and they reveal node ids and addresses, capacity, volume paths
and which node is writing each volume. Volume paths double as NBD export names,
so if the NBD port is reachable too, anyone who reads this page can attach a
volume (see [threat-model.md](threat-model.md)). Either keep port 7080 on the operator network, or put a reverse proxy in
front that requires a login and terminates TLS, and publish only the proxy. The
proxy only needs to forward `/` and `/api/v1/status` to the node after it has
checked who is asking.

The container images always contain the UI. A binary built from source without
Node.js installed shows a placeholder page that tells you to run `make ui`; see
[development.md](development.md#web-ui).

---

## Health, metrics & profiling

- **Liveness:** `GET http://<node>:7080/healthz`
- **Metrics:** `GET http://<node>:7080/metrics` (Prometheus). The `make up` stack
  scrapes all nodes and ships Grafana dashboards (`:3030`, anonymous viewer) in
  the **silo** folder. Open the **overview** dashboard first: a row of
  green/red health markers (nodes online, failed members, under-replication,
  disk pressure, fullest disk, clock-skew and protocol-incompatibility alerts)
  over per-subject graphs (membership, replication, storage, chunk I/O latency,
  namespace convergence, clock, backup).

Notable series:

- `silo_storage_capacity_bytes`, `silo_storage_used_bytes`,
  `silo_storage_available_bytes`: the filesystem backing the data directory
  (labelled by `node`). Alert when `used / capacity` crosses your headroom.
- `silo_storage_chunks`: chunks held on the node.
- `silo_replication_shortfall_chunks`: chunks this node is responsible for that
  were under-replicated at the last scrub. **Summed across nodes, this is the
  cluster's under-replication count; alert when it stays above zero.** It spikes
  after a node loss and should fall back to zero as the scrubber heals.
- `silo_replication_repairs_total`: cumulative replicas re-pushed (healing
  activity).
- `silo_rebalancer_capacity_skew`: used-fraction spread between the fullest and
  emptiest node (0 = balanced). Heterogeneous disks settle toward balanced as the
  capacity-weighted ring takes effect.
- `silo_rebalancer_disk_pressure` (this node, 1/0) and
  `silo_rebalancer_pressured_nodes` (peers seen pressured): the DiskPressure
  high-watermark condition (see [Disk high-watermarks](#disk-high-watermarks-diskpressure)).
- `silo_chunk_write_latency_seconds` / `silo_chunk_read_latency_seconds`:
  histograms of chunk put/get latency through the replication coordinator (use
  `histogram_quantile` for p50/p99).
- `silo_gossip_members{state}`: members seen by SWIM state.
  `silo_gossip_last_sync_age_seconds`: gossip lag (climbs when a node is
  isolated).
- `silo_gossip_isolated`: 1 while the node sees fewer alive peers than it
  needs (`SILO_REPLICATION`-1, at least 1) and is retrying its seeds.
  `silo_gossip_rejoin_attempts_total`: seed dials made to get back in after
  losing peers (the startup join is not counted). If `isolated` stays at 1,
  check `SILO_SEEDS` and that the seeds' gossip ports are reachable.
- `silo_gossip_protocol_version`, `silo_gossip_incompatible_messages_total`,
  `silo_gossip_newer_protocol_messages_total`: rolling-upgrade signals (see
  [Rolling upgrades](#rolling-upgrades)).
- `silo_namespace_antientropy_merges_total` and
  `silo_namespace_antientropy_last_merge_age_seconds`: namespace convergence
  activity and lag.

The **CSI node plugin** serves its own series on `SILO_CSI_HTTP_ADDR`
(`node.metricsAddress` in the Helm chart, default `:7090` per node):

- `silo_csi_nbd_attached_volumes`: volumes attached on the node.
- `silo_csi_nbd_reconnecting_volumes`: volumes whose connection to silod is
  down right now. Their I/O is paused while the plugin reconnects. **Alert
  when this stays non-zero longer than a rollout explains.**
- `silo_csi_nbd_reconnects_total`: completed reconnections. If this climbs
  without a silod rollout to explain it, connections are dying for another
  reason (network trouble, resource pressure) and you should investigate.
- `silo_namespace_inodes_reaped_total`: orphaned (unreachable) namespace inodes
  reclaimed after their directory link was removed. Removing a path tombstones
  the link; the inode it pointed at is reaped on the next gossip merge and on the
  GC sweep, so a deleted volume leaves no inode residue behind.
- `silo_gossip_sync_extension_bytes` and `silo_gossip_sync_send_failures_total`:
  the size of the namespace snapshot carried on each anti-entropy exchange and
  the count of sends that exceeded the gossip per-message cap. The extension
  bytes should stay small (the directory tree only; extent maps replicate out of
  band). A climbing `sync_send_failures_total` means snapshots are overflowing
  the cap and namespace state is not converging.
- `silo_extentmap_reaped_total` and `silo_extentmap_last_reap_reclaimed`: extent
  maps the reaper reclaimed for deleted volumes (cumulative, and last sweep). A
  steady non-zero `last_reap_reclaimed` means deletes are leaving orphans for the
  reaper to clean. That is expected only when a replica was unreachable at delete
  time; a persistent stream warrants a look at delete-path (`DeleteMap`) health.
- `silo_chunkgc_orphan_chunks`, `silo_chunkgc_reclaimed_total` and
  `silo_chunkgc_last_reclaimed`: chunks the garbage collector found reclaimable
  at the last sweep (unreferenced and past the grace window; what a dry run
  *would* delete), the cumulative count it has actually deleted, and the last
  sweep's deletions. In dry-run mode (`SILO_CHUNK_GC_ENABLE` off) `orphan_chunks`
  tracks the leak while `reclaimed_total` stays zero; after enabling, a steadily
  climbing `reclaimed_total` then a flat `orphan_chunks` near zero means the leak
  is being kept in check. `silo_chunkgc_incomplete_view` (1 when a node skipped a
  sweep because it does not hold every live volume's extent map) and
  `silo_chunkgc_unaccounted_volumes` (how many it could not see) should both be 0
  on a cluster whose replication factor covers every node; a persistent non-zero
  means the GC is abstaining and not reclaiming on that node.
  `silo_chunkgc_deferred_chunks` is what a sweep left for the next one after
  spending `SILO_CHUNK_GC_MAX_DELETES`. Non-zero for a stretch is the backlog
  draining as intended; non-zero forever means orphans are being created faster
  than the cap reclaims them, so raise the cap or shorten the interval.
  `silo_chunkgc_diverged_maps` is 1 when a node skipped a sweep because its
  extent maps were behind its replicas'. A digest mismatch alone does not
  trigger it: the GC re-probes for a second or two and then compares the
  bindings, and only a missing binding older than half the grace window counts,
  so live write and trim traffic does not cause skips. A persistent 1 means
  reconciliation is not completing and the GC is reclaiming nothing there.
  `silo_chunkgc_skipped_sweeps_total` counts every skipped sweep, for either
  reason; the status API's `gc.consecutiveSkips` gives the current streak.
- `silo_replication_unreferenced_skipped` and `silo_replication_incomplete_view`:
  local chunks the last scrub passed over as unreferenced, and whether it could
  filter at all. On a leaking store `unreferenced_skipped` is large and is the
  scrubber correctly declining to heal what the GC is reclaiming; it should fall
  toward zero as the GC drains. `incomplete_view` at 1 means the node cannot
  distinguish orphans from chunks bound by maps it does not hold, so it is
  healing everything — safe, but it will fight a concurrent GC sweep. A chunk
  the GC reclaims while a scrub is partway through its walk is checked against
  a fresh live set and passed over at debug level; the warning "scrubber could
  not read a local chunk" is kept for chunks that are gone while something
  still references them, which needs a look.
- `silo_extentmap_scrub_shortfall_maps` and `silo_extentmap_scrub_repairs_total`:
  extent maps this node is responsible for that were under-replicated at the last
  scrub (gauge), and the running count of map replicas it has re-pushed to heal
  them (counter). Summed across the cluster each map has exactly one responsible
  healer, so the gauge is the live count of under-replicated extent maps; it
  should fall to zero within a scrub interval or two after a node rejoins. A
  shortfall that stays non-zero means a target replica is unreachable or out of
  disk; pair it with `silo_replication_shortfall_chunks` to see whether chunks
  are stuck too.

The data-key cache hit-rate metric is pending the cache itself (silod currently
unwraps per-chunk keys on demand; the cache is a later optimisation).
- `silo_hlc_peer_clock_skew_seconds{peer}`: this node's clock skew to each
  peer, taken from the peer's send time on every gossip round (positive means
  the peer is ahead). It includes one-way gossip latency, so a healthy cluster
  sits a few milliseconds below zero. Peers not heard from for 5 minutes drop
  out. `silo_hlc_clock_skew_alerts_total` counts readings past
  `SILO_MAX_CLOCK_SKEW`; a rising count means a node's clock is drifting, so
  investigate NTP before write ordering is affected.
- `silo_build_info`: build/version, one series per node.

- `silo_backup_*` (runs, failures, last success time, uploads, pruning; see
  [Backups](#backups)): backup activity (when SILO_BACKUP_TARGET is set).

The same per-node figures are available on demand via `siloctl status`.

### Profiling (pprof)

Set `SILO_PPROF=true` to mount the standard `net/http/pprof`
handlers under `/debug/pprof/` on `SILO_HTTP_ADDR`. It is **off by default**:
the profiles expose runtime internals and carry a small always-on sampling cost.
Enable it on demand (e.g. add `SILO_PPROF=1` to the node's env and restart)
when chasing a heap or latency problem:

```sh
# live heap profile (objects in use): the one for memory questions
go tool pprof http://<node>:7080/debug/pprof/heap
# 30-second CPU profile
go tool pprof 'http://<node>:7080/debug/pprof/profile?seconds=30'
# goroutine dump as text (count + stacks)
curl -s 'http://<node>:7080/debug/pprof/goroutine?debug=1'
```

When judging silod's memory, read the **Go heap** (`/debug/pprof/heap`,
`inuse_space`) or the cgroup's **`anon`**, not RSS / `memory.current`. silod
writes immutable chunk files, so under sustained writes most of its RSS is
reclaimable page cache (`inactive_file`), not live heap. A multi-GB RSS with a
small heap is normal and not a leak.

### Extent-map files on disk

Each node keeps the extent maps of the volumes it replicates under
`<SILO_DATA_DIR>/extents/`. Files are named after the volume id, base64url
encoded, so `<id>` below is that encoding:

| File | What it is | May an operator delete it? |
|---|---|---|
| `<id>.emap.json` | The map's snapshot | **No.** Removing it while its log exists makes the next boot treat the volume as damaged and rebuild it from every replica. Removing it with no log drops this node's copy entirely. |
| `<id>.emap.log` | Writes since the snapshot, appended and fsynced one record per write; emptied when the snapshot is rewritten (every 4 MiB of log) and at every start | **No.** It holds acknowledged writes the snapshot does not have yet. A size of zero is normal. |
| `<id>.emap.json.tmp`, `<id>.emap.log.tmp` | Left by a snapshot write a crash interrupted | Yes. silod removes them at the next start. |
| `<id>.emap.json.corrupt`, `<id>.emap.log.corrupt` | A damaged snapshot or log, moved aside for inspection | Yes, once the volume serves again. While no rebuilt `<id>.emap.json` sits beside them they keep the volume flagged for a rebuild across restarts. See the [runbook](runbook.md). |

A log that ends in a record cut short by a crash is normal after a hard reset:
silod replays up to that record, which was never acknowledged, logs a warning
and carries on. Damage anywhere else in the log is treated like a damaged
snapshot.

The reaper (`SILO_EXTENT_REAP_AFTER`) judges a map's age by the newer of its
snapshot and its log, so a volume that is still being written is never mistaken
for an orphan because its snapshot is old.

---

## Troubleshooting

**Peers don't see each other.** Almost always a missing advertise address. If a
node binds gossip on `0.0.0.0`, set `SILO_GOSSIP_ADVERTISE` to a routable
`host:port` peers can dial; likewise `SILO_GRPC_PEER_ADVERTISE` for replication.

**`SILO_ENCRYPTION_KEY is required …`** You set `source=static` without a key.
Generate one (`openssl rand -base64 32`) and set `SILO_ENCRYPTION_KEY`, or switch
to `source=file` with `SILO_ENCRYPTION_KEY_PATH`.

**A volume won't accept writes (`lease held by another writer`).** Another holder
has a newer lease; this is the fence working. Whoever attached most recently
owns the volume; the previous holder must re-acquire. With NBD/CSI this resolves
automatically on the new attach.

**A pod's volume turned read-only after an outage or a rollout.** If a
volume's connection stays down for longer than the reconnect window
(`SILO_CSI_NBD_RECONNECT_TIMEOUT`, default `15m`), the kernel fails the
volume's queued I/O and ext4 remounts read-only to protect itself. The
attachment heals automatically once silod is back; only the filesystem needs
a fresh mount. Restart the pod. The remount replays the journal, and
everything written up to the last fsync is intact (verified under test, no
data loss). The node plugin reports this case explicitly: look for `the
kernel already failed I/O on this volume` at error level in its log, with the
device and the volume. On the node, `dmesg` (on Talos: `talosctl -n <node>
dmesg`) shows `nbdX: Dead connection, failed to find a fallback` followed by
`shutting down sockets` at the moment the window ran out.

Two things keep a connection down that long: silod away for more than the
window, or the node plugin replaced while the connection was already dead
(nothing reconnects a device until the new plugin has adopted it). Roll silod
and the plugin separately, and if outages longer than the window are normal
in your environment, raise `silod.nbdReconnectTimeout` in the Helm chart.
Short outages, such as a rolling restart or a brief network blip, never get
this far: I/O pauses and resumes.

**Clock-skew alerts.** `silo_hlc_clock_skew_alerts_total` is climbing: a peer's
wall clock differs from this node's by more than `SILO_MAX_CLOCK_SKEW`. Fix NTP
on the offending node; HLC keeps ordering correct, but large skew is a warning
sign.

**Re-replication seems slow after a node loss.** It's paced on purpose to avoid
a network-saturating herd. `SILO_SCRUB_INTERVAL` controls the cadence; the local
stack sets it aggressively (`5s`) for demos.
