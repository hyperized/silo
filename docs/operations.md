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
| `SILO_GOSSIP_ADVERTISE` | `GOSSIP_ADDR` | Routable gossip address peers dial — **set this** when binding `0.0.0.0` with >1 peer (e.g. the Pod IP) |
| `SILO_HTTP_ADDR` | `0.0.0.0:7080` | `/healthz`, `/metrics`, and `/debug/pprof/` when `SILO_PPROF` is set |
| `SILO_NBD_ADDR` | *(empty = off)* | NBD block-device server, e.g. `0.0.0.0:10809`. **Required to serve volumes.** |

### Cluster discovery

| Variable | Default | Purpose |
|---|---|---|
| `SILO_SEEDS` | *(empty)* | Comma-separated peer **gossip** addresses (port 7100), e.g. `silo-0:7100,silo-1:7100` |
| `SILO_DOMAIN` | *(empty)* | DNS SRV fallback: resolves `_silo._tcp.<domain>` when seeds are empty |

### Storage & replication

| Variable | Default | Purpose |
|---|---|---|
| `SILO_DATA_DIR` | `/var/lib/silo` | Where chunks are stored on this node |
| `SILO_CHUNK_SIZE` | `262144` (256 KiB) | Default chunk size (overridable per-inode/StorageClass). 256 KiB minimises copy-on-write amplification on small writes; raise it for large-sequential, capacity-heavy volumes |
| `SILO_REPLICATION` | `3` | Default replication factor |
| `SILO_MAX_CONCURRENT_WRITES` | `64` | Max peer replica sends in flight at once (across all writes, counting background stragglers). Caps grpc's replication send-buffer pool (≈ n·chunk-size); without it a write storm across many volumes grows the pool without limit and OOM-kills silod. `0` = unbounded. Raise it for more write parallelism if you have memory headroom |
| `SILO_SCRUB_INTERVAL` | internal default | Re-replication scrubber cadence (the local stack sets `5s` for visible healing; production paces slower) |
| `SILO_TOMBSTONE_RETENTION` | `24h` | How long namespace tombstones are kept before GC |
| `SILO_MAX_CLOCK_SKEW` | `500ms` | Warn + count an alert when a peer's HLC exceeds this skew |
| `SILO_EXTENT_REPLICATION` | `true` | Serve each volume's extent map from its replica set (out of band, like chunks) instead of the gossiped namespace — required for a volume to attach on any node, not just where it was created. Leave on. |
| `SILO_EXTENT_REAP_AFTER` | `1h` | How long a deleted volume's extent map must sit untouched before the reaper reclaims orphaned replicas. The age guard that stops a freshly-created volume whose directory entry has not yet gossiped to a node from being mistaken for a deleted one — keep it well above gossip convergence time. |
| `SILO_EXTENT_REAP_INTERVAL` | `15m` | Extent-map reaper sweep cadence (the GC backstop for the synchronous delete path). |
| `SILO_EXTENT_SCRUB_INTERVAL` | `1m` | Extent-map scrubber cadence — re-replicates an idle volume's extent map to its full replica set after a node loss (the metadata analog of `SILO_SCRUB_INTERVAL`, which heals chunks). The synchronous write-path fan-out only keeps replicas in step while writes flow; a volume written once and then left idle would otherwise stay under-replicated after a later node loss. |
| `SILO_CHUNK_GC_INTERVAL` | `10m` | Chunk garbage-collector sweep cadence. The GC reclaims chunks no live volume or file references anymore (whole-volume deletes and copy-on-write overwrite orphans) by mark-and-sweep over the cluster's live reference set. |
| `SILO_CHUNK_GC_GRACE` | `1h` | How old an unreferenced chunk must be before the GC reclaims it — the safety margin for the write-then-record gap (a chunk is stored just before its reference is recorded). Keep it well above write latency plus gossip/HLC skew. |
| `SILO_CHUNK_GC_ENABLE` | `false` | Actually delete. Left off (the default) the GC is a **dry run**: it computes the live set and reports the reclaimable orphan count (`silo_chunkgc_orphan_chunks`) without removing anything, so you can validate the computation against real data before enabling deletion. |

### Encryption (at rest)

Every chunk is AES-GCM encrypted with a per-chunk data key, wrapped by one
operator-managed cluster encryption key.

| Variable | Default | Purpose |
|---|---|---|
| `SILO_ENCRYPTION_KEY_SOURCE` | `static` | `static`, `file`, `aws-kms`, `gcp-kms`, or `azure-kv` |
| `SILO_ENCRYPTION_KEY` | — | base64 32-byte key; required when source is `static`. Generate: `openssl rand -base64 32` |
| `SILO_ENCRYPTION_KEY_PATH` | — | `file`: a raw 32-byte key (`openssl rand 32 > /etc/silo/key && chmod 0400`). KMS sources: the **KMS-wrapped** key blob. |
| `SILO_KMS_KEY_ID` | — | `aws-kms`: key ARN/id. `gcp-kms`: crypto-key resource name (`projects/…/cryptoKeys/…`). |
| `SILO_KMS_VAULT_URL` / `SILO_KMS_KEY_NAME` | — | `azure-kv`: Key Vault base URL + the RSA wrapping key's name. |

> Losing the encryption key means losing the data — the wrapped per-chunk keys
> live in the inode metadata and are useless without it. Back it up like a root
> credential.

**KMS (envelope encryption).** Generate a 32-byte cluster key, wrap it with your
cloud KMS, store the *ciphertext* at `SILO_ENCRYPTION_KEY_PATH`; silod decrypts
it once at startup, so the plaintext key never touches disk. Credentials come
from each provider's standard chain (AWS env/role, GCP ADC, Azure managed
identity). Example (AWS):

```sh
openssl rand 32 > key.bin
aws kms encrypt --key-id <arn> --plaintext fileb://key.bin \
  --query CiphertextBlob --output text | base64 -d > /etc/silo/key.wrapped
export SILO_ENCRYPTION_KEY_SOURCE=aws-kms SILO_KMS_KEY_ID=<arn> \
       SILO_ENCRYPTION_KEY_PATH=/etc/silo/key.wrapped
```

GCP wraps with `gcloud kms encrypt`; Azure wraps the key with an RSA Key Vault
key. Rotating the cluster key is "re-wrap and restart".

### TLS (cluster-internal mTLS)

| Variable | Purpose |
|---|---|
| `SILO_TLS_CA_CERT` / `SILO_TLS_CA_KEY` | Cluster CA material. `silod` self-mints on first boot if absent. |
| `SILO_TLS_CA_SEED` | Set on the one node that mints the CA into shared storage |
| `SILO_TLS_NODE_CERT` / `SILO_TLS_NODE_KEY` | This node's server cert (issued from the CA) |
| `SILO_TLS_CRL` | Path to a CA-signed certificate revocation list. When set, silod rejects any mTLS peer whose cert serial is in it. Unset = no revocation checking. |
| `SILO_REQUIRE_TOKENS` | When `1`/`true`, client-cert callers must also present a scoped capability token (`SILO_TOKEN`). Cluster nodes are exempt. Default off (mTLS-only). |

Node certs are one-year and auto-rotate: a restart within their final ~4 months
re-mints them, so rolling upgrades keep identity fresh without manual steps.

### Scoping client access with capability tokens

mTLS proves a caller is a cluster member; a **capability token** scopes what that
member may do. Enforcement is opt-in — set `SILO_REQUIRE_TOKENS=1` on silod, and
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
`status:read`, `node:admin`, or `*` for all (operator/admin tokens — use
sparingly). The token prints to stdout; the export hint goes to stderr, so
`> token.txt` captures just the token. Give it to the client via the environment:

```sh
export SILO_TOKEN=$(cat token.txt)   # siloctl, silo-csi, silo-fuse all read this
```

Tokens are signed by the cluster CA and self-expiring, so a leaked token stops
working at its TTL — keep TTLs short for automation and re-mint. There is no
revocation list for tokens themselves; to cut one off sooner, rely on the
expiry (or rotate the CA, the bigger hammer). Capabilities are operation classes,
not per-path/per-volume scopes (that is roadmapped).

### Revoking a compromised certificate

When a node or operator credential is compromised, revoke its cert so the
cluster stops trusting it — without rotating the whole CA. Run on a host that
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
count. A CRL past its `NextUpdate` still enforces but logs a staleness warning —
re-run `siloctl ca revoke` (even with no new serials) to refresh it.

### Logging & diagnostics

| Variable | Default | Values |
|---|---|---|
| `SILO_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SILO_LOG_FORMAT` | `text` | `text`, `json` (use `json` in production) |
| `SILO_PPROF` | *(empty = off)* | Any non-empty value mounts `net/http/pprof` at `/debug/pprof/` on `SILO_HTTP_ADDR` — see [Health, metrics & profiling](#health-metrics--profiling) |

---

## Deployment paths

silo runs the same single binary everywhere; what changes is *where* it runs and
how many you run. Two axes decide your shape.

**Where the disks live — co-located or dedicated.**

- **Co-located (small scale).** Run silod on machines you already have: your
  Kubernetes control-plane or worker nodes, or a couple of existing hosts. Inside
  Kubernetes that's a DaemonSet using node-local disk; outside it's the standalone
  binary under systemd. No separate storage hardware to buy or manage — this is
  the right place to start and is fully production-capable with 3+ nodes.
- **Dedicated (larger scale).** Run silod on its own set of machines sized for
  storage and point your clients (Kubernetes clusters or anything speaking the
  gRPC/NBD surface) at them. Same binary, same wiring; you've just moved the disks
  off the compute nodes so storage and compute scale independently.

**How many you run — durability.**

| Path | Nodes | Survives a node loss? | Use it for |
|---|---|---|---|
| **[Standalone](#1-standalone-single-node)** | 1 | **No** (single copy — back it up) | edge, a single host, dev, a small block-volume or object store |
| **[Cluster](#2-cluster-multi-node)** | 3+ | **Yes** (replication factor N) | co-located on your nodes, or a dedicated fleet |
| **[Kubernetes](#3-kubernetes)** | 3+ | **Yes** | pods consuming silo as a CSI StorageClass |

All paths are production-capable. The difference between Standalone and Cluster is
replication: a standalone node keeps **one** copy of each chunk (replication
clamps to the node count), so its durability comes from the disk and from
[backups](#backups), not from peers. Add nodes — same CA, same encryption key —
and it becomes a Cluster with no data migration, whether those nodes are
co-located or dedicated.

### 1. Standalone (single node)

One `silod`, one disk. No gossip, no seeds; it self-mints its cluster CA on first
boot. Good for a single block volume, an edge box, a CI artifact store, or SDK
work — anywhere you want silo's encryption + volume/namespace surface without
running a fleet.

Fastest start (containerised, durable volume):

```sh
make up-local          # one silod in Docker; generates a dev key into deploy/.env
```

Production single node — run the binary (or container) with a **file-sourced**
key, a durable data dir, and, because there is no replication, a **backup
target**:

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
single node is a single point of failure with no automatic failover — keep
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
  or a pre-distributed CA in production — see [TLS](#tls-cluster-internal-mtls)).
  The rest wait for it. Every node trusts the same CA.
- **Routable advertise addresses.** Each node sets `SILO_GOSSIP_ADVERTISE` and
  `SILO_GRPC_PEER_ADVERTISE` to addresses peers can actually dial (a hostname or
  IP, not `0.0.0.0`).
- **Seeds point at gossip ports.** `SILO_SEEDS` lists peers' **gossip**
  addresses (7100 by default), not the data port. One reachable seed is enough
  to join; gossip discovers the rest.
- **Shared encryption key.** Every node uses the same cluster encryption key
  (file or KMS source) — it is the cluster's, not the node's.

Each node serves its own gRPC/NBD locally; any node can coordinate a write.

### 3. Kubernetes

**Co-located (the common case).** Run `silod` as a `hostNetwork` DaemonSet (one
per node) so the CSI node plugin reaches it at `127.0.0.1:10809`, fronted by a
`Service` for the controller's gRPC. Set `SILO_GOSSIP_ADVERTISE` to the Pod IP
(downward API), `SILO_NBD_ADDR` to enable block serving, and mount
`SILO_DATA_DIR` on durable node-local storage. The DaemonSet lands silod on
whichever nodes you select — control-plane nodes for a small cluster, or a
labelled subset of workers — using disk you already have.

**Dedicated.** For a larger or independently-scaled store, run the silod Cluster
(path 2) on its own machines and point the CSI controller at it via
`silod.address`; the node plugin dials that fleet instead of a node-local silod.
Storage capacity then grows by adding silod machines without touching the compute
nodes.

Either way the cluster wiring is the same as path 2; the
[`silo-csi` Helm chart](../deploy/helm/silo-csi) installs the driver that turns
volumes into a StorageClass. silod itself is operator-deployed today (a packaged
`silod` chart is roadmapped — [known-gaps.md](known-gaps.md)). Full guide:
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
gRPC address. Subsequent `siloctl` commands authenticate automatically — no
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
and therefore holds roughly twice the chunks, so a cluster of mixed-size disks
keeps its used-fraction balanced instead of filling the smallest node first. When
capacity changes (a bigger node joins, or a disk is grown), the ring re-weights
so **new** chunks follow the new weighting. Rebalancing is prospective: the
scrubber adds replicas to match the ring but never deletes, so existing chunks
are not retroactively migrated (active migration/GC is roadmapped —
[known-gaps.md](known-gaps.md)). A grown or fresh node fills via new writes, not
by pulling old data off its peers.

This is automatic and needs no operator action. Watch `silo_rebalancer_capacity_skew`
trend toward zero, and per-node fill via `siloctl status` (the CAPACITY column)
or `silo_storage_used_bytes / silo_storage_capacity_bytes`. A homogeneous cluster
(equal disks) is never reshuffled — equal weights reproduce the original ring
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
  path that resolves a chunk's replicas — writes, reads, deletes, and the
  scrubber — prefers non-pressured nodes for that chunk, so a near-full node
  ceases to be chosen for new data and the scrubber heals around it instead of
  retrying it forever. Steering is **bounded**: it keeps a quorum (`n/2+1`) of
  each chunk's natural ring replicas, so the steered set always overlaps the
  unsteered set by at least one node — which is what keeps reads correct and lets
  the scrubber heal across pressure changes (no relocation of a chunk's whole
  replica set, so no read-availability gap). It steers **new** placement; it does
  not retroactively migrate or delete the chunks already on a pressured node
  (that needs active migration — see [known-gaps.md](known-gaps.md)), so the node
  drains as old chunks are deleted and as new data lands elsewhere. The response
  to sustained pressure is still yours: add capacity or [drain](#draining-a-node).
  Note steering does add some re-replication when a node first crosses the
  watermark (chunks it was a replica for gain a copy on a non-pressured node);
  it is paced by the scrubber. Disable with `SILO_DISK_PRESSURE_STEERING=false`
  to keep the plain capacity-weighted ring.
- **Hard tier (write fence).** At the hard watermark the node refuses new chunks
  with `RESOURCE_EXHAUSTED` (`ErrNoSpace`) before the disk is truly full. In the
  replication coordinator a refused replica just fails its ack, so the write
  still completes on the other replicas (quorum) and the scrubber heals the chunk
  onto the node once it has room. Existing chunks are always still served. If
  *enough* nodes are hard-full that quorum can't be met, writes fail cluster-wide
  with an actionable error — the signal that the cluster is genuinely out of
  space and needs nodes added.

When you see DiskPressure, the response is the same as a full node below:
add capacity or drain.

## Backups

Set `SILO_BACKUP_TARGET` and each node periodically exports its **encrypted
chunks** (copied as-is, still AES-GCM under the cluster key) and its **namespace
snapshot** to object storage. `SILO_BACKUP_INTERVAL` (a Go duration) paces it
(default 6h, first export fires one interval after boot so a crash-loop cannot
stampede the bucket).

| Target URL | Backend |
|---|---|
| `/path` or `file:///path` | local filesystem (single-host) |
| `s3://bucket/prefix` | AWS S3 |
| `gs://bucket/prefix` | Google Cloud Storage |
| `az://account/container/prefix` | Azure Blob |

Cloud credentials come from each provider's standard chain — no silo-specific
credential env vars. The namespace is a CRDT replicated to every node, so any
node's snapshot is the cluster manifest; chunks are node-local, so a **full
cluster backup is the union of every node's chunk export** — point every node
at the **same** bucket+prefix and they will not collide (each node namespaces
its snapshot by `SILO_NODE_ID`; chunk IDs are content-addressed so two nodes
holding the same chunk overwrite the same key with identical bytes).

### Object layout

Inside the prefix the exporter writes two flat directories:

```
<prefix>/
  namespace/<node-id>.json   # one per node, the CRDT snapshot
  chunks/<chunk-id>          # encrypted chunks, content-addressed
```

A new chunk is only added; an existing chunk is overwritten with the same
bytes. Deletion is **not** propagated to the bucket — see [lifecycle](#bucket-lifecycle-cost)
below for keeping it bounded.

### AWS S3

1. Create the bucket (any region; pick one close to your nodes for egress cost).
   Enable **default SSE** (SSE-S3 or SSE-KMS) if your policy requires it — the
   chunks are already AES-GCM encrypted under the cluster key, so SSE is
   defence-in-depth, not the primary protection.
2. Grant the silod runtime identity (EC2 instance role, EKS IRSA, ECS task role,
   or a static IAM user — in that order of preference) write access to the
   prefix:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": ["s3:PutObject"],
       "Resource": "arn:aws:s3:::my-backups/silo/*"
     }, {
       "Effect": "Allow",
       "Action": ["s3:ListBucket"],
       "Resource": "arn:aws:s3:::my-backups",
       "Condition": {"StringLike": {"s3:prefix": ["silo/*"]}}
     }]
   }
   ```

   The exporter only needs `PutObject`; `ListBucket` is for the restore side
   (and for ops, when you `aws s3 ls` to check progress).
3. Point silod at it. On EC2/EKS the SDK reads the instance/role credentials;
   off-cloud, set the usual `AWS_REGION`, `AWS_ACCESS_KEY_ID`,
   `AWS_SECRET_ACCESS_KEY` (or `AWS_PROFILE`) in silod's environment.

   ```sh
   export SILO_BACKUP_TARGET=s3://my-backups/silo \
          SILO_BACKUP_INTERVAL=1h \
          AWS_REGION=eu-west-1
   ```

### Google Cloud Storage

1. Create the bucket. **Uniform** bucket-level access is fine — the SDK uses
   IAM, not ACLs.
2. Grant the silod service account `roles/storage.objectCreator` on the bucket
   (or the broader `roles/storage.objectAdmin` if you also want it to list/read
   for restore tooling).
3. Credentials come from Application Default Credentials: a workload identity
   on GKE, a service-account JSON via `GOOGLE_APPLICATION_CREDENTIALS`, or
   `gcloud auth application-default login` for an operator host.

   ```sh
   export SILO_BACKUP_TARGET=gs://my-backups/silo
   ```

### Azure Blob

1. Create a storage account and a container under it. The URL form is
   `az://<account>/<container>/<prefix>` — the account name maps to
   `https://<account>.blob.core.windows.net`.
2. Grant the silod identity `Storage Blob Data Contributor` on the container.
   On AKS a managed identity / workload identity is the path of least
   friction; off-cloud, set `AZURE_CLIENT_ID` + `AZURE_TENANT_ID` +
   `AZURE_CLIENT_SECRET` (service principal) in silod's environment.

   ```sh
   export SILO_BACKUP_TARGET=az://mybackups/silo/prod
   ```

### Bucket lifecycle & cost

A backup is **incremental in cost, cumulative in storage**: every interval a
node re-uploads any chunk that exists, and writes the current namespace
snapshot. With content-addressed chunk IDs, an unchanged chunk is overwritten
with the same bytes (one PUT, no version churn unless you have versioning on).
With versioning on, expect one new object version per cycle per chunk — turn
on a lifecycle rule to expire old versions, e.g. on S3:

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

Deleted chunks are not GC'd from the bucket — the exporter only writes. If
that matters, expire by age (e.g. delete objects older than N days that the
current namespace no longer references; a `siloctl backup gc` is roadmapped).

### Monitoring

Each node exposes:

- `silo_backup_runs_total` — exports attempted (counter; should climb at the
  cadence of `SILO_BACKUP_INTERVAL`).
- `silo_backup_failures_total` — exports that errored. **Alert** when this
  climbs without recovery (the runbook lists it).
- `silo_backup_last_chunks` — chunk count of the last successful export
  (gauge; useful as a sanity check vs. `silo_storage_chunks`).

The first export takes one full interval; if `silo_backup_runs_total` is still
zero an hour after boot with `SILO_BACKUP_INTERVAL=1h`, look at silod's logs
for `backup failed; will retry next cycle`.

### Restore

Restore is a manual procedure today — copy the bucket back onto a fresh node
and start silod. The cluster encryption key is **not** in the backup; you
must have it independently.

1. **Provision a fresh node** with the same `SILO_DATA_DIR` layout and the
   **same cluster encryption key** (`SILO_ENCRYPTION_KEY` / `…_KEY_PATH` /
   KMS settings). Without the key, the chunks are unrecoverable ciphertext.
2. **Hydrate the data directory** from the bucket. The chunks land under
   `<DATA_DIR>/chunks/` and the namespace snapshot is loaded on first boot:

   ```sh
   aws s3 sync s3://my-backups/silo/chunks/    /var/lib/silo/chunks/
   aws s3 cp   s3://my-backups/silo/namespace/<node-id>.json \
               /var/lib/silo/namespace.json
   ```

   (For GCS use `gsutil rsync` / `gcloud storage cp`; for Azure use
   `az storage blob download-batch`.) For a multi-node restore, point every
   node at the same chunks/ directory state — chunk IDs are global, so a node
   can serve any chunk it holds.
3. **Start silod.** It rebuilds its in-memory indices from the data dir,
   reloads the namespace snapshot, and rejoins (or starts) the cluster. The
   scrubber will repair replication on the freshly joined nodes.
4. **Verify.** `siloctl status` should show the expected volumes and
   `silo_storage_chunks` should match the restored count; mount a volume via
   NBD/CSI and confirm the contents.

Cross-region disaster recovery is a special case of the same flow: configure
S3 cross-region replication (or its GCS/Azure equivalents) on the backup
bucket so the data is already in the recovery region when you need it.

> Reminder: **back up the encryption key separately** from the chunk bucket —
> losing it loses the data. KMS-wrapped key blobs are safe to keep next to
> the backups; raw static keys are not.

## Draining a node

To remove a node without risking data, drain it first:

```sh
siloctl node drain --server <node-grpc-addr>
```

This marks the node as having left the cluster and announces it over gossip.
Peers route new placement around it and the scrubber re-replicates the chunks it
held onto survivors — so every volume's replication factor is restored *before*
the node goes away. Drain is **not** shutdown: the node keeps running and serving
its chunks so survivors can rebuild from a quorum.

Watch the re-replication finish, then remove the node:

```sh
# shortfall falls back to zero once every chunk is back to full replication
curl -s http://<node>:7080/metrics | grep silo_replication_shortfall_chunks
```

When `silo_replication_shortfall_chunks` is zero across the cluster, stop and
remove the node. A drained node that is killed early is still safe as long as the
remaining replicas met quorum; drain just makes the transition graceful.

## Rolling upgrades

silod advertises a **cluster wire-protocol version** on every gossip message —
separate from the build's semver. Two builds with the same protocol interoperate
freely, which is what makes a rolling upgrade (replace nodes one at a time)
safe: the new build keeps speaking the old protocol until *every* node is
upgraded, and only a later release bumps the protocol.

Each node continuously classifies its peers:

- **Compatible** — same protocol window. Normal operation.
- **Newer peer** — a peer is ahead of this node. Its messages are still
  processed (the wire format tolerates unknown fields), but
  `silo_gossip_newer_protocol_messages_total` climbs and silod logs a warning.
  During an upgrade this is expected on the not-yet-upgraded nodes and should
  fall to a flat line once every node is on the new build.
- **Unsupported (too old)** — a peer below this build's minimum supported
  protocol. silod **fences** it: the message is dropped (neither the sender nor
  its gossip is merged) and `silo_gossip_incompatible_messages_total` climbs.
  This only happens when a release raises the minimum — the signal that a
  straggler is too far behind and must be upgraded or removed.

Each node's current protocol is exported as `silo_gossip_protocol_version`. To
verify an upgrade converged, scrape it across the fleet and confirm every node
reports the same value with `silo_gossip_incompatible_messages_total` flat at
zero:

```sh
curl -s http://<node>:7080/metrics | grep -E 'silo_gossip_(protocol_version|incompatible_messages_total|newer_protocol_messages_total)'
```

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
(default `5m`, `silod.nbdReconnectTimeout` in the Helm chart). If silod stays
away longer than that, the volume's I/O fails and the pod sees errors. Keep
the window above your worst-case restart time. Volumes attached manually with
`nbd-client` do **not** reconnect; see
[Block volumes over NBD](#block-volumes-over-nbd).

**Rebooting a node with in-use volumes works, but drain it for a clean exit.**
A whole-node shutdown stops the workload pods, silod, and the node plugin in
no particular order. A write that is still in flight when silod dies has no
server to go to, and no supervisor is left to reconnect it. The request
timeout (`SILO_CSI_NBD_REQUEST_TIMEOUT`, default `2m`,
`silod.nbdRequestTimeout` in the chart) bounds this case: the kernel fails
the orphaned write, the unmount proceeds, and the reboot completes. Verified
on Talos: about 6 minutes end to end, no data loss up to the last fsync, and
the volume re-attaches cleanly after boot.

**Never set the request timeout to 0 on Kubernetes nodes.** Without it the
kernel requeues the orphaned write forever and the shutdown hangs until
someone cuts the power. We have seen this take more than 20 minutes and end
in a BMC reset.

A drained node avoids the failed writes entirely:

- **Drain before maintenance** (`kubectl drain <node>`). Pods leave, their
  volumes unmount and detach cleanly, and nothing holds up the shutdown.
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
holder, so moving a volume between hosts is safe — the old writer's writes are
refused at the data nodes. On Kubernetes the CSI node plugin does all of this
for you.

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
                   # Hypervisor framework — no host NBD kernel module needed.
                   # Requires `qemu` (brew install qemu) and Docker.
```

---

## Health, metrics & profiling

- **Liveness:** `GET http://<node>:7080/healthz`
- **Metrics:** `GET http://<node>:7080/metrics` (Prometheus). The `make up` stack
  scrapes all nodes and ships Grafana dashboards (`:3030`, anonymous viewer) in
  the **silo** folder. **silo — overview** is the one to open: a row of
  green/red health markers (nodes online, failed members, under-replication,
  disk pressure, fullest disk, clock-skew and protocol-incompatibility alerts)
  over per-subject graphs (membership, replication, storage, chunk I/O latency,
  namespace convergence, clock, backup).

Notable series:

- `silo_storage_capacity_bytes`, `silo_storage_used_bytes`,
  `silo_storage_available_bytes` — the filesystem backing the data directory
  (labelled by `node`). Alert when `used / capacity` crosses your headroom.
- `silo_storage_chunks` — chunks held on the node.
- `silo_replication_shortfall_chunks` — chunks this node is responsible for that
  were under-replicated at the last scrub. **Summed across nodes, this is the
  cluster's under-replication count — alert when it stays above zero.** It spikes
  after a node loss and should fall back to zero as the scrubber heals.
- `silo_replication_repairs_total` — cumulative replicas re-pushed (healing
  activity).
- `silo_rebalancer_capacity_skew` — used-fraction spread between the fullest and
  emptiest node (0 = balanced). Heterogeneous disks settle toward balanced as the
  capacity-weighted ring takes effect.
- `silo_rebalancer_disk_pressure` (this node, 1/0) and
  `silo_rebalancer_pressured_nodes` (peers seen pressured) — the DiskPressure
  high-watermark condition (see [Disk high-watermarks](#disk-high-watermarks-diskpressure)).
- `silo_chunk_write_latency_seconds` / `silo_chunk_read_latency_seconds` —
  histograms of chunk put/get latency through the replication coordinator (use
  `histogram_quantile` for p50/p99).
- `silo_gossip_members{state}` — members seen by SWIM state; and
  `silo_gossip_last_sync_age_seconds` — gossip lag (climbs when a node is
  isolated).
- `silo_gossip_protocol_version`, `silo_gossip_incompatible_messages_total`,
  `silo_gossip_newer_protocol_messages_total` — rolling-upgrade signals (see
  [Rolling upgrades](#rolling-upgrades)).
- `silo_namespace_antientropy_merges_total` and
  `silo_namespace_antientropy_last_merge_age_seconds` — namespace convergence
  activity and lag.

The **CSI node plugin** serves its own series on `SILO_CSI_HTTP_ADDR`
(`node.metricsAddress` in the Helm chart, default `:7090` per node):

- `silo_csi_nbd_attached_volumes` — volumes attached on the node.
- `silo_csi_nbd_reconnecting_volumes` — volumes whose connection to silod is
  down right now. Their I/O is paused while the plugin reconnects. **Alert
  when this stays non-zero longer than a rollout explains.**
- `silo_csi_nbd_reconnects_total` — completed reconnections. If this climbs
  without a silod rollout to explain it, connections are dying for another
  reason (network trouble, resource pressure) and you should investigate.
- `silo_namespace_inodes_reaped_total` — orphaned (unreachable) namespace inodes
  reclaimed after their directory link was removed. Removing a path tombstones
  the link; the inode it pointed at is reaped on the next gossip merge and on the
  GC sweep, so a deleted volume leaves no inode residue behind.
- `silo_gossip_sync_extension_bytes` and `silo_gossip_sync_send_failures_total` —
  the size of the namespace snapshot carried on each anti-entropy exchange and
  the count of sends that exceeded the gossip per-message cap. The extension
  bytes should stay small (the directory tree only — extent maps replicate out of
  band); a climbing `sync_send_failures_total` means snapshots are overflowing
  the cap and namespace state is not converging.
- `silo_extentmap_reaped_total` and `silo_extentmap_last_reap_reclaimed` — extent
  maps the reaper reclaimed for deleted volumes (cumulative, and last sweep). A
  steady non-zero `last_reap_reclaimed` means deletes are leaving orphans for the
  reaper to clean — expected only when a replica was unreachable at delete time;
  a persistent stream warrants a look at delete-path (`DeleteMap`) health.
- `silo_chunkgc_orphan_chunks`, `silo_chunkgc_reclaimed_total` and
  `silo_chunkgc_last_reclaimed` — chunks the garbage collector found reclaimable
  at the last sweep (unreferenced and past the grace window — what a dry run
  *would* delete), the cumulative count it has actually deleted, and the last
  sweep's deletions. In dry-run mode (`SILO_CHUNK_GC_ENABLE` off) `orphan_chunks`
  tracks the leak while `reclaimed_total` stays zero; after enabling, a steadily
  climbing `reclaimed_total` then a flat `orphan_chunks` near zero means the leak
  is being kept in check. `silo_chunkgc_incomplete_view` (1 when a node skipped a
  sweep because it does not hold every live volume's extent map) and
  `silo_chunkgc_unaccounted_volumes` (how many it could not see) should both be 0
  on a cluster whose replication factor covers every node; a persistent non-zero
  means the GC is abstaining and not reclaiming on that node.
- `silo_extentmap_scrub_shortfall_maps` and `silo_extentmap_scrub_repairs_total` —
  extent maps this node is responsible for that were under-replicated at the last
  scrub (gauge), and the running count of map replicas it has re-pushed to heal
  them (counter). Summed across the cluster each map has exactly one responsible
  healer, so the gauge is the live count of under-replicated extent maps; it
  should fall to zero within a scrub interval or two after a node rejoins. A
  shortfall that stays non-zero means a target replica is unreachable or out of
  disk — pair it with `silo_replication_shortfall_chunks` to see whether chunks
  are stuck too.

The data-key cache hit-rate metric is pending the cache itself (silod currently
unwraps per-chunk keys on demand; the cache is a later optimisation).
- `silo_hlc_peer_clock_skew_seconds` and `silo_hlc_clock_skew_alerts_total` —
  rising values mean a node's clock is drifting; investigate NTP before write
  ordering is affected.
- `silo_build_info` — build/version, one series per node.

- `silo_backup_runs_total` / `silo_backup_failures_total` / `silo_backup_last_chunks`
  — backup activity (when SILO_BACKUP_TARGET is set).

The same per-node figures are available on demand via `siloctl status`.

### Profiling (pprof)

Set `SILO_PPROF` to any non-empty value to mount the standard `net/http/pprof`
handlers under `/debug/pprof/` on `SILO_HTTP_ADDR`. It is **off by default** —
the profiles expose runtime internals and carry a small always-on sampling cost
— so enable it on demand (e.g. add `SILO_PPROF=1` to the node's env and restart)
when chasing a heap or latency problem:

```sh
# live heap profile (objects in use) — the one for memory questions
go tool pprof http://<node>:7080/debug/pprof/heap
# 30-second CPU profile
go tool pprof 'http://<node>:7080/debug/pprof/profile?seconds=30'
# goroutine dump as text (count + stacks)
curl -s 'http://<node>:7080/debug/pprof/goroutine?debug=1'
```

When judging silod's memory, read the **Go heap** (`/debug/pprof/heap`,
`inuse_space`) or the cgroup's **`anon`** — not RSS / `memory.current`. silod
writes immutable chunk files, so under sustained writes most of its RSS is
reclaimable page cache (`inactive_file`), not live heap: a multi-GB RSS with a
small heap is normal and not a leak.

---

## Troubleshooting

**Peers don't see each other.** Almost always a missing advertise address. If a
node binds gossip on `0.0.0.0`, set `SILO_GOSSIP_ADVERTISE` to a routable
`host:port` peers can dial; likewise `SILO_GRPC_PEER_ADVERTISE` for replication.

**`SILO_ENCRYPTION_KEY is required …`** You set `source=static` without a key.
Generate one (`openssl rand -base64 32`) and set `SILO_ENCRYPTION_KEY`, or switch
to `source=file` with `SILO_ENCRYPTION_KEY_PATH`.

**A volume won't accept writes (`lease held by another writer`).** Another holder
has a newer lease — this is the fence working. Whoever attached most recently
owns the volume; the previous holder must re-acquire. With NBD/CSI this resolves
automatically on the new attach.

**A pod's volume turned read-only after a long silod outage.** If silod is
unreachable for longer than the reconnect window
(`SILO_CSI_NBD_RECONNECT_TIMEOUT`, default `5m`), the kernel fails the
volume's queued I/O and ext4 remounts read-only to protect itself. The
attachment heals automatically once silod is back; only the filesystem needs
a fresh mount. Restart the pod. The remount replays the journal, and
everything written up to the last fsync is intact (verified under test, no
data loss). If outages longer than the window are normal in your environment,
raise `silod.nbdReconnectTimeout` in the Helm chart. Short outages, such as a
rolling restart or a brief network blip, never get this far: I/O pauses and
resumes.

**Clock-skew alerts.** `silo_hlc_clock_skew_alerts_total` is climbing — a peer's
wall clock differs from this node's by more than `SILO_MAX_CLOCK_SKEW`. Fix NTP
on the offending node; HLC keeps ordering correct, but large skew is a warning
sign.

**Re-replication seems slow after a node loss.** It's paced on purpose to avoid
a network-saturating herd. `SILO_SCRUB_INTERVAL` controls the cadence; the local
stack sets it aggressively (`5s`) for demos.
