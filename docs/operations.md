# Operating silo

This is the operator reference for the `silod` daemon: how to configure it, the
deployment topologies, how credentials and encryption work, and how to diagnose
the common problems. For the Kubernetes CSI driver specifically, see
[kubernetes.md](kubernetes.md).

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
| `SILO_HTTP_ADDR` | `0.0.0.0:7080` | `/healthz` and `/metrics` |
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
| `SILO_CHUNK_SIZE` | `4194304` (4 MiB) | Default chunk size (overridable per-inode) |
| `SILO_REPLICATION` | `3` | Default replication factor |
| `SILO_SCRUB_INTERVAL` | internal default | Re-replication scrubber cadence (the local stack sets `5s` for visible healing; production paces slower) |
| `SILO_TOMBSTONE_RETENTION` | `24h` | How long namespace tombstones are kept before GC |
| `SILO_MAX_CLOCK_SKEW` | `500ms` | Warn + count an alert when a peer's HLC exceeds this skew |

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

### Logging

| Variable | Default | Values |
|---|---|---|
| `SILO_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SILO_LOG_FORMAT` | `text` | `text`, `json` (use `json` in production) |

---

## Deployment topologies

### Single node (development)

```sh
SILO_ENCRYPTION_KEY=$(openssl rand -base64 32) \
SILO_NBD_ADDR=0.0.0.0:10809 \
  ./bin/silod
```

One node, no seeds. Good for SDK work and a single NBD volume.

### Three nodes (the `make up` stack)

`deploy/docker-compose.yml` boots three `silod`s plus Prometheus and Grafana. It
demonstrates the production-relevant wiring:

- Each node **advertises** routable addresses (`SILO_GOSSIP_ADVERTISE`,
  `SILO_GRPC_PEER_ADVERTISE`) so peers learn real dial targets rather than
  `0.0.0.0`.
- One node is the **CA seed** (`SILO_TLS_CA_SEED=1`) and mints the shared cluster
  CA; the others wait for it to be healthy.
- `SILO_SEEDS` points at peers' **gossip** ports (7100), not the data port.

### Kubernetes

Run `silod` as a `hostNetwork` DaemonSet (one per node) so the CSI node plugin
reaches it at `127.0.0.1:10809`, fronted by a `Service` for the controller's
gRPC. Set `SILO_GOSSIP_ADVERTISE` to the Pod IP (downward API), `SILO_NBD_ADDR`
to enable block serving, and mount `SILO_DATA_DIR` on durable node-local storage.
A packaged `silod` chart is on the [M9 roadmap](../PLAN.md#m9--observability--ops);
until then this is operator-assembled — see [kubernetes.md](kubernetes.md).

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
(default 6h).

| Target URL | Backend |
|---|---|
| `/path` or `file:///path` | local filesystem (single-host) |
| `s3://bucket/prefix` | AWS S3 |
| `gs://bucket/prefix` | Google Cloud Storage |
| `az://account/container/prefix` | Azure Blob |

Cloud credentials come from each provider's standard chain. The namespace is a
CRDT replicated to every node, so any node's snapshot is the cluster manifest;
chunks are node-local, so a **full backup is the union of every node's chunk
export** — point every node at the same bucket (different `namespace/<node>.json`
keys keep them from colliding).

```sh
export SILO_BACKUP_TARGET=s3://my-backups/silo \
       SILO_BACKUP_INTERVAL=1h
```

Watch `silo_backup_runs_total`, `silo_backup_failures_total`, and
`silo_backup_last_chunks`. Restore from a backup (recreate the data dir from the
exported chunks + namespace, then start silod with the same cluster key) is an
operator-driven procedure today; an automated restore command is roadmapped.

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

## Block volumes over NBD

A silo volume is an extent map over immutable chunks. To serve it as a block
device, enable the NBD server (`SILO_NBD_ADDR`). A client attaches with the
volume's path as the export name:

```sh
nbd-client <silod-host> 10809 /dev/nbd0 -name /db -persist
mkfs.ext4 /dev/nbd0 && mount /dev/nbd0 /mnt/db     # first use only; chunks are immutable
```

Attaching takes the volume's **single-writer lease** and fences any stale
holder, so moving a volume between hosts is safe — the old writer's writes are
refused at the data nodes. On Kubernetes the CSI node plugin does all of this
for you.

---

## Health & metrics

- **Liveness:** `GET http://<node>:7080/healthz`
- **Metrics:** `GET http://<node>:7080/metrics` (Prometheus). The `make up` stack
  scrapes all nodes and ships a Grafana dashboard (`:3030`, anonymous viewer).

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

The data-key cache hit-rate metric is pending the cache itself (silod currently
unwraps per-chunk keys on demand; the cache is a later optimisation).
- `silo_hlc_peer_clock_skew_seconds` and `silo_hlc_clock_skew_alerts_total` —
  rising values mean a node's clock is drifting; investigate NTP before write
  ordering is affected.
- `silo_build_info` — build/version, one series per node.

- `silo_backup_runs_total` / `silo_backup_failures_total` / `silo_backup_last_chunks`
  — backup activity (when SILO_BACKUP_TARGET is set).

The same per-node figures are available on demand via `siloctl status`.

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

**Clock-skew alerts.** `silo_hlc_clock_skew_alerts_total` is climbing — a peer's
wall clock differs from this node's by more than `SILO_MAX_CLOCK_SKEW`. Fix NTP
on the offending node; HLC keeps ordering correct, but large skew is a warning
sign.

**Re-replication seems slow after a node loss.** It's paced on purpose to avoid
a network-saturating herd. `SILO_SCRUB_INTERVAL` controls the cadence; the local
stack sets it aggressively (`5s`) for demos.
