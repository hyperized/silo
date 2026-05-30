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
| `SILO_ENCRYPTION_KEY_SOURCE` | `static` | `static` (key in env, dev) or `file` (key in a file, prod) |
| `SILO_ENCRYPTION_KEY` | — | base64 32-byte key; required when source is `static`. Generate: `openssl rand -base64 32` |
| `SILO_ENCRYPTION_KEY_PATH` | — | Path to a raw 32-byte key; required when source is `file`. Generate: `openssl rand 32 > /etc/silo/key && chmod 0400 /etc/silo/key` |

> Losing the encryption key means losing the data — the wrapped per-chunk keys
> live in the inode metadata and are useless without it. Back it up like a root
> credential. (KMS-backed sources arrive in M10.)

### TLS (cluster-internal mTLS)

| Variable | Purpose |
|---|---|
| `SILO_TLS_CA_CERT` / `SILO_TLS_CA_KEY` | Cluster CA material. `silod` self-mints on first boot if absent. |
| `SILO_TLS_CA_SEED` | Set on the one node that mints the CA into shared storage |
| `SILO_TLS_NODE_CERT` / `SILO_TLS_NODE_KEY` | This node's server cert (issued from the CA) |

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
and the re-replication scrubber moves chunks to match — that movement is the
rebalance; no chunk is ever stored off its ring position.

This is automatic and needs no operator action. Watch `silo_rebalancer_capacity_skew`
trend toward zero, and per-node fill via `siloctl status` (the CAPACITY column)
or `silo_storage_used_bytes / silo_storage_capacity_bytes`. A homogeneous cluster
(equal disks) is never reshuffled — equal weights reproduce the original ring
exactly.

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
- `silo_hlc_peer_clock_skew_seconds` and `silo_hlc_clock_skew_alerts_total` —
  rising values mean a node's clock is drifting; investigate NTP before write
  ordering is affected.
- `silo_build_info` — build/version, one series per node.

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
