# silo production runbook

The operator's checklist for taking silo to production and the playbooks for when
something breaks. It ties together the controls documented in detail elsewhere —
[operations.md](operations.md) (procedures), [threat-model.md](threat-model.md)
(security rationale), and [known-gaps.md](known-gaps.md) (what's deferred). When a
step has a detailed home, this page links to it rather than restating it.

The throughline: **silo is recoverable by default — no protocol step needs a human
to break a tie — but two things are not recoverable, and the checklist exists to
protect them: the cluster encryption key and the CA key.**

---

## Pre-production checklist

Run through this before the first byte of real data lands. Each item links to the
full procedure.

### Identity & keys (the unrecoverable ones)

- [ ] **Cluster encryption key has a durable, off-host source.** Use `file` or a
  cloud KMS (`aws-kms`/`gcp-kms`/`azure-kv`), not `static` env, in production —
  see [Encryption](operations.md#encryption-at-rest). **Back the key (or its KMS
  grant) up offline. Losing it is total, unrecoverable data loss** — every chunk
  is AES-GCM encrypted under it.
- [ ] **CA key (`SILO_TLS_CA_KEY`) is guarded and backed up offline.** It is the
  cluster's root of trust: it mints node and client certs, signs CRLs, and signs
  capability tokens. Treat it like a root credential.
- [ ] **Node certs auto-rotate** — confirm nodes restart at least within their
  cert lifetime (1 year); a never-restarting node is the one gap
  ([known-gaps.md](known-gaps.md)).

### Access control

- [ ] **mTLS is on everywhere** (it is, by construction — silod refuses to serve
  without cluster TLS material).
- [ ] **Decide on capability tokens.** For multi-tenant or least-privilege
  deployments set `SILO_REQUIRE_TOKENS=1` and issue scoped tokens with
  [`siloctl auth mint-token`](operations.md#scoping-client-access-with-capability-tokens).
  Cluster nodes stay exempt; only client certs are scoped.
- [ ] **NBD is off or bound to a trusted network.** `SILO_NBD_ADDR` is
  unauthenticated block I/O — never expose it publicly
  ([Block volumes](operations.md#block-volumes-over-nbd)).
- [ ] **Bootstrap token discipline.** Tokens are single-use and TTL-bounded; pin
  the server fingerprint on `siloctl auth init`. Never commit tokens or keys.

### Durability & operations

- [ ] **Replication factor matches your failure budget** (`SILO_REPLICATION`,
  default 3). It must be ≤ node count.
- [ ] **Backups configured** if you want a cold copy: `SILO_BACKUP_TARGET`
  (local/S3/GCS/Azure) + `SILO_BACKUP_INTERVAL`
  ([Backups](operations.md#backups)). Note restore is a manual procedure today.
- [ ] **Metrics are scraped and alerts are wired** — see [Alerts](#alerts-the-golden-signals).
- [ ] **NTP is healthy on every node.** silo's HLC tolerates skew but write
  ordering degrades past `SILO_MAX_CLOCK_SKEW`; watch
  `silo_hlc_clock_skew_alerts_total`.

---

## Alerts: the golden signals

Wire these first; they catch the failures that matter.

| Alert | Series | Why it matters |
|---|---|---|
| **Under-replication** | `sum(silo_replication_shortfall_chunks) > 0` for >N min | Data is below its replication factor. Spikes on node loss, should heal to 0. |
| **Disk pressure** | `silo_rebalancer_disk_pressure == 1` (or `used/capacity > 0.85`) | Node crossed the soft high-watermark; add capacity or drain before it hits the hard fence. |
| **Node isolation** | `silo_gossip_last_sync_age_seconds` climbing | A node has lost gossip contact with the cluster. |
| **Clock skew** | `increase(silo_hlc_clock_skew_alerts_total[10m]) > 0` | A node's clock is drifting; fix NTP before write ordering suffers. |
| **Backup failure** | `increase(silo_backup_failures_total[1h]) > 0` | The cold copy is not being written. |
| **Incompatible peer** | `increase(silo_gossip_incompatible_messages_total[10m]) > 0` | A node is on an unsupported protocol — finish or roll back the upgrade. |
| **Write latency** | `histogram_quantile(0.99, silo_chunk_write_latency_seconds)` high | Coordinator/replication is struggling. |

Full series reference: [operations.md#health--metrics](operations.md#health--metrics).

---

## Failure-recovery playbooks

### A node is down (recoverable)

silo self-heals. The scrubber re-replicates the down node's chunks onto survivors;
`silo_replication_shortfall_chunks` spikes then falls to zero. If the node comes
back, it rejoins over gossip and the ring re-includes it. **You do nothing** unless
the shortfall is not healing (then check survivors have capacity, and that a quorum
of replicas survived).

### A node is gone for good (replace it)

If a node's disk is lost permanently, [drain](operations.md#draining-a-node) it (if
reachable) or just remove it — the survivors already re-replicated. Provision a
replacement with a **new** `SILO_NODE_ID`, the same CA material, and let it join.
Capacity rebalancing folds it back into the ring.

### Disk pressure / a node filling up

silo warns before a node is full. At the soft high-watermark (default 85%) the
node raises a **DiskPressure** condition (`silo_rebalancer_disk_pressure`) — your
cue to add capacity or [drain](operations.md#draining-a-node) it. At the hard
watermark (default 95%) it **refuses new writes** (`RESOURCE_EXHAUSTED`) before
the disk truly fills; the replication coordinator routes those writes to the
chunk's other replicas (quorum) and the scrubber heals onto the node once it has
room — existing chunks keep serving throughout. The fix is the same at either
tier: **add capacity or drain.** If writes start failing cluster-wide, enough
nodes are hard-full that quorum can't be met — add nodes. See
[Disk high-watermarks](operations.md#disk-high-watermarks-diskpressure).

### A certificate is compromised

[Revoke it](operations.md#revoking-a-compromised-certificate): add its serial to
the CRL with `siloctl ca revoke`, distribute the CRL, set `SILO_TLS_CRL`, restart.
silod then refuses that cert on the next handshake even though it still chains to
the CA. For a leaked **capability token**, rely on its TTL (tokens are not
individually revocable — [known-gaps.md](known-gaps.md)); for an urgent cut-off,
rotate the CA.

### The CA key is compromised

The bigger hammer: every cert and token it signed is now suspect. Generate a new
CA, re-mint node certs (rolling restart with the new CA key present), re-issue
client credentials and tokens, and retire the old CA. Plan this as a maintenance
window — it touches every node.

### A volume is being written from two places (split-brain)

It **cannot corrupt the volume.** The single-writer lease is fenced: the newest
holder wins cluster-wide and data nodes reject writes from a stale lease-holder.
The stale writer sees its writes refused — move the workload to the lease holder.
See [Block volumes](operations.md#block-volumes-over-nbd).

### The cluster encryption key is lost

**This is unrecoverable.** Every chunk is ciphertext with no key to decrypt it.
There is no workaround — this is why the checklist insists on an off-host,
backed-up key source. If you have a backup of the key, restore it to the configured
source and restart.

### Restoring from a backup

Backups hold each node's still-encrypted chunks plus the namespace snapshot. The
full procedure (provider-specific `aws s3 sync` / `gsutil rsync` / `az` commands,
data-dir layout, post-restore verification) lives in
[operations.md#restore](operations.md#restore). The short version: same cluster
encryption key + hydrate `<DATA_DIR>/chunks/` and the namespace snapshot from the
bucket + start silod. An automated `siloctl restore` is roadmapped
([known-gaps.md](known-gaps.md)).

---

## Routine procedures (index)

Day-2 operations, each documented in [operations.md](operations.md):

- [Claim operator credentials](operations.md#claiming-operator-credentials) — `siloctl auth init`
- [Mint a scoped token](operations.md#scoping-client-access-with-capability-tokens) — `siloctl auth mint-token`
- [Revoke a certificate](operations.md#revoking-a-compromised-certificate) — `siloctl ca revoke`
- [Drain a node](operations.md#draining-a-node) — `siloctl node drain`
- [Rolling upgrade](operations.md#rolling-upgrades) — protocol-version-aware
- [Rebalance capacity](operations.md#capacity-rebalancing)
- [Configure backups](operations.md#backups)
