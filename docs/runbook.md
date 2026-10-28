# silo production runbook

The operator's checklist for taking silo to production and the playbooks for when
something breaks. It ties together the controls documented in detail elsewhere:
[operations.md](operations.md) (procedures), [threat-model.md](threat-model.md)
(security rationale), and [known-gaps.md](known-gaps.md) (what's deferred). When a
step has a detailed home, this page links to it rather than restating it.

**silo is recoverable by default (no protocol step needs a human to break a
tie), but two things are not recoverable, and the checklist exists to protect
them: the cluster encryption key and the CA key.**

**Jump to:** [Pre-production checklist](#pre-production-checklist) · [Golden-signal alerts](#alerts-the-golden-signals) · [Failure-recovery playbooks](#failure-recovery-playbooks) · [Routine procedures](#routine-procedures-index)

---

## Pre-production checklist

Run through this before the first byte of real data lands. Each item links to the
full procedure.

### Identity & keys (the unrecoverable ones)

- [ ] **Cluster encryption key has a durable, off-host source.** Use `file` or a
  cloud KMS (`aws-kms`/`gcp-kms`/`azure-kv`), not `static` env, in production;
  see [Encryption](operations.md#encryption-at-rest). **Back the key (or its KMS
  grant) up offline. Losing it is total, unrecoverable data loss**: every chunk
  is AES-GCM encrypted under it.
- [ ] **CA key (`SILO_TLS_CA_KEY`) is guarded and backed up offline.** It is the
  cluster's root of trust: it mints node and client certs, signs CRLs, and signs
  capability tokens. Treat it like a root credential.
- [ ] **Node certs auto-rotate.** Confirm nodes restart at least within their
  cert lifetime (1 year); a never-restarting node is the one gap
  ([known-gaps.md](known-gaps.md)).

### Access control

- [ ] **mTLS is on everywhere** (it is, by construction: silod refuses to serve
  without cluster TLS material).
- [ ] **Decide on capability tokens.** For multi-tenant or least-privilege
  deployments set `SILO_REQUIRE_TOKENS=1` and issue scoped tokens with
  [`siloctl auth mint-token`](operations.md#scoping-client-access-with-capability-tokens).
  Cluster nodes stay exempt; only client certs are scoped.
- [ ] **NBD is off or bound to a trusted network.** `SILO_NBD_ADDR` is
  unauthenticated block I/O; never expose it publicly
  ([Block volumes](operations.md#block-volumes-over-nbd)).
- [ ] **Bootstrap token discipline.** Tokens are single-use and TTL-bounded; pin
  the server fingerprint on `siloctl auth init`. Never commit tokens or keys.

### Durability & operations

- [ ] **Replication factor matches your failure budget** (`SILO_REPLICATION`,
  default 3). It must be ≤ node count.
- [ ] **Backups configured** if you want a cold copy: `SILO_BACKUP_TARGET`
  (local/S3/GCS/Azure) + `SILO_BACKUP_INTERVAL`
  ([Backups](operations.md#backups)). Run `siloctl backup verify` once against
  the first backup, and keep the encryption key somewhere other than the bucket.
- [ ] **Metrics are scraped and alerts are wired.** See [Alerts](#alerts-the-golden-signals).
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
| **Node isolation** | `silo_gossip_isolated == 1` for >5 min, or `silo_gossip_last_sync_age_seconds` climbing | A node has lost gossip contact with the cluster and its seeds are not answering. Check `SILO_SEEDS` and the seeds' gossip ports. |
| **Clock skew** | `increase(silo_hlc_clock_skew_alerts_total[10m]) > 0` | A node's clock is drifting; fix NTP before write ordering suffers. |
| **Backup failure** | `increase(silo_backup_failures_total[1h]) > 0` | The cold copy is not being written. |
| **Incompatible peer** | `increase(silo_gossip_incompatible_messages_total[10m]) > 0` | A node is on an unsupported protocol; finish or roll back the upgrade. |
| **Write latency** | `histogram_quantile(0.99, silo_chunk_write_latency_seconds)` high | Coordinator/replication is struggling. |

Full series reference: [operations.md#health-metrics--profiling](operations.md#health-metrics--profiling).

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
reachable) or remove it outright; the survivors already re-replicated. Provision a
replacement with a **new** `SILO_NODE_ID`, the same CA material, and let it join.
Capacity rebalancing folds it back into the ring.

### Disk pressure / a node filling up

silo warns before a node is full. At the soft high-watermark (default 85%) the
node raises a **DiskPressure** condition (`silo_rebalancer_disk_pressure`). That
is your cue to add capacity or [drain](operations.md#draining-a-node) it. At the
hard watermark (default 95%) it **refuses new writes** (`RESOURCE_EXHAUSTED`)
before the disk truly fills; the replication coordinator routes those writes to
the chunk's other replicas (quorum) and the scrubber heals onto the node once it
has room. Existing chunks keep serving throughout. The fix is the same at either
tier: **add capacity or drain.** If writes start failing cluster-wide, enough
nodes are hard-full that quorum can't be met. Add nodes. See
[Disk high-watermarks](operations.md#disk-high-watermarks-diskpressure).

### silod memory looks high / it was OOMKilled

First decide whether it is real. silod writes immutable chunk files, so under
sustained writes most of its container **RSS / `memory.current` is reclaimable
page cache** (`inactive_file`), not live heap. A multi-GB RSS is usually
normal. Judge by the **Go heap** or the cgroup's **`anon`**, not RSS:

```sh
cat /sys/fs/cgroup/memory.stat | grep -E '^(anon|file|inactive_file) '   # anon = real footprint
```

If `anon` (or the Go heap) really is large or climbing, enable profiling
(`SILO_PPROF=1`, restart) and capture a heap profile
([operations.md#profiling-pprof](operations.md#profiling-pprof)):

```sh
go tool pprof http://<node>:7080/debug/pprof/heap   # inuse_space = live heap
```

For an actual OOMKill, the usual trigger is a transient spike during a
concurrent attach/format storm rather than steady growth: give the container
enough limit to absorb the peak (a few hundred MiB of `anon` plus headroom),
and optionally set `GOMEMLIMIT` so the GC defends a soft ceiling.

### A certificate is compromised

[Revoke it](operations.md#revoking-a-compromised-certificate): add its serial to
the CRL with `siloctl ca revoke`, distribute the CRL, set `SILO_TLS_CRL`, restart.
silod then refuses that cert on the next handshake even though it still chains to
the CA. For a leaked **capability token**, rely on its TTL (tokens are not
individually revocable; see [known-gaps.md](known-gaps.md)); for an urgent
cut-off, rotate the CA.

### The CA key is compromised

Every cert and token the CA signed is now suspect. Generate a new
CA, re-mint node certs (rolling restart with the new CA key present), re-issue
client credentials and tokens, and retire the old CA. Plan this as a maintenance
window; it touches every node.

### A volume will not attach after an unclean reboot (damaged extent map)

After a hard reset, silod may log at startup that the local copy of a volume's
extent map was damaged and has been moved aside. The damaged file is kept as
`<DATA_DIR>/extents/<id>.emap.json.corrupt`, and when the damage was in the
map's log, the log is kept beside it as `<id>.emap.log.corrupt` (the log line
names the volume and the exact files). The node then rebuilds that map by
merging the copy held by **every** replica of the volume, and refuses to serve
the volume until it has. This holds across restarts: as long as the `.corrupt`
file exists and no rebuilt map sits beside it, silod treats the volume as damaged
on every boot. An attach in the meantime fails with `will not serve volume ...
yet`, naming how many replicas could not be reached.

This is deliberate. The damaged copy may have held writes that only some of the
replicas received, so serving from whichever replica answered could roll the
volume back to an earlier state. For a database that looks like a clean
shutdown at an old checkpoint.

1. Bring the unreachable nodes back, or fix gossip connectivity between the
   nodes, until `siloctl status` lists every node.
2. Retry the attach. Once every replica answers, silod logs that it rebuilt the
   map and the volume serves again.
3. Delete the `.corrupt` files once the volume serves. Keep it if you want to
   report the failure; it holds the damaged bytes as found. Once the map is
   rebuilt, a leftover `.corrupt` file no longer blocks anything.

If the error says **no other replica holds a copy**, the damaged file was the
only one. Restore the volume from a backup. If the volume never held data,
delete its `.corrupt` file and restart the node.

Startup also removes any `<id>.emap.json.tmp` file. That is a write a crash cut
short; the map it was replacing is intact.

### A volume is being written from two places (split-brain)

It **cannot corrupt the volume.** The single-writer lease is fenced: the newest
holder wins cluster-wide and data nodes reject writes from a stale lease-holder.
The stale writer sees its writes refused; move the workload to the lease holder.
See [Block volumes](operations.md#block-volumes-over-nbd).

### The cluster encryption key is lost

**This is unrecoverable.** Every chunk is ciphertext with no key to decrypt it.
There is no workaround; this is why the checklist insists on an off-host,
backed-up key source. If you have a backup of the key, restore it to the configured
source and restart.

### Restoring from a backup

Use this when you lost more nodes than the replication factor tolerates, or when
you want to go back in time. Losing a single node is usually easier: replace it
and let re-replication heal it (see
[A node is gone for good](#a-node-is-gone-for-good-replace-it)).

Backups hold each node's chunks (still encrypted), the namespace snapshot and the
per-volume extent maps, plus a manifest per run listing all of it. You need the
cluster encryption key, because the chunks stay encrypted with it. Backups
deliberately contain no keys or certificates.

1. **Stop the node**, or prepare a replacement host.
2. **Pick a backup.** This reads the bucket directly and needs no running silod:

   ```sh
   siloctl backup list --target s3://silo-backups/prod
   ```

3. **Verify it.** This checks that every object the manifest refers to exists
   with the recorded size, and exits with status 1 if anything is wrong:

   ```sh
   siloctl backup verify --target s3://silo-backups/prod --node silo-a
   ```

   Add `--manifest STAMP` to check an older backup instead of the latest. For a
   restore you depend on, add `--deep`: it downloads everything and checks each
   SHA-256, which also catches damage that kept the size. The restore step below
   refuses any object that does not match its recorded checksum, so a damaged
   backup fails before anything is written.
4. **Restore into a new directory.** The directory must not exist or must be
   empty; siloctl never overwrites files. If the restore fails part-way, delete
   the directory and run it again.

   ```sh
   siloctl backup restore --target s3://silo-backups/prod --node silo-a \
     --data-dir /var/lib/silo-restored
   ```

   For an S3-compatible service add `--s3-endpoint`, or set the same
   `SILO_BACKUP_*` variables silod uses. Credentials come from `AWS_*`.
5. **Give the directory its TLS files.** Copy `ca.crt`, `node.crt` and
   `node.key` into it (plus `ca.key` on the node that seeds the CA), or point
   `SILO_TLS_CA_CERT`, `SILO_TLS_NODE_CERT` and `SILO_TLS_NODE_KEY` at them.
6. **Start silod** with `SILO_DATA_DIR=/var/lib/silo-restored`, the same
   `SILO_NODE_ID` as the backup came from, and the same encryption key
   (`SILO_ENCRYPTION_KEY`, or the same key source). silod checks each chunk's
   encryption tag the first time it reads it, so a wrong key shows up as read
   errors.
7. **Check the result.** `siloctl status` should show the expected volumes and
   the node healthy. Read a volume back and confirm the contents.

To restore a whole cluster, restore every node from its own backup before
starting any of them, and pick backups from around the same time.

Details and the bucket layout are in [operations.md](operations.md#backups).

---

## Routine procedures (index)

Day-2 operations, each documented in [operations.md](operations.md):

- [Claim operator credentials](operations.md#claiming-operator-credentials): `siloctl auth init`
- [Mint a scoped token](operations.md#scoping-client-access-with-capability-tokens): `siloctl auth mint-token`
- [Revoke a certificate](operations.md#revoking-a-compromised-certificate): `siloctl ca revoke`
- [Drain a node](operations.md#draining-a-node): `siloctl node drain`
- [Rolling upgrade](operations.md#rolling-upgrades): protocol-version-aware
- [Rebalance capacity](operations.md#capacity-rebalancing)
- [Configure backups](operations.md#backups)
