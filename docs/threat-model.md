# silo threat model

What silo defends against, how, and where the current edges are. Written against
the implementation — when something is not yet enforced it says so and links to
[known-gaps.md](known-gaps.md). For operational how-tos see
[operations.md](operations.md).

## Assets

1. **Tenant data** — the bytes in files and block volumes.
2. **The cluster encryption key** — the one operator-managed key that wraps every
   per-chunk data key. Compromise of this plus the chunks equals plaintext.
3. **Cluster identity** — the CA and the node/client certificates that authorise
   membership and API access.
4. **The namespace** — the directory tree, ACLs, and volume leases.

## Trust boundaries

```
   operator ──mTLS (client cert)──▶ silod gRPC API
   pods ──CSI/FUSE──▶ node plugin ──mTLS / NBD──▶ silod
   silod ◀──mTLS gossip (peer cert)──▶ silod
   silod ──AES-GCM──▶ disk (chunk files)
```

Everything inside the cluster CA is one trust domain: any holder of a
CA-signed cert is a trusted member. silo does **not** currently do intra-cluster
authorization beyond "is a cluster member" (see [Authorization](#authorization)).

## Threats and mitigations

### Network attacker (on the wire)

- **All node-to-node and operator-to-node gRPC is mTLS** (TLS 1.3, cluster-CA
  chain). Gossip is mTLS too (`PeerConfig`, CA-chain verification without
  ServerName pinning since peers are a moving set). An attacker on the network
  cannot read or inject cluster traffic without a CA-signed cert.
- **Bootstrap join** (`siloctl auth init`) is the one endpoint that serves before
  the client has a cert. It is protected by a **single-use, TTL-bounded,
  hashed-at-rest token** plus a **server-fingerprint** the operator pins on first
  connect (TOFU). A stolen token is useless after first use or expiry.
- **NBD is unauthenticated block I/O** and is opt-in (`SILO_NBD_ADDR`). Treat the
  NBD port as sensitive: bind it to localhost/the node network, not the public
  interface. The CSI node plugin connects to the node-local silod.

### Disk / stolen-media attacker

- **Every chunk is AES-256-GCM encrypted at rest** with a per-chunk data key,
  itself wrapped by the cluster encryption key. A stolen disk yields ciphertext
  only — the wrapped data keys live in the inode metadata and are useless without
  the cluster key. GCM is authenticated, so tampering is detected on read.
- **The cluster key never lands in a chunk file.** Its source is pluggable
  (`KeyProvider`): `static` (env, dev), `file` (a 0400 file), or a cloud KMS
  (`aws-kms`/`gcp-kms`/`azure-kv`) that unwraps an envelope-encrypted key at
  startup so the plaintext never touches disk. Back the key (or its KMS access)
  up like a root credential — **losing it is unrecoverable data loss**.
- The chunk *ids* are visible as filenames on disk (not secret); the *contents*
  are not.

### Malicious or buggy cluster member

- **Split-brain on a block volume cannot corrupt it.** A volume's single-writer
  lease is a fenced LWW register: the newest holder wins cluster-wide and data
  nodes **refuse writes from a stale lease-holder** (fencing, not just
  revocation). A partitioned old writer's writes are rejected, not merged.
- **Namespace conflicts are surfaced, not silently resolved** — concurrent
  same-name creates become `*.conflict-<hlc>` siblings, so a misbehaving peer
  cannot make data disappear by racing a create.
- **A compromised node or client cert can be revoked immediately.** `siloctl ca
  revoke` adds the cert's serial to a CA-signed CRL; point every node's
  `SILO_TLS_CRL` at it and silod rejects that cert on the next mTLS handshake —
  even though it still chains to the CA and has not expired. The check runs on
  both the inbound (server rejects a revoked client) and outbound (a peer dial
  rejects a revoked server) direction, and disables TLS session resumption so a
  cached session cannot skip it. A CRL that does not verify against the cluster
  CA is refused at startup, and one signed by a foreign CA can never be
  substituted. Distribute the regenerated CRL to every node and restart (hot
  reload is future work — see [known-gaps.md](known-gaps.md)).

### Credential lifecycle

- **Node certs auto-rotate**: silod re-mints a node cert that is within ~4 months
  of its 1-year expiry on restart, so a rolling upgrade refreshes identity before
  it lapses. (A background rotation loop for never-restarting nodes is pending.)
- **The CA key gates minting.** A node without the CA key cannot mint or rotate;
  it serves its existing cert. Protect `SILO_TLS_CA_KEY` — it is the cluster's
  root of trust.

## Authorization (current limits)

silo authenticates (mTLS, who are you) but does **coarse authorization**: any
valid cluster member or operator cert can call any API. There is no per-principal
RBAC, no per-volume ACL enforcement at the gRPC layer, and no scoped tokens for
CSI/FUSE clients beyond the mTLS cert they hold. Treat a cluster cert as
all-or-nothing access — a revoked cert (see above) is the one way to take that
access away. Fine-grained auth (signed scoped tokens, RBAC mapped to namespace
ACLs) is roadmapped (M10/U5).

## Operator responsibilities

- Guard `SILO_TLS_CA_KEY` and the cluster encryption key; back both up offline.
- Keep the NBD port (if enabled) off untrusted networks.
- Rotate the join-token issuance (`SILO_PRINT_BOOTSTRAP_TOKEN=1` mints a new one);
  never commit tokens or keys.
- Watch `silo_hlc_clock_skew_alerts_total` — large clock skew is a correctness
  signal, not just an operational one.

## Out of scope (today)

- Side-channel / physical attacks on a running node's memory.
- Denial of service (no rate limiting on the gRPC surface yet).
- Multi-tenancy isolation beyond the single cluster trust domain.
- Supply-chain attestation of the silod binary.
