# silo

A symmetric, partition-tolerant distributed storage system in Go, designed for Kubernetes workloads on commodity hardware.

> **Status:** pre-alpha. M2 cluster mTLS + token-based operator join. See [PLAN.md](PLAN.md) for the full design and roadmap.

## Design at a glance

- **One binary, symmetric nodes.** Every node runs `silod`. No "metadata nodes" vs "data nodes."
- **Gossip-based cluster membership** (SWIM-style) with **CRDT namespace** (no Raft). Recovers from any partition without operator action.
- **Writer-owned chunks.** After `open`, a writer derives its own chunk IDs and placement; no metadata round-trip on the data path.
- **Block volumes first** (RWO via NBD), then a **shared filesystem** (RWX via custom FUSE).
- **AES-GCM encryption at rest** with one operator-managed encryption key (env, file, or KMS later).

## Quick start

```sh
make            # alias for `make up`; boots a 3-node cluster locally
make build      # compile binaries to ./bin/
make test       # unit tests
make test-integration  # end-to-end against a real silod binary
make down       # stop the local cluster
```

### Claiming operator credentials

Every silo cluster runs under mutual TLS. The first time you bring a cluster up, `silod` mints its own cluster CA and prints a one-time join token plus its server fingerprint to stdout:

```
silo cluster bootstrap token (valid for 24h, single-use)

  token:               m5ZVB4QrWFpOG…
  server fingerprint:  sha256:6ba2902451d6…

Run this on the operator host to claim a client certificate:

  siloctl auth init \
    --token m5ZVB4QrWFpOG… \
    --server 127.0.0.1:7001 \
    --server-fingerprint sha256:6ba2902451d6…
```

Copy that command and run it on the host where you'll be using `siloctl`. It writes the cluster CA, your client certificate, and the matching key into `~/.config/silo/`. From then on every `siloctl chunk …` call authenticates over mTLS automatically — no further flags or env vars required.

To mint another token later (e.g. for a colleague's machine), restart `silod` with `SILO_PRINT_BOOTSTRAP_TOKEN=1` set. The new token is printed on the next boot.

### Inspecting credentials

```sh
siloctl auth status
```

Prints the cluster CA fingerprint, the principal on your client cert, the expiry, and the default server.

## Documentation

- [PLAN.md](PLAN.md) — full design, milestones, scope assessment
- `docs/architecture.md` — architecture deep-dive (TODO)
- `docs/operations.md` — operator guide (TODO)
- `docs/protocol.md` — wire protocol reference (TODO)
