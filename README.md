# silo

A symmetric, partition-tolerant distributed storage system in Go, designed for Kubernetes workloads on commodity hardware.

> **Status:** pre-alpha. M0 scaffolding only. See [PLAN.md](PLAN.md) for the full design and roadmap.

## Design at a glance

- **One binary, symmetric nodes.** Every node runs `silod`. No "metadata nodes" vs "data nodes."
- **Gossip-based cluster membership** (SWIM-style) with **CRDT namespace** (no Raft). Recovers from any partition without operator action.
- **Writer-owned chunks.** After `open`, a writer derives its own chunk IDs and placement; no metadata round-trip on the data path.
- **Block volumes first** (RWO via NBD), then a **shared filesystem** (RWX via custom FUSE).
- **AES-GCM encryption at rest** with one operator-managed encryption key (env, file, or KMS later).

## Quick start (M0)

```sh
make            # alias for `make up`; boots a 3-node cluster locally
make build      # compile binaries to ./bin/
make test       # unit tests
make down       # stop the local cluster
```

## Documentation

- [PLAN.md](PLAN.md) — full design, milestones, scope assessment
- `docs/architecture.md` — architecture deep-dive (TODO)
- `docs/operations.md` — operator guide (TODO)
- `docs/protocol.md` — wire protocol reference (TODO)
