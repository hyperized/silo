# silo

**Distributed block storage for Kubernetes that you can actually operate.**

silo is a symmetric storage system in Go. One binary per node, no metadata tier,
no quorum to lose, no rebalance ceremony. It gives your pods `ReadWriteOnce`
volumes through an ordinary `PersistentVolumeClaim`, and it heals itself through
network partitions without paging you.

Distributed storage tends to fail you at 3am rather than at benchmark time, so
silo spends its complexity budget on being operable. Every node runs the same
binary and can do every job, and every write is on real disk on multiple nodes
before it returns.

- **Kubernetes-native.** An ordinary PVC against the `silo` StorageClass becomes a
  `ReadWriteOnce` device, with CSI snapshots and clones.
- **Self-healing replication.** Every chunk lives on N nodes. Lose one and the
  chunks re-replicate onto the survivors in the background.
- **Partition-tolerant namespace.** A CRDT, so both sides of a split keep serving
  and converge once the network heals. There is no quorum to lose.
- **Rollout-safe attachments.** Volumes pause and reconnect across a silod
  restart instead of returning I/O errors to the pod.
- **Encrypted at rest and in flight.** AES-GCM with per-chunk keys, mTLS between
  nodes, and keys from a file or a cloud KMS.

You give up `ReadWriteMany` on block volumes, erasure coding, and some write
latency for this. The full capability list, the reasoning, and the current status
are in [docs/design.md](docs/design.md).

## Try it in 5 minutes

No Kubernetes required. This boots a real 3-node cluster with Prometheus and
Grafana, then creates and snapshots a volume.

```sh
git clone https://github.com/hyperized/silo && cd silo

make up        # build + boot 3 silod nodes (+ Prometheus on :9090, Grafana on :3030)
make build     # compile ./bin/{silod,siloctl,silo-csi}
```

`make up` scrapes silo-a's bootstrap token from the container logs and prints a
paste-ready `siloctl auth init` command. Run it to claim your operator credentials.
This writes the cluster CA and your client cert to `~/.config/silo/` and remembers
the server:

```sh
./bin/siloctl auth init \
  --token <TOKEN> \
  --server 127.0.0.1:7001 \
  --server-fingerprint <FINGERPRINT>
```

Now drive the cluster. Every command authenticates over mTLS automatically:

```sh
./bin/siloctl volume create /db --size 10G        # create a 10 GiB block volume
./bin/siloctl volume snapshot /db /db-backup      # instant copy-on-write snapshot
./bin/siloctl ns ls /                             # list the namespace
./bin/siloctl auth status                         # show your cluster credentials
```

Watch it heal: `docker kill silo-b`, then `make status` and re-list. The cluster
stays available and re-replicates in the background. `make down` tears it all down.

> Block-mounting a volume locally needs silod's NBD server (`SILO_NBD_ADDR`) plus
> `nbd-client` on the host. On Kubernetes the CSI node plugin does this for you.

## Kubernetes

Install the [`silo-csi` Helm chart](deploy/helm/silo-csi) and point it at a
running silo cluster:

```sh
helm install silo-csi deploy/helm/silo-csi \
  --namespace silo-system --create-namespace \
  --set silod.address=silo.silo-system.svc.cluster.local:7000
```

From there your app teams write an ordinary PVC against the `silo` StorageClass.
The prerequisite is the `nbd` kernel module on each node. See
[docs/kubernetes.md](docs/kubernetes.md).

## Standalone

One node is one line. Adding nodes with the same CA and encryption key grows it
into a cluster with no data migration:

```sh
SILO_ENCRYPTION_KEY_SOURCE=file SILO_ENCRYPTION_KEY_PATH=/etc/silo/key \
SILO_DATA_DIR=/var/lib/silo SILO_NBD_ADDR=0.0.0.0:10809 ./bin/silod
```

See [docs/operations.md](docs/operations.md#deployment-paths).

## Documentation

Getting started:

- **[design.md](docs/design.md)**: the principles behind silo, the full capability list, the trade-offs, and current status
- **[kubernetes.md](docs/kubernetes.md)**: install and operate silo-csi (Helm values, StorageClass, snapshots, troubleshooting)
- **[operations.md](docs/operations.md)**: the operator guide. Configuration, deployment paths, mTLS and credentials, NBD, backups, troubleshooting

Production:

- **[runbook.md](docs/runbook.md)**: readiness checklist, golden-signal alerts, failure-recovery playbooks
- **[performance.md](docs/performance.md)**: the trade against Ceph, the write path, and the measured baseline
- **[threat-model.md](docs/threat-model.md)**: what silo defends against, how, and where the current edges are
- **[known-gaps.md](docs/known-gaps.md)**: what isn't finished, and why

Reference:

- **[development.md](docs/development.md)**: build from source, test layout, local cluster
- **[.env.example](.env.example)**: annotated `silod` configuration
- **[deploy/helm/silo-csi/README.md](deploy/helm/silo-csi/README.md)**: chart reference

## License

See the repository for license details.
