// Package replication spreads immutable chunks across the cluster. Any
// node can coordinate a write: it resolves the chunk's replica set from
// the placement ring and fans the chunk out to those nodes in parallel,
// acking once a majority are durable and letting the rest heal in the
// background. Reads prefer a local replica and fall back to peers.
package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/hyperized/silo/internal/chunkstore"
)

// Placement resolves a chunk id to its ordered replica set and resolves a
// node id to the address other nodes dial to reach its data plane. It is
// the seam over membership + the consistent-hash ring; production wires
// the live cluster view, tests supply a deterministic fake.
type Placement interface {
	// Replicas returns up to n distinct node ids responsible for chunkID,
	// primary first. Fewer than n means the cluster has fewer live nodes.
	Replicas(chunkID string, n int) []string
	// DataAddr maps a node id to its gRPC data-plane dial address. ok is
	// false when the id is unknown or has not advertised an address yet.
	DataAddr(nodeID string) (addr string, ok bool)
	// SelfID is the local node's id, used to short-circuit a replica that
	// is this node into a local store access instead of a self-dial.
	SelfID() string
}

// Local is the on-node chunk store the coordinator reads from and writes
// to when this node is one of a chunk's replicas. chunkstore.FileStore
// satisfies it.
type Local interface {
	Put(ctx context.Context, id string, data []byte) (chunkstore.Info, error)
	Get(ctx context.Context, id string) ([]byte, chunkstore.Info, error)
	Delete(ctx context.Context, id string) error
	Stat(ctx context.Context, id string) (chunkstore.Info, error)
}

// Peers stores and fetches replicas on other nodes over the mTLS data
// plane. addr is the peer's advertised data address from Placement.
// Implementations MUST bound each call with their own timeout: the
// coordinator detaches the request context for background fan-out, so a
// hung peer call would otherwise leak a goroutine.
type Peers interface {
	Store(ctx context.Context, addr, id string, data []byte) (chunkstore.Info, error)
	Fetch(ctx context.Context, addr, id string) ([]byte, chunkstore.Info, error)
	Delete(ctx context.Context, addr, id string) error
	Stat(ctx context.Context, addr, id string) (chunkstore.Info, error)
}

// Coordinator turns a single Put/Get into the fan-out and fall-back logic
// across a chunk's replicas. It holds no mutable state, so one instance is
// safe to share across all in-flight RPCs.
type Coordinator struct {
	place  Placement
	local  Local
	peers  Peers
	rf     int
	logger *slog.Logger
}

// New builds a Coordinator. rf is the target replication factor (chunks
// land on min(rf, live-nodes) replicas). A rf < 1 is clamped to 1 so a
// misconfiguration degrades to single-copy rather than storing nothing.
func New(place Placement, local Local, peers Peers, rf int, logger *slog.Logger) *Coordinator {
	if rf < 1 {
		rf = 1
	}
	return &Coordinator{place: place, local: local, peers: peers, rf: rf, logger: logger}
}

// storeResult carries one replica write's outcome back to the coordinator.
type storeResult struct {
	info chunkstore.Info
	err  error
}

// Write fans chunkID out to its replica set and returns once a majority of
// those replicas are durable. Replicas that ack after the quorum keep
// writing in the background so the chunk reaches full replication without
// holding up the caller; an outright failure there is left for the
// scrubber to heal. Write fails only if the quorum is not reached.
func (c *Coordinator) Write(ctx context.Context, chunkID string, data []byte) (chunkstore.Info, error) {
	targets := c.place.Replicas(chunkID, c.rf)
	if len(targets) == 0 {
		return chunkstore.Info{}, fmt.Errorf("replication: no live node can store chunk %q; the cluster view is empty — check gossip connectivity and that at least one node is Alive", chunkID)
	}
	quorum := len(targets)/2 + 1

	// Detach so a replica that acks after the quorum keeps writing even
	// though Write has already returned to the client.
	fanCtx := context.WithoutCancel(ctx)
	results := make(chan storeResult, len(targets))
	for _, target := range targets {
		go c.storeOne(fanCtx, target, chunkID, data, results)
	}

	var (
		acks    int
		info    chunkstore.Info
		gotInfo bool
		errs    []error
	)
	for i := 0; i < len(targets); i++ {
		r := <-results
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		acks++
		if !gotInfo {
			info, gotInfo = r.info, true
		}
		if acks >= quorum {
			// Quorum durable. Drain the stragglers in the background so we
			// neither block the caller nor leak their goroutines.
			go c.drain(results, len(targets)-i-1, chunkID)
			return info, nil
		}
	}
	return chunkstore.Info{}, fmt.Errorf("replication: chunk %q reached only %d of %d replicas (quorum %d); check peer reachability and disk health: %w", chunkID, acks, len(targets), quorum, errors.Join(errs...))
}

// storeOne writes one replica, locally when target is this node and over
// the data plane otherwise, and reports the outcome on results.
func (c *Coordinator) storeOne(ctx context.Context, target, chunkID string, data []byte, results chan<- storeResult) {
	if target == c.place.SelfID() {
		info, err := c.local.Put(ctx, chunkID, data)
		results <- storeResult{info: info, err: err}
		return
	}
	addr, ok := c.place.DataAddr(target)
	if !ok {
		results <- storeResult{err: fmt.Errorf("replication: node %q has not advertised a data address yet; cannot replicate chunk %q to it", target, chunkID)}
		return
	}
	info, err := c.peers.Store(ctx, addr, chunkID, data)
	results <- storeResult{info: info, err: err}
}

// drain consumes the remaining count replica results after the quorum has
// been reached, logging any shortfall so the scrubber's later healing is
// explainable in the logs.
func (c *Coordinator) drain(results <-chan storeResult, count int, chunkID string) {
	for i := 0; i < count; i++ {
		if r := <-results; r.err != nil {
			c.logger.Warn("replica write fell short of full replication; the scrubber will re-form the missing copy",
				"chunk", chunkID, "error", r.err)
		}
	}
}

// Read returns the chunk from the nearest available replica: the local
// copy first when this node is a replica, then each peer replica in ring
// order. It fails only when no replica can serve the chunk.
func (c *Coordinator) Read(ctx context.Context, chunkID string) ([]byte, chunkstore.Info, error) {
	targets := c.place.Replicas(chunkID, c.rf)
	if len(targets) == 0 {
		return nil, chunkstore.Info{}, fmt.Errorf("replication: no live node holds chunk %q; the cluster view is empty — check gossip connectivity", chunkID)
	}

	var errs []error
	self := c.place.SelfID()
	// Prefer the local replica so reads stay on-box when possible.
	for _, target := range targets {
		if target != self {
			continue
		}
		data, info, err := c.local.Get(ctx, chunkID)
		if err == nil {
			return data, info, nil
		}
		errs = append(errs, fmt.Errorf("local: %w", err))
	}
	for _, target := range targets {
		if target == self {
			continue
		}
		addr, ok := c.place.DataAddr(target)
		if !ok {
			errs = append(errs, fmt.Errorf("node %q has no advertised data address", target))
			continue
		}
		data, info, err := c.peers.Fetch(ctx, addr, chunkID)
		if err == nil {
			return data, info, nil
		}
		errs = append(errs, fmt.Errorf("peer %s: %w", target, err))
	}
	return nil, chunkstore.Info{}, fmt.Errorf("replication: chunk %q could not be read from any of its %d replicas; the chunk may be under-replicated or those nodes are unreachable: %w", chunkID, len(targets), errors.Join(errs...))
}

// Delete removes chunkID from every replica. It must reach them all: a
// copy that survives would be re-replicated by the scrubber, resurrecting
// the chunk. A replica that already lacks the chunk counts as deleted.
// Note: a node that was down during the delete and later rejoins still
// holds its copy and can resurrect it — durable deletion across a partition
// needs tombstones, which arrive with the namespace work.
func (c *Coordinator) Delete(ctx context.Context, chunkID string) error {
	targets := c.place.Replicas(chunkID, c.rf)
	if len(targets) == 0 {
		return fmt.Errorf("replication: no live node holds chunk %q to delete; the cluster view is empty — check gossip connectivity", chunkID)
	}
	self := c.place.SelfID()
	var errs []error
	for _, target := range targets {
		var err error
		if target == self {
			err = c.local.Delete(ctx, chunkID)
		} else if addr, ok := c.place.DataAddr(target); ok {
			err = c.peers.Delete(ctx, addr, chunkID)
		} else {
			err = fmt.Errorf("node %q has no advertised data address", target)
		}
		if err != nil && !errors.Is(err, chunkstore.ErrNotFound) {
			errs = append(errs, fmt.Errorf("replica %s: %w", target, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("replication: delete of chunk %q did not reach every replica (it may resurrect from a surviving copy); retry once the cluster is healthy: %w", chunkID, errors.Join(errs...))
	}
	return nil
}

// Stat returns chunk metadata from the nearest replica: the local copy
// first when this node is a replica, then each peer replica in ring order.
// It fails only when no replica holds the chunk.
func (c *Coordinator) Stat(ctx context.Context, chunkID string) (chunkstore.Info, error) {
	targets := c.place.Replicas(chunkID, c.rf)
	if len(targets) == 0 {
		return chunkstore.Info{}, fmt.Errorf("replication: no live node holds chunk %q; the cluster view is empty — check gossip connectivity", chunkID)
	}

	var errs []error
	self := c.place.SelfID()
	for _, target := range targets {
		if target != self {
			continue
		}
		info, err := c.local.Stat(ctx, chunkID)
		if err == nil {
			return info, nil
		}
		errs = append(errs, fmt.Errorf("local: %w", err))
	}
	for _, target := range targets {
		if target == self {
			continue
		}
		addr, ok := c.place.DataAddr(target)
		if !ok {
			errs = append(errs, fmt.Errorf("node %q has no advertised data address", target))
			continue
		}
		info, err := c.peers.Stat(ctx, addr, chunkID)
		if err == nil {
			return info, nil
		}
		errs = append(errs, fmt.Errorf("peer %s: %w", target, err))
	}
	return chunkstore.Info{}, fmt.Errorf("replication: chunk %q not found on any of its %d replicas: %w", chunkID, len(targets), errors.Join(errs...))
}
