package replication

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
)

// DefaultScrubInterval is how often the scrubber re-checks local chunks for
// under-replication when no interval is configured.
const DefaultScrubInterval = 30 * time.Second

// ChunkCatalog is the scrubber's read view of the local store: enumerate
// the chunks held here and load one to re-push. chunkstore.FileStore
// satisfies it.
type ChunkCatalog interface {
	List(ctx context.Context) ([]string, error)
	Get(ctx context.Context, id string) ([]byte, chunkstore.Info, error)
}

// ReplicaProbe checks whether a peer holds a chunk and pushes it if not.
// *GRPCPeers satisfies it.
type ReplicaProbe interface {
	Stat(ctx context.Context, addr, id string) (chunkstore.Info, error)
	Store(ctx context.Context, addr, id string, data []byte) (chunkstore.Info, error)
}

// Scrubber re-forms missing replicas. On a paced loop it walks the local
// chunks and, for each one this node is the highest-priority holder of,
// pushes a copy to any ring replica that is missing it. Electing the
// highest-priority *holder* — rather than just the ring primary — means
// healing still happens when the primary was down during the original
// write and therefore never received the chunk itself.
type Scrubber struct {
	place    Placement
	catalog  ChunkCatalog
	probe    ReplicaProbe
	rf       int
	interval time.Duration
	logger   *slog.Logger

	stop chan struct{}
	done chan struct{}
}

// NewScrubber builds a scrubber. rf < 1 clamps to 1 and a non-positive
// interval falls back to DefaultScrubInterval, so a misconfiguration paces
// conservatively rather than spinning.
func NewScrubber(place Placement, catalog ChunkCatalog, probe ReplicaProbe, rf int, interval time.Duration, logger *slog.Logger) *Scrubber {
	if rf < 1 {
		rf = 1
	}
	if interval <= 0 {
		interval = DefaultScrubInterval
	}
	return &Scrubber{
		place:    place,
		catalog:  catalog,
		probe:    probe,
		rf:       rf,
		interval: interval,
		logger:   logger,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Name identifies the subsystem in silod's lifecycle logs.
func (s *Scrubber) Name() string { return "scrubber" }

// Start runs the scrub loop until Shutdown, returning nil on a clean stop
// so silod can treat it like every other subsystem.
func (s *Scrubber) Start() error {
	defer close(s.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel the in-flight cycle's context the moment Shutdown is called so
	// a slow scrub does not hold up the daemon's exit.
	go func() {
		<-s.stop
		cancel()
	}()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return nil
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}

// Shutdown stops the loop, waiting for an in-flight cycle to unwind or the
// context to expire.
func (s *Scrubber) Shutdown(ctx context.Context) error {
	close(s.stop)
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("scrubber did not stop within the shutdown deadline (%w); a scrub cycle may be stuck on an unresponsive peer", ctx.Err())
	}
}

// runOnce walks every local chunk once, healing those this node owns.
func (s *Scrubber) runOnce(ctx context.Context) {
	ids, err := s.catalog.List(ctx)
	if err != nil {
		s.logger.Warn("scrubber could not list local chunks; will retry next cycle", "error", err)
		return
	}
	self := s.place.SelfID()
	for _, id := range ids {
		if ctx.Err() != nil {
			return // shutting down mid-cycle
		}
		s.scrubChunk(ctx, self, id)
	}
}

// scrubChunk ensures one locally-held chunk reaches its full replica set,
// but only when this node is the highest-ranked replica that actually holds
// it — so exactly one node pushes, avoiding a re-replication stampede.
func (s *Scrubber) scrubChunk(ctx context.Context, self, id string) {
	targets := s.place.Replicas(id, s.rf)
	myRank := indexOf(targets, self)
	if myRank < 0 {
		return // no longer a replica for this chunk (rebalanced away)
	}
	for _, higher := range targets[:myRank] {
		if s.holds(ctx, higher, id) {
			return // a higher-priority holder will do the pushing
		}
	}

	var (
		data   []byte
		loaded bool
	)
	for _, t := range targets {
		if t == self || s.holds(ctx, t, id) {
			continue
		}
		if !loaded {
			d, _, err := s.catalog.Get(ctx, id)
			if err != nil {
				s.logger.Warn("scrubber could not read a local chunk to re-replicate it", "chunk", id, "error", err)
				return
			}
			data, loaded = d, true
		}
		addr, ok := s.place.DataAddr(t)
		if !ok {
			continue // peer has not advertised a data address yet
		}
		if _, err := s.probe.Store(ctx, addr, id, data); err != nil {
			s.logger.Warn("scrubber could not re-replicate a chunk to a peer; will retry next cycle", "chunk", id, "peer", t, "error", err)
			continue
		}
		s.logger.Info("scrubber re-replicated a chunk", "chunk", id, "peer", t)
	}
}

// holds reports whether node holds id, via a local Stat on that peer. Any
// error — NotFound or an unreachable peer — counts as "does not hold", so a
// missing or down higher-priority replica never blocks healing.
func (s *Scrubber) holds(ctx context.Context, node, id string) bool {
	addr, ok := s.place.DataAddr(node)
	if !ok {
		return false
	}
	_, err := s.probe.Stat(ctx, addr, id)
	return err == nil
}

func indexOf(ids []string, target string) int {
	for i, id := range ids {
		if id == target {
			return i
		}
	}
	return -1
}
