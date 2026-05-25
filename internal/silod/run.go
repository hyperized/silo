// Package silod composes the daemon's sub-components into a single Run
// lifecycle. cmd/silod is the thin process entry point.
package silod

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/crypto"
	"github.com/hyperized/silo/internal/observability"
	"github.com/hyperized/silo/internal/transport"
)

// shutdownTimeout bounds graceful shutdown of every sub-component. Tuned
// to outlast the slowest expected in-flight HTTP handler / gRPC stream
// but stay well under any orchestrator's default SIGTERM-to-SIGKILL gap
// (k8s defaults to 30s) so we never trip an external preemption.
const shutdownTimeout = 5 * time.Second

// subsystem is a runnable component of silod with a uniform lifecycle.
// Run treats every subsystem the same so adding gossip, replication, or
// a UI server later does not change the lifecycle code.
type subsystem interface {
	Name() string
	Start() error
	Shutdown(ctx context.Context) error
}

// Factories for each subsystem are package-level so tests can swap in
// fakes without spinning up real listeners.
var (
	newHTTPSubsystem = func(cfg *config.Config, version string, logger *slog.Logger) subsystem {
		return &httpSub{srv: observability.NewServer(cfg.HTTPAddr, cfg.NodeID, version, logger)}
	}
	newGRPCSubsystem = func(cfg *config.Config, store chunkstore.Store, logger *slog.Logger) subsystem {
		return &grpcSub{srv: transport.NewGRPCServer(cfg.GRPCAddr, store, logger)}
	}
	newChunkStore = func(cfg *config.Config, cipher *crypto.Cipher) (chunkstore.Store, error) {
		return chunkstore.NewFileStore(cfg.DataDir, cipher)
	}
)

// Run blocks until ctx is cancelled or any subsystem fails. It returns
// nil on graceful shutdown. Future sub-components plug into this single
// select loop so silod has exactly one place where the lifecycle decision
// is made.
func Run(ctx context.Context, cfg *config.Config, logger *slog.Logger, version string) error {
	if cfg == nil {
		return fmt.Errorf("silod.Run: cfg is nil; pass a *config.Config loaded from config.LoadFromEnv or constructed in tests")
	}
	if logger == nil {
		return fmt.Errorf("silod.Run: logger is nil; pass an *slog.Logger built with observability.NewLogger")
	}

	cipher, err := crypto.NewCipher(cfg.EncryptionKey)
	if err != nil {
		return fmt.Errorf("silod.Run: could not initialise the cluster encryption key (%v); regenerate SILO_ENCRYPTION_KEY with: openssl rand -base64 32", err)
	}

	store, err := newChunkStore(cfg, cipher)
	if err != nil {
		return fmt.Errorf("silod.Run: could not open the chunk store (%v); check SILO_DATA_DIR is on a writable filesystem and silod has permission", err)
	}

	logger.Info("silo starting",
		"version", version,
		"node_id", cfg.NodeID,
		"grpc_addr", cfg.GRPCAddr,
		"http_addr", cfg.HTTPAddr,
		"seeds", cfg.Seeds,
		"domain", cfg.Domain,
		"data_dir", cfg.DataDir,
		"chunk_size", cfg.ChunkSize,
		"replication", cfg.Replication,
		"encryption_key_source", cfg.KeySource,
	)

	subs := []subsystem{
		newHTTPSubsystem(cfg, version, logger),
		newGRPCSubsystem(cfg, store, logger),
	}

	type subResult struct {
		name string
		err  error
	}
	results := make(chan subResult, len(subs))
	for _, sub := range subs {
		go func(s subsystem) {
			results <- subResult{name: s.Name(), err: s.Start()}
		}(sub)
	}

	var startupErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; stopping silod")
	case r := <-results:
		if r.err != nil {
			startupErr = fmt.Errorf("subsystem %q failed before silod was fully running: %w", r.name, r.err)
		} else {
			startupErr = fmt.Errorf("subsystem %q exited cleanly without a shutdown signal; this is unexpected — please file a bug at https://github.com/hyperized/silo/issues", r.name)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	for _, sub := range subs {
		if err := sub.Shutdown(shutdownCtx); err != nil {
			logger.Error("subsystem did not shut down cleanly",
				"subsystem", sub.Name(),
				"err", err,
			)
			if startupErr == nil {
				startupErr = err
			}
		}
	}

	// Drain remaining Start results so the goroutines don't leak.
	remaining := len(subs)
	if startupErr != nil {
		remaining--
	}
	for i := 0; i < remaining; i++ {
		<-results
	}

	if startupErr != nil {
		return startupErr
	}
	logger.Info("silo stopped cleanly")
	return nil
}

type httpSub struct {
	srv *observability.Server
}

func (h *httpSub) Name() string                          { return "http" }
func (h *httpSub) Start() error                          { return h.srv.Start() }
func (h *httpSub) Shutdown(ctx context.Context) error    { return h.srv.Shutdown(ctx) }

type grpcSub struct {
	srv *transport.GRPCServer
}

func (g *grpcSub) Name() string                          { return "grpc" }
func (g *grpcSub) Start() error                          { return g.srv.Start() }
func (g *grpcSub) Shutdown(ctx context.Context) error    { return g.srv.Shutdown(ctx) }
