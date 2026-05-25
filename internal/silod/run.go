// Package silod composes the daemon's running pieces: configuration,
// structured logger, HTTP listener, and (in later milestones) gossip,
// the chunk store, and the writer/reader services. cmd/silod is the
// thin process entry point that calls Run.
package silod

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/observability"
)

// shutdownTimeout bounds graceful shutdown of sub-components.
const shutdownTimeout = 5 * time.Second

// httpService is the subset of observability.Server that Run depends on.
// Keeping this small interface lets the test suite inject fakes.
type httpService interface {
	Start() error
	Shutdown(ctx context.Context) error
}

// newHTTPService is the factory Run uses to build the HTTP listener.
// It is swapped out in tests to exercise lifecycle edge cases.
var newHTTPService = func(addr, nodeID, version string, logger *slog.Logger) httpService {
	return observability.NewServer(addr, nodeID, version, logger)
}

// Run starts silod and blocks until ctx is cancelled or a sub-component
// fails. It returns nil on graceful shutdown and a wrapped, actionable
// error otherwise.
//
// In M0 the only sub-component is the HTTP listener serving /healthz
// and /metrics. Later milestones extend Run with gossip, gRPC, and the
// chunk store while keeping the same lifecycle shape.
func Run(ctx context.Context, cfg *config.Config, logger *slog.Logger, version string) error {
	if cfg == nil {
		return fmt.Errorf("silod.Run: cfg is nil; pass a *config.Config loaded from config.LoadFromEnv or constructed in tests")
	}
	if logger == nil {
		return fmt.Errorf("silod.Run: logger is nil; pass an *slog.Logger built with observability.NewLogger")
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

	httpSrv := newHTTPService(cfg.HTTPAddr, cfg.NodeID, version, logger)

	errCh := make(chan error, 1)
	go func() {
		errCh <- httpSrv.Start()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; stopping silod")
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("the HTTP listener stopped before silod could serve traffic (%v); check SILO_HTTP_ADDR is a free, reachable host:port", err)
		}
		return fmt.Errorf("the HTTP listener exited without an error and without a shutdown signal; this is unexpected — please file a bug at https://github.com/hyperized/silo/issues")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("the HTTP listener did not shut down cleanly within %s (%v); investigate in-flight requests or increase the shutdown budget in future versions", shutdownTimeout, err)
	}

	// Drain the listener goroutine. Server.Start returns nil after a clean
	// Shutdown, so we don't inspect the value — we just block on it.
	<-errCh

	logger.Info("silo stopped cleanly")
	return nil
}
