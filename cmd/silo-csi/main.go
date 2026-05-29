// Command silo-csi is silo's Container Storage Interface driver. It runs the
// CSI Identity service plus, depending on SILO_CSI_MODE, the Controller service
// (provision/snapshot volumes via silod's namespace) and/or the Node service
// (attach volumes over NBD and mount them into pods).
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/csi"
	"github.com/hyperized/silo/internal/observability"
)

// version is set at build time via -ldflags "-X main.version=…".
var version = "dev"

// signalContext is swappable so tests can drive runMain past the blocking
// Serve with a context that cancels immediately. Production points it at
// signal.NotifyContext so SIGINT/SIGTERM trigger a graceful drain.
var signalContext = func() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func main() {
	os.Exit(runMain(os.Stdout, os.Stderr))
}

// runMain isolates main's body so error paths are unit-testable without
// os.Exit tearing down the test runner.
func runMain(stdout, stderr io.Writer) int {
	cfg, err := csi.LoadConfig(os.Getenv)
	if err != nil {
		fmt.Fprintf(stderr, "silo-csi: invalid configuration — %v\n", err)
		return 1
	}

	logger, err := observability.NewLogger(stdout, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintf(stderr, "silo-csi: could not set up the structured logger — %v\n", err)
		return 1
	}
	slog.SetDefault(logger)

	opts := []csi.ServerOption{csi.WithIdentity(csi.NewIdentityService(version))}

	if cfg.Mode.RunsController() {
		conn, err := dialer(cfg.SilodAddr)
		if err != nil {
			fmt.Fprintf(stderr, "silo-csi: %v\n", err)
			return 1
		}
		defer func() { _ = conn.Close() }()
		backend := csi.NewNamespaceBackend(namespacev1.NewNamespaceStoreClient(conn))
		opts = append(opts, csi.WithController(csi.NewControllerService(backend)))
		logger.Info("silo-csi controller enabled", "silod", cfg.SilodAddr)
	}

	if cfg.Mode.RunsNode() {
		nodeID, err := resolveNodeID(cfg.NodeID)
		if err != nil {
			fmt.Fprintf(stderr, "silo-csi: %v\n", err)
			return 1
		}
		attacher, err := csi.NewNBDAttacher(cfg.NBDAddr)
		if err != nil {
			fmt.Fprintf(stderr, "silo-csi: %v\n", err)
			return 1
		}
		opts = append(opts, csi.WithNode(csi.NewNodeService(nodeID, attacher, csi.NewHostMounter())))
		logger.Info("silo-csi node enabled", "node", nodeID, "nbd", cfg.NBDAddr)
	}

	ctx, cancel := signalContext()
	defer cancel()

	if err := csi.NewServer(cfg.Endpoint, logger, opts...).Serve(ctx); err != nil {
		logger.Error("silo-csi stopped with an error", "err", err)
		return 1
	}
	return 0
}

// resolveNodeID returns the configured node id, falling back to the host name
// (the node's Kubernetes name in a standard deployment) when none is set.
func resolveNodeID(configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("could not determine the node id; set SILO_CSI_NODE_ID to this node's name (%w)", err)
	}
	return host, nil
}
