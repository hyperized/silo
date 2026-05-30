// Command silo-fuse mounts a silo cluster as a local directory tree over FUSE.
//
//	silo-fuse <mountpoint>
//
// It dials silod (SILO_SERVER), then serves the pkg/fuse protocol against a
// silo-backed filesystem (internal/silofuse) with close-to-open coherence.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
	namespacev1 "github.com/hyperized/silo/api/proto/silo/namespace/v1"
	"github.com/hyperized/silo/internal/observability"
	"github.com/hyperized/silo/internal/silofuse"
	"github.com/hyperized/silo/pkg/fuse"
)

// version is set at build time via -ldflags "-X main.version=…".
var version = "dev"

// Swappable seams so the dial/config paths are testable without a kernel mount.
var (
	signalContext = func() (context.Context, context.CancelFunc) {
		return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	}
	mountFn = func(mountpoint string) (fuse.Conn, error) { return fuse.Mount(mountpoint) }
)

func main() {
	os.Exit(runMain(os.Args[1:], os.Stdout, os.Stderr))
}

func runMain(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(stdout, "Usage: silo-fuse <mountpoint>\n\nMounts the silo cluster at SILO_SERVER as a filesystem at <mountpoint>.")
		if len(args) != 1 {
			return 2
		}
		return 0
	}
	mountpoint := args[0]

	silodAddr := envOr("SILO_SERVER", "127.0.0.1:7000")
	nodeID, err := resolveNodeID(os.Getenv("SILO_NODE_ID"))
	if err != nil {
		fmt.Fprintf(stderr, "silo-fuse: %v\n", err)
		return 1
	}
	logger, err := observability.NewLogger(stdout, envOr("SILO_LOG_LEVEL", "info"), envOr("SILO_LOG_FORMAT", "text"))
	if err != nil {
		fmt.Fprintf(stderr, "silo-fuse: could not set up logging — %v\n", err)
		return 1
	}

	conn, err := dialer(silodAddr)
	if err != nil {
		fmt.Fprintf(stderr, "silo-fuse: could not dial silod at %q (%v); check SILO_SERVER\n", silodAddr, err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	backend, err := silofuse.NewGRPCBackend(namespacev1.NewNamespaceStoreClient(conn), chunkv1.NewChunkStoreClient(conn), nodeID)
	if err != nil {
		fmt.Fprintf(stderr, "silo-fuse: %v\n", err)
		return 1
	}

	mnt, err := mountFn(mountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "silo-fuse: %v\n", err)
		return 1
	}

	ctx, cancel := signalContext()
	defer cancel()
	// Unblock Serve on shutdown by tearing the mount down, which EOFs the read.
	go func() {
		<-ctx.Done()
		_ = mnt.Close()
	}()

	logger.Info("silo-fuse mounted", "mountpoint", mountpoint, "silod", silodAddr, "node", nodeID, "version", version)
	if err := fuse.NewSession(mnt, silofuse.New(ctx, backend), fuse.WithLogger(logger)).Serve(); err != nil {
		logger.Error("silo-fuse stopped with an error", "err", err)
		return 1
	}
	return 0
}

// resolveNodeID returns the configured node id or the host name.
func resolveNodeID(configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("could not determine a node id; set SILO_NODE_ID (%w)", err)
	}
	return host, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
