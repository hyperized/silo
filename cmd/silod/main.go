// Command silod is the silo storage daemon. It loads configuration from
// the environment (12-factor), sets up structured logging, and runs the
// daemon lifecycle until it receives SIGINT or SIGTERM.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/observability"
	"github.com/hyperized/silo/internal/silod"
)

// version is set at build time via -ldflags "-X main.version=<value>".
// Defaults to "dev" for local builds.
var version = "dev"

func main() {
	os.Exit(runMain(os.Stdout, os.Stderr))
}

// runMain is split out from main so its non-zero exit paths can be unit-tested.
// It returns the process exit code.
func runMain(stdout, stderr io.Writer) int {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		fmt.Fprintf(stderr, "silod: invalid configuration — %v\n", err)
		return 1
	}

	logger, err := observability.NewLogger(stdout, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintf(stderr, "silod: could not set up the structured logger — %v\n", err)
		return 1
	}
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := silod.Run(ctx, cfg, logger, version); err != nil {
		logger.Error("silod stopped with an error", "err", err)
		return 1
	}
	return 0
}
