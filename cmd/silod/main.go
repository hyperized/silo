// Command silod is the silo storage daemon.
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

// version is set at build time via -ldflags "-X main.version=…".
var version = "dev"

// signalContext is swappable so tests can drive runMain past the
// "wait for shutdown" point with a context that cancels immediately.
// Production points it at signal.NotifyContext so SIGINT and SIGTERM
// trigger graceful shutdown.
var signalContext = func() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func main() {
	os.Exit(runMain(os.Stdout, os.Stderr))
}

// runMain isolates main's body so error paths can be unit-tested
// without invoking os.Exit, which would tear the test runner down.
func runMain(stdout, stderr io.Writer) int {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		fmt.Fprintf(stderr, "silod: invalid configuration — %v\n", err)
		return 1
	}

	// The level/format validation in config.Validate and observability.NewLogger
	// share the same accept lists, so config.LoadFromEnv has already rejected
	// any value that would trip this branch. Kept as a defensive guard in case
	// the two lists drift in the future.
	logger, err := observability.NewLogger(stdout, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintf(stderr, "silod: could not set up the structured logger — %v\n", err)
		return 1
	}
	slog.SetDefault(logger)

	ctx, cancel := signalContext()
	defer cancel()

	if err := silod.Run(ctx, cfg, logger, stdout, version); err != nil {
		logger.Error("silod stopped with an error", "err", err)
		return 1
	}
	return 0
}
