// Package observability provides the slog logger and the HTTP listener
// used for health and metrics. The /metrics handler emits Prometheus
// exposition text directly so silod stays free of a metrics-library
// dependency until there are real metrics worth shipping.
package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// NewLogger builds an slog.Logger from level/format strings. Both are
// validated again here even though config.Validate already checks them,
// because direct callers (tests, future embedders) can reach this
// function without going through Config — failing closed on unknown
// values is cheap insurance against silent misconfiguration.
func NewLogger(out io.Writer, level, format string) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch strings.ToLower(format) {
	case "text":
		h = slog.NewTextHandler(out, opts)
	case "json":
		h = slog.NewJSONHandler(out, opts)
	default:
		return nil, fmt.Errorf("log format %q is not recognised; set SILO_LOG_FORMAT to text (human-readable) or json (machine-readable)", format)
	}
	return slog.New(h), nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("log level %q is not recognised; set SILO_LOG_LEVEL to debug, info, warn, or error", s)
	}
}
