// Package observability provides the structured logger and the HTTP
// listener used for health and metrics endpoints. It is intentionally
// dependency-free: the M0 metrics surface emits Prometheus exposition
// format directly so silod has no third-party metrics library yet.
package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// NewLogger builds a slog.Logger with the requested level and output format.
// Format must be "text" or "json"; level must be "debug", "info", "warn",
// or "error". Both arguments are validated by the config package upstream;
// this function defends against direct callers.
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
