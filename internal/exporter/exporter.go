// Package exporter renders silod's metrics as Prometheus exposition text and
// serves them at GET /metrics. Instrumented components register their instances
// (each a metrics.Source that owns its names and namespace); the exporter is
// the only package that knows the Prometheus wire format, so silod keeps no
// metrics-library dependency. Register all sources during startup, before the
// HTTP server begins serving.
package exporter

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/hyperized/silo/internal/metrics"
)

// Exporter is a registry of metric sources. It is safe for concurrent use:
// registration takes a write lock and each scrape takes a read lock, while a
// source's CollectMetrics is expected to be thread-safe in its own right.
type Exporter struct {
	mu      sync.RWMutex
	sources []metrics.Source
}

// New builds an empty exporter.
func New() *Exporter { return &Exporter{} }

// Register adds a metric source. Call it during startup, before serving.
func (e *Exporter) Register(s metrics.Source) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sources = append(e.sources, s)
}

// Render writes every registered source's metrics as Prometheus exposition
// text, namespacing each metric with its source's prefix. Values are pulled
// from the sources at call time, so a scrape always reflects current state.
func (e *Exporter) Render(w io.Writer) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, s := range e.sources {
		prefix := s.MetricPrefix()
		for _, m := range s.CollectMetrics() {
			name := prefix + "_" + m.Name
			fmt.Fprintf(w, "# HELP %s %s\n", name, m.Help)
			fmt.Fprintf(w, "# TYPE %s %s\n", name, kindString(m.Kind))
			fmt.Fprintf(w, "%s%s %g\n", name, formatLabels(m.Labels), m.Value)
		}
	}
}

// Handler serves the metrics as Prometheus text. Mount it at GET /metrics.
func (e *Exporter) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		e.Render(w)
	})
}

func kindString(k metrics.Kind) string {
	if k == metrics.Counter {
		return "counter"
	}
	return "gauge"
}

func formatLabels(labels [][2]string) string {
	if len(labels) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, kv := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", kv[0], kv[1])
	}
	b.WriteByte('}')
	return b.String()
}
