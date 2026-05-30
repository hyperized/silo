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
	"math"
	"net/http"
	"strconv"
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
			if m.Kind == metrics.Histogram {
				renderHistogram(w, name, m)
				continue
			}
			fmt.Fprintf(w, "%s%s %g\n", name, formatLabels(m.Labels), m.Value)
		}
	}
}

// renderHistogram writes the _bucket{le=…}, _sum, and _count series a Prometheus
// histogram requires.
func renderHistogram(w io.Writer, name string, m metrics.Metric) {
	for _, b := range m.Buckets {
		le := strconv.FormatFloat(b.LE, 'g', -1, 64)
		if math.IsInf(b.LE, 1) {
			le = "+Inf"
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", name, formatLabels(withLabel(m.Labels, "le", le)), b.Count)
	}
	fmt.Fprintf(w, "%s_sum%s %g\n", name, formatLabels(m.Labels), m.Sum)
	fmt.Fprintf(w, "%s_count%s %d\n", name, formatLabels(m.Labels), m.Count)
}

// withLabel returns labels with an extra key=value appended.
func withLabel(labels [][2]string, key, value string) [][2]string {
	out := make([][2]string, 0, len(labels)+1)
	out = append(out, labels...)
	return append(out, [2]string{key, value})
}

// Handler serves the metrics as Prometheus text. Mount it at GET /metrics.
func (e *Exporter) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		e.Render(w)
	})
}

func kindString(k metrics.Kind) string {
	switch k {
	case metrics.Counter:
		return "counter"
	case metrics.Histogram:
		return "histogram"
	default:
		return "gauge"
	}
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
