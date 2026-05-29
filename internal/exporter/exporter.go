// Package exporter renders silod's metrics as Prometheus exposition text and
// serves them at GET /metrics. Domain packages expose plain getters; the
// exporter owns every line of Prometheus formatting, so the wire format lives
// in one place and silod keeps no metrics-library dependency. Register all
// metrics during startup, before the HTTP server begins serving.
package exporter

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// Exporter is a small registry of metrics that render themselves as
// Prometheus text. It is safe for concurrent use: registration takes a write
// lock and each scrape takes a read lock, while the value getters a metric
// closes over are expected to be thread-safe in their own right.
type Exporter struct {
	mu      sync.RWMutex
	metrics []metric
}

type metric struct {
	name   string
	help   string
	typ    string
	render func(w io.Writer)
}

// New builds an empty exporter.
func New() *Exporter { return &Exporter{} }

// Info registers a constant info metric (value 1) carrying labels — the
// build_info pattern, where the labels are the payload and the value is just
// a presence marker.
func (e *Exporter) Info(name, help string, labels [][2]string) {
	e.add(name, help, "gauge", func(w io.Writer) {
		fmt.Fprintf(w, "%s%s 1\n", name, formatLabels(labels))
	})
}

// Gauge registers a gauge whose value is read afresh at each scrape.
func (e *Exporter) Gauge(name, help string, read func() float64) {
	e.add(name, help, "gauge", func(w io.Writer) {
		fmt.Fprintf(w, "%s %g\n", name, read())
	})
}

// Counter registers a monotonic counter whose value is read at each scrape.
func (e *Exporter) Counter(name, help string, read func() uint64) {
	e.add(name, help, "counter", func(w io.Writer) {
		fmt.Fprintf(w, "%s %d\n", name, read())
	})
}

func (e *Exporter) add(name, help, typ string, render func(io.Writer)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics = append(e.metrics, metric{name: name, help: help, typ: typ, render: render})
}

// Render writes every registered metric as Prometheus exposition text.
func (e *Exporter) Render(w io.Writer) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, m := range e.metrics {
		fmt.Fprintf(w, "# HELP %s %s\n", m.name, m.help)
		fmt.Fprintf(w, "# TYPE %s %s\n", m.name, m.typ)
		m.render(w)
	}
}

// Handler serves the metrics as Prometheus text. Mount it at GET /metrics.
func (e *Exporter) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		e.Render(w)
	})
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
