package exporter_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/exporter"
	"github.com/hyperized/silo/internal/metrics"
)

// dynSource is a metric source whose readings are produced by a closure, so a
// test can change the values it reports between scrapes.
type dynSource struct {
	prefix  string
	collect func() []metrics.Metric
}

func (d dynSource) MetricPrefix() string             { return d.prefix }
func (d dynSource) CollectMetrics() []metrics.Metric { return d.collect() }

func TestExporter_RendersRegisteredSources(t *testing.T) {
	e := exporter.New()
	e.Register(metrics.Static("silo", metrics.Metric{
		Name: "build_info", Help: "Build information.", Kind: metrics.Gauge, Value: 1,
		Labels: [][2]string{{"node", "n1"}, {"version", "v1"}},
	}))

	skew, alerts := 1.5, 3.0
	e.Register(dynSource{prefix: "silo_hlc", collect: func() []metrics.Metric {
		return []metrics.Metric{
			{Name: "peer_clock_skew_seconds", Help: "Skew.", Kind: metrics.Gauge, Value: skew},
			{Name: "clock_skew_alerts_total", Help: "Alerts.", Kind: metrics.Counter, Value: alerts},
		}
	}})

	var b strings.Builder
	e.Render(&b)
	out := b.String()
	for _, want := range []string{
		"# HELP silo_build_info Build information.",
		"# TYPE silo_build_info gauge",
		`silo_build_info{node="n1",version="v1"} 1`,
		"# TYPE silo_hlc_peer_clock_skew_seconds gauge",
		"silo_hlc_peer_clock_skew_seconds 1.5",
		"# TYPE silo_hlc_clock_skew_alerts_total counter",
		"silo_hlc_clock_skew_alerts_total 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q\n--- got ---\n%s", want, out)
		}
	}

	// Sources are pulled at render time, not registration time.
	skew, alerts = -2, 4
	var b2 strings.Builder
	e.Render(&b2)
	if !strings.Contains(b2.String(), "silo_hlc_peer_clock_skew_seconds -2") ||
		!strings.Contains(b2.String(), "silo_hlc_clock_skew_alerts_total 4") {
		t.Errorf("render did not re-pull source values: %s", b2.String())
	}
}

func TestExporter_HandlerServesText(t *testing.T) {
	e := exporter.New()
	e.Register(dynSource{prefix: "silo", collect: func() []metrics.Metric {
		return []metrics.Metric{{Name: "g", Help: "A gauge.", Kind: metrics.Gauge, Value: 0.5}}
	}})

	rec := httptest.NewRecorder()
	e.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q, want text/plain", ct)
	}
	if !strings.Contains(rec.Body.String(), "silo_g 0.5") {
		t.Errorf("handler body = %q", rec.Body.String())
	}
}

func TestExporter_EmptyRendersNothing(t *testing.T) {
	var b strings.Builder
	exporter.New().Render(&b)
	if b.Len() != 0 {
		t.Errorf("empty exporter rendered %q, want nothing", b.String())
	}
}

// histSource emits a single histogram metric for rendering tests.
type histSource struct{ h *metrics.Hist }

func (histSource) MetricPrefix() string { return "silo_test" }
func (s histSource) CollectMetrics() []metrics.Metric {
	return []metrics.Metric{s.h.Collect("op_latency_seconds", "op latency", [][2]string{{"op", "put"}})}
}

func TestExporter_RendersHistogram(t *testing.T) {
	h := metrics.NewHist(0.01, 0.1)
	h.Observe(0.005)
	h.Observe(0.5) // past both bounds -> only +Inf

	e := exporter.New()
	e.Register(histSource{h: h})
	var buf bytes.Buffer
	e.Render(&buf)
	out := buf.String()

	for _, want := range []string{
		"# TYPE silo_test_op_latency_seconds histogram",
		`silo_test_op_latency_seconds_bucket{op="put",le="0.01"} 1`,
		`silo_test_op_latency_seconds_bucket{op="put",le="0.1"} 1`,
		`silo_test_op_latency_seconds_bucket{op="put",le="+Inf"} 2`,
		`silo_test_op_latency_seconds_sum{op="put"} 0.505`,
		`silo_test_op_latency_seconds_count{op="put"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered output missing %q.\n--- got ---\n%s", want, out)
		}
	}
}
