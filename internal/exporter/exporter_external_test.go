package exporter_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/exporter"
)

func TestExporter_RendersRegisteredMetrics(t *testing.T) {
	e := exporter.New()
	e.Info("silo_build_info", "Build information.", [][2]string{{"node", "n1"}, {"version", "v1"}})

	skew := 1.5
	var alerts uint64 = 3
	e.Gauge("silo_skew_seconds", "Observed skew.", func() float64 { return skew })
	e.Counter("silo_alerts_total", "Alert count.", func() uint64 { return alerts })

	var b strings.Builder
	e.Render(&b)
	out := b.String()

	for _, want := range []string{
		"# HELP silo_build_info Build information.",
		"# TYPE silo_build_info gauge",
		`silo_build_info{node="n1",version="v1"} 1`,
		"# TYPE silo_skew_seconds gauge",
		"silo_skew_seconds 1.5",
		"# TYPE silo_alerts_total counter",
		"silo_alerts_total 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q\n--- got ---\n%s", want, out)
		}
	}

	// Getters are read at render time, not registration time.
	skew, alerts = -2, 4
	var b2 strings.Builder
	e.Render(&b2)
	if !strings.Contains(b2.String(), "silo_skew_seconds -2") || !strings.Contains(b2.String(), "silo_alerts_total 4") {
		t.Errorf("render did not re-read getters: %s", b2.String())
	}
}

func TestExporter_InfoWithoutLabels(t *testing.T) {
	e := exporter.New()
	e.Info("silo_up", "Up marker.", nil)
	var b strings.Builder
	e.Render(&b)
	if !strings.Contains(b.String(), "silo_up 1\n") {
		t.Errorf("label-less info metric malformed: %q", b.String())
	}
}

func TestExporter_HandlerServesText(t *testing.T) {
	e := exporter.New()
	e.Gauge("silo_g", "A gauge.", func() float64 { return 0.5 })

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
