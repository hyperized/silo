//go:build integration

package integration_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestMetrics_ExporterServesSiloMetrics confirms the exporter is wired into the
// running daemon: /metrics serves build info and the clock-skew metrics that
// the skew monitor feeds.
func TestMetrics_ExporterServesSiloMetrics(t *testing.T) {
	node := startSilod(t)
	defer node.teardown()

	resp, err := http.Get("http://" + node.httpAddr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	for _, want := range []string{
		"silo_build_info{",
		"silo_hlc_peer_clock_skew_seconds",
		"silo_hlc_clock_skew_alerts_total",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("/metrics missing %q\n--- got ---\n%s", want, out)
		}
	}
}
