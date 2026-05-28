package replication

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// TestDrain_LogsShortfall exercises the background straggler drain in
// isolation: after the quorum returns, drain must consume every remaining
// result and log the ones that failed (so the scrubber's later healing is
// traceable) while staying quiet about the ones that succeeded.
func TestDrain_LogsShortfall(t *testing.T) {
	var buf bytes.Buffer
	c := New(nil, nil, nil, 1, slog.New(slog.NewTextHandler(&buf, nil)))

	results := make(chan storeResult, 3)
	results <- storeResult{}                                // a late success: no log
	results <- storeResult{err: errors.New("disk full")}    // logs
	results <- storeResult{err: errors.New("conn refused")} // logs

	c.drain(results, 3, "chunk-x")

	out := buf.String()
	if !strings.Contains(out, "scrubber") {
		t.Errorf("drain should explain the scrubber will heal; got %q", out)
	}
	if !strings.Contains(out, "disk full") || !strings.Contains(out, "conn refused") {
		t.Errorf("drain should log each failed replica; got %q", out)
	}
	if strings.Count(out, "fell short") != 2 {
		t.Errorf("expected exactly 2 shortfall logs, got %q", out)
	}
}
