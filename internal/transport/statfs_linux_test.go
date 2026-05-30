//go:build linux

package transport

import "testing"

func TestStatfsUsage(t *testing.T) {
	// A real directory yields a positive capacity and a consistent breakdown.
	du, err := statfsUsage(t.TempDir())
	if err != nil {
		t.Fatalf("statfsUsage: %v", err)
	}
	if du.CapacityBytes <= 0 {
		t.Errorf("capacity = %d, want > 0", du.CapacityBytes)
	}
	if du.UsedBytes < 0 || du.AvailableBytes < 0 || du.UsedBytes > du.CapacityBytes {
		t.Errorf("inconsistent usage: %+v", du)
	}

	// A path that cannot be stat'd surfaces an actionable error.
	if _, err := statfsUsage("/nonexistent-silo-data-dir-xyz"); err == nil {
		t.Error("statfsUsage on a missing path should error")
	}
}
