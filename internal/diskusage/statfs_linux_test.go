//go:build linux

package diskusage

import "testing"

func TestMeasure(t *testing.T) {
	du, err := Measure(t.TempDir())
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	if du.CapacityBytes <= 0 {
		t.Errorf("capacity = %d, want > 0", du.CapacityBytes)
	}
	if du.UsedBytes < 0 || du.AvailableBytes < 0 || du.UsedBytes > du.CapacityBytes {
		t.Errorf("inconsistent usage: %+v", du)
	}

	if _, err := Measure("/nonexistent-silo-data-dir-xyz"); err == nil {
		t.Error("Measure on a missing path should error")
	}
}
