//go:build linux

package csi

import (
	"testing"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
)

func TestStatfsUsageReportsBytesAndInodes(t *testing.T) {
	usages, err := statfsUsage("/")
	if err != nil {
		t.Fatalf("statfsUsage(/): %v", err)
	}
	if len(usages) != 2 {
		t.Fatalf("usages = %d, want 2 (bytes and inodes)", len(usages))
	}
	units := map[csiv1.VolumeUsage_Unit]bool{}
	for _, u := range usages {
		if u.Total <= 0 {
			t.Fatalf("usage %v Total = %d, want > 0 for the root filesystem", u.Unit, u.Total)
		}
		units[u.Unit] = true
	}
	if !units[csiv1.VolumeUsage_BYTES] || !units[csiv1.VolumeUsage_INODES] {
		t.Fatalf("units = %v, want both BYTES and INODES", units)
	}
}

func TestStatfsUsageErrorsOnMissingPath(t *testing.T) {
	if _, err := statfsUsage("/does-not-exist-silo"); err == nil {
		t.Fatal("statfsUsage of a missing path should error")
	}
}
