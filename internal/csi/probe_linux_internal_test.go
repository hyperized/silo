//go:build linux

package csi

import (
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestProbeDeviceLiveness(t *testing.T) {
	// A device that does not exist cannot answer reads.
	if probeDeviceLiveness("/does-not-exist-silo", time.Second) {
		t.Fatal("a nonexistent device probed as alive")
	}

	// Opening a FIFO read-only blocks until a writer appears — the probe's
	// deadline, not the open, must decide.
	fifo := filepath.Join(t.TempDir(), "fifo")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	start := time.Now()
	if probeDeviceLiveness(fifo, 300*time.Millisecond) {
		t.Fatal("a blocked probe reported alive")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the probe did not respect its deadline")
	}
}

func TestAlignedBlock(t *testing.T) {
	b := alignedBlock(4096)
	if len(b) != 4096 {
		t.Fatalf("len = %d, want 4096", len(b))
	}
	if uintptr(unsafe.Pointer(&b[0]))%512 != 0 {
		t.Fatal("buffer is not 512-byte aligned; O_DIRECT reads would fail")
	}
}
