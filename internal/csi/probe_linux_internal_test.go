//go:build linux

package csi

import (
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestProbeDeviceLiveness(t *testing.T) {
	// A device that cannot be opened is reported as failed, the same verdict
	// the kernel's own I/O errors get.
	if v := probeDeviceLiveness("/does-not-exist-silo", time.Second); v != probeFailed {
		t.Fatalf("a nonexistent device probed as %d, want probeFailed", v)
	}

	// A regular file answers at once. Whether the filesystem allows O_DIRECT
	// decides alive or failed; either way the probe must not stall.
	file := filepath.Join(t.TempDir(), "block")
	if err := os.WriteFile(file, make([]byte, 8192), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if v := probeDeviceLiveness(file, time.Second); v == probeStalled {
		t.Fatal("a readable file probed as stalled")
	}

	// Opening a FIFO read-only blocks until a writer appears, like a read on
	// a dead link inside the reconnect window: the deadline decides.
	fifo := filepath.Join(t.TempDir(), "fifo")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	start := time.Now()
	if v := probeDeviceLiveness(fifo, 300*time.Millisecond); v != probeStalled {
		t.Fatalf("a blocked probe returned %d, want probeStalled", v)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the probe did not respect its deadline")
	}
}

func TestReadVerdict(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want probeVerdict
	}{
		{"read succeeded", nil, probeAlive},
		{"kernel failed the read", unix.EIO, probeFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := readVerdict(tc.err); got != tc.want {
				t.Fatalf("readVerdict(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
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
