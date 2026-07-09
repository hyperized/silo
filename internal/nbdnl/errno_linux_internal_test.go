//go:build linux

package nbdnl

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

// TestErrnoMappingIsRealOnLinux pins the init-time swap: kernel error codes
// must map to real errnos so callers can errors.Is against unix.ENOENT and
// friends (Dial's module-missing detection depends on it).
func TestErrnoMappingIsRealOnLinux(t *testing.T) {
	if !errors.Is(errnoToError(-2), unix.ENOENT) {
		t.Fatal("errno -2 should map to ENOENT on linux")
	}
	if !errors.Is(errnoToError(-1), unix.EPERM) {
		t.Fatal("errno -1 should map to EPERM on linux")
	}
}
