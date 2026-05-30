//go:build linux

package transport

import (
	"fmt"
	"syscall"
)

// statfsUsage reads a filesystem's capacity accounting for the volume backing
// path. It reports the whole filesystem's figures (not just silo's chunk files),
// which is what an operator planning capacity on a dedicated data volume wants.
func statfsUsage(path string) (DiskUsage, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return DiskUsage{}, fmt.Errorf("could not stat the filesystem at %s (%w); check that SILO_DATA_DIR exists and is readable", path, err)
	}
	bs := int64(st.Bsize) //nolint:unconvert // Bsize is int64 on linux already, but be explicit
	return DiskUsage{
		CapacityBytes:  int64(st.Blocks) * bs,          //nolint:gosec // block counts from the kernel
		AvailableBytes: int64(st.Bavail) * bs,          //nolint:gosec // block counts from the kernel
		UsedBytes:      int64(st.Blocks-st.Bfree) * bs, //nolint:gosec // block counts from the kernel
	}, nil
}
