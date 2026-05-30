//go:build linux

package diskusage

import (
	"fmt"
	"syscall"
)

// Measure reads the capacity accounting of the filesystem backing path.
func Measure(path string) (Usage, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return Usage{}, fmt.Errorf("could not stat the filesystem at %s (%w); check that SILO_DATA_DIR exists and is readable", path, err)
	}
	bs := st.Bsize
	return Usage{
		CapacityBytes:  int64(st.Blocks) * bs,          //nolint:gosec // block counts from the kernel
		AvailableBytes: int64(st.Bavail) * bs,          //nolint:gosec // block counts from the kernel
		UsedBytes:      int64(st.Blocks-st.Bfree) * bs, //nolint:gosec // block counts from the kernel
	}, nil
}
