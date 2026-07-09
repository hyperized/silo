//go:build linux

package csi

import (
	"golang.org/x/sys/unix"

	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
)

// statfsUsage reads a mounted volume's byte and inode usage. A block volume's
// target is a device node rather than a mount, so callers treat an error as
// "no usage to report", not a failure. A var so tests can substitute results.
var statfsUsage = func(path string) ([]*csiv1.VolumeUsage, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return nil, err
	}
	bsize := uint64(st.Bsize) // #nosec G115 -- block sizes are small positive values
	return []*csiv1.VolumeUsage{
		{
			Unit:      csiv1.VolumeUsage_BYTES,
			Total:     int64(st.Blocks * bsize),              // #nosec G115 -- fits: filesystem sizes are far below 2^63
			Available: int64(st.Bavail * bsize),              // #nosec G115 -- fits: see Total
			Used:      int64((st.Blocks - st.Bfree) * bsize), // #nosec G115 -- fits: see Total
		},
		{
			Unit:      csiv1.VolumeUsage_INODES,
			Total:     int64(st.Files),            // #nosec G115 -- fits: inode counts are far below 2^63
			Available: int64(st.Ffree),            // #nosec G115 -- fits: see Total
			Used:      int64(st.Files - st.Ffree), // #nosec G115 -- fits: see Total
		},
	}, nil
}
