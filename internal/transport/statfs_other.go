//go:build !linux

package transport

import "fmt"

// statfsUsage is unavailable off Linux, where silod does not run. The status
// service degrades gracefully — membership still reports — so this returns an
// error rather than failing the build.
func statfsUsage(path string) (DiskUsage, error) {
	return DiskUsage{}, fmt.Errorf("filesystem capacity reporting is only supported on Linux (data dir %s)", path)
}
