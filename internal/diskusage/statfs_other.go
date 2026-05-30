//go:build !linux

package diskusage

import "fmt"

// Measure is unavailable off Linux, where silod does not run. Callers degrade
// gracefully, so this returns an error rather than failing the build.
func Measure(path string) (Usage, error) {
	return Usage{}, fmt.Errorf("filesystem capacity reporting is only supported on Linux (path %s)", path)
}
