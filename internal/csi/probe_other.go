//go:build !linux

package csi

import "time"

// probeDeviceLiveness only has a real implementation on Linux, where NBD
// devices exist.
var probeDeviceLiveness = func(string, time.Duration) bool { return true }
