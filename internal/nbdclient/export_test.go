package nbdclient

import (
	"net"
	"time"
)

// SetSocketFDForTest substitutes the fd extractor so tests can attach over
// net.Pipe connections, which have no file descriptor.
func SetSocketFDForTest(fn func(net.Conn) (int, error)) (restore func()) {
	old := socketFD
	socketFD = fn
	return func() { socketFD = old }
}

// SetBackoffForTest tightens the reconnect backoff so tests converge fast.
func SetBackoffForTest(floor, ceiling time.Duration) (restore func()) {
	oldFloor, oldCap := backoffFloor, backoffCap
	backoffFloor, backoffCap = floor, ceiling
	return func() { backoffFloor, backoffCap = oldFloor, oldCap }
}
