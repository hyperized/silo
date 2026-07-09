//go:build !linux

package nbdclient

// watchSocket only has a real implementation on Linux, where NBD devices
// exist; elsewhere sessions rely on explicit kicks.
var watchSocket = func(int, func()) (stop func(), err error) {
	return func() {}, nil
}
