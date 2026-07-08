package csi

import (
	"context"
	"os/exec"
)

// commandRunner runs an external command and returns its combined output. It
// is the seam between the node host operations (mkfs, mount) and the OS; tests
// substitute a fake so command construction can be asserted without touching
// the host.
type commandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

// execRunner is the production commandRunner.
func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	//nolint:gosec // name is a fixed host tool (mkfs/mount/...) and args are driver-controlled, not user input
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}
