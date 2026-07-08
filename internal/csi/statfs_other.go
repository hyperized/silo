//go:build !linux

package csi

import (
	csiv1 "github.com/hyperized/silo/api/proto/csi/v1"
	"github.com/hyperized/silo/internal/nbdnl"
)

// statfsUsage only has a real implementation on Linux, where volumes mount;
// elsewhere stats responses simply carry no usage. A var so tests can
// substitute results.
var statfsUsage = func(string) ([]*csiv1.VolumeUsage, error) {
	return nil, nbdnl.ErrUnsupportedOS
}
