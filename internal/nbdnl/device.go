package nbdnl

import (
	"errors"
	"fmt"
)

// ErrNotificationsDropped is returned by Watcher.Next when the kernel could
// not deliver some dead-link notifications because the watcher fell behind.
// Which devices died is unknown, so the caller has to check every device's
// link itself. The watcher stays usable.
var ErrNotificationsDropped = errors.New("nbdnl: the kernel dropped dead-link notifications; every device's link must be checked")

// DevicePath returns the device node for an NBD device index, e.g. /dev/nbd3.
func DevicePath(index uint32) string { return fmt.Sprintf("/dev/nbd%d", index) }
