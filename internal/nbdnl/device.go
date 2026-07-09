package nbdnl

import "fmt"

// DevicePath returns the device node for an NBD device index, e.g. /dev/nbd3.
func DevicePath(index uint32) string { return fmt.Sprintf("/dev/nbd%d", index) }
