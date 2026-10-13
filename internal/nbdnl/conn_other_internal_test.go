//go:build !linux

package nbdnl

import (
	"errors"
	"testing"
)

// TestStubsReportUnsupportedOS pins the non-Linux contract: every operation
// fails with ErrUnsupportedOS, so callers get the instruction instead of a
// silent no-op.
func TestStubsReportUnsupportedOS(t *testing.T) {
	c, w := &Conn{}, &Watcher{}
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Dial", func() error { _, err := Dial(); return err }},
		{"Close", c.Close},
		{"Connect", func() error { _, err := c.Connect(ConnectConfig{}); return err }},
		{"Reconfigure", func() error { return c.Reconfigure(0, 3, 0, 0) }},
		{"SetTimeouts", func() error { return c.SetTimeouts(0, 0, 0) }},
		{"Disconnect", func() error { return c.Disconnect(0) }},
		{"Connected", func() error { _, err := c.Connected(0); return err }},
		{"Watch", func() error { _, err := c.Watch(); return err }},
		{"Next", func() error { _, err := w.Next(); return err }},
		{"Watcher.Close", w.Close},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, ErrUnsupportedOS) {
				t.Fatalf("%s = %v, want ErrUnsupportedOS", tc.name, err)
			}
		})
	}
}
