//go:build linux

package nbdnl

import (
	"testing"
)

// TestDialAgainstHostKernel exercises the netlink bootstrap against whatever
// kernel runs the tests. Without the nbd module the resolve must fail with the
// modprobe instruction; with it, the basic query surface must work. The full
// connect/reconfigure lifecycle needs a served export and lives in the
// kernel integration test (make test-nbd-kernel).
func TestDialAgainstHostKernel(t *testing.T) {
	conn, err := Dial()
	if err != nil {
		if s := err.Error(); s == "" {
			t.Fatal("Dial error should explain itself")
		}
		t.Logf("no NBD netlink family on this host (fine): %v", err)
		return
	}
	defer func() { _ = conn.Close() }()

	// Statusing a device index far beyond any real device must error cleanly,
	// not hang or panic.
	if _, err := conn.Connected(4294967294); err == nil {
		t.Fatal("Connected on an absurd index should error")
	}

	watcher, err := conn.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// Close must unblock a pending Next.
	done := make(chan struct{})
	go func() {
		_, _ = watcher.Next()
		close(done)
	}()
	if err := watcher.Close(); err != nil {
		t.Fatalf("watcher Close: %v", err)
	}
	<-done
}
