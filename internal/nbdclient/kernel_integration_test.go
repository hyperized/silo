//go:build integration && linux

package nbdclient_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/nbd"
	"github.com/hyperized/silo/internal/nbdclient"
	"github.com/hyperized/silo/internal/nbdnl"
)

// TestKernelAttachReconnectDetach exercises the whole restart-resilience
// stack against the real kernel: internal/nbd serves a volume from memory,
// nbdclient attaches it as /dev/nbdX over netlink, raw I/O flows through the
// kernel, the server dies and comes back (a silod restart), the dead-link
// watcher fires, the session reconnects, and I/O resumes without an error.
//
// Needs root, the nbd kernel module, and /dev/nbd*; run it in a privileged
// container:
//
//	docker run --rm --privileged -v "$PWD":/src -w /src golang:1.25-alpine \
//	  go test -tags integration -run TestKernel -v ./internal/nbdclient/
func TestKernelAttachReconnectDetach(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("needs root to configure NBD devices")
	}
	kernel, err := nbdnl.Dial()
	if err != nil {
		t.Skipf("kernel NBD netlink unavailable (%v); load the nbd module first", err)
	}
	defer func() { _ = kernel.Close() }()

	srv := nbd.NewServer(memBackend{size: 64 << 20}, discardLogger())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ctx1, cancelServe1 := context.WithCancel(context.Background())
	go func() { _ = srv.Serve(ctx1, ln) }()

	cfg := nbdclient.Config{
		Addr:            addr,
		Export:          "/integration/vol",
		Kernel:          kernel,
		ReconnectWindow: 2 * time.Minute,
		// The default socket watch runs: this is the real-fd environment.
	}
	session, err := nbdclient.Attach(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	t.Logf("attached %s", session.Device())
	defer func() { _ = session.Detach(context.Background()) }()

	// The dead-link watcher, wired the way the CSI attacher wires it.
	watcher, err := kernel.Watch()
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer func() { _ = watcher.Close() }()
	watcherFired := make(chan uint32, 8)
	go func() {
		for {
			idx, err := watcher.Next()
			if err != nil {
				return
			}
			watcherFired <- idx
			if idx == session.Index() {
				session.Kick()
			}
		}
	}()

	// Raw I/O through the kernel device; fsync forces it through to the server.
	dev, err := os.OpenFile(session.Device(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s: %v", session.Device(), err)
	}
	defer func() { _ = dev.Close() }()
	pattern := make([]byte, 1<<20)
	for i := range pattern {
		pattern[i] = byte(i)
	}
	if _, err := dev.WriteAt(pattern, 0); err != nil {
		t.Fatalf("write before restart: %v", err)
	}
	if err := dev.Sync(); err != nil {
		t.Fatalf("sync before restart: %v", err)
	}

	// The "silod restart": kill the server, let the link die, bring the
	// server back on the same address.
	cancelServe1()
	_ = ln.Close()

	select {
	case idx := <-watcherFired:
		if idx != session.Index() {
			t.Logf("watcher reported another device first (%d); still waiting", idx)
		}
		t.Logf("kernel reported the dead link for device %d", idx)
	case <-time.After(15 * time.Second):
		t.Fatal("the kernel never multicast a dead-link notification")
	}

	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("re-listen on %s: %v", addr, err)
	}
	ctx2, cancelServe2 := context.WithCancel(context.Background())
	defer cancelServe2()
	go func() { _ = srv.Serve(ctx2, ln2) }()

	waitForCondition(t, "reconnect", 30*time.Second, func() bool {
		return session.Reconnects() >= 1 && session.State() == nbdclient.StateHealthy
	})

	// I/O must flow again on the same device node, no remount, no error.
	if _, err := dev.WriteAt(pattern, 1<<20); err != nil {
		t.Fatalf("write after reconnect: %v", err)
	}
	if err := dev.Sync(); err != nil {
		t.Fatalf("sync after reconnect: %v", err)
	}
	got := make([]byte, 1<<20)
	if _, err := dev.ReadAt(got, 1<<20); err != nil {
		t.Fatalf("read after reconnect: %v", err)
	}

	// An open device pins the kernel configuration, so close before
	// detaching — the same order the kubelet guarantees (unmount, then
	// unpublish). The kernel then tears the configuration down
	// asynchronously after acknowledging the disconnect.
	if err := dev.Close(); err != nil {
		t.Fatalf("close device: %v", err)
	}
	if err := session.Detach(context.Background()); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	waitForCondition(t, "device deconfiguration", 10*time.Second, func() bool {
		_, err := os.Stat(fmt.Sprintf("/sys/block/nbd%d/pid", session.Index()))
		return errors.Is(err, os.ErrNotExist)
	})
}

func waitForCondition(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
