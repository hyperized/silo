package csi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// commandRunner runs an external command and returns its combined output. It is
// the single seam between the node host operations (NBD attach, mkfs, mount) and
// the OS; tests substitute a fake so command construction can be asserted
// without touching the host.
type commandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

// execRunner is the production commandRunner.
func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	//nolint:gosec // name is a fixed host tool (nbd-client/mkfs/mount/...) and args are driver-controlled, not user input
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// NBDAttacher attaches silo volumes by driving the mainline Linux NBD client
// (nbd-client) against the node's own silod, which serves every volume over
// NBD. Connecting takes the volume's single-writer lease — silod fences any
// prior holder — so a pod rescheduled onto this node steals the device cleanly
// from wherever it was.
//
// It tracks volume->device locally so Detach and a repeated Attach are
// idempotent. That mapping is in-memory: it is the node plugin's view of what
// it has attached, rebuilt as pods are (re)published after a restart.
type NBDAttacher struct {
	host string
	port string
	run  commandRunner
	// pickDevice returns a free /dev/nbdX; overridable in tests.
	pickDevice func() (string, error)

	mu       sync.Mutex
	attached map[string]string // volume path -> device
}

// NBDAttacherOption configures an NBDAttacher.
type NBDAttacherOption func(*NBDAttacher)

func withRunner(run commandRunner) NBDAttacherOption {
	return func(a *NBDAttacher) { a.run = run }
}

func withDevicePicker(pick func() (string, error)) NBDAttacherOption {
	return func(a *NBDAttacher) { a.pickDevice = pick }
}

// NewNBDAttacher builds an attacher that connects to the local silod NBD server
// at nbdAddr (host:port — typically the node's own silod). nbd-client must be
// present in the node plugin image.
func NewNBDAttacher(nbdAddr string, opts ...NBDAttacherOption) (*NBDAttacher, error) {
	host, port, err := splitHostPort(nbdAddr)
	if err != nil {
		return nil, err
	}
	a := &NBDAttacher{
		host:       host,
		port:       port,
		run:        execRunner,
		pickDevice: firstFreeNBDDevice,
		attached:   make(map[string]string),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

// Attach connects /dev/nbdX to the volume's NBD export (its namespace path) and
// returns the device. An already-attached volume returns its existing device.
func (a *NBDAttacher) Attach(ctx context.Context, volumePath string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if dev, ok := a.attached[volumePath]; ok {
		return dev, nil
	}
	dev, err := a.pickDevice()
	if err != nil {
		return "", err
	}
	// nbd-client <host> <port> <device> -name <export> -persist: -persist keeps
	// the device reconnecting if silod is briefly unreachable (a rolling restart
	// or a lease takeover settling).
	if out, err := a.run(ctx, "nbd-client", a.host, a.port, dev, "-name", volumePath, "-persist"); err != nil {
		return "", fmt.Errorf("nbd-client could not attach %q to %s (%w: %s)", volumePath, dev, err, strings.TrimSpace(string(out)))
	}
	a.attached[volumePath] = dev
	return dev, nil
}

// Detach disconnects the volume's device. A volume that is not attached here is
// not an error.
func (a *NBDAttacher) Detach(ctx context.Context, volumePath string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	dev, ok := a.attached[volumePath]
	if !ok {
		return nil
	}
	if out, err := a.run(ctx, "nbd-client", "-d", dev); err != nil {
		return fmt.Errorf("nbd-client could not detach %s backing %q (%w: %s)", dev, volumePath, err, strings.TrimSpace(string(out)))
	}
	delete(a.attached, volumePath)
	return nil
}

// splitHostPort splits a host:port advertise address, with an actionable error.
func splitHostPort(addr string) (host, port string, err error) {
	i := strings.LastIndex(addr, ":")
	if i <= 0 || i == len(addr)-1 {
		return "", "", fmt.Errorf("%q is not a host:port NBD address; set the node plugin's silod NBD address, e.g. 127.0.0.1:10809", addr)
	}
	return addr[:i], addr[i+1:], nil
}

// firstFreeNBDDevice returns the first /dev/nbdX whose kernel size is 0, i.e.
// not currently connected. The nbd kernel module pre-creates the device nodes,
// so a free one is just one with no backing.
func firstFreeNBDDevice() (string, error) { return freeNBDFrom("/sys/block/nbd*/size") }

// freeNBDFrom is firstFreeNBDDevice with an injectable glob root for testing.
func freeNBDFrom(pattern string) (string, error) {
	entries, err := filepath.Glob(pattern)
	if err != nil {
		return "", err
	}
	for _, sizePath := range entries {
		data, err := os.ReadFile(sizePath) //nolint:gosec // path comes from a fixed glob, not user input
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(data)) == "0" {
			name := filepath.Base(filepath.Dir(sizePath)) // nbdN
			return "/dev/" + name, nil
		}
	}
	return "", fmt.Errorf("no free /dev/nbd device is available; raise the nbd module's nbds_max or load it with `modprobe nbd nbds_max=64`")
}
