package csi

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// HostMounter implements VolumeMounter against the host's filesystem tools
// (blkid, mkfs, mount, umount). For a filesystem volume it formats the device
// on first use and mounts it; for a block volume it bind-mounts the device node
// at the target. All operations are idempotent: an already-mounted target, or
// an already-formatted device, is left as-is.
type HostMounter struct {
	run commandRunner
	// isMounted reports whether target is a current mountpoint; overridable in
	// tests. Defaults to scanning /proc/mounts.
	isMounted func(target string) (bool, error)
}

// HostMounterOption configures a HostMounter.
type HostMounterOption func(*HostMounter)

func withMountRunner(run commandRunner) HostMounterOption {
	return func(m *HostMounter) { m.run = run }
}

func withMountCheck(fn func(string) (bool, error)) HostMounterOption {
	return func(m *HostMounter) { m.isMounted = fn }
}

// NewHostMounter builds a mounter over the host's mount tooling, which must be
// present in the node plugin image (util-linux and the e2fsprogs/xfsprogs for
// the filesystems you intend to use).
func NewHostMounter(opts ...HostMounterOption) *HostMounter {
	m := &HostMounter{run: execRunner, isMounted: procMountsContains}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Mount makes device available at target, idempotently. A block volume is
// bind-mounted at a target file; a filesystem volume is formatted (only if it
// has no filesystem) and mounted at a target directory.
func (m *HostMounter) Mount(ctx context.Context, device, target, fsType string, flags []string, block, readOnly bool) error {
	mounted, err := m.isMounted(target)
	if err != nil {
		return err
	}
	if mounted {
		return nil
	}

	if block {
		if err := ensureFile(target); err != nil {
			return err
		}
		opts := "bind"
		if out, err := m.run(ctx, "mount", "-o", opts, device, target); err != nil {
			return fmt.Errorf("could not bind-mount block device %s at %s (%w: %s)", device, target, err, strings.TrimSpace(string(out)))
		}
		if readOnly {
			if out, err := m.run(ctx, "mount", "-o", "remount,ro,bind", device, target); err != nil {
				return fmt.Errorf("bind-mounted %s but could not make it read-only (%w: %s)", target, err, strings.TrimSpace(string(out)))
			}
		}
		return nil
	}

	if err := os.MkdirAll(target, 0o750); err != nil {
		return fmt.Errorf("could not create mount point %s (%w)", target, err)
	}
	if err := m.ensureFormatted(ctx, device, fsType); err != nil {
		return err
	}
	args := []string{"-t", fsType}
	if o := mountOptions(flags, readOnly); len(o) > 0 {
		args = append(args, "-o", strings.Join(o, ","))
	}
	args = append(args, device, target)
	if out, err := m.run(ctx, "mount", args...); err != nil {
		return fmt.Errorf("could not mount %s filesystem on %s at %s (%w: %s)", fsType, device, target, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Unmount unmounts target if it is mounted; an unmounted target is a no-op.
func (m *HostMounter) Unmount(ctx context.Context, target string) error {
	mounted, err := m.isMounted(target)
	if err != nil {
		return err
	}
	if !mounted {
		return nil
	}
	if out, err := m.run(ctx, "umount", target); err != nil {
		return fmt.Errorf("could not unmount %s (%w: %s)", target, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ensureFormatted formats device with fsType only if it has no filesystem yet,
// detected with blkid (which prints the type when one is present). Chunks are
// immutable, so a re-attached volume keeps its filesystem and is never
// reformatted.
func (m *HostMounter) ensureFormatted(ctx context.Context, device, fsType string) error {
	// blkid exits non-zero with empty output when the device has no filesystem.
	if out, _ := m.run(ctx, "blkid", "-o", "value", "-s", "TYPE", device); strings.TrimSpace(string(out)) != "" {
		return nil
	}
	if out, err := m.run(ctx, "mkfs."+fsType, device); err != nil {
		return fmt.Errorf("could not create a %s filesystem on %s (%w: %s)", fsType, device, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// mountOptions builds the -o option list, appending ro when read-only.
func mountOptions(flags []string, readOnly bool) []string {
	opts := append([]string(nil), flags...)
	if readOnly {
		opts = append(opts, "ro")
	}
	return opts
}

// ensureFile creates an empty file (and its parent) to serve as a block-volume
// bind-mount target.
func ensureFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("could not create directory for %s (%w)", path, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE, 0o600) //nolint:gosec // path is the kubelet-provided target
	if err != nil {
		return fmt.Errorf("could not create mount target %s (%w)", path, err)
	}
	return f.Close()
}

// procMountsContains reports whether target appears as a mountpoint in
// /proc/mounts.
func procMountsContains(target string) (bool, error) { return mountsContain("/proc/mounts", target) }

// mountsContain is procMountsContains with an injectable mounts file for tests.
func mountsContain(mountsPath, target string) (bool, error) {
	data, err := os.ReadFile(mountsPath) //nolint:gosec // mountsPath is a fixed system path in production
	if err != nil {
		return false, fmt.Errorf("could not read %s (%w)", mountsPath, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == target {
			return true, nil
		}
	}
	return false, nil
}
