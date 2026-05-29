package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// notMounted/mounted are isMounted stubs.
func notMounted(string) (bool, error) { return false, nil }
func mounted(string) (bool, error)    { return true, nil }

func TestHostMounter_FilesystemFormatsAndMounts(t *testing.T) {
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "mnt")
	rec := newRecorder() // blkid returns empty -> device is unformatted
	m := NewHostMounter(withMountRunner(rec.run), withMountCheck(notMounted))

	if err := m.Mount(ctx, "/dev/nbd0", target, "ext4", []string{"noatime"}, false, false); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if rec.lastFor("blkid") == nil {
		t.Error("expected a blkid probe")
	}
	if got := rec.lastFor("mkfs.ext4"); !equalStrings(got, []string{"mkfs.ext4", "/dev/nbd0"}) {
		t.Errorf("mkfs command = %v", got)
	}
	if got := rec.lastFor("mount"); !equalStrings(got, []string{"mount", "-t", "ext4", "-o", "noatime", "/dev/nbd0", target}) {
		t.Errorf("mount command = %v", got)
	}
	if _, err := os.Stat(target); err != nil {
		t.Errorf("mount point not created: %v", err)
	}
}

func TestHostMounter_SkipsFormatWhenFormatted(t *testing.T) {
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "mnt")
	rec := newRecorder()
	rec.out["blkid"] = []byte("ext4\n") // already has a filesystem
	m := NewHostMounter(withMountRunner(rec.run), withMountCheck(notMounted))

	if err := m.Mount(ctx, "/dev/nbd0", target, "ext4", nil, false, true); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if rec.lastFor("mkfs.ext4") != nil {
		t.Error("a formatted device must not be reformatted")
	}
	// read-only adds the ro option.
	if got := rec.lastFor("mount"); !equalStrings(got, []string{"mount", "-t", "ext4", "-o", "ro", "/dev/nbd0", target}) {
		t.Errorf("mount command = %v, want ro", got)
	}
}

func TestHostMounter_AlreadyMountedIsNoop(t *testing.T) {
	ctx := context.Background()
	rec := newRecorder()
	m := NewHostMounter(withMountRunner(rec.run), withMountCheck(mounted))
	if err := m.Mount(ctx, "/dev/nbd0", "/t", "ext4", nil, false, false); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Errorf("an already-mounted target should run no commands, got %v", rec.calls)
	}
}

func TestHostMounter_BlockBindMount(t *testing.T) {
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "sub", "blk")
	rec := newRecorder()
	m := NewHostMounter(withMountRunner(rec.run), withMountCheck(notMounted))

	if err := m.Mount(ctx, "/dev/nbd0", target, "", nil, true, true); err != nil {
		t.Fatalf("Mount block: %v", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Errorf("block target file not created: %v", err)
	}
	if rec.calls[0][0] != "mount" || !equalStrings(rec.calls[0], []string{"mount", "-o", "bind", "/dev/nbd0", target}) {
		t.Errorf("bind command = %v", rec.calls[0])
	}
	// read-only block requests a ro remount.
	if got := rec.lastFor("mount"); !equalStrings(got, []string{"mount", "-o", "remount,ro,bind", "/dev/nbd0", target}) {
		t.Errorf("remount command = %v", got)
	}
}

func TestHostMounter_BlockRemountAndTargetErrors(t *testing.T) {
	ctx := context.Background()

	// The ro remount fails after a successful bind.
	remountFail := newRecorder()
	remountFail.errOnArg["remount"] = errors.New("remount denied")
	if err := NewHostMounter(withMountRunner(remountFail.run), withMountCheck(notMounted)).Mount(ctx, "/dev/nbd0", filepath.Join(t.TempDir(), "blk"), "", nil, true, true); err == nil {
		t.Error("a failed ro remount should surface")
	}

	// A block target whose parent is a file cannot be created.
	tmp := t.TempDir()
	parent := filepath.Join(tmp, "afile")
	if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := NewHostMounter(withMountRunner(newRecorder().run), withMountCheck(notMounted)).Mount(ctx, "/dev/nbd0", filepath.Join(parent, "blk"), "", nil, true, false); err == nil {
		t.Error("creating a block target under a file should fail")
	}

	// A filesystem mount point that cannot be created (parent is a file).
	if err := NewHostMounter(withMountRunner(newRecorder().run), withMountCheck(notMounted)).Mount(ctx, "/dev/nbd0", filepath.Join(parent, "mnt"), "ext4", nil, false, false); err == nil {
		t.Error("creating a filesystem mount point under a file should fail")
	}

	// An empty block target cannot be created as a file.
	if err := NewHostMounter(withMountRunner(newRecorder().run), withMountCheck(notMounted)).Mount(ctx, "/dev/nbd0", "", "", nil, true, false); err == nil {
		t.Error("an empty block target should fail to create")
	}
}

func TestProcMountsContains(t *testing.T) {
	// Exercises the production delegate; root ("/") is always a mountpoint.
	if ok, err := procMountsContains("/"); err != nil || !ok {
		t.Errorf("procMountsContains(/) = (%v, %v), want true", ok, err)
	}
}

func TestMountsContain(t *testing.T) {
	f := filepath.Join(t.TempDir(), "mounts")
	if err := os.WriteFile(f, []byte("/dev/nbd0 /mnt/data ext4 rw 0 0\nproc /proc proc rw 0 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ok, err := mountsContain(f, "/mnt/data"); err != nil || !ok {
		t.Errorf("mountsContain(/mnt/data) = (%v, %v), want true", ok, err)
	}
	if ok, err := mountsContain(f, "/mnt/other"); err != nil || ok {
		t.Errorf("mountsContain(/mnt/other) = (%v, %v), want false", ok, err)
	}
	if _, err := mountsContain(filepath.Join(t.TempDir(), "nope"), "/x"); err == nil {
		t.Error("mountsContain on a missing file should error")
	}
}

func TestHostMounter_Errors(t *testing.T) {
	ctx := context.Background()
	target := filepath.Join(t.TempDir(), "mnt")

	// mount failure.
	mf := newRecorder()
	mf.err["mount"] = errors.New("mount: bad fs")
	if err := NewHostMounter(withMountRunner(mf.run), withMountCheck(notMounted)).Mount(ctx, "/dev/nbd0", target, "ext4", nil, false, false); err == nil {
		t.Error("mount failure should surface")
	}

	// mkfs failure.
	kf := newRecorder()
	kf.err["mkfs.ext4"] = errors.New("mkfs: busy")
	if err := NewHostMounter(withMountRunner(kf.run), withMountCheck(notMounted)).Mount(ctx, "/dev/nbd0", filepath.Join(t.TempDir(), "m2"), "ext4", nil, false, false); err == nil {
		t.Error("mkfs failure should surface")
	}

	// bind-mount failure for a block volume.
	bf := newRecorder()
	bf.err["mount"] = errors.New("bind failed")
	if err := NewHostMounter(withMountRunner(bf.run), withMountCheck(notMounted)).Mount(ctx, "/dev/nbd0", filepath.Join(t.TempDir(), "blk"), "", nil, true, false); err == nil {
		t.Error("bind-mount failure should surface")
	}

	// isMounted error propagates.
	probeErr := func(string) (bool, error) { return false, errors.New("cannot read /proc/mounts") }
	if err := NewHostMounter(withMountRunner(newRecorder().run), withMountCheck(probeErr)).Mount(ctx, "/dev/nbd0", target, "ext4", nil, false, false); err == nil {
		t.Error("isMounted error should surface")
	}
}

func TestHostMounter_Unmount(t *testing.T) {
	ctx := context.Background()

	// Mounted target -> umount.
	rec := newRecorder()
	if err := NewHostMounter(withMountRunner(rec.run), withMountCheck(mounted)).Unmount(ctx, "/t"); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	if got := rec.lastFor("umount"); !equalStrings(got, []string{"umount", "/t"}) {
		t.Errorf("umount command = %v", got)
	}

	// Unmounted target -> no-op.
	rec2 := newRecorder()
	if err := NewHostMounter(withMountRunner(rec2.run), withMountCheck(notMounted)).Unmount(ctx, "/t"); err != nil || len(rec2.calls) != 0 {
		t.Errorf("unmount of an unmounted target should be a no-op (err=%v, calls=%d)", err, len(rec2.calls))
	}

	// umount failure surfaces.
	uf := newRecorder()
	uf.err["umount"] = errors.New("target busy")
	if err := NewHostMounter(withMountRunner(uf.run), withMountCheck(mounted)).Unmount(ctx, "/t"); err == nil {
		t.Error("umount failure should surface")
	}

	// isMounted error surfaces.
	probeErr := func(string) (bool, error) { return false, errors.New("boom") }
	if err := NewHostMounter(withMountRunner(uf.run), withMountCheck(probeErr)).Unmount(ctx, "/t"); err == nil {
		t.Error("isMounted error should surface on unmount")
	}
}
