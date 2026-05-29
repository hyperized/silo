package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecRunner(t *testing.T) {
	// The production seam actually runs a command; echo is universally present.
	out, err := execRunner(context.Background(), "echo", "hello")
	if err != nil || strings.TrimSpace(string(out)) != "hello" {
		t.Errorf("execRunner echo = (%q, %v), want hello", out, err)
	}
	if _, err := execRunner(context.Background(), "this-binary-does-not-exist-silo"); err == nil {
		t.Error("execRunner of a missing binary should error")
	}
}

func TestFirstFreeNBDDevice(t *testing.T) {
	// On a host with no nbd module this returns an error; on one with a free
	// device it returns a path. Either way it exercises the production delegate.
	if dev, err := firstFreeNBDDevice(); err == nil && dev == "" {
		t.Error("firstFreeNBDDevice returned no device and no error")
	}
}

// recordingRunner records every command and returns canned output keyed by the
// binary name (args[0] of the underlying command).
type recordingRunner struct {
	calls    [][]string
	out      map[string][]byte
	err      map[string]error
	errOnArg map[string]error // fail any call whose args contain this substring
}

func newRecorder() *recordingRunner {
	return &recordingRunner{out: map[string][]byte{}, err: map[string]error{}, errOnArg: map[string]error{}}
}

func (r *recordingRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, args...))
	for sub, err := range r.errOnArg {
		for _, a := range args {
			if strings.Contains(a, sub) {
				return nil, err
			}
		}
	}
	return r.out[name], r.err[name]
}

func (r *recordingRunner) lastFor(name string) []string {
	for i := len(r.calls) - 1; i >= 0; i-- {
		if r.calls[i][0] == name {
			return r.calls[i]
		}
	}
	return nil
}

func TestNBDAttacher_AttachDetach(t *testing.T) {
	ctx := context.Background()
	rec := newRecorder()
	att, err := NewNBDAttacher("10.0.0.5:10809", withRunner(rec.run), withDevicePicker(func() (string, error) { return "/dev/nbd5", nil }))
	if err != nil {
		t.Fatalf("NewNBDAttacher: %v", err)
	}

	dev, err := att.Attach(ctx, "/csi/volumes/pvc-1")
	if err != nil || dev != "/dev/nbd5" {
		t.Fatalf("Attach = (%q, %v), want /dev/nbd5", dev, err)
	}
	want := []string{"nbd-client", "10.0.0.5", "10809", "/dev/nbd5", "-name", "/csi/volumes/pvc-1", "-persist"}
	if got := rec.lastFor("nbd-client"); !equalStrings(got, want) {
		t.Errorf("attach command = %v, want %v", got, want)
	}

	// A second Attach is idempotent and issues no new command.
	before := len(rec.calls)
	if dev2, err := att.Attach(ctx, "/csi/volumes/pvc-1"); err != nil || dev2 != "/dev/nbd5" || len(rec.calls) != before {
		t.Errorf("idempotent Attach = (%q, %v), calls %d->%d", dev2, err, before, len(rec.calls))
	}

	if err := att.Detach(ctx, "/csi/volumes/pvc-1"); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	if got := rec.lastFor("nbd-client"); !equalStrings(got, []string{"nbd-client", "-d", "/dev/nbd5"}) {
		t.Errorf("detach command = %v", got)
	}
	// Detaching an unknown volume is a no-op.
	before = len(rec.calls)
	if err := att.Detach(ctx, "/csi/volumes/unknown"); err != nil || len(rec.calls) != before {
		t.Errorf("detach unknown = %v, calls changed", err)
	}
}

func TestNBDAttacher_Errors(t *testing.T) {
	ctx := context.Background()

	// nbd-client failure surfaces with its output.
	rec := newRecorder()
	rec.err["nbd-client"] = errors.New("connection refused")
	att, _ := NewNBDAttacher("h:1", withRunner(rec.run), withDevicePicker(func() (string, error) { return "/dev/nbd0", nil }))
	if _, err := att.Attach(ctx, "/v"); err == nil {
		t.Error("attach should fail when nbd-client fails")
	}

	// Device-picker failure aborts before any command.
	pickErr := newRecorder()
	bad, _ := NewNBDAttacher("h:1", withRunner(pickErr.run), withDevicePicker(func() (string, error) { return "", errors.New("no devices") }))
	if _, err := bad.Attach(ctx, "/v"); err == nil || len(pickErr.calls) != 0 {
		t.Errorf("picker failure should abort before running nbd-client (err=%v, calls=%d)", err, len(pickErr.calls))
	}

	// Detach failure surfaces.
	delRec := newRecorder()
	delRec.err["nbd-client"] = errors.New("not connected")
	att2, _ := NewNBDAttacher("h:1", withRunner(delRec.run), withDevicePicker(func() (string, error) { return "/dev/nbd0", nil }))
	delRec2 := newRecorder() // attach with a working runner first
	att2.run = delRec2.run
	if _, err := att2.Attach(ctx, "/v"); err != nil {
		t.Fatalf("setup attach: %v", err)
	}
	att2.run = delRec.run
	if err := att2.Detach(ctx, "/v"); err == nil {
		t.Error("detach should fail when nbd-client -d fails")
	}
}

func TestSplitHostPort(t *testing.T) {
	if h, p, err := splitHostPort("127.0.0.1:10809"); err != nil || h != "127.0.0.1" || p != "10809" {
		t.Errorf("split = (%q, %q, %v)", h, p, err)
	}
	for _, bad := range []string{"", "noport", ":10809", "host:"} {
		if _, _, err := splitHostPort(bad); err == nil {
			t.Errorf("splitHostPort(%q) should error", bad)
		}
	}
	if _, err := NewNBDAttacher("garbage"); err == nil {
		t.Error("NewNBDAttacher with a bad address should error")
	}
}

func TestFreeNBDDevice(t *testing.T) {
	dir := t.TempDir()
	// Two devices: nbd0 connected (size>0), nbd1 free (size 0).
	for name, size := range map[string]string{"nbd0": "2048", "nbd1": "0"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "size"), []byte(size+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	dev, err := freeNBDFrom(filepath.Join(dir, "nbd*", "size"))
	if err != nil || dev != "/dev/nbd1" {
		t.Errorf("freeNBDFrom = (%q, %v), want /dev/nbd1", dev, err)
	}

	// No free device → an actionable error.
	if _, err := freeNBDFrom(filepath.Join(dir, "none*", "size")); err == nil {
		t.Error("freeNBDFrom with no matches should error")
	}

	// An unreadable "size" (here a directory) is skipped, not fatal.
	bad := t.TempDir()
	if err := os.MkdirAll(filepath.Join(bad, "nbd0", "size"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := freeNBDFrom(filepath.Join(bad, "nbd*", "size")); err == nil {
		t.Error("freeNBDFrom should skip an unreadable size and report no free device")
	}

	// A malformed glob pattern surfaces as an error.
	if _, err := freeNBDFrom("[bad-pattern"); err == nil {
		t.Error("freeNBDFrom with a malformed pattern should error")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
