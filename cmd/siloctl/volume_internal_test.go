package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"
)

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		bad  bool
	}{
		{"4096", 4096, false},
		{"1K", 1024, false},
		{"512m", 512 << 20, false},
		{"10G", 10 << 30, false},
		{"2T", 2 << 40, false},
		{"", 0, true},
		{"-5", 0, true},
		{"-1G", 0, true},
		{"abc", 0, true},
		{"99999999999999T", 0, true}, // overflows int64
	}
	for _, tc := range cases {
		got, err := parseSize(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("parseSize(%q) = %d, want error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("parseSize(%q) = (%d,%v), want (%d,nil)", tc.in, got, err, tc.want)
		}
	}
}

func TestVolume_UsageAndUnknown(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runVolume(nil, &out, &errBuf); code != 2 {
		t.Errorf("no args code = %d, want 2", code)
	}
	if !strings.Contains(out.String(), "siloctl volume") {
		t.Error("usage should be printed on no args")
	}
	out.Reset()
	if code := runVolume([]string{"help"}, &out, &errBuf); code != 0 {
		t.Errorf("help code = %d, want 0", code)
	}
	errBuf.Reset()
	if code := runVolume([]string{"frobnicate"}, &out, &errBuf); code != 2 || !strings.Contains(errBuf.String(), "unknown subcommand") {
		t.Errorf("unknown subcommand code = %d", code)
	}
}

func TestVolumeCreate_UsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no path", []string{"create", "--size=1G"}, ""},
		{"bad flag", []string{"create", "--bogus"}, ""},
		{"missing size", []string{"create", "/vol"}, "--size is required"},
		{"bad size", []string{"create", "--size=nope", "/vol"}, "invalid --size"},
		{"bad extent size", []string{"create", "--size=1G", "--extent-size=nope", "/vol"}, "invalid --extent-size"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			if code := runVolume(tc.args, &out, &errBuf); code != 2 {
				t.Errorf("code = %d, want 2", code)
			}
			if tc.want != "" && !strings.Contains(errBuf.String(), tc.want) {
				t.Errorf("stderr = %q, want containing %q", errBuf.String(), tc.want)
			}
		})
	}
}

func TestVolumeCreate_DialError(t *testing.T) {
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) { return nil, errors.New("dial boom") }

	var out, errBuf bytes.Buffer
	if code := runVolume([]string{"create", "--server=x", "--size=1G", "/vol"}, &out, &errBuf); code != 1 {
		t.Errorf("dial-error code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "could not dial") {
		t.Errorf("stderr = %q, want a dial failure", errBuf.String())
	}
}

func TestVolumeCreate_Succeeds(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	var out, errBuf bytes.Buffer
	code := runVolume([]string{"create", "--server=" + addr, "--size=1M", "--extent-size=64K", "/vol"}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("create code = %d, err = %s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "Created volume /vol") {
		t.Errorf("stdout = %q, want a creation confirmation", out.String())
	}

	// The volume now lists as a volume-typed entry.
	lsOut, _, lsCode := runNSArgs(t, addr, "ls", "/")
	if lsCode != 0 || !strings.Contains(lsOut, "vol") {
		t.Errorf("ls / = %q (code %d), want the new volume", lsOut, lsCode)
	}

	// Creating it again fails (already exists).
	out.Reset()
	errBuf.Reset()
	if code := runVolume([]string{"create", "--server=" + addr, "--size=1M", "/vol"}, &out, &errBuf); code == 0 {
		t.Error("duplicate volume create should fail")
	}

	// A snapshot of the volume succeeds and lists as a new volume entry.
	out.Reset()
	errBuf.Reset()
	if code := runVolume([]string{"snapshot", "--server=" + addr, "/vol", "/snap"}, &out, &errBuf); code != 0 {
		t.Fatalf("snapshot code = %d, err = %s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "Snapshotted /vol to /snap") {
		t.Errorf("stdout = %q, want a snapshot confirmation", out.String())
	}
	lsOut, _, lsCode = runNSArgs(t, addr, "ls", "/")
	if lsCode != 0 || !strings.Contains(lsOut, "snap") {
		t.Errorf("ls / = %q (code %d), want the new snapshot", lsOut, lsCode)
	}

	// Snapshotting a missing source fails.
	out.Reset()
	errBuf.Reset()
	if code := runVolume([]string{"snapshot", "--server=" + addr, "/missing", "/snap2"}, &out, &errBuf); code == 0 {
		t.Error("snapshot of a missing source should fail")
	}
}

func TestVolumeSnapshot_UsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"no paths", []string{"snapshot"}},
		{"one path", []string{"snapshot", "/vol"}},
		{"three paths", []string{"snapshot", "/a", "/b", "/c"}},
		{"bad flag", []string{"snapshot", "--bogus"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			if code := runVolume(tc.args, &out, &errBuf); code != 2 {
				t.Errorf("code = %d, want 2", code)
			}
		})
	}
}

func TestVolumeSnapshot_DialError(t *testing.T) {
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) { return nil, errors.New("dial boom") }

	var out, errBuf bytes.Buffer
	if code := runVolume([]string{"snapshot", "--server=x", "/vol", "/snap"}, &out, &errBuf); code != 1 {
		t.Errorf("dial-error code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "could not dial") {
		t.Errorf("stderr = %q, want a dial failure", errBuf.String())
	}
}
