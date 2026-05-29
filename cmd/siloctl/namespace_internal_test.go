package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"
)

func TestNS_DialErrors(t *testing.T) {
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) { return nil, errors.New("dial boom") }

	for _, args := range [][]string{{"mkdir", "/x"}, {"ls", "/"}} {
		var out, errBuf bytes.Buffer
		full := append([]string{args[0], "--server=x"}, args[1:]...)
		if code := runNS(full, &out, &errBuf); code != 1 {
			t.Errorf("%v dial-error code = %d, want 1", args, code)
		}
		if !strings.Contains(errBuf.String(), "could not dial") {
			t.Errorf("%v should report a dial failure, got %q", args, errBuf.String())
		}
	}
}

func TestNS_BadFlag(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runNS([]string{"mkdir", "--bogus"}, &out, &errBuf); code != 2 {
		t.Errorf("bad flag (mkdir) code = %d, want 2", code)
	}
	out.Reset()
	errBuf.Reset()
	if code := runNS([]string{"ls", "--bogus"}, &out, &errBuf); code != 2 {
		t.Errorf("bad flag (ls) code = %d, want 2", code)
	}
}

func runNSArgs(t *testing.T, addr string, args ...string) (string, string, int) {
	t.Helper()
	// Subcommand first, then the --server flag, then its operands.
	full := append([]string{args[0], "--server=" + addr}, args[1:]...)
	var out, errBuf bytes.Buffer
	code := runNS(full, &out, &errBuf)
	return out.String(), errBuf.String(), code
}

func TestNS_MkdirTouchListRemove(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	if out, errStr, code := runNSArgs(t, addr, "mkdir", "/docs"); code != 0 {
		t.Fatalf("mkdir: code=%d err=%s out=%s", code, errStr, out)
	}
	if _, errStr, code := runNSArgs(t, addr, "touch", "/docs/readme"); code != 0 {
		t.Fatalf("touch: code=%d err=%s", code, errStr)
	}

	// ls / shows docs as a directory (trailing slash).
	out, _, code := runNSArgs(t, addr, "ls", "/")
	if code != 0 {
		t.Fatalf("ls /: code=%d", code)
	}
	if strings.TrimSpace(out) != "docs/" {
		t.Errorf("ls / = %q, want docs/", out)
	}

	// ls of the nested dir shows the file (no trailing slash).
	out, _, code = runNSArgs(t, addr, "ls", "/docs")
	if code != 0 || strings.TrimSpace(out) != "readme" {
		t.Errorf("ls /docs = %q (code %d), want readme", out, code)
	}

	// rm then ls is empty.
	if _, errStr, code := runNSArgs(t, addr, "rm", "/docs/readme"); code != 0 {
		t.Fatalf("rm: code=%d err=%s", code, errStr)
	}
	out, _, _ = runNSArgs(t, addr, "ls", "/docs")
	if strings.TrimSpace(out) != "" {
		t.Errorf("ls after rm = %q, want empty", out)
	}
}

func TestNS_DefaultLsIsRoot(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()
	runNSArgs(t, addr, "mkdir", "/top")

	// ls with no path defaults to "/".
	out, _, code := runNSArgs(t, addr, "ls")
	if code != 0 || strings.TrimSpace(out) != "top/" {
		t.Errorf("ls (default) = %q (code %d), want top/", out, code)
	}
}

func TestNS_ErrorsSurface(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	// mkdir under a missing parent fails with a non-zero code.
	if _, errStr, code := runNSArgs(t, addr, "mkdir", "/nope/child"); code == 0 {
		t.Errorf("mkdir under missing parent should fail; err=%s", errStr)
	}
	// Duplicate create fails.
	runNSArgs(t, addr, "touch", "/dup")
	if _, _, code := runNSArgs(t, addr, "touch", "/dup"); code == 0 {
		t.Error("duplicate create should fail")
	}
	// ls of a missing directory surfaces the RPC error as a non-zero code.
	if _, errStr, code := runNSArgs(t, addr, "ls", "/missing"); code == 0 {
		t.Errorf("ls of a missing path should fail; err=%s", errStr)
	}
}

func TestNS_UsageAndUnknown(t *testing.T) {
	var out, errBuf bytes.Buffer

	// No args prints usage and returns 2.
	if code := runNS(nil, &out, &errBuf); code != 2 {
		t.Errorf("no args code = %d, want 2", code)
	}
	if !strings.Contains(out.String(), "siloctl ns") {
		t.Error("usage should be printed on no args")
	}

	// Explicit help returns 0.
	out.Reset()
	if code := runNS([]string{"help"}, &out, &errBuf); code != 0 {
		t.Errorf("help code = %d, want 0", code)
	}

	// Unknown subcommand returns 2.
	errBuf.Reset()
	if code := runNS([]string{"frobnicate"}, &out, &errBuf); code != 2 {
		t.Errorf("unknown subcommand code = %d, want 2", code)
	}
	if !strings.Contains(errBuf.String(), "unknown subcommand") {
		t.Error("unknown subcommand should be reported")
	}
}

func TestNS_BadUsagePerSubcommand(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	// mkdir with no path is a usage error (2).
	if _, _, code := runNSArgs(t, addr, "mkdir"); code != 2 {
		t.Errorf("mkdir with no path code = %d, want 2", code)
	}
	// ls with too many paths is a usage error (2).
	if _, _, code := runNSArgs(t, addr, "ls", "/a", "/b"); code != 2 {
		t.Errorf("ls with two paths code = %d, want 2", code)
	}
}
