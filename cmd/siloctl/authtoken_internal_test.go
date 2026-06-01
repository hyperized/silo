package main

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/captoken"
)

func TestAuthMintToken_HappyPath(t *testing.T) {
	certPath, keyPath, ca := caFixture(t)

	var out, errBuf bytes.Buffer
	code := runAuth([]string{
		"mint-token", "--ca-cert=" + certPath, "--ca-key=" + keyPath,
		"--principal=csi@cluster", "--cap=chunk:read", "--cap=chunk:write", "--ttl=2h",
	}, nil, &out, &errBuf)
	if code != 0 {
		t.Fatalf("mint-token code = %d, err = %s", code, errBuf.String())
	}

	token := strings.TrimSpace(out.String())
	pub, ok := ca.Cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		t.Fatal("CA public key is not Ed25519")
	}
	tok, err := captoken.Parse(token, pub)
	if err != nil {
		t.Fatalf("the minted token should verify against the CA: %v", err)
	}
	if tok.Principal != "csi@cluster" {
		t.Errorf("principal = %q", tok.Principal)
	}
	if !tok.Allows(captoken.CapChunkRead) || !tok.Allows(captoken.CapChunkWrite) {
		t.Error("token should grant the requested capabilities")
	}
	if err := tok.Validate(time.Now()); err != nil {
		t.Errorf("freshly minted token should be valid now: %v", err)
	}
	// The export hint goes to stderr so stdout is just the token (pipeable).
	if !strings.Contains(errBuf.String(), "SILO_TOKEN") {
		t.Error("expected an export hint on stderr")
	}
}

func TestAuthMintToken_UsageErrors(t *testing.T) {
	certPath, keyPath, _ := caFixture(t)
	cases := []struct {
		name string
		args []string
		want int
	}{
		{"no principal", []string{"mint-token", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--cap=chunk:read"}, 2},
		{"no caps", []string{"mint-token", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--principal=p"}, 2},
		{"bad flag", []string{"mint-token", "--nope"}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			if code := runAuth(tc.args, nil, &out, &errBuf); code != tc.want {
				t.Errorf("code = %d, want %d (err=%s)", code, tc.want, errBuf.String())
			}
		})
	}
}

func TestAuthMintToken_RuntimeErrors(t *testing.T) {
	certPath, keyPath, _ := caFixture(t)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"bad ca cert", []string{"mint-token", "--ca-cert=/no/such.crt", "--ca-key=" + keyPath, "--principal=p", "--cap=chunk:read"}, "could not read the CA certificate"},
		{"no ca key", []string{"mint-token", "--ca-cert=" + certPath, "--principal=p", "--cap=chunk:read"}, "CA private key is required"},
		{"empty capability", []string{"mint-token", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--principal=p", "--cap="}, "empty capability"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			if code := runAuth(tc.args, nil, &out, &errBuf); code != 1 {
				t.Errorf("code = %d, want 1", code)
			}
			if !strings.Contains(errBuf.String(), tc.want) {
				t.Errorf("err = %q, want substring %q", errBuf.String(), tc.want)
			}
		})
	}
}
