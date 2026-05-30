package main

import (
	"bytes"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clustertls"
)

// caFixture writes a fresh cluster CA (cert + key) to a temp dir and returns
// their paths plus the loaded CA for minting test certs.
func caFixture(t *testing.T) (certPath, keyPath string, ca *clustertls.CA) {
	t.Helper()
	dir := t.TempDir()
	certPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	certPath = filepath.Join(dir, "ca.crt")
	keyPath = filepath.Join(dir, "ca.key")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write ca cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write ca key: %v", err)
	}
	ca, err = clustertls.LoadCA(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	return certPath, keyPath, ca
}

func mintCertFile(t *testing.T, ca *clustertls.CA, id string) string {
	t.Helper()
	nc, err := clustertls.MintNodeCert(ca, id, nil, nil, time.Hour)
	if err != nil {
		t.Fatalf("MintNodeCert: %v", err)
	}
	path := filepath.Join(t.TempDir(), id+".crt")
	if err := os.WriteFile(path, nc.CertPEM, 0o600); err != nil {
		t.Fatalf("write node cert: %v", err)
	}
	return path
}

func TestCA_UsageAndUnknown(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runCA(nil, &out, &errBuf); code != 2 {
		t.Errorf("no args code = %d, want 2", code)
	}
	if !strings.Contains(out.String(), "siloctl ca") {
		t.Error("usage should print on no args")
	}
	out.Reset()
	if code := runCA([]string{"help"}, &out, &errBuf); code != 0 {
		t.Errorf("help code = %d, want 0", code)
	}
	errBuf.Reset()
	if code := runCA([]string{"bogus"}, &out, &errBuf); code != 2 || !strings.Contains(errBuf.String(), "unknown subcommand") {
		t.Errorf("unknown subcommand code = %d", code)
	}
}

func TestCARevoke_BySerialThenListRevoked(t *testing.T) {
	certPath, keyPath, ca := caFixture(t)
	crlPath := filepath.Join(t.TempDir(), "crl.pem")

	var out, errBuf bytes.Buffer
	code := runCA([]string{
		"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath,
		"--crl=" + crlPath, "--serial=2A",
	}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("revoke code = %d, err = %s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "1 revoked certificate") {
		t.Errorf("unexpected output: %s", out.String())
	}

	// The written CRL verifies against the CA and lists serial 0x2A = 42.
	crlPEM, err := os.ReadFile(crlPath)
	if err != nil {
		t.Fatalf("read crl: %v", err)
	}
	crl, err := clustertls.LoadCRL(crlPEM, ca)
	if err != nil {
		t.Fatalf("LoadCRL: %v", err)
	}
	if !crl.IsRevoked(big.NewInt(42)) {
		t.Error("serial 0x2A should be revoked")
	}

	// list-revoked prints it.
	out.Reset()
	if code := runCA([]string{"list-revoked", "--ca-cert=" + certPath, "--crl=" + crlPath}, &out, &errBuf); code != 0 {
		t.Fatalf("list-revoked code = %d, err = %s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "42") || !strings.Contains(out.String(), "1 revoked") {
		t.Errorf("list-revoked output: %s", out.String())
	}
}

func TestCARevoke_ByCertFile(t *testing.T) {
	certPath, keyPath, ca := caFixture(t)
	target := mintCertFile(t, ca, "doomed")
	crlPath := filepath.Join(t.TempDir(), "crl.pem")

	var out, errBuf bytes.Buffer
	code := runCA([]string{
		"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath,
		"--crl=" + crlPath, "--cert=" + target,
	}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("revoke code = %d, err = %s", code, errBuf.String())
	}

	wantSerial, err := clustertls.SerialOf(readFile(t, target))
	if err != nil {
		t.Fatalf("SerialOf: %v", err)
	}
	crl, err := clustertls.LoadCRL(readFile(t, crlPath), ca)
	if err != nil {
		t.Fatalf("LoadCRL: %v", err)
	}
	if !crl.IsRevoked(wantSerial) {
		t.Error("the certificate's serial should be revoked")
	}
}

func TestCARevoke_ExtendsExistingCRL(t *testing.T) {
	certPath, keyPath, ca := caFixture(t)
	crlPath := filepath.Join(t.TempDir(), "crl.pem")
	var out, errBuf bytes.Buffer

	if code := runCA([]string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + crlPath, "--serial=01"}, &out, &errBuf); code != 0 {
		t.Fatalf("first revoke: %s", errBuf.String())
	}
	if code := runCA([]string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + crlPath, "--serial=02"}, &out, &errBuf); code != 0 {
		t.Fatalf("second revoke: %s", errBuf.String())
	}

	crl, err := clustertls.LoadCRL(readFile(t, crlPath), ca)
	if err != nil {
		t.Fatalf("LoadCRL: %v", err)
	}
	if !crl.IsRevoked(big.NewInt(1)) || !crl.IsRevoked(big.NewInt(2)) {
		t.Error("both serials should survive the second revoke")
	}
	if crl.Number.Cmp(big.NewInt(2)) != 0 {
		t.Errorf("sequence number = %s, want 2 after re-issue", crl.Number)
	}
}

func TestCARevoke_UsageErrors(t *testing.T) {
	certPath, keyPath, _ := caFixture(t)
	cases := []struct {
		name string
		args []string
		want int
	}{
		{"no crl flag", []string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--serial=01"}, 2},
		{"nothing to revoke", []string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=/tmp/x.pem"}, 2},
		{"bad flag", []string{"revoke", "--nope"}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			if code := runCA(tc.args, &out, &errBuf); code != tc.want {
				t.Errorf("code = %d, want %d (err=%s)", code, tc.want, errBuf.String())
			}
		})
	}
}

func TestCARevoke_RuntimeErrors(t *testing.T) {
	certPath, keyPath, ca := caFixture(t)
	target := mintCertFile(t, ca, "n")
	otherCertPath, otherKeyPath, _ := caFixture(t)

	dir := t.TempDir()
	goodCRL := filepath.Join(dir, "good.pem")
	if code := runCA([]string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + goodCRL, "--serial=01"}, &bytes.Buffer{}, &bytes.Buffer{}); code != 0 {
		t.Fatalf("seed CRL failed")
	}

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"missing ca cert", []string{"revoke", "--ca-cert=/no/such.crt", "--ca-key=" + keyPath, "--crl=" + filepath.Join(dir, "a.pem"), "--serial=01"}, "could not read the CA certificate"},
		{"ca cert not a CA", []string{"revoke", "--ca-cert=" + keyPath, "--ca-key=" + keyPath, "--crl=" + filepath.Join(dir, "g.pem"), "--serial=01"}, "CA certificate"},
		{"unreadable ca key", []string{"revoke", "--ca-cert=" + certPath, "--ca-key=/no/such.key", "--crl=" + filepath.Join(dir, "k.pem"), "--serial=01"}, "could not read the CA key"},
		{"no ca key", []string{"revoke", "--ca-cert=" + certPath, "--crl=" + filepath.Join(dir, "b.pem"), "--serial=01"}, "CA private key"},
		{"bad serial", []string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + filepath.Join(dir, "c.pem"), "--serial=zz"}, "not valid hexadecimal"},
		{"unreadable cert", []string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + filepath.Join(dir, "d.pem"), "--cert=/no/such.crt"}, "could not read the certificate"},
		{"cert not a cert", []string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + filepath.Join(dir, "f.pem"), "--cert=" + keyPath}, "could not read the certificate to revoke"},
		{"existing crl wrong CA", []string{"revoke", "--ca-cert=" + otherCertPath, "--ca-key=" + otherKeyPath, "--crl=" + goodCRL, "--serial=02"}, "did not verify"},
		{"existing crl is a dir", []string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + dir, "--serial=01"}, "could not read the existing CRL"},
		{"unwritable output", []string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + filepath.Join(dir, "nope", "e.pem"), "--cert=" + target}, "could not write the CRL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			if code := runCA(tc.args, &out, &errBuf); code != 1 {
				t.Errorf("code = %d, want 1", code)
			}
			if !strings.Contains(errBuf.String(), tc.want) {
				t.Errorf("err = %q, want substring %q", errBuf.String(), tc.want)
			}
		})
	}
}

func TestCAListRevoked_Errors(t *testing.T) {
	certPath, keyPath, _ := caFixture(t)
	dir := t.TempDir()
	goodCRL := filepath.Join(dir, "good.pem")
	if code := runCA([]string{"revoke", "--ca-cert=" + certPath, "--ca-key=" + keyPath, "--crl=" + goodCRL, "--serial=01"}, &bytes.Buffer{}, &bytes.Buffer{}); code != 0 {
		t.Fatalf("seed CRL failed")
	}

	cases := []struct {
		name string
		args []string
		want int
	}{
		{"no crl flag", []string{"list-revoked", "--ca-cert=" + certPath}, 2},
		{"bad flag", []string{"list-revoked", "--nope"}, 2},
		{"missing ca", []string{"list-revoked", "--ca-cert=/no/such.crt", "--crl=" + goodCRL}, 1},
		{"missing crl", []string{"list-revoked", "--ca-cert=" + certPath, "--crl=/no/such.pem"}, 1},
		{"garbage crl", []string{"list-revoked", "--ca-cert=" + certPath, "--crl=" + certPath}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			if code := runCA(tc.args, &out, &errBuf); code != tc.want {
				t.Errorf("code = %d, want %d (err=%s)", code, tc.want, errBuf.String())
			}
		})
	}
}

func TestLoadCAMaterial_EmptyCert(t *testing.T) {
	if _, err := loadCAMaterial("", ""); err == nil || !strings.Contains(err.Error(), "no CA certificate") {
		t.Errorf("empty cert path should error, got %v", err)
	}
}

func TestParseSerial(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"2A", 42, true},
		{"0x2a", 42, true},
		{"0X2A", 42, true},
		{" 1A:2B ", 0x1A2B, true},
		{"", 0, false},
		{"zz", 0, false},
	}
	for _, tc := range cases {
		got, err := parseSerial(tc.in)
		if tc.ok {
			if err != nil {
				t.Errorf("parseSerial(%q): %v", tc.in, err)
				continue
			}
			if got.Cmp(big.NewInt(tc.want)) != 0 {
				t.Errorf("parseSerial(%q) = %s, want %d", tc.in, got, tc.want)
			}
		} else if err == nil {
			t.Errorf("parseSerial(%q) should have failed", tc.in)
		}
	}
}

func TestMergeSerials(t *testing.T) {
	got := mergeSerials([]*big.Int{big.NewInt(3), big.NewInt(1)}, []*big.Int{big.NewInt(1), big.NewInt(2)})
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3 (deduped)", len(got))
	}
	if got[0].Cmp(big.NewInt(1)) != 0 || got[1].Cmp(big.NewInt(2)) != 0 || got[2].Cmp(big.NewInt(3)) != 0 {
		t.Errorf("merge = %v, want [1 2 3] sorted", got)
	}
}

func TestStringSliceString(t *testing.T) {
	var s stringSlice
	_ = s.Set("a")
	_ = s.Set("b")
	if s.String() != "a,b" {
		t.Errorf("String() = %q, want a,b", s.String())
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}
