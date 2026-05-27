package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestRunMain_ConfigErrorIsActionable verifies the daemon refuses to start
// without SILO_ENCRYPTION_KEY and prints the operator the exact fix.
func TestRunMain_ConfigErrorIsActionable(t *testing.T) {
	// Clear required fields so config.LoadFromEnv fails.
	t.Setenv("SILO_NODE_ID", "test-runmain")
	t.Setenv("SILO_ENCRYPTION_KEY", "")
	t.Setenv("SILO_ENCRYPTION_KEY_SOURCE", "static")

	var stdout, stderr bytes.Buffer
	code := runMain(&stdout, &stderr)

	if code != 1 {
		t.Errorf("exit code: got %d, want 1", code)
	}
	msg := stderr.String()
	if !strings.Contains(msg, "invalid configuration") {
		t.Errorf("stderr should explain it is a configuration problem; got: %q", msg)
	}
	if !strings.Contains(msg, "SILO_ENCRYPTION_KEY") {
		t.Errorf("stderr should name the offending variable; got: %q", msg)
	}
	if !strings.Contains(msg, "openssl rand -base64 32") {
		t.Errorf("stderr should tell the operator how to generate the key; got: %q", msg)
	}
}

// TestRunMain_ValidationErrorIsActionable hits a config that parses but
// fails validation; the operator should still get an actionable message.
func TestRunMain_ValidationErrorIsActionable(t *testing.T) {
	t.Setenv("SILO_NODE_ID", "test-logger-err")
	t.Setenv("SILO_ENCRYPTION_KEY", "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=") // 32 raw bytes b64
	t.Setenv("SILO_LOG_FORMAT", "yaml")

	var stdout, stderr bytes.Buffer
	code := runMain(&stdout, &stderr)

	if code != 1 {
		t.Errorf("exit code: got %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "SILO_LOG_FORMAT") {
		t.Errorf("stderr should mention SILO_LOG_FORMAT; got: %q", stderr.String())
	}
}

// TestSignalContext_Default invokes the package-level signalContext
// closure once so its initializer is recorded as covered. The closure
// is normally overridden in every other test; without this call, its
// body shows as 0% coverage.
func TestSignalContext_Default(t *testing.T) {
	ctx, cancel := signalContext()
	defer cancel()
	if ctx == nil {
		t.Fatal("signalContext returned a nil context")
	}
}

// TestRunMain_SilodRunFailureReturnsOne covers the late failure path:
// configuration parses cleanly, the logger initialises, but silod.Run
// rejects the work (here: CA file is unreadable garbage). runMain
// should surface that as exit code 1.
func TestRunMain_SilodRunFailureReturnsOne(t *testing.T) {
	dir := t.TempDir()
	badCA := filepath.Join(dir, "garbage-ca.crt")
	if err := os.WriteFile(badCA, []byte("not a PEM cert"), 0o600); err != nil {
		t.Fatalf("seed garbage CA: %v", err)
	}

	t.Setenv("SILO_NODE_ID", "run-fail")
	t.Setenv("SILO_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("SILO_DATA_DIR", filepath.Join(dir, "data"))
	t.Setenv("SILO_GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("SILO_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("SILO_TLS_CA_CERT", badCA)
	t.Setenv("SILO_TLS_CA_KEY", "")

	prev := signalContext
	t.Cleanup(func() { signalContext = prev })
	signalContext = func() (context.Context, context.CancelFunc) {
		return context.WithCancel(context.Background())
	}

	var stdout, stderr bytes.Buffer
	code := runMain(&stdout, &stderr)
	if code != 1 {
		t.Errorf("got code %d, want 1 for silod.Run failure (stderr=%q)", code, stderr.String())
	}
}

// TestRunMain_HappyPathReturnsZero exercises the success branch end-to-end.
// We swap signalContext for one that's already cancelled so silod's Run
// shuts down immediately after subsystems start. Without the swap this
// path is unreachable from a unit test because production silod blocks
// on SIGINT/SIGTERM.
func TestRunMain_HappyPathReturnsZero(t *testing.T) {
	dir := t.TempDir()
	caCertPath, caKeyPath := writeDevCA(t, dir)

	t.Setenv("SILO_NODE_ID", "happy-path")
	t.Setenv("SILO_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("SILO_DATA_DIR", filepath.Join(dir, "data"))
	t.Setenv("SILO_GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("SILO_HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("SILO_TLS_CA_CERT", caCertPath)
	t.Setenv("SILO_TLS_CA_KEY", caKeyPath)

	prev := signalContext
	t.Cleanup(func() { signalContext = prev })
	signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // signal Run to exit immediately on first ctx.Done() check
		return ctx, cancel
	}

	var stdout, stderr bytes.Buffer
	code := runMain(&stdout, &stderr)
	if code != 0 {
		t.Errorf("runMain happy path: got code %d, want 0; stderr=%q", code, stderr.String())
	}
}

// writeDevCA generates an Ed25519 CA on the fly and persists it to dir.
// Mirrors what `make tls-bootstrap` will do at the operator level.
func writeDevCA(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "silo-dev"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	certPath = filepath.Join(dir, "ca.crt")
	keyPath = filepath.Join(dir, "ca.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}
