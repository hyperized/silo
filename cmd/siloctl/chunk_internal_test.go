package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clustertls"
)

func TestFileReadable(t *testing.T) {
	if fileReadable("") {
		t.Error("empty path should be unreadable")
	}
	tmp := t.TempDir()
	if fileReadable(filepath.Join(tmp, "missing")) {
		t.Error("missing file should be unreadable")
	}
	present := filepath.Join(tmp, "present")
	if err := os.WriteFile(present, []byte{0}, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !fileReadable(present) {
		t.Error("existing file should be readable")
	}
}

func TestLoadClientTLS_NoCreds(t *testing.T) {
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return t.TempDir(), nil }
	// Ensure env-var overrides don't leak in from the host shell.
	t.Setenv("SILO_CA_CERT", "")
	t.Setenv("SILO_CLIENT_CERT", "")
	t.Setenv("SILO_CLIENT_KEY", "")

	cfg, err := loadClientTLS()
	if err != nil {
		t.Fatalf("loadClientTLS: %v", err)
	}
	if cfg != nil {
		t.Errorf("expected nil tls.Config when no creds on disk, got %+v", cfg)
	}
}

func TestLoadClientTLS_ConfigDirFailure(t *testing.T) {
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return "", errors.New("simulated lookup failure") }
	if _, err := loadClientTLS(); err == nil || !strings.Contains(err.Error(), "simulated") {
		t.Errorf("got %v, want resolution failure", err)
	}
}

func TestLoadClientTLS_HappyPath(t *testing.T) {
	dir := t.TempDir()
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return dir, nil }
	t.Setenv("SILO_CA_CERT", "")
	t.Setenv("SILO_CLIENT_CERT", "")
	t.Setenv("SILO_CLIENT_KEY", "")

	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := clustertls.LoadCA(caPEM, keyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	client, err := clustertls.MintClientCert(ca, "alice@host", time.Hour)
	if err != nil {
		t.Fatalf("MintClientCert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.crt"), client.CertPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.key"), client.KeyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	cfg, err := loadClientTLS()
	if err != nil {
		t.Fatalf("loadClientTLS: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil tls.Config when creds are on disk")
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("Certificates: got %d, want 1", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs should be populated")
	}
}

func TestLoadClientTLS_BadCAPEM(t *testing.T) {
	dir := t.TempDir()
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return dir, nil }
	t.Setenv("SILO_CA_CERT", "")
	t.Setenv("SILO_CLIENT_CERT", "")
	t.Setenv("SILO_CLIENT_KEY", "")

	caPEM, keyPEM, _ := clustertls.GenerateCA("silo-test", time.Hour)
	ca, _ := clustertls.LoadCA(caPEM, keyPEM)
	client, _ := clustertls.MintClientCert(ca, "alice@host", time.Hour)

	// Garbage CA but valid client material — exercises the
	// "AppendCertsFromPEM returned false" branch.
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), []byte("not pem"), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.crt"), client.CertPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.key"), client.KeyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	if _, err := loadClientTLS(); err == nil || !strings.Contains(err.Error(), "not valid PEM") {
		t.Errorf("got %v, want invalid-PEM error", err)
	}
}

func TestLoadClientTLS_BadClientPair(t *testing.T) {
	dir := t.TempDir()
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return dir, nil }
	t.Setenv("SILO_CA_CERT", "")
	t.Setenv("SILO_CLIENT_CERT", "")
	t.Setenv("SILO_CLIENT_KEY", "")

	caPEM, _, _ := clustertls.GenerateCA("silo-test", time.Hour)
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.crt"), []byte("not pem"), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.key"), []byte("not pem"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	if _, err := loadClientTLS(); err == nil || !strings.Contains(err.Error(), "client cert+key") {
		t.Errorf("got %v, want client-pair error", err)
	}
}

func TestLoadClientTLS_UnreadableCAFileSurfacesError(t *testing.T) {
	// Place a directory at the CA path so ReadFile fails even though
	// fileReadable says it exists.
	dir := t.TempDir()
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return dir, nil }
	t.Setenv("SILO_CA_CERT", "")
	t.Setenv("SILO_CLIENT_CERT", "")
	t.Setenv("SILO_CLIENT_KEY", "")

	if err := os.MkdirAll(filepath.Join(dir, "ca.crt"), 0o700); err != nil {
		t.Fatalf("seed dir as ca.crt: %v", err)
	}
	// client.crt + client.key need to exist (regular files) so the
	// fileReadable gate lets us through to the ReadFile failure.
	if err := os.WriteFile(filepath.Join(dir, "client.crt"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.key"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	if _, err := loadClientTLS(); err == nil || !strings.Contains(err.Error(), "CA cert") {
		t.Errorf("got %v, want CA-read error", err)
	}
}

func TestDefaultDialer_NoCredsUsesInsecure(t *testing.T) {
	// With no on-disk creds, defaultDialer should hand back a usable
	// conn (insecure). We can't easily prove "insecure" from a unit
	// test, but a successful NewClient call against a non-routable
	// target proves the code path runs without erroring.
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return t.TempDir(), nil }
	t.Setenv("SILO_CA_CERT", "")
	t.Setenv("SILO_CLIENT_CERT", "")
	t.Setenv("SILO_CLIENT_KEY", "")

	conn, err := defaultDialer("passthrough:///fake")
	if err != nil {
		t.Fatalf("defaultDialer: %v", err)
	}
	_ = conn.Close()
}

func TestDefaultDialer_LoadClientTLSError(t *testing.T) {
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return "", errors.New("simulated") }
	if _, err := defaultDialer("x"); err == nil {
		t.Error("defaultDialer should surface loadClientTLS errors")
	}
}

func TestDefaultDialer_WithCredsUsesTLS(t *testing.T) {
	// Plant credentials, then dial — the production path through
	// credentials.NewTLS runs. We can't easily complete a handshake
	// without a server, but we can prove the dial is attempted.
	dir := t.TempDir()
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return dir, nil }
	t.Setenv("SILO_CA_CERT", "")
	t.Setenv("SILO_CLIENT_CERT", "")
	t.Setenv("SILO_CLIENT_KEY", "")

	caPEM, keyPEM, _ := clustertls.GenerateCA("silo-test", time.Hour)
	ca, _ := clustertls.LoadCA(caPEM, keyPEM)
	client, _ := clustertls.MintClientCert(ca, "alice@host", time.Hour)
	for name, data := range map[string][]byte{
		"ca.crt":     caPEM,
		"client.crt": client.CertPEM,
		"client.key": client.KeyPEM,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	conn, err := defaultDialer("passthrough:///fake")
	if err != nil {
		t.Fatalf("defaultDialer with creds: %v", err)
	}
	_ = conn.Close()
}

func TestSiloctlConfigDir_DelegatesToUserConfigDir(t *testing.T) {
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	want := t.TempDir()
	userConfigDir = func() (string, error) { return want, nil }
	got, err := siloctlConfigDir()
	if err != nil {
		t.Fatalf("siloctlConfigDir: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
