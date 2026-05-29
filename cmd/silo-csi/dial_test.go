package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDialSilod_Insecure(t *testing.T) {
	// With no cert env, dialSilod returns a lazy insecure client.
	conn, err := dialSilod("127.0.0.1:7000")
	if err != nil {
		t.Fatalf("dialSilod insecure: %v", err)
	}
	_ = conn.Close()
}

func TestDialSilod_MTLS(t *testing.T) {
	ca, cert, key := writeCertMaterial(t)
	t.Setenv("SILO_CA_CERT", ca)
	t.Setenv("SILO_CLIENT_CERT", cert)
	t.Setenv("SILO_CLIENT_KEY", key)

	conn, err := dialSilod("127.0.0.1:7000")
	if err != nil {
		t.Fatalf("dialSilod mTLS: %v", err)
	}
	_ = conn.Close()
}

func TestDialSilod_TLSError(t *testing.T) {
	// A partial TLS config makes dialSilod fail before creating a client.
	t.Setenv("SILO_CA_CERT", "/only-ca")
	if _, err := dialSilod("127.0.0.1:7000"); err == nil {
		t.Error("dialSilod should fail on a partial TLS config")
	}
}

func TestClientTLSFromEnv(t *testing.T) {
	// Nothing set -> insecure signal.
	if cfg, err := clientTLSFromEnv(); err != nil || cfg != nil {
		t.Errorf("no env = (%v, %v), want (nil, nil)", cfg, err)
	}

	// Partial config is an error.
	t.Setenv("SILO_CA_CERT", "/x")
	if _, err := clientTLSFromEnv(); err == nil {
		t.Error("partial TLS config should error")
	}

	// Full config but unreadable CA.
	ca, cert, key := writeCertMaterial(t)
	t.Setenv("SILO_CA_CERT", filepath.Join(t.TempDir(), "missing-ca.crt"))
	t.Setenv("SILO_CLIENT_CERT", cert)
	t.Setenv("SILO_CLIENT_KEY", key)
	if _, err := clientTLSFromEnv(); err == nil {
		t.Error("an unreadable CA should error")
	}

	// Full config but a CA that is not valid PEM.
	badCA := filepath.Join(t.TempDir(), "bad-ca.crt")
	if err := os.WriteFile(badCA, []byte("not pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SILO_CA_CERT", badCA)
	if _, err := clientTLSFromEnv(); err == nil {
		t.Error("an invalid-PEM CA should error")
	}

	// Bad client key path.
	t.Setenv("SILO_CA_CERT", ca)
	t.Setenv("SILO_CLIENT_KEY", filepath.Join(t.TempDir(), "missing.key"))
	if _, err := clientTLSFromEnv(); err == nil {
		t.Error("an unreadable client key should error")
	}
}

// writeCertMaterial writes a self-signed cert (used as both CA and client cert)
// and its key to temp files and returns their paths.
func writeCertMaterial(t *testing.T) (caPath, certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "silo-csi-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	caPath = filepath.Join(dir, "ca.crt")
	certPath = filepath.Join(dir, "client.crt")
	keyPath = filepath.Join(dir, "client.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	for path, data := range map[string][]byte{caPath: certPEM, certPath: certPEM, keyPath: keyPEM} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return caPath, certPath, keyPath
}
