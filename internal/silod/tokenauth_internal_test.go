package silod

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clustertls"
	"github.com/hyperized/silo/internal/config"
)

func TestNewTokenAuthenticator_Disabled(t *testing.T) {
	auth, err := newTokenAuthenticator(&config.Config{RequireTokens: false}, testCA(t), discardLogger())
	if err != nil {
		t.Fatalf("newTokenAuthenticator: %v", err)
	}
	if auth != nil {
		t.Error("token enforcement off should yield a nil authenticator")
	}
}

func TestNewTokenAuthenticator_Enabled(t *testing.T) {
	auth, err := newTokenAuthenticator(&config.Config{RequireTokens: true}, testCA(t), discardLogger())
	if err != nil {
		t.Fatalf("newTokenAuthenticator: %v", err)
	}
	if auth == nil {
		t.Error("token enforcement on should yield an authenticator")
	}
}

func TestNewTokenAuthenticator_NoCA(t *testing.T) {
	if _, err := newTokenAuthenticator(&config.Config{RequireTokens: true}, nil, discardLogger()); err == nil {
		t.Error("enforcement without a CA should error")
	}
}

func TestNewTokenAuthenticator_NonEd25519CA(t *testing.T) {
	ca := &clustertls.CA{Cert: ecdsaCert(t)}
	if _, err := newTokenAuthenticator(&config.Config{RequireTokens: true}, ca, discardLogger()); err == nil {
		t.Error("a non-Ed25519 CA should be rejected for token verification")
	}
}

// ecdsaCert builds a certificate whose public key is ECDSA, not Ed25519, to
// exercise the type-assertion failure path.
func ecdsaCert(t *testing.T) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa key: %v", err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ecdsa"}, NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	return cert
}
