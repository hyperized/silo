package silod

import (
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/clustertls"
	"github.com/hyperized/silo/internal/config"
)

func testCA(t *testing.T) *clustertls.CA {
	t.Helper()
	certPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := clustertls.LoadCA(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	return ca
}

func TestLoadRevocation_Disabled(t *testing.T) {
	crl, err := loadRevocation(&config.Config{}, testCA(t), discardLogger())
	if err != nil {
		t.Fatalf("loadRevocation: %v", err)
	}
	if crl != nil {
		t.Error("an empty CRLPath should disable revocation (nil list)")
	}
}

func TestLoadRevocation_LoadsAndCounts(t *testing.T) {
	ca := testCA(t)
	crlPEM, err := clustertls.GenerateCRL(ca, []*big.Int{big.NewInt(42)}, big.NewInt(1), time.Hour)
	if err != nil {
		t.Fatalf("GenerateCRL: %v", err)
	}
	path := filepath.Join(t.TempDir(), "crl.pem")
	if err := os.WriteFile(path, crlPEM, 0o600); err != nil {
		t.Fatalf("write CRL: %v", err)
	}

	crl, err := loadRevocation(&config.Config{CRLPath: path}, ca, discardLogger())
	if err != nil {
		t.Fatalf("loadRevocation: %v", err)
	}
	if crl == nil || !crl.IsRevoked(big.NewInt(42)) {
		t.Error("the configured CRL should be loaded and enforce its serial")
	}
}

func TestLoadRevocation_StaleWarns(t *testing.T) {
	ca := testCA(t)
	// A CRL that is already expired: ThisUpdate is backdated a minute, so a
	// sub-minute lifetime lands NextUpdate in the past.
	crlPEM, err := clustertls.GenerateCRL(ca, nil, big.NewInt(1), time.Millisecond)
	if err != nil {
		t.Fatalf("GenerateCRL: %v", err)
	}
	path := filepath.Join(t.TempDir(), "crl.pem")
	if err := os.WriteFile(path, crlPEM, 0o600); err != nil {
		t.Fatalf("write CRL: %v", err)
	}
	// Still loads (fails open with a warning), just stale.
	crl, err := loadRevocation(&config.Config{CRLPath: path}, ca, discardLogger())
	if err != nil {
		t.Fatalf("loadRevocation: %v", err)
	}
	if !crl.Stale() {
		t.Error("expected the loaded CRL to report itself stale")
	}
}

func TestLoadRevocation_Errors(t *testing.T) {
	ca := testCA(t)
	otherCA := testCA(t)

	// Unreadable path.
	if _, err := loadRevocation(&config.Config{CRLPath: filepath.Join(t.TempDir(), "missing.pem")}, ca, discardLogger()); err == nil {
		t.Error("a missing CRL file should error")
	}

	// Present but signed by a different CA.
	crlPEM, _ := clustertls.GenerateCRL(otherCA, nil, big.NewInt(1), time.Hour)
	path := filepath.Join(t.TempDir(), "crl.pem")
	if err := os.WriteFile(path, crlPEM, 0o600); err != nil {
		t.Fatalf("write CRL: %v", err)
	}
	_, err := loadRevocation(&config.Config{CRLPath: path}, ca, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "does not verify") {
		t.Errorf("a foreign-signed CRL should be rejected, got %v", err)
	}
}
