package silod

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/hyperized/silo/internal/clustertls"
	"github.com/hyperized/silo/internal/config"
)

// loadRevocation loads the configured certificate revocation list, verifies it
// against the cluster CA, and returns it for the mTLS configs to enforce. When
// SILO_TLS_CRL is unset it returns (nil, nil) — revocation checking is off and
// every CA-signed cert is accepted, the pre-CRL behaviour.
//
// A configured-but-unreadable or unverifiable CRL is a hard error: silod would
// rather refuse to start than run with a revocation list the operator believes
// is in force but silently is not.
func loadRevocation(cfg *config.Config, ca *clustertls.CA, logger *slog.Logger) (*clustertls.RevocationList, error) {
	if cfg.CRLPath == "" {
		return nil, nil
	}
	// Path is operator config (SILO_TLS_CRL), not request input.
	crlPEM, err := os.ReadFile(cfg.CRLPath) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("silod: could not read the CRL at %s (%w); unset SILO_TLS_CRL to disable revocation checking, or point it at a file produced by 'siloctl ca revoke'", cfg.CRLPath, err)
	}
	crl, err := clustertls.LoadCRL(crlPEM, ca)
	if err != nil {
		return nil, fmt.Errorf("silod: %w", err)
	}
	if crl.Stale() {
		logger.Warn("the configured CRL is past its NextUpdate; re-issue it with 'siloctl ca revoke' so revocations stay fresh",
			"crl_path", cfg.CRLPath, "next_update", crl.NextUpdate, "revoked", crl.Count())
	}
	logger.Info("certificate revocation list loaded", "crl_path", cfg.CRLPath, "revoked", crl.Count(), "number", crl.Number)
	return crl, nil
}
