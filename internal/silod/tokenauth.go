package silod

import (
	"crypto/ed25519"
	"errors"
	"log/slog"

	"github.com/hyperized/silo/internal/clustertls"
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/transport"
)

// newTokenAuthenticator builds the capability-token interceptor when
// SILO_REQUIRE_TOKENS is set, verifying tokens against the cluster CA's public
// key (the same Ed25519 key that anchors node and client certs). It returns
// (nil, nil) when token enforcement is off, which leaves the gRPC server in its
// mTLS-only mode.
func newTokenAuthenticator(cfg *config.Config, ca *clustertls.CA, logger *slog.Logger) (*transport.TokenAuthenticator, error) {
	if !cfg.RequireTokens {
		return nil, nil
	}
	if ca == nil || ca.Cert == nil {
		return nil, errors.New("silod: SILO_REQUIRE_TOKENS is set but the cluster CA is not loaded; token verification needs the CA certificate")
	}
	pub, ok := ca.Cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("silod: the cluster CA does not carry an Ed25519 public key; capability tokens require the Ed25519 CA produced by 'siloctl ca init'")
	}
	logger.Info("capability-token authorization enabled; client-cert callers must present a scoped SILO_TOKEN")
	return transport.NewTokenAuthenticator(pub), nil
}
