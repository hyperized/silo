package transport

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bootstrapv1 "github.com/hyperized/silo/api/proto/silo/bootstrap/v1"
	"github.com/hyperized/silo/internal/bootstraptoken"
	"github.com/hyperized/silo/internal/clustertls"
)

// TokenRedeemer is the subset of bootstraptoken.Store the bootstrap
// service consumes. Carved out as an interface so tests can drop in a
// fake that returns ErrTokenNotFound or a persist failure without
// touching the disk.
type TokenRedeemer interface {
	Redeem(plaintext string) error
}

// ClientCertMinter signs a fresh per-operator certificate. Pulled out
// of clustertls so tests can substitute a deterministic minter without
// burning entropy or generating a real CA. Production wires this to a
// closure around clustertls.MintClientCert.
type ClientCertMinter func(principal string) (caCertPEM, clientCertPEM, clientKeyPEM []byte, err error)

// BootstrapService implements the Bootstrap.Join RPC. The service
// deliberately exposes no other endpoints: every other silod surface is
// mTLS-only, so the trust boundary is "anyone with a valid token and
// network reach to this port can obtain a client cert, exactly once."
// That's why tokens are short-lived (24h default) and single-use.
type BootstrapService struct {
	bootstrapv1.UnimplementedBootstrapServer

	tokens        TokenRedeemer
	minter        ClientCertMinter
	grpcAdvertise string
	logger        *slog.Logger
}

// NewBootstrapService wires the token store and client-cert minter onto
// a service ready for grpc.RegisterService. grpcAdvertise is the dial
// target operators get back in the Join response so siloctl knows where
// to send subsequent chunk RPCs — typically the mTLS gRPC listener on
// the same node, but cluster fronts where one address serves the join
// API on behalf of a separately-addressable data plane are also valid.
// The logger sees one line per Join, with the principal and outcome —
// silod operators can audit who joined when by reading silod's stdout.
func NewBootstrapService(tokens TokenRedeemer, minter ClientCertMinter, grpcAdvertise string, logger *slog.Logger) *BootstrapService {
	return &BootstrapService{tokens: tokens, minter: minter, grpcAdvertise: grpcAdvertise, logger: logger}
}

// Join consumes a one-time token and returns a freshly-signed client
// certificate plus the cluster CA cert. The principal is folded into the
// issued cert's CommonName for audit purposes.
func (s *BootstrapService) Join(_ context.Context, req *bootstrapv1.JoinRequest) (*bootstrapv1.JoinResponse, error) {
	if req == nil || strings.TrimSpace(req.Token) == "" {
		return nil, status.Error(codes.InvalidArgument, "Join: token is required; pass the value silod printed on first boot (or one minted via SILO_PRINT_BOOTSTRAP_TOKEN=1)")
	}
	principal := strings.TrimSpace(req.Principal)
	if principal == "" {
		return nil, status.Error(codes.InvalidArgument, "Join: principal is required; pass <os-user>@<hostname> so the issued certificate is attributable in audit logs")
	}

	if err := s.tokens.Redeem(req.Token); err != nil {
		// Never echo the token in error output, and never distinguish
		// "expired" from "unknown" — the constant-time guarantee in
		// bootstraptoken depends on the wire response being uniform.
		if errors.Is(err, bootstraptoken.ErrTokenNotFound) {
			s.logger.Warn("bootstrap join refused", "principal", principal, "reason", "token not recognised")
			return nil, status.Error(codes.PermissionDenied, "Join: token is not recognised; it may have been used already, expired, or never issued on this node — ask the cluster operator to mint a fresh one with SILO_PRINT_BOOTSTRAP_TOKEN=1")
		}
		s.logger.Error("bootstrap join failed during token redemption", "principal", principal, "err", err)
		return nil, status.Errorf(codes.Internal, "Join: the cluster could not record token consumption (%v); silod logs name the underlying issue", err)
	}

	caPEM, certPEM, keyPEM, err := s.minter(principal)
	if err != nil {
		s.logger.Error("bootstrap join failed during cert minting", "principal", principal, "err", err)
		return nil, status.Errorf(codes.Internal, "Join: the cluster could not sign a client certificate (%v); silod logs name the underlying issue", err)
	}

	s.logger.Info("bootstrap join completed", "principal", principal)
	return &bootstrapv1.JoinResponse{
		CaCertPem:     caPEM,
		ClientCertPem: certPEM,
		ClientKeyPem:  keyPEM,
		GrpcAddress:   s.grpcAdvertise,
	}, nil
}

// NewClientCertMinter returns a ClientCertMinter that issues client-only
// certificates signed by the supplied CA. The lifetime is the same as
// node certs: long enough that operators don't see weekly rotation, short
// enough that a lost laptop is recoverable by waiting it out.
func NewClientCertMinter(ca *clustertls.CA) ClientCertMinter {
	return func(principal string) ([]byte, []byte, []byte, error) {
		// The client cert's DNS-SAN list is empty: clients connect to
		// silod, not the other way around, so server-side verification of
		// the client's hostname is meaningless. The principal lives in
		// the Subject CommonName and the SPIFFE URI.
		nc, err := clustertls.MintClientCert(ca, principal, clustertls.DefaultNodeCertLifetime)
		if err != nil {
			return nil, nil, nil, err
		}
		caPEM := clustertls.EncodeCertPEM(ca.Cert.Raw)
		return caPEM, nc.CertPEM, nc.KeyPEM, nil
	}
}

// Compile-time check.
var _ bootstrapv1.BootstrapServer = (*BootstrapService)(nil)
