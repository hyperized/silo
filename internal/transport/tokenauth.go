package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/hyperized/silo/internal/captoken"
)

// TokenAuthenticator enforces capability tokens on the gRPC surface. It is the
// authorization layer above mTLS: mTLS proves the caller is a cluster member;
// the token says which operations that member may perform.
//
// Cluster nodes (certs with a spiffe://silo/node/ identity) are exempt — peer-
// to-peer replication is trusted by membership, exactly as before. Only client
// certs (spiffe://silo/client/) must present a token scoped to the operation.
// This is what lets token enforcement be enabled without breaking inter-node
// traffic.
type TokenAuthenticator struct {
	verifyKey ed25519.PublicKey
	now       func() time.Time
}

// NewTokenAuthenticator builds an authenticator that verifies tokens against the
// cluster CA's public key (pass ca.Cert.PublicKey, the Ed25519 half that also
// verifies node certs).
func NewTokenAuthenticator(verifyKey ed25519.PublicKey) *TokenAuthenticator {
	return &TokenAuthenticator{verifyKey: verifyKey, now: time.Now}
}

// methodCapabilities maps a gRPC full-method name to the capability it requires.
// A method absent from this map is denied when enforcement is on — a new RPC
// must be classified deliberately rather than fall through to "allowed".
var methodCapabilities = map[string]captoken.Capability{
	"/silo.chunk.v1.ChunkStore/Get":    captoken.CapChunkRead,
	"/silo.chunk.v1.ChunkStore/Stat":   captoken.CapChunkRead,
	"/silo.chunk.v1.ChunkStore/Put":    captoken.CapChunkWrite,
	"/silo.chunk.v1.ChunkStore/Delete": captoken.CapChunkWrite,

	"/silo.namespace.v1.NamespaceStore/List":           captoken.CapNamespaceRead,
	"/silo.namespace.v1.NamespaceStore/Manifest":       captoken.CapNamespaceRead,
	"/silo.namespace.v1.NamespaceStore/Mkdir":          captoken.CapNamespaceWrite,
	"/silo.namespace.v1.NamespaceStore/Touch":          captoken.CapNamespaceWrite,
	"/silo.namespace.v1.NamespaceStore/Remove":         captoken.CapNamespaceWrite,
	"/silo.namespace.v1.NamespaceStore/AppendChunk":    captoken.CapNamespaceWrite,
	"/silo.namespace.v1.NamespaceStore/CreateVolume":   captoken.CapNamespaceWrite,
	"/silo.namespace.v1.NamespaceStore/SnapshotVolume": captoken.CapNamespaceWrite,

	"/silo.status.v1.ClusterStatus/GetStatus": captoken.CapStatusRead,
	"/silo.node.v1.NodeAdmin/Drain":           captoken.CapNodeAdmin,
}

// UnaryInterceptor authorises a unary RPC. Stream RPCs go through
// StreamInterceptor; both share authorize().
func (a *TokenAuthenticator) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := a.authorize(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamInterceptor authorises a streaming RPC (the chunk Put/Get streams).
func (a *TokenAuthenticator) StreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := a.authorize(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}

// authorize is the shared decision: classify the caller, exempt nodes, and for
// client callers require a verified, unexpired token that grants the method's
// capability.
func (a *TokenAuthenticator) authorize(ctx context.Context, fullMethod string) error {
	if a.callerIsNode(ctx) {
		return nil // peer-to-peer traffic is trusted by node identity (mTLS)
	}

	required, known := methodCapabilities[fullMethod]
	if !known {
		return status.Errorf(codes.PermissionDenied, "silo: %s is not authorised for token-scoped clients", fullMethod)
	}

	raw, err := tokenFromContext(ctx)
	if err != nil {
		return err
	}
	tok, err := captoken.Parse(raw, a.verifyKey)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "silo: %v", err)
	}
	if err := tok.Validate(a.now()); err != nil {
		return status.Errorf(codes.Unauthenticated, "silo: %v", err)
	}
	if !tok.Allows(required) {
		return status.Errorf(codes.PermissionDenied,
			"silo: token for %q lacks the %q capability needed for %s; mint a token with that scope",
			tok.Principal, required, fullMethod)
	}
	return nil
}

// callerIsNode reports whether the verified client certificate carries a
// spiffe://silo/node/ identity. Such callers are cluster peers and bypass the
// token check. A call with no verified client cert (should not happen under
// RequireAndVerifyClientCert) is treated as a non-node, so it must present a
// token.
func (a *TokenAuthenticator) callerIsNode(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return false
	}
	return leafIsNode(tlsInfo.State.VerifiedChains)
}

// leafIsNode inspects the verified chains' leaf certificate for a node SPIFFE
// URI. Pulled out so the classification is unit-testable without a live TLS
// handshake.
func leafIsNode(chains [][]*x509.Certificate) bool {
	if len(chains) == 0 || len(chains[0]) == 0 {
		return false
	}
	for _, uri := range chains[0][0].URIs {
		if uri != nil && uri.Scheme == "spiffe" && strings.HasPrefix(uri.Path, "/node/") {
			return true
		}
	}
	return false
}

// tokenFromContext extracts the bearer token from the request metadata. silo
// uses the standard "authorization: Bearer <token>" header so the same token
// works with any gRPC client tooling.
func tokenFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "silo: request carries no metadata; a capability token is required (set SILO_TOKEN on the client)")
	}
	for _, v := range md.Get("authorization") {
		if after, found := strings.CutPrefix(v, "Bearer "); found {
			if after != "" {
				return after, nil
			}
		}
	}
	return "", status.Error(codes.Unauthenticated, "silo: no bearer token in the request; set SILO_TOKEN on the client (mint one with 'siloctl auth mint-token')")
}
