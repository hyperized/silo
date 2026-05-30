package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/hyperized/silo/internal/captoken"
)

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", s, err)
	}
	return u
}

// certWithURI fabricates a minimal leaf carrying a SPIFFE URI. leafIsNode reads
// only URIs, so no real signing is needed.
func certWithURI(uri *url.URL) *x509.Certificate {
	return &x509.Certificate{URIs: []*url.URL{uri}}
}

func peerCtx(leaf *x509.Certificate) context.Context {
	chains := [][]*x509.Certificate{}
	if leaf != nil {
		chains = [][]*x509.Certificate{{leaf}}
	}
	info := credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: chains}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
}

func withBearer(ctx context.Context, token string) context.Context {
	return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
}

// authFixture returns an authenticator pinned to a fixed clock plus a token
// minter that signs with the matching CA key.
func authFixture(t *testing.T) (*TokenAuthenticator, func(principal string, caps ...captoken.Capability) string, time.Time) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	clock := time.Unix(1_700_000_000, 0).UTC()
	a := NewTokenAuthenticator(pub)
	a.now = func() time.Time { return clock }
	mint := func(principal string, caps ...captoken.Capability) string {
		s, err := captoken.Mint(priv, captoken.Token{
			Principal:    principal,
			Capabilities: caps,
			IssuedAt:     clock,
			Expiry:       clock.Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("Mint: %v", err)
		}
		return s
	}
	return a, mint, clock
}

func unaryRan(ctx context.Context, method string, a *TokenAuthenticator) (bool, error) {
	ran := false
	_, err := a.UnaryInterceptor(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: method},
		func(context.Context, any) (any, error) { ran = true; return nil, nil })
	return ran, err
}

func codeOf(err error) codes.Code { return status.Code(err) }

const (
	nodeURI   = "spiffe://silo/node/n1"
	clientURI = "spiffe://silo/client/csi"
	mGet      = "/silo.chunk.v1.ChunkStore/Get"
	mPut      = "/silo.chunk.v1.ChunkStore/Put"
)

func TestAuthorize_NodeCallerExempt(t *testing.T) {
	a, _, _ := authFixture(t)
	ctx := peerCtx(certWithURI(mustURL(t, nodeURI))) // no token at all
	ran, err := unaryRan(ctx, mPut, a)
	if err != nil || !ran {
		t.Errorf("node caller should be allowed without a token; ran=%v err=%v", ran, err)
	}
}

func TestAuthorize_ClientWithScopeAllowed(t *testing.T) {
	a, mint, _ := authFixture(t)
	ctx := withBearer(peerCtx(certWithURI(mustURL(t, clientURI))), mint("csi", captoken.CapChunkRead))
	ran, err := unaryRan(ctx, mGet, a)
	if err != nil || !ran {
		t.Errorf("client with chunk:read should call Get; ran=%v err=%v", ran, err)
	}
}

func TestAuthorize_ClientMissingScopeDenied(t *testing.T) {
	a, mint, _ := authFixture(t)
	// Has chunk:read but calls Put (needs chunk:write).
	ctx := withBearer(peerCtx(certWithURI(mustURL(t, clientURI))), mint("csi", captoken.CapChunkRead))
	ran, err := unaryRan(ctx, mPut, a)
	if ran || codeOf(err) != codes.PermissionDenied {
		t.Errorf("missing scope should be PermissionDenied; ran=%v err=%v", ran, err)
	}
}

func TestAuthorize_UnknownMethodDenied(t *testing.T) {
	a, mint, _ := authFixture(t)
	ctx := withBearer(peerCtx(certWithURI(mustURL(t, clientURI))), mint("csi", captoken.CapAll))
	_, err := unaryRan(ctx, "/silo.unknown.v1.Svc/Method", a)
	if codeOf(err) != codes.PermissionDenied {
		t.Errorf("unknown method should be PermissionDenied, got %v", err)
	}
}

func TestAuthorize_TokenProblems(t *testing.T) {
	a, mint, clock := authFixture(t)
	clientCtx := peerCtx(certWithURI(mustURL(t, clientURI)))

	// No metadata.
	if _, err := unaryRan(clientCtx, mGet, a); codeOf(err) != codes.Unauthenticated {
		t.Errorf("no metadata should be Unauthenticated, got %v", err)
	}
	// Metadata without a bearer token.
	noBearer := metadata.NewIncomingContext(clientCtx, metadata.Pairs("x", "y"))
	if _, err := unaryRan(noBearer, mGet, a); codeOf(err) != codes.Unauthenticated {
		t.Errorf("no bearer should be Unauthenticated, got %v", err)
	}
	// Empty bearer value.
	emptyBearer := metadata.NewIncomingContext(clientCtx, metadata.Pairs("authorization", "Bearer "))
	if _, err := unaryRan(emptyBearer, mGet, a); codeOf(err) != codes.Unauthenticated {
		t.Errorf("empty bearer should be Unauthenticated, got %v", err)
	}
	// Forged token.
	if _, err := unaryRan(withBearer(clientCtx, "garbage.token"), mGet, a); codeOf(err) != codes.Unauthenticated {
		t.Errorf("forged token should be Unauthenticated, got %v", err)
	}
	// Expired token: advance the clock past expiry.
	expired := mint("csi", captoken.CapChunkRead)
	a.now = func() time.Time { return clock.Add(2 * time.Hour) }
	if _, err := unaryRan(withBearer(clientCtx, expired), mGet, a); codeOf(err) != codes.Unauthenticated {
		t.Errorf("expired token should be Unauthenticated, got %v", err)
	}
}

func TestStreamInterceptor(t *testing.T) {
	a, mint, _ := authFixture(t)

	run := func(ctx context.Context, method string) (bool, error) {
		ran := false
		err := a.StreamInterceptor(nil, fakeServerStream{ctx: ctx},
			&grpc.StreamServerInfo{FullMethod: method},
			func(any, grpc.ServerStream) error { ran = true; return nil })
		return ran, err
	}

	// Node caller streams freely.
	if ran, err := run(peerCtx(certWithURI(mustURL(t, nodeURI))), mGet); err != nil || !ran {
		t.Errorf("node stream should run; ran=%v err=%v", ran, err)
	}
	// Client without scope is denied before the handler runs.
	ctx := withBearer(peerCtx(certWithURI(mustURL(t, clientURI))), mint("csi", captoken.CapStatusRead))
	if ran, err := run(ctx, mGet); ran || codeOf(err) != codes.PermissionDenied {
		t.Errorf("client stream without scope should be denied; ran=%v err=%v", ran, err)
	}
}

func TestCallerIsNode_And_LeafIsNode(t *testing.T) {
	a, _, _ := authFixture(t)

	// No peer in context.
	if a.callerIsNode(context.Background()) {
		t.Error("no peer should not be a node")
	}
	// Peer with non-TLS auth info.
	ctx := peer.NewContext(context.Background(), &peer.Peer{})
	if a.callerIsNode(ctx) {
		t.Error("non-TLS auth info should not be a node")
	}
	// Client cert is not a node.
	if a.callerIsNode(peerCtx(certWithURI(mustURL(t, clientURI)))) {
		t.Error("a client cert is not a node")
	}

	// leafIsNode direct edges.
	if leafIsNode(nil) {
		t.Error("no chains is not a node")
	}
	if leafIsNode([][]*x509.Certificate{{}}) {
		t.Error("empty chain is not a node")
	}
	if !leafIsNode([][]*x509.Certificate{{certWithURI(mustURL(t, nodeURI))}}) {
		t.Error("a node SPIFFE URI should classify as node")
	}
}

// fakeServerStream is the minimal ServerStream the interceptor touches: it only
// reads Context().
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeServerStream) Context() context.Context { return f.ctx }
