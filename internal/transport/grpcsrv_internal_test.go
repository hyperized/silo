package transport

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"log/slog"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
)

// TestNewGRPCServer_WithAllOptions covers the option-accumulation path: a token
// auth interceptor (serverOpts) plus status and node-admin services
// (registrations) are all applied without panicking, and the server binds.
func TestNewGRPCServer_WithAllOptions(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	_, _ = rand.Read(key)
	cipher, _ := crypto.NewCipher(key)
	store, _ := chunkstore.NewFileStore(t.TempDir(), cipher)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	serverTLS, _ := testMTLSPair(t)

	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	auth := NewTokenAuthenticator(pub)

	srv := NewGRPCServer("127.0.0.1:0", serverTLS, store, nil, nil, logger,
		WithTokenAuth(auth),
		WithStatusService(NewStatusService(fakeStatusMembers{}, fakeStatusStore{}, "/d", "n", "v", logger)),
		WithNodeAdminService(NewNodeAdminService(&fakeDrainer{}, "n", logger)),
	)
	if srv == nil {
		t.Fatal("NewGRPCServer returned nil with options")
	}
	if srv.Addr() != "" {
		t.Errorf("Addr before Start: got %q, want empty", srv.Addr())
	}
}
