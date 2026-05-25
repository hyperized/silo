package chunkstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
)

// TestStore_InterfaceContract pins the public surface: FileStore must
// satisfy Store and round-trip via the interface, not just the struct.
func TestStore_InterfaceContract(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	_, _ = rand.Read(key)
	c, _ := crypto.NewCipher(key)
	fs, err := chunkstore.NewFileStore(t.TempDir(), c)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	var s chunkstore.Store = fs

	ctx := context.Background()
	payload := []byte("through the interface")
	if _, err := s.Put(ctx, "via-iface", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, _, err := s.Get(ctx, "via-iface")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch via Store interface")
	}
}

func TestValidateID_Accepts(t *testing.T) {
	for _, id := range []string{
		"a",
		"with-dash",
		"with_underscore",
		"MixedCase123",
		"0123456789",
	} {
		if err := chunkstore.ValidateID(id); err != nil {
			t.Errorf("ValidateID(%q): got %v, want nil", id, err)
		}
	}
}
