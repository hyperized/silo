package crypto_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/hyperized/silo/internal/crypto"
)

// TestPublicAPI_RoundTrip is a black-box check that the public surface
// (NewCipher / EncryptChunk / DecryptChunk) round-trips.
func TestPublicAPI_RoundTrip(t *testing.T) {
	key := make([]byte, crypto.ClusterKeyBytes)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	c, err := crypto.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	in := []byte("hello from the public API")
	env, err := c.EncryptChunk(in)
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}
	if len(env) != len(in)+crypto.OverheadBytes {
		t.Errorf("envelope length: got %d, want %d", len(env), len(in)+crypto.OverheadBytes)
	}
	out, err := c.DecryptChunk(env)
	if err != nil {
		t.Fatalf("DecryptChunk: %v", err)
	}
	if !bytes.Equal(out, in) {
		t.Errorf("round-trip mismatch")
	}
}

func TestEnvelopeIsCiphertext(t *testing.T) {
	// Verify the envelope does not contain the plaintext literally — this
	// is the load-bearing guarantee: an attacker who reads the on-disk
	// file should not see plaintext.
	key := make([]byte, crypto.ClusterKeyBytes)
	_, _ = rand.Read(key)
	c, _ := crypto.NewCipher(key)
	plaintext := []byte("THE_SECRET_PLAINTEXT_MUST_NOT_LEAK")
	env, err := c.EncryptChunk(plaintext)
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}
	if bytes.Contains(env, plaintext) {
		t.Fatal("envelope contained plaintext verbatim; encryption produced a copy of the input")
	}
}
