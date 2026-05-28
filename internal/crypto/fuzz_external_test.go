package crypto_test

import (
	"crypto/rand"
	"testing"

	"github.com/hyperized/silo/internal/crypto"
)

// FuzzDecryptChunk hardens the one place that turns disk- or peer-supplied
// bytes back into plaintext. Arbitrary, truncated, or tampered input must
// surface as an error and never panic or yield a silent partial result.
// Seeded with a real envelope and truncations so the fuzzer starts near
// valid framing.
func FuzzDecryptChunk(f *testing.F) {
	key := make([]byte, crypto.ClusterKeyBytes)
	if _, err := rand.Read(key); err != nil {
		f.Fatalf("rand: %v", err)
	}
	c, err := crypto.NewCipher(key)
	if err != nil {
		f.Fatalf("NewCipher: %v", err)
	}

	if env, err := c.EncryptChunk([]byte("seed plaintext")); err == nil {
		f.Add(env)
		f.Add(env[:len(env)-1]) // truncated tag
		if len(env) > 10 {
			f.Add(env[:10]) // truncated header
		}
	}
	f.Add([]byte{})

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = c.DecryptChunk(data)
	})
}
