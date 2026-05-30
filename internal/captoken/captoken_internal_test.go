package captoken

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMint_MarshalError(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	prev := marshal
	marshal = func(any) ([]byte, error) { return nil, errors.New("boom") }
	t.Cleanup(func() { marshal = prev })

	_, err = Mint(priv, Token{
		Principal:    "p",
		Capabilities: []Capability{CapChunkRead},
		Expiry:       time.Now().Add(time.Hour),
	})
	if err == nil || !strings.Contains(err.Error(), "could not encode the token payload") {
		t.Errorf("got %v, want a marshal error", err)
	}
}

// TestParse_VerifiedButNotJSON reaches the unmarshal branch that runs only after
// the signature verifies: a token whose payload is correctly signed but is not
// valid JSON. Mint can never produce this, so the test signs raw bytes itself.
func TestParse_VerifiedButNotJSON(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	payload := []byte("not-json")
	sig := ed25519.Sign(priv, payload)
	token := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)

	if _, err := Parse(token, pub); err == nil || !strings.Contains(err.Error(), "did not decode") {
		t.Errorf("got %v, want a payload decode error", err)
	}
}
