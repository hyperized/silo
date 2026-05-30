package captoken_test

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/captoken"
)

// FuzzParse hardens the capability-token parser — the gRPC bearer-auth boundary
// that decodes an untrusted "payload.signature" string from a client before any
// authorization decision. Malformed base64, a truncated half, a tampered
// signature, and hostile JSON must all surface as an error, never a panic, and
// must never yield a usable token. Seeded with a real minted token so the
// fuzzer mutates outward from valid input.
func FuzzParse(f *testing.F) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		f.Fatalf("generate key: %v", err)
	}
	if tok, err := captoken.Mint(priv, captoken.Token{
		Principal:    "csi",
		Capabilities: []captoken.Capability{captoken.CapChunkRead},
		IssuedAt:     time.Unix(1000, 0),
		Expiry:       time.Unix(100000, 0),
	}); err == nil {
		f.Add(tok)
	}
	for _, s := range []string{"", ".", "notbase64.notbase64", "YWJj.", "YWJj.ZGVm", "a.b.c"} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		tok, err := captoken.Parse(s, pub)
		if err == nil && tok == nil {
			t.Fatal("Parse returned a nil token with a nil error")
		}
		// A wrong-length verification key must error cleanly, never panic.
		if _, err := captoken.Parse(s, ed25519.PublicKey("short")); err == nil {
			t.Fatal("Parse accepted a token against an invalid-size key")
		}
	})
}
