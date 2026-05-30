package captoken_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/captoken"
)

func keypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return pub, priv
}

func TestMintParseRoundTrip(t *testing.T) {
	pub, priv := keypair(t)
	now := time.Now().UTC().Truncate(time.Second)
	in := captoken.Token{
		Principal:    "csi@cluster",
		Capabilities: []captoken.Capability{captoken.CapChunkRead, captoken.CapChunkWrite},
		IssuedAt:     now,
		Expiry:       now.Add(time.Hour),
	}
	s, err := captoken.Mint(priv, in)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	out, err := captoken.Parse(s, pub)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if out.Principal != in.Principal || !out.Expiry.Equal(in.Expiry) || !out.IssuedAt.Equal(in.IssuedAt) {
		t.Errorf("round trip mismatch: %+v vs %+v", out, in)
	}
	if !out.Allows(captoken.CapChunkRead) || !out.Allows(captoken.CapChunkWrite) {
		t.Error("granted capabilities should be allowed")
	}
	if out.Allows(captoken.CapNodeAdmin) {
		t.Error("ungranted capability should not be allowed")
	}
}

func TestAllows_Wildcard(t *testing.T) {
	pub, priv := keypair(t)
	now := time.Now().UTC()
	s, err := captoken.Mint(priv, captoken.Token{
		Principal:    "admin",
		Capabilities: []captoken.Capability{captoken.CapAll},
		IssuedAt:     now,
		Expiry:       now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	tok, err := captoken.Parse(s, pub)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	for _, c := range []captoken.Capability{captoken.CapChunkRead, captoken.CapNodeAdmin, captoken.CapStatusRead} {
		if !tok.Allows(c) {
			t.Errorf("wildcard token should allow %q", c)
		}
	}
}

func TestMint_Validation(t *testing.T) {
	_, priv := keypair(t)
	now := time.Now().UTC()
	cases := []struct {
		name string
		key  ed25519.PrivateKey
		tok  captoken.Token
		want string
	}{
		{"bad key", ed25519.PrivateKey("short"), captoken.Token{Principal: "p", Capabilities: []captoken.Capability{captoken.CapChunkRead}, Expiry: now.Add(time.Hour)}, "Ed25519 private key"},
		{"no principal", priv, captoken.Token{Capabilities: []captoken.Capability{captoken.CapChunkRead}, Expiry: now.Add(time.Hour)}, "needs a principal"},
		{"no caps", priv, captoken.Token{Principal: "p", Expiry: now.Add(time.Hour)}, "at least one capability"},
		{"no expiry", priv, captoken.Token{Principal: "p", Capabilities: []captoken.Capability{captoken.CapChunkRead}}, "needs an expiry"},
		{"empty cap", priv, captoken.Token{Principal: "p", Capabilities: []captoken.Capability{""}, Expiry: now.Add(time.Hour)}, "empty capability"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := captoken.Mint(tc.key, tc.tok); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestParse_Errors(t *testing.T) {
	pub, priv := keypair(t)
	otherPub, _ := keypair(t)
	now := time.Now().UTC()
	good, err := captoken.Mint(priv, captoken.Token{
		Principal:    "p",
		Capabilities: []captoken.Capability{captoken.CapChunkRead},
		IssuedAt:     now,
		Expiry:       now.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	cases := []struct {
		name string
		in   string
		pub  ed25519.PublicKey
		want string
	}{
		{"bad pubkey", good, ed25519.PublicKey("short"), "public key"},
		{"no dot", "noseparator", pub, "malformed token"},
		{"bad payload b64", "!!!.YWJj", pub, "payload is not valid base64url"},
		{"bad sig b64", strings.SplitN(good, ".", 2)[0] + ".!!!", pub, "signature is not valid base64url"},
		{"wrong signer", good, otherPub, "does not verify"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := captoken.Parse(tc.in, tc.pub); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestParse_TamperedPayloadFailsSignature(t *testing.T) {
	pub, priv := keypair(t)
	now := time.Now().UTC()
	good, _ := captoken.Mint(priv, captoken.Token{
		Principal:    "p",
		Capabilities: []captoken.Capability{captoken.CapChunkRead},
		IssuedAt:     now,
		Expiry:       now.Add(time.Hour),
	})
	// Swap the payload for a different valid base64url so the signature, which
	// covers the original payload, no longer matches.
	_, sig, _ := strings.Cut(good, ".")
	tampered := "eyJwIjoiZXZpbCJ9." + sig // base64url of {"p":"evil"}
	if _, err := captoken.Parse(tampered, pub); err == nil {
		t.Error("a tampered payload must fail signature verification")
	}
}

func TestValidate_Window(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	tok := &captoken.Token{Principal: "p", IssuedAt: base, Expiry: base.Add(time.Hour)}

	if err := tok.Validate(base.Add(30 * time.Minute)); err != nil {
		t.Errorf("a token mid-window should validate: %v", err)
	}
	if err := tok.Validate(base.Add(2 * time.Hour)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Errorf("an expired token should fail: %v", err)
	}
	if err := tok.Validate(base.Add(-10 * time.Minute)); err == nil || !strings.Contains(err.Error(), "not valid yet") {
		t.Errorf("a not-yet-valid token should fail: %v", err)
	}
	// Within the one-minute skew allowance before IssuedAt.
	if err := tok.Validate(base.Add(-30 * time.Second)); err != nil {
		t.Errorf("a token within skew should validate: %v", err)
	}
}
