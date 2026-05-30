// Package captoken implements silo's signed capability tokens: a compact,
// Ed25519-signed assertion that a named principal may perform a specific set of
// operations until an expiry. They give CSI/FUSE/operator clients an authority
// narrower than the all-or-nothing cluster client certificate they hold — the
// cert proves membership, the token scopes what that member may do.
//
// The wire format is deliberately small and dependency-free:
//
//	base64url(json-payload) "." base64url(ed25519-signature-over-the-json)
//
// signed by the cluster CA key and verified with the CA public key, so the same
// root of trust that issues certs issues tokens. It is not a JWT — silo controls
// both ends, so a minimal format avoids the well-known JWT footguns (alg
// confusion, "none", header injection).
package captoken

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Capability names one class of operation a token may authorise. Capabilities
// are coarse (operation classes, not individual resources); per-resource
// scoping (a path prefix, a single volume) is a future refinement.
type Capability string

const (
	// CapChunkRead permits reading chunks (Get, Stat).
	CapChunkRead Capability = "chunk:read"
	// CapChunkWrite permits mutating chunks (Put, Delete).
	CapChunkWrite Capability = "chunk:write"
	// CapNamespaceRead permits read-only namespace operations (List, Manifest).
	CapNamespaceRead Capability = "namespace:read"
	// CapNamespaceWrite permits mutating namespace operations (Mkdir, Touch,
	// Remove, AppendChunk, CreateVolume, SnapshotVolume).
	CapNamespaceWrite Capability = "namespace:write"
	// CapStatusRead permits reading cluster status.
	CapStatusRead Capability = "status:read"
	// CapNodeAdmin permits node lifecycle operations (drain).
	CapNodeAdmin Capability = "node:admin"
	// CapAll is a wildcard authorising every capability — for operator/admin
	// tokens. Use sparingly; the point of tokens is least privilege.
	CapAll Capability = "*"
)

// Token is the decoded, verified contents of a capability token. It is produced
// by Parse (which checks the signature) or constructed by a caller for Mint.
type Token struct {
	Principal    string       `json:"p"`
	Capabilities []Capability `json:"c"`
	IssuedAt     time.Time    `json:"iat"`
	Expiry       time.Time    `json:"exp"`
}

// wirePayload is the JSON shape actually signed and transmitted. Times are unix
// seconds so the encoding is stable and compact regardless of monotonic-clock
// or location data that time.Time's own JSON would carry.
type wirePayload struct {
	Principal    string   `json:"p"`
	Capabilities []string `json:"c"`
	IssuedAt     int64    `json:"iat"`
	Expiry       int64    `json:"exp"`
}

var enc = base64.RawURLEncoding

// marshal is a seam over json.Marshal so the (otherwise unreachable) encode-
// error path in Mint is testable; the wirePayload is all simple types and never
// fails to marshal in production.
var marshal = json.Marshal

// Mint serialises tok, signs the payload with the CA key, and returns the
// encoded token string. The token must carry a principal, at least one
// capability, and a non-zero expiry — an unbounded token is a standing liability
// and is refused here rather than minted by mistake.
func Mint(key ed25519.PrivateKey, tok Token) (string, error) {
	if len(key) != ed25519.PrivateKeySize {
		return "", errors.New("captoken: signing key is not a valid Ed25519 private key; mint on a host that holds the cluster CA key")
	}
	if tok.Principal == "" {
		return "", errors.New("captoken: a token needs a principal; pass --principal=<who> so the token is attributable")
	}
	if len(tok.Capabilities) == 0 {
		return "", errors.New("captoken: a token needs at least one capability; pass --cap=chunk:read (etc.)")
	}
	if tok.Expiry.IsZero() {
		return "", errors.New("captoken: a token needs an expiry; pass --ttl so it cannot be replayed forever")
	}

	caps := make([]string, len(tok.Capabilities))
	for i, c := range tok.Capabilities {
		if c == "" {
			return "", errors.New("captoken: empty capability in the token; remove it or use a known capability like chunk:read")
		}
		caps[i] = string(c)
	}
	payload, err := marshal(wirePayload{
		Principal:    tok.Principal,
		Capabilities: caps,
		IssuedAt:     tok.IssuedAt.UTC().Unix(),
		Expiry:       tok.Expiry.UTC().Unix(),
	})
	if err != nil {
		return "", fmt.Errorf("captoken: could not encode the token payload (%w); this is a programming error", err)
	}
	sig := ed25519.Sign(key, payload)
	return enc.EncodeToString(payload) + "." + enc.EncodeToString(sig), nil
}

// Parse decodes a token string and verifies its signature against the CA public
// key. It does NOT check expiry — the caller does that with Validate against its
// own clock, so signature verification and time validation are separable
// concerns (and separately testable). A token that does not verify is rejected
// without revealing which half failed.
func Parse(s string, pub ed25519.PublicKey) (*Token, error) {
	if len(pub) != ed25519.PublicKeySize {
		return nil, errors.New("captoken: verification key is not a valid Ed25519 public key")
	}
	payloadB64, sigB64, ok := strings.Cut(s, ".")
	if !ok {
		return nil, errors.New("captoken: malformed token (expected 'payload.signature'); it may be truncated or not a silo token")
	}
	payload, err := enc.DecodeString(payloadB64)
	if err != nil {
		return nil, fmt.Errorf("captoken: token payload is not valid base64url (%w)", err)
	}
	sig, err := enc.DecodeString(sigB64)
	if err != nil {
		return nil, fmt.Errorf("captoken: token signature is not valid base64url (%w)", err)
	}
	if !ed25519.Verify(pub, payload, sig) {
		return nil, errors.New("captoken: token signature does not verify against the cluster CA; it was forged, tampered with, or signed by a different cluster")
	}
	var wp wirePayload
	if err := json.Unmarshal(payload, &wp); err != nil {
		return nil, fmt.Errorf("captoken: token payload did not decode (%w)", err)
	}
	caps := make([]Capability, len(wp.Capabilities))
	for i, c := range wp.Capabilities {
		caps[i] = Capability(c)
	}
	return &Token{
		Principal:    wp.Principal,
		Capabilities: caps,
		IssuedAt:     time.Unix(wp.IssuedAt, 0).UTC(),
		Expiry:       time.Unix(wp.Expiry, 0).UTC(),
	}, nil
}

// Validate checks that the token is currently in force at now: issued (allowing
// a minute of clock skew) and not expired. Signature verification happens in
// Parse; this is purely the time window.
func (t *Token) Validate(now time.Time) error {
	const skew = time.Minute
	if !t.IssuedAt.IsZero() && now.Add(skew).Before(t.IssuedAt) {
		return fmt.Errorf("captoken: token for %q is not valid yet (issued %s); check clock skew between the client and silod", t.Principal, t.IssuedAt.Format(time.RFC3339))
	}
	if now.After(t.Expiry) {
		return fmt.Errorf("captoken: token for %q expired at %s; mint a fresh one with 'siloctl auth mint-token'", t.Principal, t.Expiry.Format(time.RFC3339))
	}
	return nil
}

// Allows reports whether the token authorises the given capability, honouring
// the CapAll wildcard.
func (t *Token) Allows(c Capability) bool {
	for _, granted := range t.Capabilities {
		if granted == CapAll || granted == c {
			return true
		}
	}
	return false
}
