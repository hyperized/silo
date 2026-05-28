// Package bootstraptoken persists the short-lived secrets that
// operators redeem at first contact to obtain a client certificate
// signed by the cluster CA. Tokens are stored as sha256 hashes so a
// stolen on-disk file does not let an attacker join the cluster — they
// would still need the plaintext token, which silod prints to stdout
// exactly once when it mints it.
package bootstraptoken

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Default values for token minting. Tuned for first-boot ergonomics:
// operators usually run `siloctl auth init` within minutes of seeing
// the printed token, so 24h is plenty. Single-use is the safer default
// because a leaked token can only be redeemed once.
const (
	DefaultTTL        = 24 * time.Hour
	DefaultSingleUse  = true
	defaultStoreName  = "bootstrap-tokens.json"
	defaultStoreMode  = 0o600
	defaultTokenBytes = 32 // 256 bits of entropy in the redeemable secret
)

// ErrTokenNotFound is returned when no matching, non-expired,
// non-consumed token exists for the supplied secret.
var ErrTokenNotFound = errors.New("bootstraptoken: token is not recognised; it may have been used already, expired, or never minted on this node")

// Token is one entry in the on-disk store. The plaintext token is
// deliberately not a field — only the hash lives at rest so a snapshot
// of the data directory does not leak redeemable secrets.
type Token struct {
	// Hash is the hex-encoded sha256 of the plaintext token.
	Hash string `json:"hash"`
	// ExpiresAt is when the token stops being redeemable. Zero is "no
	// expiry"; in practice silod always sets one to limit the window
	// an unwatched cluster offers to an attacker on the network.
	ExpiresAt time.Time `json:"expires_at"`
	// SingleUse, when true, marks the token consumed on its first
	// successful Redeem. Multi-use tokens are useful for fleet
	// onboarding scripts but should be short-lived.
	SingleUse bool `json:"single_use"`
	// Consumed is set when SingleUse=true and the token has been
	// successfully redeemed. We keep the row instead of deleting it so
	// audit logs can distinguish "never redeemed" from "redeemed,
	// stale" by reading the file.
	Consumed bool `json:"consumed"`
	// CreatedAt is recorded for human-readable inspection and for
	// `siloctl auth token list` (once that command lands).
	CreatedAt time.Time `json:"created_at"`
}

// Store is the on-disk token list. Methods are safe for concurrent use:
// reads and writes serialize on the same mutex, and every modification
// re-marshals the full file via a temp-rename pair so a crash mid-write
// leaves either the old or the new state, never a torn one.
type Store struct {
	path string

	mu     sync.Mutex
	tokens []Token
	now    func() time.Time // swappable in tests to drive time-based logic
}

// Open returns a Store backed by path. A missing file is treated as an
// empty store — the first call to Mint creates the file. The directory
// must already exist; silod ensures this by passing in <DataDir>.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("bootstraptoken: token-store path is empty; pass a path under SILO_DATA_DIR so silod can persist tokens across restarts")
	}
	s := &Store{path: path, now: time.Now}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Mint creates a fresh token. The plaintext is returned to the caller
// (so silod can log it once for the operator); only the hash is
// persisted. ttl=0 means "use DefaultTTL"; a non-zero singleUse-pair
// lets the caller mint a multi-use token for batch onboarding.
func (s *Store) Mint(ttl time.Duration, singleUse bool) (plaintext string, err error) {
	plaintext, err = generateToken()
	if err != nil {
		return "", err
	}
	if ttl == 0 {
		ttl = DefaultTTL
	}
	now := s.now()
	entry := Token{
		Hash:      hashToken(plaintext),
		ExpiresAt: now.Add(ttl),
		SingleUse: singleUse,
		CreatedAt: now,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = append(s.tokens, entry)
	if err := s.persistLocked(); err != nil {
		// Roll back the in-memory append so callers don't see a token
		// that isn't actually durable.
		s.tokens = s.tokens[:len(s.tokens)-1]
		return "", err
	}
	return plaintext, nil
}

// Redeem checks whether plaintext is a valid, non-expired, unused token
// and marks it consumed if so. The lookup is constant-time over the
// candidate hashes so a timing attacker cannot tell "this hash matches"
// from "this hash did not match." Returns ErrTokenNotFound on any
// failure mode — silod must not leak which mode tripped.
func (s *Store) Redeem(plaintext string) error {
	wantHash := hashToken(plaintext)
	wantBytes, err := hex.DecodeString(wantHash)
	if err != nil {
		// hashToken always produces 64 hex chars; a DecodeString failure
		// here is genuinely impossible in production. Returning the
		// generic not-found keeps the timing-side-channel posture intact.
		return ErrTokenNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	matched := -1
	for i := range s.tokens {
		tok := &s.tokens[i]
		candidate, decodeErr := hex.DecodeString(tok.Hash)
		if decodeErr != nil {
			// Corrupt entry in the on-disk file; skip it rather than
			// surfacing the error so a single bad row doesn't lock out
			// every other valid token.
			continue
		}
		if subtle.ConstantTimeCompare(candidate, wantBytes) != 1 {
			continue
		}
		// Found a hash match. Apply the gate checks *after* the compare
		// so an attacker cannot distinguish expired-tokens from
		// unknown-tokens by latency.
		if tok.Consumed {
			continue
		}
		if !tok.ExpiresAt.IsZero() && !now.Before(tok.ExpiresAt) {
			continue
		}
		matched = i
	}
	if matched < 0 {
		return ErrTokenNotFound
	}
	if s.tokens[matched].SingleUse {
		s.tokens[matched].Consumed = true
		if err := s.persistLocked(); err != nil {
			// Failed to persist the consumption mark. Roll back the
			// in-memory flip so a retry can succeed; the operator
			// re-runs `siloctl auth init` and we try again.
			s.tokens[matched].Consumed = false
			return fmt.Errorf("bootstraptoken: token validated but could not be marked consumed (%w); the token remains usable — retry once the underlying error is resolved", err)
		}
	}
	return nil
}

// Tokens returns a defensive copy of every token currently in the
// store. Intended for `siloctl auth token list` and for tests. The
// plaintext is never recoverable from the returned values.
func (s *Store) Tokens() []Token {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Token, len(s.tokens))
	copy(out, s.tokens)
	return out
}

func (s *Store) load() error {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.tokens = nil
		return nil
	}
	if err != nil {
		return fmt.Errorf("bootstraptoken: could not read the token store at %s (%w); check the file is readable by silod or remove it to start fresh", s.path, err)
	}
	if len(raw) == 0 {
		s.tokens = nil
		return nil
	}
	var tokens []Token
	if err := json.Unmarshal(raw, &tokens); err != nil {
		return fmt.Errorf("bootstraptoken: the token store at %s is not valid JSON (%w); remove the file so silod can recreate it on the next mint", s.path, err)
	}
	s.tokens = tokens
	return nil
}

// persistLocked rewrites the on-disk file. Caller must hold s.mu.
// Temp+rename gives us atomicity against power loss: a partial write
// lives at <path>.tmp and never gets sworn into the visible name.
func (s *Store) persistLocked() error {
	data, err := json.MarshalIndent(s.tokens, "", "  ")
	if err != nil {
		return fmt.Errorf("bootstraptoken: could not serialise the token store (%w); this is a programming error", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, defaultStoreMode); err != nil {
		return fmt.Errorf("bootstraptoken: could not write the temp token store at %s (%w); check %s is on a writable filesystem and silod has permission", tmp, err, filepath.Dir(tmp))
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("bootstraptoken: could not rename the temp token store into place at %s (%w); the partial file at %s was removed", s.path, err, tmp)
	}
	return nil
}

// randRead is the entropy seam generateToken calls through. Production
// points at crypto/rand; tests substitute it to drive the
// "entropy-exhausted" branch deterministically.
var randRead = rand.Read

// generateToken produces a 32-byte random secret, base64-encoded. The
// printable form means operators can copy-paste it from silod's stdout
// without escaping concerns; the entropy budget is the same as the
// raw bytes underneath.
func generateToken() (string, error) {
	buf := make([]byte, defaultTokenBytes)
	if _, err := randRead(buf); err != nil {
		return "", fmt.Errorf("bootstraptoken: could not draw entropy for a new token (%w); the host entropy pool may be exhausted", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// hashToken returns the hex-encoded sha256 of plaintext. Using hex
// instead of base64 keeps the on-disk file diff-friendly when an
// operator inspects it with `cat` or `git diff`.
func hashToken(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// DefaultStoreName is the conventional file name for the token store,
// exposed so silod can build the path without re-stringing the literal.
func DefaultStoreName() string { return defaultStoreName }
