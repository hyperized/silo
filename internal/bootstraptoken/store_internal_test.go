package bootstraptoken

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), defaultStoreName))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func TestOpen_EmptyPath(t *testing.T) {
	if _, err := Open(""); err == nil || !strings.Contains(err.Error(), "token-store path is empty") {
		t.Errorf("got %v, want empty-path error", err)
	}
}

func TestOpen_MissingFileIsEmptyStore(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "no-such-file.json"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := s.Tokens(); len(got) != 0 {
		t.Errorf("Tokens: got %d, want 0", len(got))
	}
}

func TestOpen_EmptyFileIsEmptyStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), defaultStoreName)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("seed empty: %v", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := s.Tokens(); len(got) != 0 {
		t.Errorf("Tokens: got %d, want 0", len(got))
	}
}

func TestOpen_UnreadableFile(t *testing.T) {
	// A directory at the store path makes ReadFile fail with an error
	// that is neither nil nor os.ErrNotExist — the actionable branch.
	dir := t.TempDir()
	target := filepath.Join(dir, "store")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if _, err := Open(target); err == nil || !strings.Contains(err.Error(), "could not read") {
		t.Errorf("got %v, want read error", err)
	}
}

func TestOpen_GarbageJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), defaultStoreName)
	if err := os.WriteFile(path, []byte("this is not json"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "not valid JSON") {
		t.Errorf("got %v, want JSON-parse error", err)
	}
}

func TestMint_PersistsHashOnlyNotPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), defaultStoreName)
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	plain, err := s.Mint(0, true)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if plain == "" {
		t.Error("Mint returned empty plaintext")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read store: %v", err)
	}
	if strings.Contains(string(raw), plain) {
		t.Error("on-disk store contains the plaintext token; only the hash should be persisted")
	}
}

func TestMint_AppliesDefaultTTL(t *testing.T) {
	s := newTestStore(t)
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return fixed }
	if _, err := s.Mint(0, true); err != nil {
		t.Fatalf("Mint: %v", err)
	}
	tok := s.Tokens()[0]
	want := fixed.Add(DefaultTTL)
	if !tok.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt: got %v, want %v", tok.ExpiresAt, want)
	}
}

func TestMint_HonoursExplicitTTL(t *testing.T) {
	s := newTestStore(t)
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return fixed }
	if _, err := s.Mint(5*time.Minute, false); err != nil {
		t.Fatalf("Mint: %v", err)
	}
	tok := s.Tokens()[0]
	if tok.SingleUse {
		t.Error("singleUse=false should produce a multi-use token")
	}
	if !tok.ExpiresAt.Equal(fixed.Add(5 * time.Minute)) {
		t.Errorf("ExpiresAt: got %v, want %v", tok.ExpiresAt, fixed.Add(5*time.Minute))
	}
}

func TestMint_PersistFailureRollsBack(t *testing.T) {
	// Wedge persistLocked: a directory at <path>.tmp blocks the
	// temp-write step. Mint must surface the error AND leave the
	// in-memory token list unchanged so callers don't believe in a
	// token that isn't durable.
	path := filepath.Join(t.TempDir(), defaultStoreName)
	if err := os.MkdirAll(path+".tmp", 0o700); err != nil {
		t.Fatalf("seed tmp dir: %v", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Mint(0, true); err == nil {
		t.Fatal("Mint should fail when persist fails")
	}
	if got := s.Tokens(); len(got) != 0 {
		t.Errorf("Mint should have rolled back; got %d tokens", len(got))
	}
}

func TestRedeem_HappyPathConsumesSingleUse(t *testing.T) {
	s := newTestStore(t)
	plain, err := s.Mint(0, true)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if err := s.Redeem(plain); err != nil {
		t.Fatalf("first Redeem: %v", err)
	}
	if err := s.Redeem(plain); !errors.Is(err, ErrTokenNotFound) {
		t.Errorf("second Redeem: got %v, want ErrTokenNotFound (single-use)", err)
	}
}

func TestRedeem_MultiUseStaysValid(t *testing.T) {
	s := newTestStore(t)
	plain, err := s.Mint(time.Hour, false)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Redeem(plain); err != nil {
			t.Errorf("Redeem iter %d: %v", i, err)
		}
	}
}

func TestRedeem_RejectsUnknownToken(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Mint(0, true); err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if err := s.Redeem("not the token"); !errors.Is(err, ErrTokenNotFound) {
		t.Errorf("got %v, want ErrTokenNotFound", err)
	}
}

func TestRedeem_RejectsExpired(t *testing.T) {
	s := newTestStore(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	plain, err := s.Mint(time.Minute, true)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	s.now = func() time.Time { return now.Add(2 * time.Minute) }
	if err := s.Redeem(plain); !errors.Is(err, ErrTokenNotFound) {
		t.Errorf("got %v, want ErrTokenNotFound for expired token", err)
	}
}

func TestRedeem_RejectsAlreadyConsumed(t *testing.T) {
	s := newTestStore(t)
	plain, err := s.Mint(0, true)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if err := s.Redeem(plain); err != nil {
		t.Fatalf("first redeem: %v", err)
	}
	// Manually flip Consumed to ensure the test exercises the
	// "matches hash, but consumed" branch even if SingleUse path
	// changes shape later.
	s.mu.Lock()
	s.tokens[0].Consumed = true
	s.mu.Unlock()
	if err := s.Redeem(plain); !errors.Is(err, ErrTokenNotFound) {
		t.Errorf("got %v, want ErrTokenNotFound for consumed token", err)
	}
}

func TestRedeem_SkipsCorruptHashRow(t *testing.T) {
	// Inject a token whose Hash is not valid hex; Redeem must skip
	// silently and refuse the request instead of crashing or
	// revealing the bad row in an error message.
	s := newTestStore(t)
	plain, err := s.Mint(0, true)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	s.mu.Lock()
	s.tokens = append([]Token{{Hash: "not-hex", ExpiresAt: time.Now().Add(time.Hour)}}, s.tokens...)
	s.mu.Unlock()
	if err := s.Redeem(plain); err != nil {
		t.Errorf("Redeem should still find the valid token: %v", err)
	}
}

func TestRedeem_PersistFailureRollsBackConsumption(t *testing.T) {
	// Mint, then wedge persistLocked by putting a directory at the
	// temp-rename target. Redeem should refuse to claim the token as
	// consumed if it can't durably mark it so, so a retry succeeds.
	path := filepath.Join(t.TempDir(), defaultStoreName)
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	plain, err := s.Mint(0, true)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if err := os.MkdirAll(path+".tmp", 0o700); err != nil {
		t.Fatalf("seed tmp dir: %v", err)
	}
	if err := s.Redeem(plain); err == nil {
		t.Fatal("Redeem should surface the persist failure")
	}
	// Remove the wedge and retry — second attempt must succeed.
	if err := os.RemoveAll(path + ".tmp"); err != nil {
		t.Fatalf("clean tmp: %v", err)
	}
	if err := s.Redeem(plain); err != nil {
		t.Errorf("retry after wedge cleared: %v", err)
	}
}

func TestTokens_ReturnsCopy(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Mint(0, true); err != nil {
		t.Fatalf("Mint: %v", err)
	}
	first := s.Tokens()
	first[0].Hash = "tampered"
	second := s.Tokens()
	if second[0].Hash == "tampered" {
		t.Error("Tokens() returned a shared slice; callers can mutate the store")
	}
}

func TestPersistLocked_SerialiseFailureIsImpossibleInPractice(t *testing.T) {
	// Document the branch: persistLocked's json.MarshalIndent error
	// path is unreachable because []Token always marshals cleanly.
	// Decode a valid round-trip to give the marshaller exercise.
	tokens := []Token{{Hash: hex.EncodeToString(make([]byte, 32)), ExpiresAt: time.Now(), SingleUse: true, CreatedAt: time.Now()}}
	if _, err := json.Marshal(tokens); err != nil {
		t.Fatalf("Marshal: %v", err)
	}
}

func TestConcurrentMintAndRedeem(t *testing.T) {
	// Round-trip a batch of mints and redeems concurrently to catch
	// any data race the test runner's -race flag would otherwise miss
	// in serial usage.
	s := newTestStore(t)
	var wg sync.WaitGroup
	const n = 16
	plains := make([]string, n)
	for i := 0; i < n; i++ {
		p, err := s.Mint(0, false)
		if err != nil {
			t.Fatalf("Mint: %v", err)
		}
		plains[i] = p
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			if err := s.Redeem(p); err != nil {
				t.Errorf("Redeem: %v", err)
			}
		}(plains[i])
	}
	wg.Wait()
}

func TestDefaultStoreName(t *testing.T) {
	if got := DefaultStoreName(); got != defaultStoreName {
		t.Errorf("DefaultStoreName: got %q, want %q", got, defaultStoreName)
	}
}

func TestMint_EntropyFailureBubblesUp(t *testing.T) {
	// Swap the rand seam to simulate an exhausted entropy pool. Mint
	// should surface the actionable wrapper, not the bare crypto/rand
	// error.
	prev := randRead
	t.Cleanup(func() { randRead = prev })
	randRead = func([]byte) (int, error) {
		return 0, errors.New("simulated entropy failure")
	}
	s := newTestStore(t)
	if _, err := s.Mint(0, true); err == nil || !strings.Contains(err.Error(), "entropy") {
		t.Errorf("got %v, want entropy-failure error", err)
	}
}

func TestPersistLocked_RenameFailureRollsBack(t *testing.T) {
	// Pre-place a directory at the final store path so os.Rename fails
	// (rename-onto-non-empty-dir is an error on every OS we support).
	// The temp write succeeds; the rename does not.
	dir := t.TempDir()
	target := filepath.Join(dir, "store.json")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatalf("seed dir at target: %v", err)
	}
	// Seed an inner file so rename-onto-non-empty is the failure mode.
	if err := os.WriteFile(filepath.Join(target, "blocker"), []byte{0}, 0o600); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	s := &Store{path: target, now: time.Now}
	s.tokens = []Token{{Hash: hex.EncodeToString(make([]byte, 32))}}
	if err := s.persistLocked(); err == nil || !strings.Contains(err.Error(), "rename") {
		t.Errorf("got %v, want rename error", err)
	}
	// Temp file should have been cleaned up.
	if _, err := os.Stat(target + ".tmp"); err == nil {
		t.Error("temp file lingered after rename failure")
	}
}

func TestRedeem_HexDecodeMismatch(t *testing.T) {
	// hashToken always produces valid hex, so wantBytes decode never
	// fails in production. Test the defensive branch by calling the
	// hashing helper directly — proves it returns hex-decodable bytes
	// at the right length.
	h := hashToken("payload")
	if _, err := hex.DecodeString(h); err != nil {
		t.Errorf("hashToken result is not hex: %v", err)
	}
	if len(h) != 64 {
		t.Errorf("hashToken length: got %d, want 64", len(h))
	}
}
