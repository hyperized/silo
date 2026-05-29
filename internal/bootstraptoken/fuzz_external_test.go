package bootstraptoken_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/bootstraptoken"
)

// FuzzStoreOpen hardens the token-store loader. The store file lives in
// silod's data dir and can be edited or corrupted on disk, so Open must
// reject arbitrary contents with an error rather than panic, and a store
// that does load must stay usable.
func FuzzStoreOpen(f *testing.F) {
	dir := f.TempDir()
	path := filepath.Join(dir, "tokens.json")

	// Seed with a real, valid store file plus degenerate inputs.
	if seed, err := bootstraptoken.Open(path); err == nil {
		_, _ = seed.Mint(time.Hour, true)
		if raw, err := os.ReadFile(path); err == nil {
			f.Add(raw)
		}
	}
	f.Add([]byte("[]"))
	f.Add([]byte(""))
	f.Add([]byte("not json"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}
		store, err := bootstraptoken.Open(path)
		if err == nil && store != nil {
			_ = store.Tokens()
		}
	})
}

// FuzzRedeem hardens token redemption. The plaintext arrives over the
// network in a Join request, so an arbitrary or malformed token must
// resolve to a clean error, never a panic.
func FuzzRedeem(f *testing.F) {
	store, err := bootstraptoken.Open(filepath.Join(f.TempDir(), "tokens.json"))
	if err != nil {
		f.Fatalf("Open: %v", err)
	}
	if _, err := store.Mint(time.Hour, true); err != nil {
		f.Fatalf("Mint: %v", err)
	}
	f.Add("sometoken")
	f.Add("")
	f.Add("not-valid-token-!!")

	f.Fuzz(func(_ *testing.T, plaintext string) {
		_ = store.Redeem(plaintext)
	})
}
