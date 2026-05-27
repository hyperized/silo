package bootstraptoken_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/hyperized/silo/internal/bootstraptoken"
)

func TestRoundTrip_MintRedeem(t *testing.T) {
	path := filepath.Join(t.TempDir(), bootstraptoken.DefaultStoreName())
	s, err := bootstraptoken.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	plain, err := s.Mint(0, true)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if err := s.Redeem(plain); err != nil {
		t.Fatalf("Redeem: %v", err)
	}
}

func TestPersistedAcrossOpens(t *testing.T) {
	path := filepath.Join(t.TempDir(), bootstraptoken.DefaultStoreName())
	s, err := bootstraptoken.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	plain, err := s.Mint(0, true)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	// Reopen the same path to simulate a silod restart.
	s2, err := bootstraptoken.Open(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	if err := s2.Redeem(plain); err != nil {
		t.Errorf("token did not survive restart: %v", err)
	}
}

func TestErrTokenNotFound_Sentinel(t *testing.T) {
	s, err := bootstraptoken.Open(filepath.Join(t.TempDir(), "x.json"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	err = s.Redeem("anything")
	if !errors.Is(err, bootstraptoken.ErrTokenNotFound) {
		t.Errorf("got %v, want errors.Is(err, ErrTokenNotFound)", err)
	}
}
