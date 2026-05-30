package chunkstore_test

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/crypto"
)

func guardedStore(t *testing.T, hard float64, usage func() (int64, int64, error)) *chunkstore.FileStore {
	t.Helper()
	key := make([]byte, crypto.ClusterKeyBytes)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	c, err := crypto.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	fs, err := chunkstore.NewFileStore(t.TempDir(), c, chunkstore.WithDiskGuard(hard, usage))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	return fs
}

func TestDiskGuard_RefusesAtHardWatermark(t *testing.T) {
	// capacity 1000, hard 0.95 -> reserve 50 bytes must stay free.
	// available 40 < reserve -> refuse before even counting the payload.
	fs := guardedStore(t, 0.95, func() (int64, int64, error) { return 40, 1000, nil })
	_, err := fs.Put(context.Background(), "c1", []byte("data"))
	if !errors.Is(err, chunkstore.ErrNoSpace) {
		t.Fatalf("expected ErrNoSpace at the hard watermark, got %v", err)
	}
}

func TestDiskGuard_AllowsWithHeadroom(t *testing.T) {
	// available 500, reserve 50, small payload -> allowed.
	fs := guardedStore(t, 0.95, func() (int64, int64, error) { return 500, 1000, nil })
	if _, err := fs.Put(context.Background(), "c1", []byte("data")); err != nil {
		t.Fatalf("write with headroom should succeed: %v", err)
	}
}

func TestDiskGuard_PayloadPushesPastReserve(t *testing.T) {
	// available 60, reserve 50: a 20-byte envelope would leave 40 < 50 -> refuse.
	// (The plaintext is padded by crypto overhead, so any small payload exceeds the 10-byte margin.)
	fs := guardedStore(t, 0.95, func() (int64, int64, error) { return 60, 1000, nil })
	_, err := fs.Put(context.Background(), "c1", []byte("0123456789abcdef0123456789"))
	if !errors.Is(err, chunkstore.ErrNoSpace) {
		t.Fatalf("a write that would breach the reserve should be refused, got %v", err)
	}
}

func TestDiskGuard_FailsOpenOnMeasureError(t *testing.T) {
	// A measurement error must not wedge writes — the FS's own ENOSPC is the backstop.
	fs := guardedStore(t, 0.95, func() (int64, int64, error) { return 0, 0, errors.New("statfs boom") })
	if _, err := fs.Put(context.Background(), "c1", []byte("data")); err != nil {
		t.Fatalf("a measure error should fail open, got %v", err)
	}
}

func TestDiskGuard_FailsOpenOnZeroCapacity(t *testing.T) {
	fs := guardedStore(t, 0.95, func() (int64, int64, error) { return 0, 0, nil })
	if _, err := fs.Put(context.Background(), "c1", []byte("data")); err != nil {
		t.Fatalf("zero capacity should fail open, got %v", err)
	}
}

func TestWithDiskGuard_IgnoredWhenDisabled(t *testing.T) {
	// hard=0 or nil usage means no guard installed; a store built that way writes freely.
	fs := guardedStore(t, 0, nil)
	if _, err := fs.Put(context.Background(), "c1", []byte("data")); err != nil {
		t.Fatalf("a disabled guard should not block writes: %v", err)
	}
}
