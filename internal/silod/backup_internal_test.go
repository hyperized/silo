package silod

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/config"
)

// minimalStore implements chunkstore.Store but NOT RawChunk, so it is not a
// backup.ChunkSource.
type minimalStore struct{}

func (minimalStore) Put(context.Context, string, []byte) (chunkstore.Info, error) {
	return chunkstore.Info{}, nil
}

func (minimalStore) Get(context.Context, string) ([]byte, chunkstore.Info, error) {
	return nil, chunkstore.Info{}, nil
}
func (minimalStore) Delete(context.Context, string) error { return nil }
func (minimalStore) Stat(context.Context, string) (chunkstore.Info, error) {
	return chunkstore.Info{}, nil
}
func (minimalStore) List(context.Context) ([]string, error) { return nil, nil }

// rawStore adds RawChunk, so it satisfies backup.ChunkSource.
type rawStore struct{ minimalStore }

func (rawStore) RawChunk(context.Context, string) ([]byte, error) { return []byte("x"), nil }

type fakeSnap struct{}

func (fakeSnap) Snapshot() ([]byte, error) { return []byte("ns"), nil }

func TestNewBackupSubsystem(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// A bad target URL is rejected.
	if _, err := newBackupSubsystem(&config.Config{BackupTarget: "ftp://nope"}, rawStore{}, fakeSnap{}, nil, logger); err == nil {
		t.Error("a bad target URL should error")
	}

	// A store without raw reads cannot be backed up.
	if _, err := newBackupSubsystem(&config.Config{BackupTarget: t.TempDir()}, minimalStore{}, fakeSnap{}, nil, logger); err == nil {
		t.Error("a store without RawChunk should error")
	}

	// A valid local target + raw-capable store builds the subsystem.
	sub, err := newBackupSubsystem(&config.Config{BackupTarget: t.TempDir(), NodeID: "n"}, rawStore{}, fakeSnap{}, nil, logger)
	if err != nil {
		t.Fatalf("newBackupSubsystem: %v", err)
	}
	if sub.Name() != "backup" {
		t.Errorf("name = %q", sub.Name())
	}
}
