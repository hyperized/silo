package silod

import (
	"context"
	"io"
	"log/slog"
	"strings"
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

// TestNewBackupSubsystem_S3Options checks that an s3:// target with a custom
// endpoint builds without contacting the network, with and without the
// optional region and path-style settings.
func TestNewBackupSubsystem_S3Options(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name string
		cfg  config.Config
	}{
		{"endpoint with defaults", config.Config{
			BackupS3Endpoint: "http://127.0.0.1:9000", BackupS3Region: config.DefaultBackupS3Region, BackupS3PathStyle: true,
		}},
		{"endpoint with virtual-hosted style and region", config.Config{
			BackupS3Endpoint: "https://s3.example.com", BackupS3Region: "eu-west-1",
		}},
		{"no endpoint", config.Config{BackupS3Region: "eu-west-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.BackupTarget = "s3://bucket/prefix"
			cfg.NodeID = "n"
			cfg.BackupRetain = 3
			sub, err := newBackupSubsystem(&cfg, rawStore{}, fakeSnap{}, nil, "test", logger)
			if err != nil {
				t.Fatalf("newBackupSubsystem: %v", err)
			}
			if !sub.Status().Configured {
				t.Error("subsystem should report configured")
			}
		})
	}
}

func TestNewBackupSubsystem(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// A bad target URL is rejected.
	if _, err := newBackupSubsystem(&config.Config{BackupTarget: "ftp://nope"}, rawStore{}, fakeSnap{}, nil, "test", logger); err == nil {
		t.Error("a bad target URL should error")
	}

	// A store without raw reads cannot be backed up.
	if _, err := newBackupSubsystem(&config.Config{BackupTarget: t.TempDir(), NodeID: "n"}, minimalStore{}, fakeSnap{}, nil, "test", logger); err == nil {
		t.Error("a store without RawChunk should error")
	} else if !strings.Contains(err.Error(), "SILO_BACKUP_TARGET") {
		t.Errorf("error %q should tell the operator to unset SILO_BACKUP_TARGET", err)
	}

	// A node id that cannot be used in object names is refused.
	if _, err := newBackupSubsystem(&config.Config{BackupTarget: t.TempDir(), NodeID: "../evil"}, rawStore{}, fakeSnap{}, nil, "test", logger); err == nil {
		t.Error("an unsafe node id should error")
	}

	// A valid local target + raw-capable store builds the subsystem.
	sub, err := newBackupSubsystem(&config.Config{BackupTarget: t.TempDir(), NodeID: "n"}, rawStore{}, fakeSnap{}, nil, "test", logger)
	if err != nil {
		t.Fatalf("newBackupSubsystem: %v", err)
	}
	if sub.Name() != "backup" {
		t.Errorf("name = %q", sub.Name())
	}
}
