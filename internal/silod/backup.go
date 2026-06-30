package silod

import (
	"fmt"
	"log/slog"

	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/config"
)

// newBackupSubsystem builds the periodic backup subsystem for the configured
// target. The chunk store must support raw (still-encrypted) reads — the
// production FileStore does — so the backup stays encrypted at rest.
func newBackupSubsystem(cfg *config.Config, store chunkstore.Store, ns backup.NamespaceSnapshot, extents backup.ExtentSource, logger *slog.Logger) (*backup.Subsystem, error) {
	target, err := blobstore.Open(cfg.BackupTarget)
	if err != nil {
		return nil, fmt.Errorf("silod: invalid SILO_BACKUP_TARGET (%w)", err)
	}
	chunks, ok := store.(backup.ChunkSource)
	if !ok {
		return nil, fmt.Errorf("silod: the chunk store does not support raw reads for backup")
	}
	exporter := backup.NewExporter(chunks, ns, cfg.NodeID, backup.WithExtentSource(extents))
	return backup.NewSubsystem(exporter, target, cfg.BackupInterval, logger), nil
}
