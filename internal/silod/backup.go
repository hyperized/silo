package silod

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/config"
)

// newBackupSubsystem builds the periodic backup subsystem for the configured
// target. The chunk store must support raw (still-encrypted) reads, as the
// production FileStore does, so the backup stays encrypted at rest.
func newBackupSubsystem(cfg *config.Config, store chunkstore.Store, ns backup.NamespaceSnapshot, extents backup.ExtentSource, version string, logger *slog.Logger) (*backup.Subsystem, error) {
	target, err := blobstore.Open(cfg.BackupTarget,
		blobstore.WithS3Endpoint(cfg.BackupS3Endpoint),
		blobstore.WithS3Region(cfg.BackupS3Region),
		blobstore.WithS3PathStyle(cfg.BackupS3PathStyle))
	if err != nil {
		return nil, fmt.Errorf("silod: invalid SILO_BACKUP_TARGET (%w)", err)
	}
	if err := backup.ValidateNodeID(cfg.NodeID); err != nil {
		return nil, fmt.Errorf("silod: backups need a node id that is safe in object names (%w)", err)
	}
	chunks, ok := store.(backup.ChunkSource)
	if !ok {
		return nil, errors.New("silod: the chunk store does not support raw reads, so it cannot be backed up; unset SILO_BACKUP_TARGET or use the default file chunk store")
	}
	exporter := backup.NewExporter(chunks, ns, cfg.NodeID, backup.WithVersion(version), backup.WithExtentSource(extents))
	return backup.NewSubsystem(exporter, target, cfg.BackupInterval, logger, backup.WithRetain(cfg.BackupRetain), backup.WithRetainFor(cfg.BackupRetainFor)), nil
}
