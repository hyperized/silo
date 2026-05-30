package silod

import (
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/crypto"
)

// keyProvider resolves the cluster encryption-key provider from config. The
// source string is validated in config.Load, so an unrecognised value cannot
// reach here; static is the safe default. Adding a KMS source is a new case
// plus a crypto.KeyProvider implementation — nothing else changes.
func keyProvider(cfg *config.Config) crypto.KeyProvider {
	if cfg.KeySource == config.KeySourceFile {
		return crypto.FileKeyProvider(cfg.KeyPath)
	}
	return crypto.StaticKeyProvider(cfg.EncryptionKey)
}
