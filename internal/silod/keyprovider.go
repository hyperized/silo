package silod

import (
	"github.com/hyperized/silo/internal/config"
	"github.com/hyperized/silo/internal/crypto"
	"github.com/hyperized/silo/internal/kms"
)

// keyProvider resolves the cluster encryption-key provider from config. The
// source string is validated in config.Load, so an unrecognised value cannot
// reach here; static is the safe default. The KMS sources wrap a cloud
// Decrypter (envelope encryption) around the wrapped-key file at KeyPath.
func keyProvider(cfg *config.Config) crypto.KeyProvider {
	switch cfg.KeySource {
	case config.KeySourceFile:
		return crypto.FileKeyProvider(cfg.KeyPath)
	case config.KeySourceAWSKMS:
		return crypto.KMSKeyProvider(kms.NewAWS(cfg.KMSKeyID), cfg.KeyPath)
	case config.KeySourceGCPKMS:
		return crypto.KMSKeyProvider(kms.NewGCP(cfg.KMSKeyID), cfg.KeyPath)
	case config.KeySourceAzureKV:
		return crypto.KMSKeyProvider(kms.NewAzure(cfg.KMSVaultURL, cfg.KMSKeyName), cfg.KeyPath)
	default:
		return crypto.StaticKeyProvider(cfg.EncryptionKey)
	}
}
