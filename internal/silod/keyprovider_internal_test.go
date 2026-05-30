package silod

import (
	"testing"

	"github.com/hyperized/silo/internal/config"
)

func TestKeyProvider_SelectsSource(t *testing.T) {
	staticKey := make([]byte, 32)
	if p := keyProvider(&config.Config{KeySource: config.KeySourceStatic, EncryptionKey: staticKey}); p.SourceName() != "static" {
		t.Errorf("static source = %q", p.SourceName())
	}
	if p := keyProvider(&config.Config{KeySource: config.KeySourceFile, KeyPath: "/etc/silo/key"}); p.SourceName() != "file" {
		t.Errorf("file source = %q", p.SourceName())
	}
	// An unset/unknown source falls back to static (config.Load validates the
	// real input, so this is just a safe default).
	if p := keyProvider(&config.Config{}); p.SourceName() != "static" {
		t.Errorf("default source = %q", p.SourceName())
	}

	// The KMS sources resolve to their cloud decrypters.
	kmsCases := map[config.KeySource]string{
		config.KeySourceAWSKMS:  "aws-kms",
		config.KeySourceGCPKMS:  "gcp-kms",
		config.KeySourceAzureKV: "azure-kv",
	}
	for src, want := range kmsCases {
		cfg := &config.Config{KeySource: src, KeyPath: "/wrapped", KMSKeyID: "k", KMSVaultURL: "https://v.vault.azure.net/", KMSKeyName: "n"}
		if p := keyProvider(cfg); p.SourceName() != want {
			t.Errorf("%s source = %q, want %q", src, p.SourceName(), want)
		}
	}
}
