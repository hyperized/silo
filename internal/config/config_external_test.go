package config_test

import (
	"encoding/base64"
	"testing"

	"github.com/hyperized/silo/internal/config"
)

// TestLoadFromEnv exercises the process-environment entry point.
// Internal logic is covered by the white-box tests in config_internal_test.go;
// this test asserts only that LoadFromEnv reads what os.Getenv reads.
func TestLoadFromEnv(t *testing.T) {
	t.Setenv("SILO_NODE_ID", "ext-test-node")
	t.Setenv("SILO_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))

	cfg, err := config.LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.NodeID != "ext-test-node" {
		t.Errorf("NodeID: got %q, want ext-test-node", cfg.NodeID)
	}
	if cfg.KeySource != config.KeySourceStatic {
		t.Errorf("KeySource: got %q, want %q", cfg.KeySource, config.KeySourceStatic)
	}
}

func TestLoadFromEnv_PropagatesValidationError(t *testing.T) {
	t.Setenv("SILO_NODE_ID", "n1")
	t.Setenv("SILO_ENCRYPTION_KEY", "") // missing required field
	t.Setenv("SILO_ENCRYPTION_KEY_SOURCE", "static")

	if _, err := config.LoadFromEnv(); err == nil {
		t.Fatal("expected error when SILO_ENCRYPTION_KEY is missing")
	}
}
