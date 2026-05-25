package config

import (
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// envMap turns a map into an EnvFunc for the tests below.
func envMap(m map[string]string) EnvFunc {
	return func(k string) string {
		return m[k]
	}
}

func validBase64Key(t *testing.T) string {
	t.Helper()
	return base64.StdEncoding.EncodeToString(make([]byte, 32))
}

func TestLoad_AppliesDefaults(t *testing.T) {
	cfg, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "node-a",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.GRPCAddr != DefaultGRPCAddr {
		t.Errorf("GRPCAddr default: got %q, want %q", cfg.GRPCAddr, DefaultGRPCAddr)
	}
	if cfg.HTTPAddr != DefaultHTTPAddr {
		t.Errorf("HTTPAddr default: got %q, want %q", cfg.HTTPAddr, DefaultHTTPAddr)
	}
	if cfg.DataDir != DefaultDataDir {
		t.Errorf("DataDir default: got %q, want %q", cfg.DataDir, DefaultDataDir)
	}
	if cfg.ChunkSize != DefaultChunkSize {
		t.Errorf("ChunkSize default: got %d, want %d", cfg.ChunkSize, DefaultChunkSize)
	}
	if cfg.Replication != DefaultReplication {
		t.Errorf("Replication default: got %d, want %d", cfg.Replication, DefaultReplication)
	}
	if cfg.KeySource != DefaultKeySource {
		t.Errorf("KeySource default: got %q, want %q", cfg.KeySource, DefaultKeySource)
	}
	if cfg.LogLevel != DefaultLogLevel {
		t.Errorf("LogLevel default: got %q, want %q", cfg.LogLevel, DefaultLogLevel)
	}
	if cfg.LogFormat != DefaultLogFormat {
		t.Errorf("LogFormat default: got %q, want %q", cfg.LogFormat, DefaultLogFormat)
	}
	if len(cfg.Seeds) != 0 {
		t.Errorf("Seeds default: got %v, want empty", cfg.Seeds)
	}
	if len(cfg.EncryptionKey) != 32 {
		t.Errorf("EncryptionKey length: got %d, want 32", len(cfg.EncryptionKey))
	}
}

func TestLoad_OverridesFromEnv(t *testing.T) {
	cfg, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "alpha",
		"SILO_GRPC_ADDR":      "127.0.0.1:9000",
		"SILO_HTTP_ADDR":      "127.0.0.1:9080",
		"SILO_DOMAIN":         "silo.example",
		"SILO_DATA_DIR":       "/tmp/silo",
		"SILO_CHUNK_SIZE":     "1048576",
		"SILO_REPLICATION":    "5",
		"SILO_LOG_LEVEL":      "debug",
		"SILO_LOG_FORMAT":     "json",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := &Config{
		NodeID:        "alpha",
		GRPCAddr:      "127.0.0.1:9000",
		HTTPAddr:      "127.0.0.1:9080",
		Domain:        "silo.example",
		DataDir:       "/tmp/silo",
		ChunkSize:     1 << 20,
		Replication:   5,
		KeySource:     KeySourceStatic,
		LogLevel:      "debug",
		LogFormat:     "json",
		EncryptionKey: cfg.EncryptionKey, // compared by length below
	}
	got := *cfg
	got.Seeds = nil
	got.EncryptionKey = want.EncryptionKey
	if !reflect.DeepEqual(&got, want) {
		t.Errorf("Config mismatch:\n got  %+v\n want %+v", got, *want)
	}
}

func TestLoad_ParsesSeeds(t *testing.T) {
	cfg, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n1",
		"SILO_SEEDS":          "  host1:7000 , host2:7000,, host3:7000 ",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"host1:7000", "host2:7000", "host3:7000"}
	if !reflect.DeepEqual(cfg.Seeds, want) {
		t.Errorf("Seeds: got %v, want %v", cfg.Seeds, want)
	}
}

func TestLoad_EmptySeedsString(t *testing.T) {
	cfg, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n1",
		"SILO_SEEDS":          " , , ",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Seeds != nil {
		t.Errorf("Seeds: got %v, want nil", cfg.Seeds)
	}
}

func TestLoad_HostnameFallback(t *testing.T) {
	prev := osHostname
	t.Cleanup(func() { osHostname = prev })
	osHostname = func() (string, error) { return "derived-host", nil }

	cfg, err := Load(envMap(map[string]string{
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.NodeID != "derived-host" {
		t.Errorf("NodeID: got %q, want derived-host", cfg.NodeID)
	}
}

func TestLoad_HostnameFailure(t *testing.T) {
	prev := osHostname
	t.Cleanup(func() { osHostname = prev })
	osHostname = func() (string, error) { return "", errors.New("nope") }

	_, err := Load(envMap(map[string]string{
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err == nil || !strings.Contains(err.Error(), "node id") || !strings.Contains(err.Error(), "SILO_NODE_ID") {
		t.Errorf("expected actionable hostname-derive error, got %v", err)
	}
}

func TestLoad_StaticKeyMissing(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID": "n1",
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_ENCRYPTION_KEY is required") {
		t.Errorf("expected missing-key error, got %v", err)
	}
}

func TestLoad_StaticKeyInvalidBase64(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n1",
		"SILO_ENCRYPTION_KEY": "not!!base64!!",
	}))
	if err == nil || !strings.Contains(err.Error(), "valid base64") {
		t.Errorf("expected base64 error, got %v", err)
	}
}

func TestLoad_StaticKeyWrongLength(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n1",
		"SILO_ENCRYPTION_KEY": base64.StdEncoding.EncodeToString([]byte("too-short")),
	}))
	if err == nil || !strings.Contains(err.Error(), "32 bytes") {
		t.Errorf("expected 32-byte error, got %v", err)
	}
}

func TestLoad_FileKeyRequiresPath(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":               "n1",
		"SILO_ENCRYPTION_KEY_SOURCE": "file",
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_ENCRYPTION_KEY_PATH is required") {
		t.Errorf("expected SILO_ENCRYPTION_KEY_PATH error, got %v", err)
	}
}

func TestLoad_FileKeyAccepted(t *testing.T) {
	cfg, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":               "n1",
		"SILO_ENCRYPTION_KEY_SOURCE": "file",
		"SILO_ENCRYPTION_KEY_PATH":   "/etc/silo/key",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.KeySource != KeySourceFile || cfg.KeyPath != "/etc/silo/key" {
		t.Errorf("file key: got %q/%q", cfg.KeySource, cfg.KeyPath)
	}
	if cfg.EncryptionKey != nil {
		t.Errorf("EncryptionKey material should be unset for file source, got %d bytes", len(cfg.EncryptionKey))
	}
}

func TestLoad_UnknownKeySource(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":               "n1",
		"SILO_ENCRYPTION_KEY_SOURCE": "vault",
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_ENCRYPTION_KEY_SOURCE") {
		t.Errorf("expected unknown-source error, got %v", err)
	}
}

func TestLoad_BadChunkSize(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n1",
		"SILO_CHUNK_SIZE":     "not-a-number",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_CHUNK_SIZE") {
		t.Errorf("expected chunk-size parse error, got %v", err)
	}
}

func TestLoad_ValidationErrorAfterParse(t *testing.T) {
	// Values parse fine but Validate rejects them — covers the
	// post-parse Validate path in Load.
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n1",
		"SILO_LOG_LEVEL":      "shout",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_LOG_LEVEL") {
		t.Errorf("expected validation error from Load, got %v", err)
	}
}

func TestLoad_BadReplication(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n1",
		"SILO_REPLICATION":    "many",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_REPLICATION") {
		t.Errorf("expected replication parse error, got %v", err)
	}
}

func TestValidate(t *testing.T) {
	base := Config{
		NodeID:      "n1",
		GRPCAddr:    "0:0",
		HTTPAddr:    "0:0",
		DataDir:     "/d",
		ChunkSize:   1,
		Replication: 1,
		LogLevel:    "info",
		LogFormat:   "text",
	}

	cases := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"missing node id", func(c *Config) { c.NodeID = "" }, "node id is empty"},
		{"missing grpc addr", func(c *Config) { c.GRPCAddr = "" }, "SILO_GRPC_ADDR"},
		{"missing http addr", func(c *Config) { c.HTTPAddr = "" }, "SILO_HTTP_ADDR"},
		{"missing data dir", func(c *Config) { c.DataDir = "" }, "SILO_DATA_DIR"},
		{"zero chunk", func(c *Config) { c.ChunkSize = 0 }, "SILO_CHUNK_SIZE"},
		{"negative replication", func(c *Config) { c.Replication = 0 }, "SILO_REPLICATION"},
		{"unknown log level", func(c *Config) { c.LogLevel = "loud" }, "SILO_LOG_LEVEL"},
		{"unknown log format", func(c *Config) { c.LogFormat = "yaml" }, "SILO_LOG_FORMAT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mut(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Validate: got %v, want substring %q", err, tc.want)
			}
		})
	}

	t.Run("ok", func(t *testing.T) {
		c := base
		if err := c.Validate(); err != nil {
			t.Errorf("Validate baseline: %v", err)
		}
	})
}
