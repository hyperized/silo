package config

import (
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hyperized/silo/internal/diskpressure"
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
	if cfg.BootstrapAddr != DefaultBootstrapAddr {
		t.Errorf("BootstrapAddr default: got %q, want %q", cfg.BootstrapAddr, DefaultBootstrapAddr)
	}
	if cfg.GossipAddr != DefaultGossipAddr {
		t.Errorf("GossipAddr default: got %q, want %q", cfg.GossipAddr, DefaultGossipAddr)
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
	// TLS paths default to siblings of DataDir when unset.
	if cfg.CACertPath != DefaultDataDir+"/ca.crt" {
		t.Errorf("CACertPath default: got %q, want %s/ca.crt", cfg.CACertPath, DefaultDataDir)
	}
	if cfg.CAKeyPath != DefaultDataDir+"/ca.key" {
		t.Errorf("CAKeyPath default: got %q, want %s/ca.key", cfg.CAKeyPath, DefaultDataDir)
	}
	if cfg.NodeCertPath != DefaultDataDir+"/node.crt" {
		t.Errorf("NodeCertPath default: got %q, want %s/node.crt", cfg.NodeCertPath, DefaultDataDir)
	}
	if cfg.NodeKeyPath != DefaultDataDir+"/node.key" {
		t.Errorf("NodeKeyPath default: got %q, want %s/node.key", cfg.NodeKeyPath, DefaultDataDir)
	}
	// Both advertise addresses default to the loopback rewrite of the
	// 0.0.0.0 listen address so single-node dev needs no advertise config.
	if cfg.GRPCAdvertise != "127.0.0.1:7000" {
		t.Errorf("GRPCAdvertise default: got %q, want 127.0.0.1:7000", cfg.GRPCAdvertise)
	}
	if cfg.GRPCPeerAdvertise != "127.0.0.1:7000" {
		t.Errorf("GRPCPeerAdvertise default: got %q, want 127.0.0.1:7000", cfg.GRPCPeerAdvertise)
	}
	// Zero means "let the scrubber apply its own default"; Load does not
	// bake one in so there is a single source of truth.
	if cfg.ScrubInterval != 0 {
		t.Errorf("ScrubInterval default: got %v, want 0", cfg.ScrubInterval)
	}
	// The reaper's pacing/age likewise default to 0 so the reaper owns the
	// fallbacks (a single source of truth).
	if cfg.ExtentReapAfter != 0 || cfg.ExtentReapInterval != 0 {
		t.Errorf("extent reap defaults: got after=%v interval=%v, want 0/0", cfg.ExtentReapAfter, cfg.ExtentReapInterval)
	}
	// Retention, unlike the scrub interval, defaults to a concrete value:
	// zero would let GC resurrect deleted entries.
	if cfg.TombstoneRetention != DefaultTombstoneRetention {
		t.Errorf("TombstoneRetention default: got %v, want %v", cfg.TombstoneRetention, DefaultTombstoneRetention)
	}
	// Skew threshold, like retention, defaults concretely: zero would alert on
	// every observation.
	if cfg.MaxClockSkew != DefaultMaxClockSkew {
		t.Errorf("MaxClockSkew default: got %v, want %v", cfg.MaxClockSkew, DefaultMaxClockSkew)
	}
}

func TestLoad_BadTombstoneRetention(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":             "n",
		"SILO_ENCRYPTION_KEY":      validBase64Key(t),
		"SILO_TOMBSTONE_RETENTION": "eventually",
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_TOMBSTONE_RETENTION") {
		t.Fatalf("got %v, want a SILO_TOMBSTONE_RETENTION parse error", err)
	}
}

func TestLoad_DiskThresholds(t *testing.T) {
	// Custom valid thresholds are parsed and applied.
	cfg, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":             "n",
		"SILO_ENCRYPTION_KEY":      validBase64Key(t),
		"SILO_DISK_PRESSURE_HIGH":  "0.90",
		"SILO_DISK_PRESSURE_CLEAR": "0.85",
		"SILO_DISK_PRESSURE_HARD":  "0.97",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DiskThresholds.High != 0.90 || cfg.DiskThresholds.Clear != 0.85 || cfg.DiskThresholds.Hard != 0.97 {
		t.Errorf("DiskThresholds = %+v, want 0.90/0.85/0.97", cfg.DiskThresholds)
	}
}

func TestLoad_ExtentReplicationDefaultsOnAndOptsOut(t *testing.T) {
	base := map[string]string{"SILO_NODE_ID": "n", "SILO_ENCRYPTION_KEY": validBase64Key(t)}
	cfg, err := Load(envMap(base))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.ExtentReplication {
		t.Error("ExtentReplication should default to true")
	}

	out := map[string]string{"SILO_NODE_ID": "n", "SILO_ENCRYPTION_KEY": validBase64Key(t), "SILO_EXTENT_REPLICATION": "false"}
	cfg, err = Load(envMap(out))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ExtentReplication {
		t.Error("SILO_EXTENT_REPLICATION=false should disable replica-set serving")
	}
}

func TestLoad_DiskSteeringDefaultsOnAndOptsOut(t *testing.T) {
	base := map[string]string{"SILO_NODE_ID": "n", "SILO_ENCRYPTION_KEY": validBase64Key(t)}

	// Unset -> default true.
	cfg, err := Load(envMap(base))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.DiskSteering {
		t.Error("DiskSteering should default to true")
	}

	// Explicit opt-out.
	out := map[string]string{"SILO_NODE_ID": "n", "SILO_ENCRYPTION_KEY": validBase64Key(t), "SILO_DISK_PRESSURE_STEERING": "false"}
	cfg, err = Load(envMap(out))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DiskSteering {
		t.Error("SILO_DISK_PRESSURE_STEERING=false should disable steering")
	}

	// Explicit opt-in (truthy spelling).
	on := map[string]string{"SILO_NODE_ID": "n", "SILO_ENCRYPTION_KEY": validBase64Key(t), "SILO_DISK_PRESSURE_STEERING": "on"}
	cfg, err = Load(envMap(on))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.DiskSteering {
		t.Error("SILO_DISK_PRESSURE_STEERING=on should enable steering")
	}
}

func TestLoad_DiskThresholdErrors(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"high unparseable", map[string]string{"SILO_DISK_PRESSURE_HIGH": "lots"}, "SILO_DISK_PRESSURE_HIGH"},
		{"clear unparseable", map[string]string{"SILO_DISK_PRESSURE_CLEAR": "lots"}, "SILO_DISK_PRESSURE_CLEAR"},
		{"hard unparseable", map[string]string{"SILO_DISK_PRESSURE_HARD": "lots"}, "SILO_DISK_PRESSURE_HARD"},
		{"invalid policy", map[string]string{"SILO_DISK_PRESSURE_CLEAR": "0.90"}, "must be below high"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"SILO_NODE_ID": "n", "SILO_ENCRYPTION_KEY": validBase64Key(t)}
			for k, v := range tc.env {
				env[k] = v
			}
			_, err := Load(envMap(env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestLoad_BadMaxClockSkew(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
		"SILO_MAX_CLOCK_SKEW": "a-while",
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_MAX_CLOCK_SKEW") {
		t.Fatalf("got %v, want a SILO_MAX_CLOCK_SKEW parse error", err)
	}
}

func TestLoad_BadScrubInterval(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
		"SILO_SCRUB_INTERVAL": "soon",
	}))
	if err == nil || !strings.Contains(err.Error(), "SILO_SCRUB_INTERVAL") {
		t.Fatalf("got %v, want a SILO_SCRUB_INTERVAL parse error", err)
	}
}

func TestLoad_NegativeScrubInterval(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":        "n",
		"SILO_ENCRYPTION_KEY": validBase64Key(t),
		"SILO_SCRUB_INTERVAL": "-5s",
	}))
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("got %v, want a negative-duration error", err)
	}
}

func TestLoad_BadExtentReapDurations(t *testing.T) {
	for _, key := range []string{"SILO_EXTENT_REAP_AFTER", "SILO_EXTENT_REAP_INTERVAL"} {
		_, err := Load(envMap(map[string]string{
			"SILO_NODE_ID":        "n",
			"SILO_ENCRYPTION_KEY": validBase64Key(t),
			key:                   "whenever",
		}))
		if err == nil || !strings.Contains(err.Error(), key) {
			t.Fatalf("%s: got %v, want a parse error mentioning the key", key, err)
		}
	}
}

func TestLoad_OverridesFromEnv(t *testing.T) {
	cfg, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":               "alpha",
		"SILO_GRPC_ADDR":             "127.0.0.1:9000",
		"SILO_BOOTSTRAP_ADDR":        "127.0.0.1:9001",
		"SILO_GOSSIP_ADDR":           "127.0.0.1:9100",
		"SILO_GOSSIP_ADVERTISE":      "node-host:9100",
		"SILO_HTTP_ADDR":             "127.0.0.1:9080",
		"SILO_NBD_ADDR":              "127.0.0.1:10809",
		"SILO_DOMAIN":                "silo.example",
		"SILO_DATA_DIR":              "/tmp/silo",
		"SILO_CHUNK_SIZE":            "1048576",
		"SILO_REPLICATION":           "5",
		"SILO_SCRUB_INTERVAL":        "5s",
		"SILO_TOMBSTONE_RETENTION":   "12h",
		"SILO_MAX_CLOCK_SKEW":        "2s",
		"SILO_LOG_LEVEL":             "debug",
		"SILO_LOG_FORMAT":            "json",
		"SILO_ENCRYPTION_KEY":        validBase64Key(t),
		"SILO_TLS_CA_CERT":           "/etc/silo/ca.crt",
		"SILO_TLS_CA_KEY":            "/etc/silo/ca.key",
		"SILO_TLS_NODE_CERT":         "/etc/silo/node.crt",
		"SILO_TLS_NODE_KEY":          "/etc/silo/node.key",
		"SILO_TLS_CRL":               "/etc/silo/revoked.crl",
		"SILO_REQUIRE_TOKENS":        "1",
		"SILO_EXTENT_REPLICATION":    "false",
		"SILO_EXTENT_REAP_AFTER":     "30m",
		"SILO_EXTENT_REAP_INTERVAL":  "2m",
		"SILO_PRINT_BOOTSTRAP_TOKEN": "yes",
		"SILO_TLS_CA_SEED":           "true",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := &Config{
		NodeID:              "alpha",
		GRPCAddr:            "127.0.0.1:9000",
		GRPCAdvertise:       "127.0.0.1:9000",
		GRPCPeerAdvertise:   "127.0.0.1:9000",
		BootstrapAddr:       "127.0.0.1:9001",
		BootstrapAdvertise:  "127.0.0.1:9001",
		GossipAddr:          "127.0.0.1:9100",
		GossipAdvertise:     "node-host:9100",
		HTTPAddr:            "127.0.0.1:9080",
		NBDAddr:             "127.0.0.1:10809",
		Domain:              "silo.example",
		DataDir:             "/tmp/silo",
		ChunkSize:           1 << 20,
		Replication:         5,
		ScrubInterval:       5 * time.Second,
		TombstoneRetention:  12 * time.Hour,
		MaxClockSkew:        2 * time.Second,
		KeySource:           KeySourceStatic,
		LogLevel:            "debug",
		LogFormat:           "json",
		EncryptionKey:       cfg.EncryptionKey, // compared by length below
		CACertPath:          "/etc/silo/ca.crt",
		CAKeyPath:           "/etc/silo/ca.key",
		NodeCertPath:        "/etc/silo/node.crt",
		NodeKeyPath:         "/etc/silo/node.key",
		CRLPath:             "/etc/silo/revoked.crl",
		RequireTokens:       true,
		DiskSteering:        true,
		ExtentReplication:   false, // overridden from its default of true
		ExtentReapAfter:     30 * time.Minute,
		ExtentReapInterval:  2 * time.Minute,
		DiskThresholds:      diskpressure.DefaultThresholds(),
		CAExternal:          true,
		CASeed:              true,
		PrintBootstrapToken: true,
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

func TestLoad_KMSSources(t *testing.T) {
	// AWS/GCP: accepted with a wrapped-key path + key id.
	for _, src := range []string{"aws-kms", "gcp-kms"} {
		cfg, err := Load(envMap(map[string]string{
			"SILO_NODE_ID":               "n1",
			"SILO_ENCRYPTION_KEY_SOURCE": src,
			"SILO_ENCRYPTION_KEY_PATH":   "/etc/silo/wrapped",
			"SILO_KMS_KEY_ID":            "key-123",
		}))
		if err != nil {
			t.Fatalf("%s Load: %v", src, err)
		}
		if string(cfg.KeySource) != src || cfg.KMSKeyID != "key-123" {
			t.Errorf("%s: got %q / %q", src, cfg.KeySource, cfg.KMSKeyID)
		}
		// Missing key id is rejected.
		if _, err := Load(envMap(map[string]string{
			"SILO_NODE_ID": "n1", "SILO_ENCRYPTION_KEY_SOURCE": src, "SILO_ENCRYPTION_KEY_PATH": "/w",
		})); err == nil || !strings.Contains(err.Error(), "SILO_KMS_KEY_ID is required") {
			t.Errorf("%s without key id: got %v", src, err)
		}
		// Missing wrapped-key path is rejected.
		if _, err := Load(envMap(map[string]string{
			"SILO_NODE_ID": "n1", "SILO_ENCRYPTION_KEY_SOURCE": src, "SILO_KMS_KEY_ID": "k",
		})); err == nil || !strings.Contains(err.Error(), "SILO_ENCRYPTION_KEY_PATH is required") {
			t.Errorf("%s without key path: got %v", src, err)
		}
	}

	// Azure: needs the vault URL + key name.
	cfg, err := Load(envMap(map[string]string{
		"SILO_NODE_ID":               "n1",
		"SILO_ENCRYPTION_KEY_SOURCE": "azure-kv",
		"SILO_ENCRYPTION_KEY_PATH":   "/etc/silo/wrapped",
		"SILO_KMS_VAULT_URL":         "https://v.vault.azure.net/",
		"SILO_KMS_KEY_NAME":          "wrapkey",
	}))
	if err != nil || cfg.KMSVaultURL == "" || cfg.KMSKeyName != "wrapkey" {
		t.Fatalf("azure-kv: cfg=%+v err=%v", cfg, err)
	}
	if _, err := Load(envMap(map[string]string{
		"SILO_NODE_ID": "n1", "SILO_ENCRYPTION_KEY_SOURCE": "azure-kv", "SILO_ENCRYPTION_KEY_PATH": "/w",
	})); err == nil || !strings.Contains(err.Error(), "SILO_KMS_VAULT_URL") {
		t.Errorf("azure-kv without vault: got %v", err)
	}
	// Azure without the wrapped-key path is rejected too.
	if _, err := Load(envMap(map[string]string{
		"SILO_NODE_ID": "n1", "SILO_ENCRYPTION_KEY_SOURCE": "azure-kv", "SILO_KMS_VAULT_URL": "https://v/", "SILO_KMS_KEY_NAME": "k",
	})); err == nil || !strings.Contains(err.Error(), "SILO_ENCRYPTION_KEY_PATH is required") {
		t.Errorf("azure-kv without key path: got %v", err)
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

func TestEnvBool(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"1", true},
		{"true", true},
		{"True", true},
		{"YES", true},
		{"on", true},
		{"", false},
		{"0", false},
		{"false", false},
		{"no", false},
		{"definitely", false},
	}
	for _, tc := range cases {
		t.Run(tc.val, func(t *testing.T) {
			env := func(string) string { return tc.val }
			if got := envBool(env, "X"); got != tc.want {
				t.Errorf("envBool(%q): got %v, want %v", tc.val, got, tc.want)
			}
		})
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
		NodeID:        "n1",
		GRPCAddr:      "0:0",
		BootstrapAddr: "0:1",
		GossipAddr:    "0:2",
		HTTPAddr:      "0:0",
		DataDir:       "/d",
		ChunkSize:     1,
		Replication:   1,
		LogLevel:      "info",
		LogFormat:     "text",
	}

	cases := []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"missing node id", func(c *Config) { c.NodeID = "" }, "node id is empty"},
		{"missing grpc addr", func(c *Config) { c.GRPCAddr = "" }, "SILO_GRPC_ADDR"},
		{"missing bootstrap addr", func(c *Config) { c.BootstrapAddr = "" }, "SILO_BOOTSTRAP_ADDR"},
		{"missing gossip addr", func(c *Config) { c.GossipAddr = "" }, "SILO_GOSSIP_ADDR"},
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

func TestAdvertiseFallback(t *testing.T) {
	cases := []struct {
		name   string
		listen string
		want   string
	}{
		{"ipv4 unspecified rewritten to loopback", "0.0.0.0:7000", "127.0.0.1:7000"},
		{"ipv6 unspecified rewritten to loopback", "[::]:7000", "127.0.0.1:7000"},
		{"empty host rewritten to loopback", ":7000", "127.0.0.1:7000"},
		{"routable host left unchanged", "10.0.0.5:7000", "10.0.0.5:7000"},
		{"address with no port returned verbatim", "no-port-here", "no-port-here"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := advertiseFallback(tc.listen); got != tc.want {
				t.Errorf("advertiseFallback(%q) = %q, want %q", tc.listen, got, tc.want)
			}
		})
	}
}
