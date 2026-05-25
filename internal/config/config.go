// Package config loads silod's runtime configuration from environment
// variables, following the 12-factor convention. It applies sensible
// defaults so the daemon boots with minimal explicit configuration,
// matching silo's "approachable defaults" guideline.
package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// KeySource selects where silo's encryption key comes from.
//
// In cryptography terms this is a key-encryption-key (KEK) that wraps
// per-chunk data-encryption-keys (DEKs); the DEK/KEK split is an
// implementation detail of the chunk store. From the operator's
// perspective there is one key to manage: the silo encryption key.
type KeySource string

const (
	// KeySourceStatic reads the encryption key from SILO_ENCRYPTION_KEY
	// as a base64 string. Intended for development; production
	// deployments should use file or (later) a KMS.
	KeySourceStatic KeySource = "static"
	// KeySourceFile reads the encryption key from the path in SILO_ENCRYPTION_KEY_PATH.
	KeySourceFile KeySource = "file"
)

// Defaults used when an environment variable is unset.
const (
	DefaultGRPCAddr    = "0.0.0.0:7000"
	DefaultHTTPAddr    = "0.0.0.0:7080"
	DefaultDataDir     = "/var/lib/silo"
	DefaultChunkSize   = 4 * 1024 * 1024 // 4 MiB
	DefaultReplication = 3
	DefaultKeySource   = KeySourceStatic
	DefaultLogLevel    = "info"
	DefaultLogFormat   = "text"
)

// Config holds the validated runtime configuration for a silod process.
type Config struct {
	NodeID         string
	GRPCAddr       string
	HTTPAddr       string
	Seeds          []string
	Domain         string
	DataDir        string
	ChunkSize      int64
	Replication    int
	KeySource      KeySource
	EncryptionKey  []byte
	KeyPath        string
	LogLevel       string
	LogFormat      string
}

// EnvFunc resolves an environment variable name to its value.
// Allows Load to be exercised without touching the process environment.
type EnvFunc func(string) string

// osHostname is the hostname resolver used to derive NodeID when SILO_NODE_ID
// is unset. Swapped out in tests.
var osHostname = os.Hostname

// LoadFromEnv loads configuration from the process environment.
// Equivalent to Load(os.Getenv).
func LoadFromEnv() (*Config, error) {
	return Load(os.Getenv)
}

// Load reads configuration values from the given env resolver, applies
// defaults, and validates the result. The returned Config is ready to use.
func Load(env EnvFunc) (*Config, error) {
	cfg := &Config{
		NodeID:    env("SILO_NODE_ID"),
		GRPCAddr:  envDefault(env, "SILO_GRPC_ADDR", DefaultGRPCAddr),
		HTTPAddr:  envDefault(env, "SILO_HTTP_ADDR", DefaultHTTPAddr),
		Domain:    env("SILO_DOMAIN"),
		DataDir:   envDefault(env, "SILO_DATA_DIR", DefaultDataDir),
		KeySource: KeySource(envDefault(env, "SILO_ENCRYPTION_KEY_SOURCE", string(DefaultKeySource))),
		KeyPath:   env("SILO_ENCRYPTION_KEY_PATH"),
		LogLevel:  envDefault(env, "SILO_LOG_LEVEL", DefaultLogLevel),
		LogFormat: envDefault(env, "SILO_LOG_FORMAT", DefaultLogFormat),
	}

	cfg.Seeds = parseSeeds(env("SILO_SEEDS"))

	chunkSize, err := envInt64(env, "SILO_CHUNK_SIZE", DefaultChunkSize)
	if err != nil {
		return nil, err
	}
	cfg.ChunkSize = chunkSize

	repl, err := envInt(env, "SILO_REPLICATION", DefaultReplication)
	if err != nil {
		return nil, err
	}
	cfg.Replication = repl

	if err := loadEncryptionKey(env, cfg); err != nil {
		return nil, err
	}

	if cfg.NodeID == "" {
		h, err := osHostname()
		if err != nil {
			return nil, fmt.Errorf("could not derive a node id from the OS hostname (%v); set SILO_NODE_ID explicitly to a stable, unique value", err)
		}
		cfg.NodeID = h
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate enforces the invariants every silod process needs to start.
// Every returned error is written as an instruction: it names the
// offending variable, shows the bad value when useful, and tells the
// operator what to set it to. See PLAN.md §1 "Errors are instructions".
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return errors.New("node id is empty; set SILO_NODE_ID explicitly, or run on a host with a non-empty hostname")
	}
	if c.GRPCAddr == "" {
		return errors.New("SILO_GRPC_ADDR is required; set it to the host:port silod should listen on, e.g. 0.0.0.0:7000 (the default)")
	}
	if c.HTTPAddr == "" {
		return errors.New("SILO_HTTP_ADDR is required; set it to the host:port that serves /healthz and /metrics, e.g. 0.0.0.0:7080 (the default)")
	}
	if c.DataDir == "" {
		return errors.New("SILO_DATA_DIR is required; set it to a directory silod can read and write, e.g. /var/lib/silo (the default)")
	}
	if c.ChunkSize <= 0 {
		return fmt.Errorf("SILO_CHUNK_SIZE must be a positive number of bytes (got %d); use 4194304 for 4 MiB (the default) or unset the variable to fall back", c.ChunkSize)
	}
	if c.Replication < 1 {
		return fmt.Errorf("SILO_REPLICATION must be at least 1 (got %d); use 3 for production, 2 for two-node test setups, 1 for single-node-only", c.Replication)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("SILO_LOG_LEVEL %q is not recognised; set it to debug, info, warn, or error", c.LogLevel)
	}
	switch c.LogFormat {
	case "text", "json":
	default:
		return fmt.Errorf("SILO_LOG_FORMAT %q is not recognised; set it to text (human-readable) or json (machine-readable)", c.LogFormat)
	}
	return nil
}

func envDefault(env EnvFunc, key, fallback string) string {
	if v := env(key); v != "" {
		return v
	}
	return fallback
}

func envInt(env EnvFunc, key string, fallback int) (int, error) {
	v := env(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number, got %q; see .env.example for a working value", key, v)
	}
	return n, nil
}

func envInt64(env EnvFunc, key string, fallback int64) (int64, error) {
	v := env(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number of bytes, got %q; see .env.example for a working value", key, v)
	}
	return n, nil
}

func parseSeeds(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func loadEncryptionKey(env EnvFunc, cfg *Config) error {
	switch cfg.KeySource {
	case KeySourceStatic:
		raw := env("SILO_ENCRYPTION_KEY")
		if raw == "" {
			return errors.New("SILO_ENCRYPTION_KEY is required when SILO_ENCRYPTION_KEY_SOURCE=static; generate one with: openssl rand -base64 32")
		}
		key, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return fmt.Errorf("SILO_ENCRYPTION_KEY must be a valid base64-encoded value (%v); regenerate with: openssl rand -base64 32", err)
		}
		if len(key) != 32 {
			return fmt.Errorf("SILO_ENCRYPTION_KEY must decode to 32 bytes (AES-256 key length), got %d bytes; regenerate with: openssl rand -base64 32", len(key))
		}
		cfg.EncryptionKey = key
		return nil
	case KeySourceFile:
		if cfg.KeyPath == "" {
			return errors.New("SILO_ENCRYPTION_KEY_PATH is required when SILO_ENCRYPTION_KEY_SOURCE=file; point it at a file containing 32 raw bytes (create one with: openssl rand 32 > /etc/silo/key && chmod 0400 /etc/silo/key)")
		}
		// Actual file read happens in the crypto package at startup; we
		// only validate that the operator told us where to look.
		return nil
	default:
		return fmt.Errorf("SILO_ENCRYPTION_KEY_SOURCE %q is not recognised; set it to static (key in SILO_ENCRYPTION_KEY env var) or file (key at SILO_ENCRYPTION_KEY_PATH)", cfg.KeySource)
	}
}
