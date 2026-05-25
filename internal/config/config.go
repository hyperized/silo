// Package config loads silod's 12-factor environment configuration.
// Defaults match what `make up` writes to deploy/.env so the daemon
// boots with no manual configuration beyond SILO_ENCRYPTION_KEY.
package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// KeySource selects where silod reads the cluster encryption key from.
// Only the operator-managed key is surfaced here; the KEK/DEK split that
// the crypto package uses internally is deliberately hidden so operators
// have one thing to manage, not two.
type KeySource string

const (
	// KeySourceStatic reads the key from SILO_ENCRYPTION_KEY (base64).
	// Convenient for development and docker-compose; not appropriate for
	// production because env vars leak through process listings and
	// process inspection.
	KeySourceStatic KeySource = "static"
	// KeySourceFile reads the key from a file at SILO_ENCRYPTION_KEY_PATH.
	// The recommended production path because filesystem ACLs (0400 on a
	// secret-mounted volume) give a tighter access boundary than env vars.
	KeySourceFile KeySource = "file"
)

const (
	DefaultGRPCAddr    = "0.0.0.0:7000"
	DefaultHTTPAddr    = "0.0.0.0:7080"
	DefaultDataDir     = "/var/lib/silo"
	DefaultChunkSize   = 4 * 1024 * 1024
	DefaultReplication = 3
	DefaultKeySource   = KeySourceStatic
	DefaultLogLevel    = "info"
	DefaultLogFormat   = "text"
)

type Config struct {
	NodeID        string
	GRPCAddr      string
	HTTPAddr      string
	Seeds         []string
	Domain        string
	DataDir       string
	ChunkSize     int64
	Replication   int
	KeySource     KeySource
	EncryptionKey []byte
	KeyPath       string
	LogLevel      string
	LogFormat     string
}

// EnvFunc resolves an env-var name to its value. Injected into Load so
// tests can drive the configuration without mutating the real process
// environment (which would force serial execution and leak between
// packages).
type EnvFunc func(string) string

// osHostname is swappable so tests can exercise the SILO_NODE_ID
// hostname-fallback path deterministically.
var osHostname = os.Hostname

// LoadFromEnv is the production entry point — silod calls this from main.
func LoadFromEnv() (*Config, error) {
	return Load(os.Getenv)
}

// Load reads, defaults, and validates a Config. Defaults are applied
// before validation so the operator's reported error always references
// the value silod would actually use, not the half-built struct.
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

// Validate's errors are intentionally instruction-shaped: see the
// "errors are instructions" project rule.
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
		// File contents are read by the crypto package at startup, not here.
		return nil
	default:
		return fmt.Errorf("SILO_ENCRYPTION_KEY_SOURCE %q is not recognised; set it to static (key in SILO_ENCRYPTION_KEY env var) or file (key at SILO_ENCRYPTION_KEY_PATH)", cfg.KeySource)
	}
}
