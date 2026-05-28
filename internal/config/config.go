// Package config loads silod's 12-factor environment configuration.
// Defaults match what `make up` writes to deploy/.env so the daemon
// boots with no manual configuration beyond SILO_ENCRYPTION_KEY.
package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
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

// Defaults applied by Load when the matching SILO_* env var is unset.
// Every value is overridable; these are the documented out-of-the-box
// listen addresses and tunables.
const (
	DefaultGRPCAddr      = "0.0.0.0:7000"
	DefaultBootstrapAddr = "0.0.0.0:7001"
	DefaultGossipAddr    = "0.0.0.0:7100"
	DefaultHTTPAddr      = "0.0.0.0:7080"
	DefaultDataDir       = "/var/lib/silo"
	DefaultChunkSize     = 4 * 1024 * 1024
	DefaultReplication   = 3
	DefaultKeySource     = KeySourceStatic
	DefaultLogLevel      = "info"
	DefaultLogFormat     = "text"
)

// Config is silod's fully-resolved runtime configuration. Load builds it
// from the environment and Validate must pass before it is used.
type Config struct {
	NodeID             string
	GRPCAddr           string
	GRPCAdvertise      string
	GRPCPeerAdvertise  string
	BootstrapAddr      string
	BootstrapAdvertise string
	GossipAddr         string
	GossipAdvertise    string
	HTTPAddr           string
	Seeds              []string
	Domain             string
	DataDir            string
	ChunkSize          int64
	Replication        int
	// ScrubInterval paces the re-replication scrubber. Zero means "use the
	// scrubber's built-in default"; set SILO_SCRUB_INTERVAL to a Go
	// duration (e.g. 5s, 1m) to override.
	ScrubInterval time.Duration
	KeySource     KeySource
	EncryptionKey []byte
	KeyPath       string
	LogLevel      string
	LogFormat     string

	// TLS material for inter-node and client traffic. All four paths
	// default to siblings under DataDir so a fresh silod boots without
	// any TLS env vars: it mints its own cluster CA (ca.crt + ca.key)
	// and its own node cert (node.crt + node.key) into DataDir on first
	// run, reusing them on every subsequent boot. Operators who want to
	// bring their own CA (the production "bring-your-own-CA" path) point
	// SILO_TLS_CA_CERT and SILO_TLS_CA_KEY at the externally-managed
	// files instead.
	CACertPath   string
	CAKeyPath    string
	NodeCertPath string
	NodeKeyPath  string

	// CAExternal is true when the operator explicitly pointed
	// SILO_TLS_CA_CERT/_KEY at paths outside DataDir — typically a
	// shared volume (docker-compose) or a Kubernetes secret. silod
	// treats those paths as authoritative and waits for them to appear
	// instead of self-minting on first boot, because in a multi-node
	// deployment self-mint would produce a different CA on each node.
	CAExternal bool

	// CASeed flips the wait-for-CA behavior into mint-if-missing for
	// this one node. In docker-compose this is set only on silo-a so
	// it can write the inaugural CA into the shared volume; silo-b/c
	// keep CASeed=false and wait. In production Kubernetes you would
	// pre-populate the secret out-of-band and leave CASeed unset on
	// every node.
	CASeed bool

	// PrintBootstrapToken forces silod to mint and print an additional
	// join token on boot even if a token store already exists. Useful
	// when an operator has lost the first-boot token; flip the env var,
	// restart, then unset it. Default false: silod prints a token only
	// on the very first boot when the store is empty.
	PrintBootstrapToken bool
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
	rawCACert := env("SILO_TLS_CA_CERT")
	cfg := &Config{
		NodeID:              env("SILO_NODE_ID"),
		GRPCAddr:            envDefault(env, "SILO_GRPC_ADDR", DefaultGRPCAddr),
		GRPCAdvertise:       env("SILO_GRPC_ADVERTISE"),
		GRPCPeerAdvertise:   env("SILO_GRPC_PEER_ADVERTISE"),
		BootstrapAddr:       envDefault(env, "SILO_BOOTSTRAP_ADDR", DefaultBootstrapAddr),
		BootstrapAdvertise:  env("SILO_BOOTSTRAP_ADVERTISE"),
		GossipAddr:          envDefault(env, "SILO_GOSSIP_ADDR", DefaultGossipAddr),
		GossipAdvertise:     env("SILO_GOSSIP_ADVERTISE"),
		HTTPAddr:            envDefault(env, "SILO_HTTP_ADDR", DefaultHTTPAddr),
		Domain:              env("SILO_DOMAIN"),
		DataDir:             envDefault(env, "SILO_DATA_DIR", DefaultDataDir),
		KeySource:           KeySource(envDefault(env, "SILO_ENCRYPTION_KEY_SOURCE", string(DefaultKeySource))),
		KeyPath:             env("SILO_ENCRYPTION_KEY_PATH"),
		LogLevel:            envDefault(env, "SILO_LOG_LEVEL", DefaultLogLevel),
		LogFormat:           envDefault(env, "SILO_LOG_FORMAT", DefaultLogFormat),
		CACertPath:          rawCACert,
		CAKeyPath:           env("SILO_TLS_CA_KEY"),
		NodeCertPath:        env("SILO_TLS_NODE_CERT"),
		NodeKeyPath:         env("SILO_TLS_NODE_KEY"),
		CAExternal:          rawCACert != "",
		CASeed:              envBool(env, "SILO_TLS_CA_SEED"),
		PrintBootstrapToken: envBool(env, "SILO_PRINT_BOOTSTRAP_TOKEN"),
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

	scrubInterval, err := envDuration(env, "SILO_SCRUB_INTERVAL")
	if err != nil {
		return nil, err
	}
	cfg.ScrubInterval = scrubInterval

	if err := loadEncryptionKey(env, cfg); err != nil {
		return nil, err
	}

	if cfg.NodeID == "" {
		h, err := osHostname()
		if err != nil {
			return nil, fmt.Errorf("could not derive a node id from the OS hostname (%w); set SILO_NODE_ID explicitly to a stable, unique value", err)
		}
		cfg.NodeID = h
	}

	// Default every TLS path inside DataDir. silod self-mints a cluster
	// CA into ca.{crt,key} on first boot when nothing is on disk yet, so
	// SILO_TLS_CA_CERT/_KEY only need to be set for the bring-your-own-CA
	// production path.
	if cfg.CACertPath == "" {
		cfg.CACertPath = cfg.DataDir + "/ca.crt"
	}
	if cfg.CAKeyPath == "" {
		cfg.CAKeyPath = cfg.DataDir + "/ca.key"
	}
	if cfg.NodeCertPath == "" {
		cfg.NodeCertPath = cfg.DataDir + "/node.crt"
	}
	if cfg.NodeKeyPath == "" {
		cfg.NodeKeyPath = cfg.DataDir + "/node.key"
	}

	// Default each advertise address to the matching listen address with
	// the unspecified host swapped for the loopback. Listening on 0.0.0.0
	// is what every container does, but it is not a routable dial target;
	// publishing 127.0.0.1 means single-node `make up-local` works without
	// extra configuration. Multi-node deployments override SILO_*_ADVERTISE
	// with the container's routable hostname.
	if cfg.GRPCAdvertise == "" {
		cfg.GRPCAdvertise = advertiseFallback(cfg.GRPCAddr)
	}
	// GRPCAdvertise is the operator-facing dial target (what siloctl learns
	// from the Join response); GRPCPeerAdvertise is the data address peers
	// dial for replication. They differ whenever operators reach a node by
	// a different route than peers do — e.g. docker-compose publishes silo-a
	// to the host on 127.0.0.1 while peers reach it on the bridge network.
	// Default the peer address to the loopback fallback so single-node dev
	// works untouched; multi-node deployments set SILO_GRPC_PEER_ADVERTISE
	// to the node's cluster-routable host:port.
	if cfg.GRPCPeerAdvertise == "" {
		cfg.GRPCPeerAdvertise = advertiseFallback(cfg.GRPCAddr)
	}
	if cfg.BootstrapAdvertise == "" {
		cfg.BootstrapAdvertise = advertiseFallback(cfg.BootstrapAddr)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate returns instruction-shaped errors: each message names the
// env var to set and a sane example, per the "errors are instructions"
// project rule.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return errors.New("node id is empty; set SILO_NODE_ID explicitly, or run on a host with a non-empty hostname")
	}
	if c.GRPCAddr == "" {
		return errors.New("SILO_GRPC_ADDR is required; set it to the host:port silod should listen on, e.g. 0.0.0.0:7000 (the default)")
	}
	if c.BootstrapAddr == "" {
		return errors.New("SILO_BOOTSTRAP_ADDR is required; set it to the host:port that serves the one-time join API, e.g. 0.0.0.0:7001 (the default)")
	}
	if c.GossipAddr == "" {
		return errors.New("SILO_GOSSIP_ADDR is required; set it to the host:port silod uses for cluster gossip, e.g. 0.0.0.0:7100 (the default)")
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

// advertiseFallback derives a dial-friendly advertise address from a
// listen address. Listening on the unspecified host (0.0.0.0 or [::])
// is the normal container pattern, but the same string cannot be used
// as a dial target by an outside client. Rewriting the host portion to
// the loopback gives a value that works for same-machine clients and is
// still obviously wrong for any multi-host scenario, prompting the
// operator to set SILO_*_ADVERTISE explicitly.
func advertiseFallback(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return net.JoinHostPort("127.0.0.1", port)
	}
	return listen
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

// envBool reads a boolean env var with a permissive truthy set ("1",
// "true", "yes", case-insensitive). Missing or "0"/"false"/"no"/"" is
// false. Strict parsing would force operators to know the exact spelling;
// a permissive set matches the docker-compose ergonomics they already
// expect from variables like SILO_LOG_FORMAT.
func envBool(env EnvFunc, key string) bool {
	switch strings.ToLower(env(key)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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

// envDuration parses a Go duration from env, returning 0 when unset so the
// consumer can apply its own default. Negative values are rejected: a
// negative scrub interval is always a misconfiguration.
func envDuration(env EnvFunc, key string) (time.Duration, error) {
	v := env(key)
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration like 30s or 5m, got %q", key, v)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s must not be negative, got %q", key, v)
	}
	return d, nil
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
			return fmt.Errorf("SILO_ENCRYPTION_KEY must be a valid base64-encoded value (%w); regenerate with: openssl rand -base64 32", err)
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
