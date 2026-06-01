package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	bootstrapv1 "github.com/hyperized/silo/api/proto/silo/bootstrap/v1"
	"github.com/hyperized/silo/internal/captoken"
)

// defaultTokenTTL is how long a minted capability token is valid when --ttl is
// omitted. A day balances "long enough to be useful" against "short enough that
// a leaked token expires on its own"; longer-lived automation should mint with
// an explicit, justified --ttl.
const defaultTokenTTL = 24 * time.Hour

// authConfig is the small JSON file siloctl drops next to its certs so
// subsequent commands can find the default server without re-typing it.
// Kept deliberately small — there is no global config registry to drift
// out of sync with.
type authConfig struct {
	// DefaultServer was the bootstrap host:port the operator typed into
	// 'auth init'. Kept for diagnostic display in 'auth status' so an
	// operator can tell which silod minted these credentials, but it is
	// not where chunk RPCs go.
	DefaultServer string `json:"default_server"`
	// DefaultGRPCServer is the mTLS gRPC dial target siloctl uses for
	// every command that follows 'auth init'. silod returns this value
	// in the Join response so the operator never has to know which port
	// the data plane is on.
	DefaultGRPCServer string `json:"default_grpc_server,omitempty"`
	Principal         string `json:"principal"`
	IssuedAt          string `json:"issued_at"`
}

// authDialer is the seam siloctl auth uses to reach silod's bootstrap
// listener. Production wires it to grpc.NewClient with server-only TLS;
// tests substitute a passthrough dialer so they can drive Join over an
// in-process pair without keeping a real socket open.
var authDialer = func(target string, tlsCfg *tls.Config) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
}

// userConfigDir returns the directory siloctl uses for cluster-issued
// material. Wrapped so tests can substitute a temp dir without needing
// to mutate the host's HOME env var.
var userConfigDir = func() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("could not determine the user config dir (%w); pass --config-dir to siloctl explicitly", err)
	}
	return filepath.Join(dir, "silo"), nil
}

func runAuth(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelpFlag(args[0]) {
		printAuthUsage(stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}

	sub := args[0]
	rest := args[1:]
	switch sub {
	case "init":
		return runAuthInit(rest, stdout, stderr)
	case "status":
		return runAuthStatus(rest, stdout, stderr)
	case "mint-token":
		return runAuthMintToken(rest, stdout, stderr)
	case "clean":
		return runAuthClean(rest, stdin, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "siloctl auth: unknown subcommand %q. Run 'siloctl auth help'.\n", sub)
		return 2
	}
}

func printAuthUsage(w io.Writer) {
	fmt.Fprint(w, `siloctl auth — claim and inspect cluster credentials

Usage:
  siloctl auth init --token <token> --server <host:port> [--server-fingerprint sha256:<hex>]
                    [--principal <user@host>] [--config-dir <path>]

  siloctl auth status [--config-dir <path>]

  siloctl auth mint-token --principal <who> --cap <capability> [--cap …]
                    [--ttl <duration>] [--ca-cert <path>] [--ca-key <path>]

  siloctl auth clean [--config-dir <path>] [--yes]

'init' redeems a one-time join token printed by silod on first boot and writes
the resulting CA cert, client cert, and client key into the config directory
(default: $XDG_CONFIG_HOME/silo or ~/.config/silo).

Pin the server's fingerprint with --server-fingerprint to refuse mismatched
servers; omit it for trust-on-first-use (a loud warning is printed).

'mint-token' signs a scoped capability token (offline, with the cluster CA key)
that a CSI/FUSE/operator client presents via SILO_TOKEN when silod runs with
SILO_REQUIRE_TOKENS=1. Capabilities: chunk:read, chunk:write, namespace:read,
namespace:write, status:read, node:admin, or '*' for all. Run it on a host that
holds the CA key.

'clean' deletes the cached cluster credentials so the next 'auth init' starts
from a clean slate. Prompts before deleting; pass --yes to skip the prompt.
`)
}

func runAuthInit(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("siloctl auth init", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		token       = fs.String("token", "", "one-time join token printed by silod on first boot (required)")
		server      = fs.String("server", envDefault("SILO_BOOTSTRAP_SERVER", ""), "silod bootstrap address (host:port)")
		fingerprint = fs.String("server-fingerprint", "", "expected server cert fingerprint, e.g. sha256:abcdef…")
		principal   = fs.String("principal", defaultPrincipal(), "identity to attribute the cert to in audit logs")
		configDir   = fs.String("config-dir", "", "where to write ca.crt, client.crt, client.key (default: per-user config dir)")
	)
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	if *token == "" {
		fmt.Fprintln(stderr, "siloctl auth init: --token is required; copy the value silod printed on first boot")
		return 2
	}
	if *server == "" {
		fmt.Fprintln(stderr, "siloctl auth init: --server is required; set it to the silod bootstrap address (e.g. 127.0.0.1:7001) or export SILO_BOOTSTRAP_SERVER")
		return 2
	}

	dir, err := resolveConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth init: %v\n", err)
		return 1
	}

	tlsCfg, fpCheck := buildBootstrapTLSConfig(*fingerprint, *server)
	conn, err := authDialer(*server, tlsCfg)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth init: could not dial silod at %q (%v); check the server address and that the bootstrap listener is up\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := bootstrapv1.NewBootstrapClient(conn).Join(ctx, &bootstrapv1.JoinRequest{Token: *token, Principal: *principal})
	if err != nil {
		return reportRPC(stderr, "auth init", err)
	}

	if *fingerprint == "" {
		fmt.Fprintf(stderr, "siloctl auth init: WARNING: connecting without --server-fingerprint (trust-on-first-use). Verify this fingerprint matches the value silod printed on boot:\n  %s\n", fpCheck.observed())
	}

	if err := writeAuthMaterial(dir, resp.CaCertPem, resp.ClientCertPem, resp.ClientKeyPem, *server, resp.GrpcAddress, *principal); err != nil {
		fmt.Fprintf(stderr, "siloctl auth init: %v\n", err)
		return 1
	}

	chunkTarget := resp.GrpcAddress
	if chunkTarget == "" {
		// Older silods (or a misconfigured deployment) may not advertise
		// the gRPC address; falling back to the bootstrap server keeps the
		// "you can now run" line honest about what's actually in the
		// config file, even though chunk RPCs will not work against a
		// bootstrap-only port.
		chunkTarget = *server
	}
	fmt.Fprintf(stdout, "Wrote cluster credentials to %s\n  ca.crt       %d bytes\n  client.crt   %d bytes\n  client.key   %d bytes\nYou can now run siloctl chunk … against %s.\n",
		dir, len(resp.CaCertPem), len(resp.ClientCertPem), len(resp.ClientKeyPem), chunkTarget)
	return 0
}

func runAuthMintToken(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("siloctl auth mint-token", flag.ContinueOnError)
	fs.SetOutput(stderr)
	caCert := fs.String("ca-cert", envDefault("SILO_TLS_CA_CERT", ""), "cluster CA certificate (PEM)")
	caKey := fs.String("ca-key", envDefault("SILO_TLS_CA_KEY", ""), "cluster CA private key (PEM)")
	principal := fs.String("principal", "", "who the token is for, e.g. csi@cluster (required)")
	ttl := fs.Duration("ttl", defaultTokenTTL, "how long the token is valid")
	var caps stringSlice
	fs.Var(&caps, "cap", "capability to grant (repeatable): chunk:read, chunk:write, namespace:read, namespace:write, status:read, node:admin, *")
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	if *principal == "" {
		fmt.Fprintln(stderr, "siloctl auth mint-token: --principal is required so the token is attributable")
		return 2
	}
	if len(caps) == 0 {
		fmt.Fprintln(stderr, "siloctl auth mint-token: pass at least one --cap (e.g. --cap=chunk:read)")
		return 2
	}

	ca, err := loadCAMaterial(*caCert, *caKey)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth mint-token: %v\n", err)
		return 1
	}
	if ca.Key == nil {
		fmt.Fprintln(stderr, "siloctl auth mint-token: the CA private key is required to sign tokens; pass --ca-key or set SILO_TLS_CA_KEY on a host that holds it")
		return 1
	}

	capabilities := make([]captoken.Capability, len(caps))
	for i, c := range caps {
		capabilities[i] = captoken.Capability(c)
	}
	now := time.Now().UTC()
	token, err := captoken.Mint(ca.Key, captoken.Token{
		Principal:    *principal,
		Capabilities: capabilities,
		IssuedAt:     now,
		Expiry:       now.Add(*ttl),
	})
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth mint-token: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout, token)
	fmt.Fprintf(stderr, "Token for %q valid until %s. Set it on the client:\n  export SILO_TOKEN=<token>\n",
		*principal, now.Add(*ttl).Format(time.RFC3339))
	return 0
}

func runAuthStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("siloctl auth status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configDir := fs.String("config-dir", "", "where to read ca.crt, client.crt, client.key from (default: per-user config dir)")
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}
	dir, err := resolveConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth status: %v\n", err)
		return 1
	}

	// dir is the operator's own config dir (per-user or --config-dir), not request input.
	caBytes, err := os.ReadFile(filepath.Join(dir, "ca.crt")) // #nosec G304
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth status: no cluster credentials at %s (%v); run 'siloctl auth init --token … --server …' first\n", dir, err)
		return 1
	}
	clientBytes, err := os.ReadFile(filepath.Join(dir, "client.crt")) // #nosec G304
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth status: no client cert at %s (%v); run 'siloctl auth init' to claim one\n", dir, err)
		return 1
	}
	caCert, err := parseSingleCert(caBytes)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth status: cluster CA at %s is unreadable (%v); remove the file and re-run 'siloctl auth init'\n", filepath.Join(dir, "ca.crt"), err)
		return 1
	}
	clientCert, err := parseSingleCert(clientBytes)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth status: client cert at %s is unreadable (%v); remove the file and re-run 'siloctl auth init'\n", filepath.Join(dir, "client.crt"), err)
		return 1
	}

	cfg, _ := loadAuthConfig(dir)
	fmt.Fprintf(stdout, "siloctl credentials at %s\n", dir)
	fmt.Fprintf(stdout, "  CA fingerprint:   %s\n", fingerprintCert(caCert))
	fmt.Fprintf(stdout, "  Client principal: %s\n", clientCert.Subject.CommonName)
	fmt.Fprintf(stdout, "  Client expires:   %s (in %s)\n", clientCert.NotAfter.UTC().Format(time.RFC3339), time.Until(clientCert.NotAfter).Round(time.Second))
	if cfg.DefaultGRPCServer != "" {
		fmt.Fprintf(stdout, "  Chunk server:     %s\n", cfg.DefaultGRPCServer)
	}
	if cfg.DefaultServer != "" {
		fmt.Fprintf(stdout, "  Joined via:       %s (bootstrap port)\n", cfg.DefaultServer)
	}
	return 0
}

// credentialFiles is the set of files 'auth init' writes; 'auth clean'
// removes the same set so the two commands stay in lockstep when fields
// are added.
var credentialFiles = []string{"ca.crt", "client.crt", "client.key", "config.json"}

func runAuthClean(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("siloctl auth clean", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configDir := fs.String("config-dir", "", "where to remove ca.crt, client.crt, client.key, config.json (default: per-user config dir)")
	yes := fs.Bool("yes", false, "skip the confirmation prompt")
	if err := parseFlexible(fs, args); err != nil {
		return 2
	}

	dir, err := resolveConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth clean: %v\n", err)
		return 1
	}

	present := make([]string, 0, len(credentialFiles))
	for _, name := range credentialFiles {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			present = append(present, name)
		}
	}
	if len(present) == 0 {
		fmt.Fprintf(stdout, "Nothing to clean at %s (no cluster credentials found).\n", dir)
		return 0
	}

	fmt.Fprintf(stdout, "About to delete cluster credentials at %s:\n", dir)
	for _, name := range present {
		fmt.Fprintf(stdout, "  %s\n", name)
	}
	// Best-effort context so the operator can tell which cluster they're
	// about to forget; failures here are not fatal — the user already
	// asked to clean.
	if cfg, err := loadAuthConfig(dir); err == nil {
		if cfg.Principal != "" {
			fmt.Fprintf(stdout, "Principal: %s\n", cfg.Principal)
		}
		if cfg.DefaultServer != "" {
			fmt.Fprintf(stdout, "Joined via: %s\n", cfg.DefaultServer)
		}
	}
	if caBytes, err := os.ReadFile(filepath.Join(dir, "ca.crt")); err == nil { // #nosec G304 -- operator's own config dir
		if caCert, err := parseSingleCert(caBytes); err == nil {
			fmt.Fprintf(stdout, "CA fingerprint: %s\n", fingerprintCert(caCert))
		}
	}

	if !*yes {
		fmt.Fprint(stdout, "Delete these credentials? [y/N]: ")
		reader := bufio.NewReader(stdin)
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			fmt.Fprintf(stderr, "siloctl auth clean: could not read confirmation (%v); re-run with --yes to skip the prompt\n", err)
			return 1
		}
		answer := strings.ToLower(strings.TrimSpace(line))
		if answer != "y" && answer != "yes" {
			fmt.Fprintln(stdout, "Aborted; nothing was deleted.")
			return 0
		}
	}

	var firstErr error
	removed := 0
	for _, name := range present {
		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		removed++
	}
	if firstErr != nil {
		fmt.Fprintf(stderr, "siloctl auth clean: removed %d of %d files; first error: %v; check filesystem permissions on %s\n", removed, len(present), firstErr, dir)
		return 1
	}
	fmt.Fprintf(stdout, "Deleted %d credential file(s) from %s. Run 'siloctl auth init' to claim new credentials.\n", removed, dir)
	return 0
}

// fpCheck holds the fingerprint observed during the bootstrap TLS
// handshake; with pinning enabled, mismatches abort the handshake
// before this struct is read.
type fpCheck struct {
	observedFP string
}

func (f *fpCheck) observed() string { return f.observedFP }

// buildBootstrapTLSConfig returns a tls.Config wired for the bootstrap
// listener. The cluster CA is not yet trusted on this host (the whole
// point of the call is to obtain it), so InsecureSkipVerify is true and
// authenticity is enforced inside VerifyConnection — either by matching
// the operator-supplied fingerprint or by capturing the observed
// fingerprint for trust-on-first-use display.
func buildBootstrapTLSConfig(fingerprint, serverName string) (*tls.Config, *fpCheck) {
	check := &fpCheck{}
	host := hostFromAddr(serverName)
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // intentional: VerifyConnection below enforces authenticity via fingerprint pinning or TOFU
		ServerName:         host,
		MinVersion:         tls.VersionTLS13,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("siloctl auth init: server presented no certificate; check that SILO_BOOTSTRAP_ADDR points at a real silod")
			}
			observed := fingerprintCert(cs.PeerCertificates[0])
			check.observedFP = observed
			if fingerprint != "" && !strings.EqualFold(fingerprint, observed) {
				return fmt.Errorf("siloctl auth init: server fingerprint mismatch (got %s, expected %s); refusing to connect — verify the address and the fingerprint silod printed on boot", observed, fingerprint)
			}
			return nil
		},
	}, check
}

func writeAuthMaterial(dir string, caPEM, certPEM, keyPEM []byte, server, grpcServer, principal string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("could not create %s (%w); pick a writable path with --config-dir", dir, err)
	}
	files := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"ca.crt", caPEM, 0o644},
		{"client.crt", certPEM, 0o644},
		{"client.key", keyPEM, 0o600},
	}
	for _, f := range files {
		path := filepath.Join(dir, f.name)
		if err := os.WriteFile(path, f.data, f.mode); err != nil {
			return fmt.Errorf("could not write %s (%w); check the path is on a writable filesystem", path, err)
		}
	}
	cfg := authConfig{DefaultServer: server, DefaultGRPCServer: grpcServer, Principal: principal, IssuedAt: time.Now().UTC().Format(time.RFC3339)}
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("could not serialise siloctl config (%w); this is a programming error", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), raw, 0o600); err != nil {
		return fmt.Errorf("could not write %s/config.json (%w); check the path is on a writable filesystem", dir, err)
	}
	return nil
}

func loadAuthConfig(dir string) (authConfig, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "config.json")) // #nosec G304 -- operator's own config dir
	if err != nil {
		return authConfig{}, err
	}
	var cfg authConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return authConfig{}, err
	}
	return cfg, nil
}

func resolveConfigDir(override string) (string, error) {
	if override != "" {
		return override, nil
	}
	return userConfigDir()
}

// defaultPrincipal builds a sensible <user>@<host> identity to label a
// fresh client cert. Both lookups are best-effort: on a misconfigured
// container or chrooted environment user.Current can fail without /etc/passwd
// access, and os.Hostname can return an empty string. The fallbacks
// ("siloctl", "unknown") keep the cert minting path unblocked at the
// cost of a less attributable principal — operators can override with
// --principal when they care.
func defaultPrincipal() string {
	u, err := user.Current()
	if err != nil {
		return "siloctl"
	}
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	return u.Username + "@" + host
}

// parseSingleCert decodes the first CERTIFICATE block from PEM input.
// siloctl writes one cert per file, so we don't bother with multi-block
// PEM parsing.
func parseSingleCert(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("no CERTIFICATE block in PEM input")
	}
	return x509.ParseCertificate(block.Bytes)
}

func fingerprintCert(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// hostFromAddr strips a ":port" suffix if present. silod's bootstrap
// listener is reached by host:port but the TLS handshake's ServerName
// check needs just the host, so we trim it ourselves rather than
// pulling net.SplitHostPort and its error handling into the callers.
// Bare hostnames pass through unchanged.
func hostFromAddr(s string) string {
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return s[:i]
	}
	return s
}
