package main

import (
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
)

// authConfig is the small JSON file siloctl drops next to its certs so
// subsequent commands can find the default server without re-typing it.
// Kept deliberately small — there is no global config registry to drift
// out of sync with.
type authConfig struct {
	DefaultServer string `json:"default_server"`
	Principal     string `json:"principal"`
	IssuedAt      string `json:"issued_at"`
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
		return "", fmt.Errorf("could not determine the user config dir (%v); pass --config-dir to siloctl explicitly", err)
	}
	return filepath.Join(dir, "silo"), nil
}

func runAuth(args []string, stdout, stderr io.Writer) int {
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

'init' redeems a one-time join token printed by silod on first boot and writes
the resulting CA cert, client cert, and client key into the config directory
(default: $XDG_CONFIG_HOME/silo or ~/.config/silo).

Pin the server's fingerprint with --server-fingerprint to refuse mismatched
servers; omit it for trust-on-first-use (a loud warning is printed).
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
	if err := fs.Parse(args); err != nil {
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
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := bootstrapv1.NewBootstrapClient(conn).Join(ctx, &bootstrapv1.JoinRequest{Token: *token, Principal: *principal})
	if err != nil {
		return reportRPC(stderr, "auth init", err)
	}

	if *fingerprint == "" {
		fmt.Fprintf(stderr, "siloctl auth init: WARNING: connecting without --server-fingerprint (trust-on-first-use). Verify this fingerprint matches the value silod printed on boot:\n  %s\n", fpCheck.observed())
	}

	if err := writeAuthMaterial(dir, resp.CaCertPem, resp.ClientCertPem, resp.ClientKeyPem, *server, *principal); err != nil {
		fmt.Fprintf(stderr, "siloctl auth init: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "Wrote cluster credentials to %s\n  ca.crt       %d bytes\n  client.crt   %d bytes\n  client.key   %d bytes\nYou can now run siloctl chunk … against %s.\n",
		dir, len(resp.CaCertPem), len(resp.ClientCertPem), len(resp.ClientKeyPem), *server)
	return 0
}

func runAuthStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("siloctl auth status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configDir := fs.String("config-dir", "", "where to read ca.crt, client.crt, client.key from (default: per-user config dir)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	dir, err := resolveConfigDir(*configDir)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth status: %v\n", err)
		return 1
	}

	caBytes, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		fmt.Fprintf(stderr, "siloctl auth status: no cluster credentials at %s (%v); run 'siloctl auth init --token … --server …' first\n", dir, err)
		return 1
	}
	clientBytes, err := os.ReadFile(filepath.Join(dir, "client.crt"))
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
	if cfg.DefaultServer != "" {
		fmt.Fprintf(stdout, "  Default server:   %s\n", cfg.DefaultServer)
	}
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

func writeAuthMaterial(dir string, caPEM, certPEM, keyPEM []byte, server, principal string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("could not create %s (%v); pick a writable path with --config-dir", dir, err)
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
			return fmt.Errorf("could not write %s (%v); check the path is on a writable filesystem", path, err)
		}
	}
	cfg := authConfig{DefaultServer: server, Principal: principal, IssuedAt: time.Now().UTC().Format(time.RFC3339)}
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("could not serialise siloctl config (%v); this is a programming error", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), raw, 0o600); err != nil {
		return fmt.Errorf("could not write %s/config.json (%v); check the path is on a writable filesystem", dir, err)
	}
	return nil
}

func loadAuthConfig(dir string) (authConfig, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "config.json"))
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
