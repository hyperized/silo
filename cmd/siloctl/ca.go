package main

import (
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/hyperized/silo/internal/clustertls"
)

// runCA dispatches the `siloctl ca` subcommands. These are local crypto
// operations on the cluster CA material — they read SILO_TLS_CA_CERT /
// SILO_TLS_CA_KEY off disk and never talk to silod — so they run on whichever
// host holds the CA key, not against a --server.
func runCA(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelpFlag(args[0]) {
		printCAUsage(stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "revoke":
		return runCARevoke(rest, stdout, stderr)
	case "list-revoked":
		return runCAListRevoked(rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "siloctl ca: unknown subcommand %q. Run 'siloctl ca help'.\n", sub)
		return 2
	}
}

func printCAUsage(w io.Writer) {
	fmt.Fprint(w, `siloctl ca — cluster certificate-authority operations (local, offline)

Usage:
  siloctl ca revoke       [--ca-cert=PATH] [--ca-key=PATH] --crl=PATH
                          (--cert=PATH | --serial=HEX) [more...] [--lifetime=DUR]
  siloctl ca list-revoked [--ca-cert=PATH] --crl=PATH

Revoke adds one or more certificates to a CA-signed revocation list and rewrites
--crl in place (creating it on first use, extending it thereafter). Point a
node's serials at it with SILO_TLS_CRL and restart — silod then rejects any mTLS
handshake from a revoked node or client cert, even one that still chains to the
CA and has not expired. Distribute the regenerated CRL to every node.

Identify a cert to revoke either by file (--cert=node.crt, the serial is read
out of it) or by raw serial (--serial=1A2B, hex, ':'-separators allowed). Both
flags repeat.

CA material defaults to $SILO_TLS_CA_CERT / $SILO_TLS_CA_KEY. The CA private key
is required to sign the CRL, so run this on a host that holds it.

  --lifetime  how long the new CRL stays fresh before silod logs it as stale
              (default 168h / 7 days)
`)
}

// stringSlice collects a repeatable string flag (--cert=a --cert=b).
type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }

func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func runCARevoke(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ca revoke", flag.ContinueOnError)
	fs.SetOutput(stderr)
	caCert := fs.String("ca-cert", envDefault("SILO_TLS_CA_CERT", ""), "cluster CA certificate (PEM)")
	caKey := fs.String("ca-key", envDefault("SILO_TLS_CA_KEY", ""), "cluster CA private key (PEM)")
	crlPath := fs.String("crl", "", "CRL file to extend and rewrite (created if absent)")
	lifetime := fs.Duration("lifetime", clustertls.DefaultCRLLifetime, "how long the new CRL stays fresh")
	var certFiles, serials stringSlice
	fs.Var(&certFiles, "cert", "certificate file whose serial to revoke (repeatable)")
	fs.Var(&serials, "serial", "hex serial to revoke (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *crlPath == "" {
		fmt.Fprintln(stderr, "siloctl ca revoke: --crl is required (the file to write the revocation list to)")
		return 2
	}
	if len(certFiles) == 0 && len(serials) == 0 {
		fmt.Fprintln(stderr, "siloctl ca revoke: nothing to revoke; pass at least one --cert=PATH or --serial=HEX")
		return 2
	}

	ca, err := loadCAMaterial(*caCert, *caKey)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl ca revoke: %v\n", err)
		return 1
	}

	toRevoke, err := collectSerials(certFiles, serials)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl ca revoke: %v\n", err)
		return 1
	}

	number, existing, err := existingCRLState(*crlPath, ca)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl ca revoke: %v\n", err)
		return 1
	}

	merged := mergeSerials(existing, toRevoke)
	crlPEM, err := clustertls.GenerateCRL(ca, merged, number, *lifetime)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl ca revoke: %v\n", err)
		return 1
	}
	if err := os.WriteFile(*crlPath, crlPEM, 0o644); err != nil { //nolint:gosec // a CRL is public, integrity comes from the CA signature, not file perms
		fmt.Fprintf(stderr, "siloctl ca revoke: could not write the CRL to %s (%v)\n", *crlPath, err)
		return 1
	}

	fmt.Fprintf(stdout, "Wrote CRL %s (sequence %s) with %d revoked certificate(s).\n", *crlPath, number, len(merged))
	fmt.Fprintln(stdout, "Distribute it to every node and point SILO_TLS_CRL at it, then restart silod (or wait for the next reload).")
	return 0
}

func runCAListRevoked(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ca list-revoked", flag.ContinueOnError)
	fs.SetOutput(stderr)
	caCert := fs.String("ca-cert", envDefault("SILO_TLS_CA_CERT", ""), "cluster CA certificate (PEM)")
	crlPath := fs.String("crl", "", "CRL file to read")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *crlPath == "" {
		fmt.Fprintln(stderr, "siloctl ca list-revoked: --crl is required")
		return 2
	}
	// Only the CA certificate is needed to verify the CRL signature.
	ca, err := loadCAMaterial(*caCert, "")
	if err != nil {
		fmt.Fprintf(stderr, "siloctl ca list-revoked: %v\n", err)
		return 1
	}
	crlPEM, err := os.ReadFile(*crlPath)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl ca list-revoked: could not read the CRL at %s (%v)\n", *crlPath, err)
		return 1
	}
	crl, err := clustertls.LoadCRL(crlPEM, ca)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl ca list-revoked: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "CRL %s — sequence %s, %d revoked, next update %s\n",
		*crlPath, crl.Number, crl.Count(), crl.NextUpdate.UTC().Format(time.RFC3339))
	for _, s := range crl.Serials() {
		fmt.Fprintf(stdout, "  %s\n", s)
	}
	return 0
}

// loadCAMaterial reads the CA cert (always) and key (when keyPath is non-empty)
// and parses them into a *clustertls.CA. A revoke needs the key to sign;
// list-revoked passes an empty keyPath because verifying the CRL only needs the
// public cert.
func loadCAMaterial(certPath, keyPath string) (*clustertls.CA, error) {
	if certPath == "" {
		return nil, fmt.Errorf("no CA certificate; pass --ca-cert or set SILO_TLS_CA_CERT")
	}
	// certPath/keyPath are operator-supplied CLI flags, not request input.
	certPEM, err := os.ReadFile(certPath) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("could not read the CA certificate at %s (%w)", certPath, err)
	}
	var keyPEM []byte
	if keyPath != "" {
		keyPEM, err = os.ReadFile(keyPath) // #nosec G304
		if err != nil {
			return nil, fmt.Errorf("could not read the CA key at %s (%w)", keyPath, err)
		}
	}
	ca, err := clustertls.LoadCA(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return ca, nil
}

// collectSerials turns the --cert files and --serial hex strings into the set
// of serial numbers to add to the CRL.
func collectSerials(certFiles, serials stringSlice) ([]*big.Int, error) {
	out := make([]*big.Int, 0, len(certFiles)+len(serials))
	for _, path := range certFiles {
		// path is an operator-supplied CLI flag, not request input.
		pem, err := os.ReadFile(path) // #nosec G304
		if err != nil {
			return nil, fmt.Errorf("could not read the certificate at %s (%w)", path, err)
		}
		serial, err := clustertls.SerialOf(pem)
		if err != nil {
			return nil, err
		}
		out = append(out, serial)
	}
	for _, raw := range serials {
		serial, err := parseSerial(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, serial)
	}
	return out, nil
}

// parseSerial accepts a hex serial as openssl prints it ("1A:2B:3C"), with an
// optional 0x prefix and surrounding whitespace.
func parseSerial(raw string) (*big.Int, error) {
	clean := strings.TrimSpace(raw)
	clean = strings.TrimPrefix(clean, "0x")
	clean = strings.TrimPrefix(clean, "0X")
	clean = strings.ReplaceAll(clean, ":", "")
	if clean == "" {
		return nil, fmt.Errorf("empty serial number")
	}
	n, ok := new(big.Int).SetString(clean, 16)
	if !ok {
		return nil, fmt.Errorf("serial %q is not valid hexadecimal", raw)
	}
	return n, nil
}

// existingCRLState reads the current CRL (if any) so a revoke extends rather
// than replaces it: the returned number is the next sequence value and the
// returned serials are the already-revoked set. A missing file is the first
// issue (sequence 1, no prior serials); a present-but-unverifiable file is an
// error so we never silently drop prior revocations.
func existingCRLState(crlPath string, ca *clustertls.CA) (*big.Int, []*big.Int, error) {
	// crlPath is an operator-supplied CLI flag, not request input.
	crlPEM, err := os.ReadFile(crlPath) // #nosec G304
	if err != nil {
		if os.IsNotExist(err) {
			return big.NewInt(1), nil, nil
		}
		return nil, nil, fmt.Errorf("could not read the existing CRL at %s (%w)", crlPath, err)
	}
	crl, err := clustertls.LoadCRL(crlPEM, ca)
	if err != nil {
		return nil, nil, fmt.Errorf("the existing CRL at %s did not verify (%w); move it aside to start a fresh list", crlPath, err)
	}
	return new(big.Int).Add(crl.Number, big.NewInt(1)), crl.Serials(), nil
}

// mergeSerials unions the prior and new serials, de-duplicating so a serial
// revoked twice appears once. The result is sorted for a stable CRL.
func mergeSerials(prior, added []*big.Int) []*big.Int {
	seen := make(map[string]*big.Int, len(prior)+len(added))
	for _, s := range append(append([]*big.Int{}, prior...), added...) {
		seen[s.String()] = s
	}
	out := make([]*big.Int, 0, len(seen))
	for _, s := range seen {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cmp(out[j]) < 0 })
	return out
}
