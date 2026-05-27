package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	bootstrapv1 "github.com/hyperized/silo/api/proto/silo/bootstrap/v1"
	"github.com/hyperized/silo/internal/clustertls"
)

// fakeBootstrapServer spins up a real Bootstrap gRPC server on an
// ephemeral port so the auth-init command can exercise the full TLS
// handshake and Join RPC path against a deterministic backend. The
// service handler is scriptable per test.
type fakeBootstrapServer struct {
	bootstrapv1.UnimplementedBootstrapServer
	joinErr      error
	joinResp     *bootstrapv1.JoinResponse
	gotPrincipal string
	gotToken     string
}

func (f *fakeBootstrapServer) Join(_ context.Context, req *bootstrapv1.JoinRequest) (*bootstrapv1.JoinResponse, error) {
	f.gotToken = req.Token
	f.gotPrincipal = req.Principal
	if f.joinErr != nil {
		return nil, f.joinErr
	}
	return f.joinResp, nil
}

// startBootstrapServer mints a CA + server cert, registers the fake
// handler, and starts a server-only-TLS listener. Returns the address,
// the leaf fingerprint silod would have printed, and a teardown.
func startBootstrapServer(t *testing.T, svc *fakeBootstrapServer) (addr, fingerprint string, teardown func()) {
	t.Helper()
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := clustertls.LoadCA(caPEM, keyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	nodeCert, err := clustertls.MintNodeCert(ca, "test-server", []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)}, time.Hour)
	if err != nil {
		t.Fatalf("MintNodeCert: %v", err)
	}
	tlsCfg, err := clustertls.ServerOnlyConfig(nodeCert)
	if err != nil {
		t.Fatalf("ServerOnlyConfig: %v", err)
	}
	fingerprint, err = nodeCert.LeafFingerprint()
	if err != nil {
		t.Fatalf("LeafFingerprint: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	bootstrapv1.RegisterBootstrapServer(s, svc)
	go func() { _ = s.Serve(ln) }()
	teardown = func() {
		s.GracefulStop()
		_ = ln.Close()
	}
	return ln.Addr().String(), fingerprint, teardown
}

// mintCAResponse builds a JoinResponse that looks like silod's: a real
// CA cert + freshly-signed client cert + key. The TLS configs built
// out of this material are usable by chunk.go's loadClientTLS later in
// the test pipeline.
func mintCAResponse(t *testing.T) *bootstrapv1.JoinResponse {
	t.Helper()
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test-reply", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := clustertls.LoadCA(caPEM, keyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	client, err := clustertls.MintClientCert(ca, "user@host", time.Hour)
	if err != nil {
		t.Fatalf("MintClientCert: %v", err)
	}
	return &bootstrapv1.JoinResponse{CaCertPem: caPEM, ClientCertPem: client.CertPEM, ClientKeyPem: client.KeyPEM}
}

func TestRunAuth_NoArgsShowsUsage(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runAuth(nil, &out, &errBuf); code != 2 {
		t.Errorf("got %d, want 2", code)
	}
	if !strings.Contains(out.String(), "siloctl auth") {
		t.Errorf("usage missing, got %q", out.String())
	}
}

func TestRunAuth_HelpAndUnknown(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"help"}, &out, &errBuf); code != 0 {
		t.Errorf("help exit: got %d, want 0", code)
	}
	out.Reset()
	errBuf.Reset()
	if code := runAuth([]string{"banana"}, &out, &errBuf); code != 2 {
		t.Errorf("unknown sub exit: got %d, want 2", code)
	}
	if !strings.Contains(errBuf.String(), "unknown subcommand") {
		t.Errorf("stderr should explain failure, got %q", errBuf.String())
	}
}

func TestRunAuthInit_MissingFlags(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no token", []string{"init", "--server", "127.0.0.1:1234"}, "--token is required"},
		{"no server", []string{"init", "--token", "abc"}, "--server is required"},
		{"flag parse error", []string{"init", "--no-such-flag"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errBuf bytes.Buffer
			code := runAuth(tc.args, &out, &errBuf)
			if code != 2 {
				t.Errorf("got %d, want 2 (stderr=%q)", code, errBuf.String())
			}
			if tc.want != "" && !strings.Contains(errBuf.String(), tc.want) {
				t.Errorf("stderr should mention %q, got %q", tc.want, errBuf.String())
			}
		})
	}
}

func TestRunAuthInit_HappyPathWritesMaterial(t *testing.T) {
	svc := &fakeBootstrapServer{joinResp: mintCAResponse(t)}
	addr, fp, teardown := startBootstrapServer(t, svc)
	defer teardown()

	dir := t.TempDir()
	var out, errBuf bytes.Buffer
	code := runAuth([]string{
		"init",
		"--token", "first-boot-token",
		"--server", addr,
		"--server-fingerprint", fp,
		"--principal", "alice@host",
		"--config-dir", dir,
	}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("got %d, want 0; stderr=%q", code, errBuf.String())
	}
	if svc.gotToken != "first-boot-token" {
		t.Errorf("server saw token %q, want first-boot-token", svc.gotToken)
	}
	if svc.gotPrincipal != "alice@host" {
		t.Errorf("server saw principal %q, want alice@host", svc.gotPrincipal)
	}
	for _, name := range []string{"ca.crt", "client.crt", "client.key", "config.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("missing %s: %v", name, err)
		}
	}
	if !strings.Contains(out.String(), "Wrote cluster credentials to "+dir) {
		t.Errorf("stdout should confirm material location, got %q", out.String())
	}
}

func TestRunAuthInit_TOFUWarningWhenNoFingerprint(t *testing.T) {
	svc := &fakeBootstrapServer{joinResp: mintCAResponse(t)}
	addr, _, teardown := startBootstrapServer(t, svc)
	defer teardown()

	dir := t.TempDir()
	var out, errBuf bytes.Buffer
	code := runAuth([]string{
		"init",
		"--token", "t",
		"--server", addr,
		"--config-dir", dir,
	}, &out, &errBuf)
	if code != 0 {
		t.Fatalf("got %d, want 0; stderr=%q", code, errBuf.String())
	}
	if !strings.Contains(errBuf.String(), "trust-on-first-use") {
		t.Errorf("stderr should warn about TOFU, got %q", errBuf.String())
	}
	if !strings.Contains(errBuf.String(), "sha256:") {
		t.Errorf("stderr should print the observed fingerprint, got %q", errBuf.String())
	}
}

func TestRunAuthInit_FingerprintMismatchRefuses(t *testing.T) {
	svc := &fakeBootstrapServer{joinResp: mintCAResponse(t)}
	addr, _, teardown := startBootstrapServer(t, svc)
	defer teardown()

	dir := t.TempDir()
	var out, errBuf bytes.Buffer
	code := runAuth([]string{
		"init",
		"--token", "t",
		"--server", addr,
		"--server-fingerprint", "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		"--config-dir", dir,
	}, &out, &errBuf)
	if code == 0 {
		t.Errorf("got 0, want non-zero; stderr=%q", errBuf.String())
	}
}

func TestRunAuthInit_DialFailure(t *testing.T) {
	prev := authDialer
	t.Cleanup(func() { authDialer = prev })
	authDialer = func(string, *tls.Config) (*grpc.ClientConn, error) {
		return nil, errors.New("simulated dial failure")
	}
	dir := t.TempDir()
	var out, errBuf bytes.Buffer
	code := runAuth([]string{"init", "--token", "t", "--server", "x:1", "--config-dir", dir}, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "could not dial silod") {
		t.Errorf("stderr should mention dial failure, got %q", errBuf.String())
	}
}

func TestRunAuthInit_JoinRPCError(t *testing.T) {
	svc := &fakeBootstrapServer{joinErr: errors.New("server boom")}
	addr, fp, teardown := startBootstrapServer(t, svc)
	defer teardown()

	dir := t.TempDir()
	var out, errBuf bytes.Buffer
	code := runAuth([]string{
		"init",
		"--token", "t",
		"--server", addr,
		"--server-fingerprint", fp,
		"--config-dir", dir,
	}, &out, &errBuf)
	if code == 0 {
		t.Error("want non-zero exit on join failure")
	}
}

func TestRunAuthInit_WriteMaterialFailure(t *testing.T) {
	// Pre-place a regular file where config-dir should be a directory.
	// MkdirAll inside writeAuthMaterial fails and Run reports it.
	tmp := t.TempDir()
	blocker := filepath.Join(tmp, "blocker")
	if err := os.WriteFile(blocker, []byte{0}, 0o600); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	svc := &fakeBootstrapServer{joinResp: mintCAResponse(t)}
	addr, fp, teardown := startBootstrapServer(t, svc)
	defer teardown()

	var out, errBuf bytes.Buffer
	code := runAuth([]string{
		"init",
		"--token", "t",
		"--server", addr,
		"--server-fingerprint", fp,
		"--config-dir", filepath.Join(blocker, "subdir"),
	}, &out, &errBuf)
	if code == 0 {
		t.Error("want non-zero exit when config dir is unwritable")
	}
	if !strings.Contains(errBuf.String(), "could not create") {
		t.Errorf("stderr should mention the mkdir failure, got %q", errBuf.String())
	}
}

func TestRunAuthInit_ConfigDirResolveFailure(t *testing.T) {
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return "", errors.New("simulated user-dir lookup failure") }
	var out, errBuf bytes.Buffer
	code := runAuth([]string{"init", "--token", "t", "--server", "x:1"}, &out, &errBuf)
	if code != 1 {
		t.Errorf("got %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "simulated user-dir lookup failure") {
		t.Errorf("stderr should bubble the resolution failure, got %q", errBuf.String())
	}
}

func TestRunAuthInit_DefaultPrincipalSent(t *testing.T) {
	// The minted principal must always be non-empty so the server's
	// Subject CN is attributable. Exact value depends on the host's
	// $USER, so we just assert non-empty here.
	svc := &fakeBootstrapServer{joinResp: mintCAResponse(t)}
	addr, fp, teardown := startBootstrapServer(t, svc)
	defer teardown()

	dir := t.TempDir()
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{
		"init",
		"--token", "t",
		"--server", addr,
		"--server-fingerprint", fp,
		"--config-dir", dir,
	}, &out, &errBuf); code != 0 {
		t.Fatalf("got %d, want 0; stderr=%q", code, errBuf.String())
	}
	if svc.gotPrincipal == "" {
		t.Error("principal should default to <user>@<host>, got empty")
	}
}

func TestRunAuthStatus_HappyPath(t *testing.T) {
	dir := t.TempDir()
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, _ := clustertls.LoadCA(caPEM, keyPEM)
	client, _ := clustertls.MintClientCert(ca, "alice@host", time.Hour)
	if err := writeAuthMaterial(dir, caPEM, client.CertPEM, client.KeyPEM, "silo.example:7000", "alice@host"); err != nil {
		t.Fatalf("writeAuthMaterial: %v", err)
	}

	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"status", "--config-dir", dir}, &out, &errBuf); code != 0 {
		t.Fatalf("got %d, want 0; stderr=%q", code, errBuf.String())
	}
	for _, want := range []string{
		"siloctl credentials at " + dir,
		"CA fingerprint:   sha256:",
		"Client principal: alice@host",
		"Default server:   silo.example:7000",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("status output missing %q; got:\n%s", want, out.String())
		}
	}
}

func TestRunAuthStatus_NoCredentials(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"status", "--config-dir", t.TempDir()}, &out, &errBuf); code != 1 {
		t.Errorf("got %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "auth init") {
		t.Errorf("stderr should suggest 'siloctl auth init', got %q", errBuf.String())
	}
}

func TestRunAuthStatus_MissingClientCert(t *testing.T) {
	dir := t.TempDir()
	// Place only ca.crt; the client cert read should fail.
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), []byte("X"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"status", "--config-dir", dir}, &out, &errBuf); code != 1 {
		t.Errorf("got %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "client cert") {
		t.Errorf("stderr should mention missing client cert, got %q", errBuf.String())
	}
}

func TestRunAuthStatus_GarbageCAcert(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"ca.crt", "client.crt", "client.key"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("garbage"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"status", "--config-dir", dir}, &out, &errBuf); code != 1 {
		t.Errorf("got %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "cluster CA") {
		t.Errorf("stderr should mention CA failure, got %q", errBuf.String())
	}
}

func TestRunAuthStatus_GarbageClientCert(t *testing.T) {
	dir := t.TempDir()
	caPEM, keyPEM, _ := clustertls.GenerateCA("silo-test", time.Hour)
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.crt"), []byte("garbage"), 0o600); err != nil {
		t.Fatalf("write client cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.key"), keyPEM, 0o600); err != nil {
		t.Fatalf("write client key: %v", err)
	}
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"status", "--config-dir", dir}, &out, &errBuf); code != 1 {
		t.Errorf("got %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "client cert") {
		t.Errorf("stderr should mention client cert failure, got %q", errBuf.String())
	}
}

func TestRunAuthStatus_ConfigDirResolveFailure(t *testing.T) {
	prev := userConfigDir
	t.Cleanup(func() { userConfigDir = prev })
	userConfigDir = func() (string, error) { return "", errors.New("simulated user-dir lookup failure") }
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"status"}, &out, &errBuf); code != 1 {
		t.Errorf("got %d, want 1", code)
	}
}

func TestRunAuthStatus_NoConfigJSONStillSucceeds(t *testing.T) {
	// Some operators copy the cert files manually without the config.json
	// shim; status should still print the principal+fingerprint and skip
	// the Default server line silently.
	dir := t.TempDir()
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, _ := clustertls.LoadCA(caPEM, keyPEM)
	client, _ := clustertls.MintClientCert(ca, "alice@host", time.Hour)
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.crt"), client.CertPEM, 0o600); err != nil {
		t.Fatalf("write client cert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client.key"), client.KeyPEM, 0o600); err != nil {
		t.Fatalf("write client key: %v", err)
	}
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"status", "--config-dir", dir}, &out, &errBuf); code != 0 {
		t.Errorf("got %d, want 0; stderr=%q", code, errBuf.String())
	}
	if strings.Contains(out.String(), "Default server:") {
		t.Errorf("status should omit 'Default server' when config.json is absent, got %q", out.String())
	}
}

func TestRunAuthStatus_FlagParseError(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runAuth([]string{"status", "--no-such-flag"}, &out, &errBuf); code != 2 {
		t.Errorf("got %d, want 2", code)
	}
}

func TestUserConfigDirDefault(_ *testing.T) {
	// Cover the production seam itself: invoking userConfigDir directly
	// hits the os.UserConfigDir path with whatever HOME the test runner
	// has. If HOME is unset (CI containers) we get an error which is
	// also a covered branch.
	if _, err := userConfigDir(); err != nil {
		// Some CI environments fail UserConfigDir; either branch is OK.
		return
	}
}

func TestDefaultPrincipal_NeverEmpty(t *testing.T) {
	p := defaultPrincipal()
	if p == "" {
		t.Error("defaultPrincipal should never be empty")
	}
}

func TestHostFromAddr(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"127.0.0.1:7001", "127.0.0.1"},
		{"silo-a:7000", "silo-a"},
		{"noport", "noport"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := hostFromAddr(tc.in); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFingerprintCert_DeterministicOutput(t *testing.T) {
	// fingerprintCert is the bridge between the operator (who sees the
	// fingerprint silod printed) and the verifier (siloctl). The hash
	// must be stable for the same DER input across runs.
	caPEM, _, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	cert, err := parseSingleCert(caPEM)
	if err != nil {
		t.Fatalf("parseSingleCert: %v", err)
	}
	first := fingerprintCert(cert)
	second := fingerprintCert(cert)
	if !strings.HasPrefix(first, "sha256:") {
		t.Errorf("fingerprintCert should be sha256-prefixed")
	}
	if first != second {
		t.Error("fingerprintCert should be deterministic")
	}
}

func TestParseSingleCert_RejectsGarbage(t *testing.T) {
	if _, err := parseSingleCert([]byte("not pem")); err == nil {
		t.Error("parseSingleCert should fail on garbage")
	}
	bad := pem.EncodeToMemory(&pem.Block{Type: "WRONG", Bytes: []byte{1, 2, 3}})
	if _, err := parseSingleCert(bad); err == nil {
		t.Error("parseSingleCert should reject non-CERTIFICATE blocks")
	}
	// Valid PEM type but garbage DER inside.
	bad = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{0, 1, 2, 3}})
	if _, err := parseSingleCert(bad); err == nil {
		t.Error("parseSingleCert should reject garbage DER")
	}
}

func TestLoadAuthConfig_GarbageJSON(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte("not json"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := loadAuthConfig(dir); err == nil {
		t.Error("loadAuthConfig should refuse garbage JSON")
	}
}

func TestWriteAuthMaterial_RoundTrip(t *testing.T) {
	// Round-trip: write material, read it back, parse it. Catches the
	// rare regression where the on-disk format drifts from what siloctl
	// expects to read on the next command.
	dir := t.TempDir()
	caPEM, _, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	if err := writeAuthMaterial(dir, caPEM, caPEM, caPEM, "host:1", "alice"); err != nil {
		t.Fatalf("writeAuthMaterial: %v", err)
	}
	cfg, err := loadAuthConfig(dir)
	if err != nil {
		t.Fatalf("loadAuthConfig: %v", err)
	}
	if cfg.DefaultServer != "host:1" || cfg.Principal != "alice" {
		t.Errorf("config round-trip: %+v", cfg)
	}
}

func TestWriteAuthMaterial_WriteFailure(t *testing.T) {
	// Pre-place a directory at the cert path so WriteFile fails.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ca.crt"), 0o700); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := writeAuthMaterial(dir, []byte("CA"), []byte("CERT"), []byte("KEY"), "s", "p"); err == nil {
		t.Error("writeAuthMaterial should fail when a child path is a directory")
	}
}

func TestWriteAuthMaterial_ConfigWriteFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "config.json"), 0o700); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := writeAuthMaterial(dir, []byte("CA"), []byte("CERT"), []byte("KEY"), "s", "p"); err == nil || !strings.Contains(err.Error(), "config.json") {
		t.Errorf("got %v, want config.json write failure", err)
	}
}

func TestFpCheck_ObservedReadback(t *testing.T) {
	// Confirm fpCheck propagates the observed fingerprint set inside
	// the TLS callback. We can't easily fire that callback synchronously
	// here, so set the field directly and read it back through observed().
	c := &fpCheck{observedFP: "sha256:abc"}
	if got := c.observed(); got != "sha256:abc" {
		t.Errorf("observed: got %q, want sha256:abc", got)
	}
}

func TestBuildBootstrapTLSConfig_PortStrippedFromServerName(t *testing.T) {
	cfg, _ := buildBootstrapTLSConfig("", "host.example:7001")
	if cfg.ServerName != "host.example" {
		t.Errorf("ServerName: got %q, want host.example", cfg.ServerName)
	}
	cfg2, _ := buildBootstrapTLSConfig("", "noport")
	if cfg2.ServerName != "noport" {
		t.Errorf("ServerName (no port): got %q, want noport", cfg2.ServerName)
	}
}

func TestBuildBootstrapTLSConfig_VerifyConnection(t *testing.T) {
	// Drive the VerifyConnection callback directly: empty peer certs,
	// mismatch fingerprint, match fingerprint.
	_, check := buildBootstrapTLSConfig("sha256:abc", "host:1")
	cfg, _ := buildBootstrapTLSConfig("sha256:abc", "host:1")
	verify := cfg.VerifyConnection

	if err := verify(tls.ConnectionState{}); err == nil {
		t.Error("VerifyConnection should reject empty peer cert list")
	}

	// Real cert with a known fingerprint; mismatch should fail, match
	// should succeed.
	caPEM, _, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	cert, err := parseSingleCert(caPEM)
	if err != nil {
		t.Fatalf("parseSingleCert: %v", err)
	}
	if err := verify(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}); err == nil {
		t.Error("VerifyConnection should reject fingerprint mismatch")
	}

	// Build a new config that pins the observed fingerprint, then re-verify.
	matchedCfg, matchedCheck := buildBootstrapTLSConfig(fingerprintCert(cert), "host:1")
	_ = matchedCheck // populated by the callback below
	if err := matchedCfg.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}); err != nil {
		t.Errorf("VerifyConnection should accept matched fingerprint, got %v", err)
	}
	if check != nil {
		_ = check
	}
}
