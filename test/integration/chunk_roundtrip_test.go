//go:build integration

// Package integration runs end-to-end tests against the real silod
// binary. They are kept under a build tag because they:
//   - require `go build` to have produced bin/silod (or they build it),
//   - spawn an OS process,
//   - bind real TCP ports,
//   - touch the filesystem outside of the test's working dir.
//
// Run with: make test-integration
package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	bootstrapv1 "github.com/hyperized/silo/api/proto/silo/bootstrap/v1"
	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
)

// silodNode holds the addresses and stdout capture from a running silod
// child process. The bootstrap token + fingerprint are scraped from
// stdout so the integration test can drive the inaugural Join RPC the
// same way an operator would.
type silodNode struct {
	grpcAddr      string
	bootstrapAddr string
	httpAddr      string
	token         string
	fingerprint   string
	teardown      func()
}

// startSilod builds silod, runs it on ephemeral ports, and waits until
// the bootstrap token has been printed to stdout. The bootstrap
// listener is up by the time waitForGRPC succeeds, so the join handshake
// can run immediately after this function returns.
func startSilod(t *testing.T) *silodNode {
	t.Helper()

	silodBin := filepath.Join(t.TempDir(), "silod")
	build := exec.Command("go", "build", "-o", silodBin, "../../cmd/silod")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		t.Fatalf("go build silod: %v", err)
	}

	httpAddr := freePort(t)
	grpcAddr := freePort(t)
	bootstrapAddr := freePort(t)
	dataDir := t.TempDir()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}

	cmd := exec.Command(silodBin)
	cmd.Env = append(os.Environ(),
		"SILO_NODE_ID=integration",
		"SILO_HTTP_ADDR="+httpAddr,
		"SILO_GRPC_ADDR="+grpcAddr,
		"SILO_BOOTSTRAP_ADDR="+bootstrapAddr,
		"SILO_DATA_DIR="+dataDir,
		"SILO_ENCRYPTION_KEY="+base64.StdEncoding.EncodeToString(key),
		"SILO_LOG_LEVEL=warn",
	)

	// Pipe stdout so we can scrape the inaugural token + fingerprint.
	// silod prints them once on first boot before any subsystem starts
	// serving — by tee'ing stdout we can both surface the lines in test
	// output and parse them programmatically.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start silod: %v", err)
	}

	tokenCh := make(chan string, 1)
	fpCh := make(chan string, 1)
	go scrapeBootstrap(stdout, tokenCh, fpCh)

	if err := waitForTCP(bootstrapAddr, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("silod bootstrap listener not reachable on %s: %v", bootstrapAddr, err)
	}
	if err := waitForTCP(grpcAddr, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("silod gRPC listener not reachable on %s: %v", grpcAddr, err)
	}

	var token, fp string
	select {
	case token = <-tokenCh:
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("silod did not print a bootstrap token within 2s")
	}
	select {
	case fp = <-fpCh:
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("silod did not print a server fingerprint within 2s")
	}

	teardown := func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	}
	return &silodNode{
		grpcAddr:      grpcAddr,
		bootstrapAddr: bootstrapAddr,
		httpAddr:      httpAddr,
		token:         token,
		fingerprint:   fp,
		teardown:      teardown,
	}
}

// scrapeBootstrap reads stdout line-by-line until it has seen both the
// token and the fingerprint. The handshake string silod prints is the
// authoritative format; if its phrasing changes, this scraper must
// follow.
var (
	tokenRE = regexp.MustCompile(`^\s*token:\s+(\S+)`)
	fpRE    = regexp.MustCompile(`^\s*server fingerprint:\s+(sha256:[0-9a-fA-F]+)`)
)

func scrapeBootstrap(r io.Reader, tokenCh, fpCh chan<- string) {
	// bufio.Scanner buffers until a full line is available, so the
	// regexes always see the complete fingerprint rather than a
	// half-arrived prefix. Tee'ing each line back to stderr keeps the
	// human-readable banner visible in test output.
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	var sentToken, sentFP bool
	for scanner.Scan() {
		line := scanner.Bytes()
		fmt.Fprintln(os.Stderr, string(line))
		if m := tokenRE.FindSubmatch(line); m != nil && !sentToken {
			tokenCh <- string(m[1])
			sentToken = true
		}
		if m := fpRE.FindSubmatch(line); m != nil && !sentFP {
			fpCh <- string(m[1])
			sentFP = true
		}
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// waitForTCP polls until a TCP dial to addr succeeds, mirroring the
// usual k8s readiness pattern.
func waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout dialing %s", addr)
}

// claimClientCert runs the Join RPC against the bootstrap listener with
// fingerprint pinning — the same flow `siloctl auth init
// --server-fingerprint ...` exercises. Returns the CA cert PEM and the
// client key pair for use in the subsequent chunk RPCs.
func claimClientCert(t *testing.T, node *silodNode) (caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	host, _, err := net.SplitHostPort(node.bootstrapAddr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	var observed string
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // VerifyConnection pins the fingerprint silod printed
		ServerName:         host,
		MinVersion:         tls.VersionTLS13,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("no peer cert")
			}
			observed = fingerprintCert(cs.PeerCertificates[0])
			if !strings.EqualFold(observed, node.fingerprint) {
				return fmt.Errorf("fingerprint mismatch: observed %s, expected %s", observed, node.fingerprint)
			}
			return nil
		},
	}
	conn, err := grpc.NewClient(node.bootstrapAddr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		t.Fatalf("dial bootstrap: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := bootstrapv1.NewBootstrapClient(conn).Join(ctx, &bootstrapv1.JoinRequest{Token: node.token, Principal: "integration@test"})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if observed != node.fingerprint {
		t.Errorf("server fingerprint not observed (got %q, want %q)", observed, node.fingerprint)
	}
	return resp.CaCertPem, resp.ClientCertPem, resp.ClientKeyPem
}

// fingerprintCert mirrors the helper in cmd/siloctl/auth.go without
// importing the main package (which would force `package main` linkage
// to live with the test binary). Same algorithm, same output format.
func fingerprintCert(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// chunkClient assembles a chunk-store client over the mTLS port using
// the freshly-minted credentials.
func chunkClient(t *testing.T, node *silodNode, caPEM, certPEM, keyPEM []byte) (chunkv1.ChunkStoreClient, func()) {
	t.Helper()
	host, _, err := net.SplitHostPort(node.grpcAddr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA PEM did not parse")
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   host,
		MinVersion:   tls.VersionTLS13,
	}
	conn, err := grpc.NewClient(node.grpcAddr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		t.Fatalf("dial chunk: %v", err)
	}
	return chunkv1.NewChunkStoreClient(conn), func() { _ = conn.Close() }
}

func TestChunkRoundTrip_AgainstRealSilodBinaryViaBootstrapJoin(t *testing.T) {
	node := startSilod(t)
	defer node.teardown()

	caPEM, certPEM, keyPEM := claimClientCert(t, node)
	if !bytes.Contains(caPEM, []byte("BEGIN CERTIFICATE")) {
		t.Fatalf("CA PEM looks malformed:\n%s", caPEM)
	}
	if !bytes.Contains(certPEM, []byte("BEGIN CERTIFICATE")) {
		t.Fatalf("client cert PEM looks malformed:\n%s", certPEM)
	}
	if !bytes.Contains(keyPEM, []byte("PRIVATE KEY")) {
		t.Fatalf("client key PEM looks malformed:\n%s", keyPEM)
	}

	client, closeConn := chunkClient(t, node, caPEM, certPEM, keyPEM)
	defer closeConn()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := bytes.Repeat([]byte("silo-integration-"), 1024) // ~17 KiB
	const id = "integration-demo"

	putStream, err := client.Put(ctx)
	if err != nil {
		t.Fatalf("Put open: %v", err)
	}
	if err := putStream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: id}}}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	if err := putStream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Data{Data: payload}}); err != nil {
		t.Fatalf("send data: %v", err)
	}
	putResp, err := putStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("Put close: %v", err)
	}
	if putResp.Info.PlainBytes != int64(len(payload)) {
		t.Errorf("put plain_bytes: got %d, want %d", putResp.Info.PlainBytes, len(payload))
	}

	getStream, err := client.Get(ctx, &chunkv1.GetRequest{ChunkId: id})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var received bytes.Buffer
	for {
		msg, err := getStream.Recv()
		if err != nil {
			break
		}
		if d := msg.GetData(); len(d) > 0 {
			received.Write(d)
		}
	}
	if !bytes.Equal(received.Bytes(), payload) {
		t.Errorf("get round-trip mismatch (%d vs %d bytes)", received.Len(), len(payload))
	}

	if _, err := client.Delete(ctx, &chunkv1.DeleteRequest{ChunkId: id}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
