package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	chunkv1 "github.com/hyperized/silo/api/proto/silo/chunk/v1"
)

const (
	// putDataFrameSize keeps Put streaming frames well under gRPC's
	// 4 MiB default message ceiling.
	putDataFrameSize = 64 * 1024
)

// rpcTimeout caps a single chunk operation. Chunks are small (4 MiB
// default), so 30s is generous even over slow links. A package-level var
// so tests can shorten it to drive the deadline-expired error paths.
var rpcTimeout = 30 * time.Second

// dialer is overridable in tests so they can dial a test gRPC server
// without touching the real network. The production implementation
// loads mTLS material from the user's config dir; if no credentials
// are present it falls back to insecure (for the dev loop where
// silo-local is running without TLS hardening or for unit tests that
// inject a plaintext gRPC server).
var dialer = defaultDialer

func defaultDialer(target string) (*grpc.ClientConn, error) {
	tlsCfg, err := loadClientTLS()
	if err != nil {
		return nil, err
	}
	if tlsCfg == nil {
		return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return grpc.NewClient(target, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
}

// loadClientTLS reads ca.crt + client.crt + client.key from the per-user
// config dir (or the env-var override) and assembles a tls.Config wired
// for mTLS to silod. Returns (nil, nil) when no credentials are on disk
// — the caller falls back to insecure, which is the right behaviour for
// `siloctl chunk …` runs against a single-node dev cluster before the
// operator has claimed credentials. Errors are surfaced when files
// exist but are unreadable, so a half-broken config doesn't masquerade
// as "no credentials present".
func loadClientTLS() (*tls.Config, error) {
	dir, err := siloctlConfigDir()
	if err != nil {
		return nil, err
	}
	caPath := envDefault("SILO_CA_CERT", filepath.Join(dir, "ca.crt"))
	certPath := envDefault("SILO_CLIENT_CERT", filepath.Join(dir, "client.crt"))
	keyPath := envDefault("SILO_CLIENT_KEY", filepath.Join(dir, "client.key"))

	// All three files must be present for mTLS. The "no creds at all"
	// case is the common dev-loop pattern, so we treat it as a clean
	// "fall back to insecure" signal.
	if !fileReadable(caPath) || !fileReadable(certPath) || !fileReadable(keyPath) {
		return nil, nil
	}

	// caPath is from the operator's own config dir / env, not request input.
	caBytes, err := os.ReadFile(caPath) // #nosec G304
	if err != nil {
		return nil, fmt.Errorf("siloctl: could not read CA cert at %s (%w); remove the file or fix the path with SILO_CA_CERT", caPath, err)
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("siloctl: could not load client cert+key from %s / %s (%w); regenerate them with 'siloctl auth init'", certPath, keyPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("siloctl: CA cert at %s is not valid PEM; remove the file and re-run 'siloctl auth init'", caPath)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		// ServerName is intentionally unset; the caller can override
		// via tls.Config in fully-mTLS deployments. silod's node cert
		// includes both the node id (DNS-SAN) and 127.0.0.1 (IP-SAN),
		// so the default verification picks the right SAN as long as
		// --server points at one of them.
		MinVersion: tls.VersionTLS13,
	}, nil
}

// siloctlConfigDir is the seam shared between auth.go and chunk.go for
// locating per-user state. Indirection so tests can swap a temp dir in
// place of os.UserConfigDir().
func siloctlConfigDir() (string, error) {
	return userConfigDir()
}

func fileReadable(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// newChunkClient is overridable in tests so we can inject a client that
// fails on Send/Recv mid-stream. Production builds always wrap the
// generated client over the dialed conn.
var newChunkClient = func(conn *grpc.ClientConn) chunkv1.ChunkStoreClient {
	return chunkv1.NewChunkStoreClient(conn)
}

func runChunk(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelpFlag(args[0]) {
		printChunkUsage(stdout)
		if len(args) == 0 {
			return 2
		}
		return 0
	}

	sub := args[0]
	rest := args[1:]

	switch sub {
	case "put":
		return runChunkPut(rest, stdin, stdout, stderr)
	case "get":
		return runChunkGet(rest, stdout, stderr)
	case "delete":
		return runChunkDelete(rest, stdout, stderr)
	case "stat":
		return runChunkStat(rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "siloctl chunk: unknown subcommand %q. Run 'siloctl chunk help'.\n", sub)
		return 2
	}
}

func printChunkUsage(w io.Writer) {
	fmt.Fprint(w, `siloctl chunk — manage chunks on a silod node

Usage:
  siloctl chunk put     <chunk-id> <file>      Upload a file as a chunk
  siloctl chunk get     <chunk-id> [<file>]    Download a chunk (default: stdout)
  siloctl chunk delete  <chunk-id>             Remove a chunk
  siloctl chunk stat    <chunk-id>             Show chunk size and timestamps

Each subcommand accepts --server=host:port to point at a different silod.
Defaults to SILO_SERVER (or 127.0.0.1:7000 if unset).
`)
}

// newSubFlagSet returns a flag.FlagSet that errors-out cleanly to stderr
// instead of os.Exit'ing on a parse error, keeping runMain in control of
// the exit code. The --server default cascades through SILO_SERVER, the
// default_grpc_server stored by 'siloctl auth init', and finally the
// hard-coded loopback target — so a freshly-bootstrapped operator never
// has to remember a port number after running 'auth init'.
func newSubFlagSet(name string, stderr io.Writer) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet("siloctl chunk "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", envDefault("SILO_SERVER", configuredChunkServer()), "silod gRPC address (host:port)")
	return fs, server
}

// configuredChunkServer reads default_grpc_server from the auth config
// the operator wrote during 'auth init'. Returns the hard-coded loopback
// fallback when no config is present so single-node dev still works
// before any bootstrap has happened.
func configuredChunkServer() string {
	dir, err := siloctlConfigDir()
	if err != nil {
		return defaultServer
	}
	cfg, err := loadAuthConfig(dir)
	if err != nil || cfg.DefaultGRPCServer == "" {
		return defaultServer
	}
	return cfg.DefaultGRPCServer
}

func runChunkPut(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("put", stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 2 {
		fmt.Fprintln(stderr, "Usage: siloctl chunk put [--server=host:port] <chunk-id> <file>")
		return 2
	}
	id, path := rest[0], rest[1]

	source, err := openInput(path, stdin)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not open %q for reading (%v); check the path exists and you have read permission\n", path, err)
		return 1
	}
	defer func() { _ = source.Close() }()

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()
	client := newChunkClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	stream, err := client.Put(ctx)
	if err != nil {
		return reportRPC(stderr, "chunk put", err)
	}
	if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Header{Header: &chunkv1.PutHeader{ChunkId: id}}}); err != nil {
		return reportRPC(stderr, "chunk put", err)
	}

	buf := make([]byte, putDataFrameSize)
	for {
		n, readErr := source.Read(buf)
		if n > 0 {
			if err := stream.Send(&chunkv1.PutRequest{Body: &chunkv1.PutRequest_Data{Data: append([]byte(nil), buf[:n]...)}}); err != nil {
				return reportRPC(stderr, "chunk put", err)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			fmt.Fprintf(stderr, "siloctl: could not read from %q (%v); the file may have been truncated or your disk is unhealthy\n", path, readErr)
			return 1
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return reportRPC(stderr, "chunk put", err)
	}
	fmt.Fprintf(stdout, "Stored chunk %s — %d bytes plaintext, %d bytes on disk.\n",
		resp.Info.ChunkId, resp.Info.PlainBytes, resp.Info.StoredBytes)
	return 0
}

func runChunkGet(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("get", stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) < 1 || len(rest) > 2 {
		fmt.Fprintln(stderr, "Usage: siloctl chunk get [--server=host:port] <chunk-id> [<file>]")
		return 2
	}
	id := rest[0]
	sink := stdout
	var (
		out     *os.File
		outPath string
	)
	if len(rest) == 2 {
		outPath = rest[1]
		f, err := os.Create(outPath) // #nosec G304 -- operator's own CLI destination path
		if err != nil {
			fmt.Fprintf(stderr, "siloctl: could not open %q for writing (%v); check the parent directory exists and is writable\n", outPath, err)
			return 1
		}
		// Safety net for the error-return paths below; the success path
		// closes explicitly and checks the result, since a failed flush
		// on close means the chunk was not fully written.
		defer func() { _ = f.Close() }()
		out = f
		sink = f
	}

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()
	client := newChunkClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	stream, err := client.Get(ctx, &chunkv1.GetRequest{ChunkId: id})
	if err != nil {
		return reportRPC(stderr, "chunk get", err)
	}

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return reportRPC(stderr, "chunk get", err)
		}
		if data := msg.GetData(); len(data) > 0 {
			if _, werr := sink.Write(data); werr != nil {
				fmt.Fprintf(stderr, "siloctl: could not write chunk data to the destination (%v)\n", werr)
				return 1
			}
		}
	}
	if out != nil {
		if err := out.Close(); err != nil {
			fmt.Fprintf(stderr, "siloctl: could not finalize %q (%v); the chunk may be incompletely written — re-run the get\n", outPath, err)
			return 1
		}
	}
	return 0
}

func runChunkDelete(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("delete", stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintln(stderr, "Usage: siloctl chunk delete [--server=host:port] <chunk-id>")
		return 2
	}
	id := rest[0]

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()
	client := newChunkClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	if _, err := client.Delete(ctx, &chunkv1.DeleteRequest{ChunkId: id}); err != nil {
		return reportRPC(stderr, "chunk delete", err)
	}
	fmt.Fprintf(stdout, "Deleted chunk %s.\n", id)
	return 0
}

func runChunkStat(args []string, stdout, stderr io.Writer) int {
	fs, server := newSubFlagSet("stat", stderr)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintln(stderr, "Usage: siloctl chunk stat [--server=host:port] <chunk-id>")
		return 2
	}
	id := rest[0]

	conn, err := dialer(*server)
	if err != nil {
		fmt.Fprintf(stderr, "siloctl: could not dial silod at %q (%v); check that silod is running and SILO_SERVER points at its gRPC address\n", *server, err)
		return 1
	}
	defer func() { _ = conn.Close() }()
	client := newChunkClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := client.Stat(ctx, &chunkv1.StatRequest{ChunkId: id})
	if err != nil {
		return reportRPC(stderr, "chunk stat", err)
	}
	info := resp.Info
	fmt.Fprintf(stdout, "chunk %s\n", info.ChunkId)
	fmt.Fprintf(stdout, "  plaintext bytes: %d\n", info.PlainBytes)
	fmt.Fprintf(stdout, "  on-disk bytes:   %d\n", info.StoredBytes)
	if t := info.CreatedAt; t != nil {
		fmt.Fprintf(stdout, "  created at:      %s\n", t.AsTime().Format(time.RFC3339))
	}
	return 0
}

// openInput returns either an open file or wraps stdin when path is "-".
// The stdin special-case lets `cat foo | siloctl chunk put id -` work.
func openInput(path string, stdin io.Reader) (io.ReadCloser, error) {
	if path == "-" {
		return io.NopCloser(stdin), nil
	}
	return os.Open(path) // #nosec G304 -- operator's own CLI source path
}

// reportRPC turns a gRPC error into an actionable stderr line. Known
// codes get a "this is what to do" hint; unknown codes pass through.
func reportRPC(stderr io.Writer, op string, err error) int {
	st, ok := status.FromError(err)
	if !ok {
		fmt.Fprintf(stderr, "siloctl: %s failed (%v); check connectivity to silod\n", op, err)
		return 1
	}
	switch st.Code() {
	case codes.NotFound:
		fmt.Fprintf(stderr, "siloctl: %s — %s\n", op, st.Message())
		return 1
	case codes.InvalidArgument:
		fmt.Fprintf(stderr, "siloctl: %s — %s\n", op, st.Message())
		return 2
	case codes.Unavailable:
		fmt.Fprintf(stderr, "siloctl: %s — silod is unavailable (%s); confirm the daemon is running and reachable\n", op, st.Message())
		return 1
	default:
		fmt.Fprintf(stderr, "siloctl: %s — %s (gRPC %s)\n", op, st.Message(), st.Code())
		return 1
	}
}
