package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// dialer is swappable so tests can stand in a fake. Production dials silod.
var dialer = dialSilod

// dialSilod opens a lazy gRPC connection to silod at target. It uses mTLS when
// the cluster client credentials are present in the environment and falls back
// to an insecure connection otherwise, matching siloctl — convenient for a
// single-node dev cluster, while a real cluster mounts the cert material and
// gets mutual TLS.
func dialSilod(target string) (*grpc.ClientConn, error) {
	tlsCfg, err := clientTLSFromEnv()
	if err != nil {
		return nil, err
	}
	if tlsCfg == nil {
		return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return grpc.NewClient(target, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
}

// clientTLSFromEnv assembles an mTLS client config from SILO_CA_CERT,
// SILO_CLIENT_CERT, and SILO_CLIENT_KEY. It returns (nil, nil) when none are
// set — the signal to dial insecurely — and an error only when a path is set
// but unusable, so a half-configured deployment fails loudly instead of
// silently dropping to insecure.
func clientTLSFromEnv() (*tls.Config, error) {
	caPath := os.Getenv("SILO_CA_CERT")
	certPath := os.Getenv("SILO_CLIENT_CERT")
	keyPath := os.Getenv("SILO_CLIENT_KEY")
	if caPath == "" && certPath == "" && keyPath == "" {
		return nil, nil
	}
	if caPath == "" || certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("for mTLS to silod, set all of SILO_CA_CERT, SILO_CLIENT_CERT, and SILO_CLIENT_KEY (or none to connect insecurely on a dev cluster)")
	}

	caBytes, err := os.ReadFile(caPath) //nolint:gosec // operator-supplied config path, not request input
	if err != nil {
		return nil, fmt.Errorf("could not read the CA cert at %s (%w); fix SILO_CA_CERT", caPath, err)
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("could not load the client cert+key from %s / %s (%w); check SILO_CLIENT_CERT and SILO_CLIENT_KEY", certPath, keyPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("the CA cert at %s is not valid PEM; replace it with silod's cluster CA", caPath)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}
