package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hyperized/silo/internal/clienttoken"
)

// dialer is swappable so tests can stand in a fake.
var dialer = dialSilod

// dialSilod opens a lazy gRPC connection to silod. It uses mTLS when the cluster
// client credentials are in the environment (SILO_CA_CERT/SILO_CLIENT_CERT/
// SILO_CLIENT_KEY) and falls back to insecure otherwise, matching siloctl and
// silo-csi.
func dialSilod(target string) (*grpc.ClientConn, error) {
	tlsCfg, err := clientTLSFromEnv()
	if err != nil {
		return nil, err
	}
	if tlsCfg == nil {
		return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		clienttoken.FromEnv())
}

func clientTLSFromEnv() (*tls.Config, error) {
	caPath := os.Getenv("SILO_CA_CERT")
	certPath := os.Getenv("SILO_CLIENT_CERT")
	keyPath := os.Getenv("SILO_CLIENT_KEY")
	if caPath == "" && certPath == "" && keyPath == "" {
		return nil, nil
	}
	if caPath == "" || certPath == "" || keyPath == "" {
		return nil, fmt.Errorf("for mTLS set all of SILO_CA_CERT, SILO_CLIENT_CERT, and SILO_CLIENT_KEY (or none for an insecure dev connection)")
	}
	caBytes, err := os.ReadFile(caPath) //nolint:gosec // operator-supplied config path
	if err != nil {
		return nil, fmt.Errorf("could not read the CA cert at %s (%w)", caPath, err)
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("could not load the client cert+key (%w)", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("the CA cert at %s is not valid PEM", caPath)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS13}, nil
}
