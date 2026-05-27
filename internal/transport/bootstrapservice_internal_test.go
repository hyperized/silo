package transport

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bootstrapv1 "github.com/hyperized/silo/api/proto/silo/bootstrap/v1"
	"github.com/hyperized/silo/internal/bootstraptoken"
	"github.com/hyperized/silo/internal/clustertls"
)

// fakeRedeemer scripts the response of TokenRedeemer.Redeem; the bootstrap
// service uses only that one method so this is the entire interface.
type fakeRedeemer struct {
	err      error
	calls    int
	gotPlain string
}

func (f *fakeRedeemer) Redeem(plaintext string) error {
	f.calls++
	f.gotPlain = plaintext
	return f.err
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stubMinter returns a deterministic three-tuple so tests can assert on
// the exact payload without burning entropy. Production wires through
// clustertls.MintClientCert.
func stubMinter(caPEM, certPEM, keyPEM []byte, err error) ClientCertMinter {
	return func(_ string) ([]byte, []byte, []byte, error) {
		return caPEM, certPEM, keyPEM, err
	}
}

func TestBootstrapJoin_HappyPath(t *testing.T) {
	red := &fakeRedeemer{}
	minter := stubMinter([]byte("CA"), []byte("CERT"), []byte("KEY"), nil)
	svc := NewBootstrapService(red, minter, discardLogger())

	resp, err := svc.Join(context.Background(), &bootstrapv1.JoinRequest{Token: "tok", Principal: "user@host"})
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if string(resp.CaCertPem) != "CA" || string(resp.ClientCertPem) != "CERT" || string(resp.ClientKeyPem) != "KEY" {
		t.Errorf("Join response payload mismatch: %+v", resp)
	}
	if red.calls != 1 || red.gotPlain != "tok" {
		t.Errorf("redeemer not called as expected (calls=%d, plain=%q)", red.calls, red.gotPlain)
	}
}

func TestBootstrapJoin_RejectsEmptyInputs(t *testing.T) {
	svc := NewBootstrapService(&fakeRedeemer{}, stubMinter(nil, nil, nil, nil), discardLogger())
	cases := []struct {
		name string
		req  *bootstrapv1.JoinRequest
		want codes.Code
		sub  string
	}{
		{"nil request", nil, codes.InvalidArgument, "token is required"},
		{"empty token", &bootstrapv1.JoinRequest{Token: "  ", Principal: "p"}, codes.InvalidArgument, "token is required"},
		{"empty principal", &bootstrapv1.JoinRequest{Token: "t", Principal: " "}, codes.InvalidArgument, "principal is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Join(context.Background(), tc.req)
			st, ok := status.FromError(err)
			if !ok || st.Code() != tc.want {
				t.Errorf("got %v, want code %v", err, tc.want)
			}
			if !strings.Contains(st.Message(), tc.sub) {
				t.Errorf("message %q missing substring %q", st.Message(), tc.sub)
			}
		})
	}
}

func TestBootstrapJoin_TokenNotRecognised(t *testing.T) {
	red := &fakeRedeemer{err: bootstraptoken.ErrTokenNotFound}
	svc := NewBootstrapService(red, stubMinter(nil, nil, nil, nil), discardLogger())

	_, err := svc.Join(context.Background(), &bootstrapv1.JoinRequest{Token: "bad", Principal: "user@host"})
	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Errorf("got code %v, want PermissionDenied", st.Code())
	}
	if !strings.Contains(st.Message(), "not recognised") || !strings.Contains(st.Message(), "SILO_PRINT_BOOTSTRAP_TOKEN") {
		t.Errorf("message should name the env var fix, got %q", st.Message())
	}
}

func TestBootstrapJoin_TokenStoreInternalFailure(t *testing.T) {
	// A non-sentinel error from the redeemer (e.g. disk failure mid-Redeem)
	// should surface as Internal so operators see something is wrong with
	// the cluster, not a credentials problem with the caller.
	red := &fakeRedeemer{err: errors.New("disk on fire")}
	svc := NewBootstrapService(red, stubMinter(nil, nil, nil, nil), discardLogger())

	_, err := svc.Join(context.Background(), &bootstrapv1.JoinRequest{Token: "ok", Principal: "user@host"})
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Errorf("got %v, want Internal for non-sentinel redeem error", st.Code())
	}
	if !strings.Contains(st.Message(), "could not record token consumption") {
		t.Errorf("message %q missing actionable wrapper", st.Message())
	}
}

func TestBootstrapJoin_MinterFailure(t *testing.T) {
	red := &fakeRedeemer{}
	svc := NewBootstrapService(red, stubMinter(nil, nil, nil, errors.New("simulated CA-key missing")), discardLogger())

	_, err := svc.Join(context.Background(), &bootstrapv1.JoinRequest{Token: "ok", Principal: "user@host"})
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Errorf("got %v, want Internal", st.Code())
	}
	if !strings.Contains(st.Message(), "could not sign a client certificate") {
		t.Errorf("message %q missing actionable wrapper", st.Message())
	}
}

func TestNewClientCertMinter_RoundTrip(t *testing.T) {
	caPEM, keyPEM, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := clustertls.LoadCA(caPEM, keyPEM)
	if err != nil {
		t.Fatalf("LoadCA: %v", err)
	}
	minter := NewClientCertMinter(ca)
	gotCA, gotCert, gotKey, err := minter("user@host")
	if err != nil {
		t.Fatalf("minter: %v", err)
	}
	if string(gotCA) == "" || string(gotCert) == "" || string(gotKey) == "" {
		t.Fatal("minter returned empty PEM")
	}
	// The minted client cert must validate against the CA we passed in.
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(gotCA) {
		t.Fatal("returned CA PEM is not parseable")
	}
	cert := parseClientPEM(t, gotCert)
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Errorf("client cert does not chain to the returned CA: %v", err)
	}
	if cert.Subject.CommonName != "user@host" {
		t.Errorf("CN: got %q, want user@host", cert.Subject.CommonName)
	}
}

func TestNewClientCertMinter_PropagatesMintError(t *testing.T) {
	// A CA missing its private key cannot sign new identities; the
	// minter must surface clustertls's actionable error so the join
	// handler can pass it back to the caller.
	caPEM, _, err := clustertls.GenerateCA("silo-test", time.Hour)
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	ca, err := clustertls.LoadCA(caPEM, nil)
	if err != nil {
		t.Fatalf("LoadCA cert-only: %v", err)
	}
	minter := NewClientCertMinter(ca)
	if _, _, _, err := minter("user@host"); err == nil || !strings.Contains(err.Error(), "private key") {
		t.Errorf("got %v, want missing-private-key error", err)
	}
}

// parseClientPEM turns a PEM-encoded client cert into an x509.Certificate.
func parseClientPEM(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("no PEM block in client cert")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	return cert
}
