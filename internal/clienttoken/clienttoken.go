// Package clienttoken attaches silo capability tokens to outbound gRPC calls.
// CSI, FUSE, and siloctl all read SILO_TOKEN and present it as a bearer token so
// silod's TokenAuthenticator can scope what the call may do. It is the client
// half of internal/captoken + internal/transport's interceptor.
package clienttoken

import (
	"context"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// EnvVar is the environment variable each client reads its token from.
const EnvVar = "SILO_TOKEN"

// bearer is a grpc.PerRPCCredentials that sends "authorization: Bearer <token>"
// on every call. It requires transport security so the token is never sent in
// the clear — silod always runs mTLS, so this is always satisfied in practice.
type bearer struct {
	token string
}

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearer) RequireTransportSecurity() bool { return true }

// compile-time assertion that bearer satisfies the gRPC contract.
var _ credentials.PerRPCCredentials = bearer{}

// DialOption returns a grpc.DialOption that attaches the given token to every
// call, or grpc.EmptyDialOption() when the token is empty — so callers can
// thread it unconditionally without branching.
func DialOption(token string) grpc.DialOption {
	if token == "" {
		return grpc.EmptyDialOption{}
	}
	return grpc.WithPerRPCCredentials(bearer{token: token})
}

// FromEnv returns the DialOption built from SILO_TOKEN, or a no-op option when
// it is unset (an mTLS-only deployment, or one where token enforcement is off).
func FromEnv() grpc.DialOption {
	return DialOption(os.Getenv(EnvVar))
}
