package clienttoken

import (
	"context"
	"testing"

	"google.golang.org/grpc"
)

func TestBearer_Metadata(t *testing.T) {
	b := bearer{token: "abc"}
	md, err := b.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if md["authorization"] != "Bearer abc" {
		t.Errorf("authorization = %q, want %q", md["authorization"], "Bearer abc")
	}
	if !b.RequireTransportSecurity() {
		t.Error("bearer must require transport security so tokens are never sent in the clear")
	}
}

func TestDialOption_EmptyIsNoOp(t *testing.T) {
	if _, ok := DialOption("").(grpc.EmptyDialOption); !ok {
		t.Error("an empty token should yield grpc.EmptyDialOption")
	}
	if _, ok := DialOption("tok").(grpc.EmptyDialOption); ok {
		t.Error("a non-empty token should yield a real per-RPC credential option, not EmptyDialOption")
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv(EnvVar, "")
	if _, ok := FromEnv().(grpc.EmptyDialOption); !ok {
		t.Error("unset SILO_TOKEN should yield a no-op option")
	}
	t.Setenv(EnvVar, "a-token")
	if _, ok := FromEnv().(grpc.EmptyDialOption); ok {
		t.Error("set SILO_TOKEN should yield a real option")
	}
}
