package transport

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/grpc"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
)

type fakeStatusBuilder struct{ resp *statusv1.GetStatusResponse }

func (f fakeStatusBuilder) Build(context.Context) *statusv1.GetStatusResponse { return f.resp }

func discardStatusLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestStatusService_GetStatusReturnsBuilderSnapshot(t *testing.T) {
	want := &statusv1.GetStatusResponse{RespondingNodeId: "silo-a", Version: "v1.2.3"}
	resp, err := NewStatusService(fakeStatusBuilder{resp: want}).GetStatus(context.Background(), &statusv1.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if resp != want {
		t.Errorf("GetStatus returned %+v, want the builder's snapshot %+v", resp, want)
	}
}

func TestWithStatusService_Registers(t *testing.T) {
	var cfg grpcConfig
	WithStatusService(NewStatusService(fakeStatusBuilder{}))(&cfg)
	s := grpc.NewServer()
	for _, register := range cfg.services {
		register(s)
	}
	if _, ok := s.GetServiceInfo()["silo.status.v1.ClusterStatus"]; !ok {
		t.Error("WithStatusService did not register the ClusterStatus service")
	}
}
