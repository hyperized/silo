package transport

import (
	"context"
	"testing"

	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
	"google.golang.org/grpc"
)

type fakeDrainer struct {
	calls    int
	announce bool
}

func (f *fakeDrainer) Drain() bool {
	f.calls++
	return f.announce
}

func TestNodeAdminService_Drain(t *testing.T) {
	d := &fakeDrainer{announce: true}
	svc := NewNodeAdminService(d, "silo-a", discardStatusLogger())

	resp, err := svc.Drain(context.Background(), &nodev1.DrainRequest{})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if resp.GetNodeId() != "silo-a" || !resp.GetAnnounced() {
		t.Errorf("response = %+v, want silo-a announced", resp)
	}
	if d.calls != 1 {
		t.Errorf("drainer called %d times, want 1", d.calls)
	}

	// Already drained: announced=false, still no error (idempotent).
	d.announce = false
	resp, err = svc.Drain(context.Background(), &nodev1.DrainRequest{})
	if err != nil || resp.GetAnnounced() {
		t.Errorf("second drain = (%+v, %v), want announced=false", resp, err)
	}
}

func TestWithNodeAdminService_Registers(t *testing.T) {
	var cfg grpcConfig
	WithNodeAdminService(NewNodeAdminService(&fakeDrainer{}, "n", discardStatusLogger()))(&cfg)
	s := grpc.NewServer()
	for _, register := range cfg.services {
		register(s)
	}
	if _, ok := s.GetServiceInfo()["silo.node.v1.NodeAdmin"]; !ok {
		t.Error("WithNodeAdminService did not register the NodeAdmin service")
	}
}
