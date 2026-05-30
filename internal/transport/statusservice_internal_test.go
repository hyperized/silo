package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/membership"
)

type fakeStatusMembers struct{ nodes []membership.Node }

func (f fakeStatusMembers) Members() []membership.Node { return f.nodes }

type fakeStatusStore struct {
	ids []string
	err error
}

func (f fakeStatusStore) List(context.Context) ([]string, error) { return f.ids, f.err }

func discardStatusLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestStatusService_GetStatus(t *testing.T) {
	changed := time.Unix(1_700_000_000, 0)
	members := fakeStatusMembers{nodes: []membership.Node{
		{ID: "silo-a", Address: "silo-a:7100", DataAddress: "silo-a:7000", State: membership.StateAlive, Incarnation: 3, LastChange: changed},
		{ID: "silo-b", Address: "silo-b:7100", DataAddress: "silo-b:7000", State: membership.StateSuspect, Incarnation: 1, LastChange: changed},
	}}
	store := fakeStatusStore{ids: []string{"c0", "c1", "c2"}}
	svc := NewStatusService(members, store, "/var/lib/silo", "silo-a", "v1.2.3", discardStatusLogger(),
		WithDiskUsage(func(string) (DiskUsage, error) {
			return DiskUsage{CapacityBytes: 1000, UsedBytes: 600, AvailableBytes: 400}, nil
		}))

	resp, err := svc.GetStatus(context.Background(), &statusv1.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if resp.GetRespondingNodeId() != "silo-a" || resp.GetVersion() != "v1.2.3" {
		t.Errorf("identity = (%q, %q)", resp.GetRespondingNodeId(), resp.GetVersion())
	}
	if len(resp.GetNodes()) != 2 {
		t.Fatalf("nodes = %d, want 2", len(resp.GetNodes()))
	}
	a := resp.GetNodes()[0]
	if a.GetId() != "silo-a" || a.GetGossipAddress() != "silo-a:7100" || a.GetDataAddress() != "silo-a:7000" ||
		a.GetState() != statusv1.NodeState_NODE_STATE_ALIVE || a.GetIncarnation() != 3 || a.GetLastChangeUnix() != changed.Unix() {
		t.Errorf("node a = %+v", a)
	}
	if resp.GetNodes()[1].GetState() != statusv1.NodeState_NODE_STATE_SUSPECT {
		t.Errorf("node b state = %v, want SUSPECT", resp.GetNodes()[1].GetState())
	}

	st := resp.GetStorage()
	if st.GetDataDir() != "/var/lib/silo" || st.GetCapacityBytes() != 1000 || st.GetUsedBytes() != 600 || st.GetAvailableBytes() != 400 || st.GetChunkCount() != 3 {
		t.Errorf("storage = %+v", st)
	}
}

func TestStatusService_DegradesOnStorageErrors(t *testing.T) {
	svc := NewStatusService(
		fakeStatusMembers{nodes: []membership.Node{{ID: "n", State: membership.StateAlive}}},
		fakeStatusStore{err: errors.New("disk gone")},
		"/data", "n", "v1", discardStatusLogger(),
		WithDiskUsage(func(string) (DiskUsage, error) { return DiskUsage{}, errors.New("statfs failed") }),
	)
	resp, err := svc.GetStatus(context.Background(), &statusv1.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus should not fail on storage errors: %v", err)
	}
	// Membership still reports; storage figures are left zero.
	if len(resp.GetNodes()) != 1 {
		t.Errorf("nodes = %d, want 1", len(resp.GetNodes()))
	}
	st := resp.GetStorage()
	if st.GetDataDir() != "/data" || st.GetCapacityBytes() != 0 || st.GetChunkCount() != 0 {
		t.Errorf("storage on error = %+v, want zeroed figures with the dir set", st)
	}
}

func TestWithStatusService_Registers(t *testing.T) {
	s := grpc.NewServer()
	svc := NewStatusService(fakeStatusMembers{}, fakeStatusStore{}, "/d", "n", "v", discardStatusLogger())
	WithStatusService(svc)(s)
	if _, ok := s.GetServiceInfo()["silo.status.v1.ClusterStatus"]; !ok {
		t.Error("WithStatusService did not register the ClusterStatus service")
	}
}

func TestProtoNodeState(t *testing.T) {
	cases := map[membership.State]statusv1.NodeState{
		membership.StateAlive:   statusv1.NodeState_NODE_STATE_ALIVE,
		membership.StateSuspect: statusv1.NodeState_NODE_STATE_SUSPECT,
		membership.StateDead:    statusv1.NodeState_NODE_STATE_DEAD,
		membership.StateLeft:    statusv1.NodeState_NODE_STATE_LEFT,
		membership.State(99):    statusv1.NodeState_NODE_STATE_UNSPECIFIED,
	}
	for in, want := range cases {
		if got := protoNodeState(in); got != want {
			t.Errorf("protoNodeState(%v) = %v, want %v", in, got, want)
		}
	}
}
