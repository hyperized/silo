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
	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/diskusage"
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
		WithDiskUsage(func(string) (diskusage.Usage, error) {
			return diskusage.Usage{CapacityBytes: 1000, UsedBytes: 600, AvailableBytes: 400}, nil
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
		WithDiskUsage(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, errors.New("statfs failed") }),
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
	var cfg grpcConfig
	svc := NewStatusService(fakeStatusMembers{}, fakeStatusStore{}, "/d", "n", "v", discardStatusLogger())
	WithStatusService(svc)(&cfg)
	s := grpc.NewServer()
	for _, register := range cfg.services {
		register(s)
	}
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

type fakeBackupReporter struct{ st backup.Status }

func (f fakeBackupReporter) Status() backup.Status { return f.st }

func TestStatusService_GetStatus_Backup(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	next := time.Unix(1_700_021_600, 0)
	run := &backup.Run{
		StartedAt: started, FinishedAt: started.Add(time.Minute), OK: false, Error: "target unreachable",
		ChunksUploaded: 5, ChunksSkipped: 3, BytesUploaded: 8192, Manifest: "silo-a/manifest.json",
	}
	cases := []struct {
		name     string
		reporter BackupReporter
		check    func(t *testing.T, b *statusv1.BackupStatus)
	}{
		{"no reporter means not configured", nil, func(t *testing.T, b *statusv1.BackupStatus) {
			if b == nil || b.GetConfigured() || b.GetTarget() != "" || b.GetLastRun() != nil || b.GetNextRunUnix() != 0 {
				t.Errorf("backup = %+v, want zero value with configured=false", b)
			}
		}},
		{"configured, never ran, nothing scheduled", fakeBackupReporter{backup.Status{
			Configured: true, Target: "s3://bucket/p", Interval: 6 * time.Hour,
		}}, func(t *testing.T, b *statusv1.BackupStatus) {
			if !b.GetConfigured() || b.GetTarget() != "s3://bucket/p" || b.GetIntervalSeconds() != 21600 || b.GetRunning() {
				t.Errorf("backup = %+v", b)
			}
			if b.GetNextRunUnix() != 0 || b.GetLastRun() != nil {
				t.Errorf("next=%d last=%v, want 0 and nil", b.GetNextRunUnix(), b.GetLastRun())
			}
		}},
		{"running with schedule and last run", fakeBackupReporter{backup.Status{
			Configured: true, Target: "gs://b", Interval: time.Hour, Running: true, NextRun: next, LastRun: run,
		}}, func(t *testing.T, b *statusv1.BackupStatus) {
			if !b.GetRunning() || b.GetIntervalSeconds() != 3600 || b.GetNextRunUnix() != next.Unix() {
				t.Errorf("backup = %+v", b)
			}
			r := b.GetLastRun()
			if r.GetStartedUnix() != started.Unix() || r.GetFinishedUnix() != started.Add(time.Minute).Unix() ||
				r.GetOk() || r.GetError() != "target unreachable" || r.GetChunksUploaded() != 5 ||
				r.GetChunksSkipped() != 3 || r.GetBytesUploaded() != 8192 || r.GetManifest() != "silo-a/manifest.json" {
				t.Errorf("last run = %+v", r)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opts []StatusOption
			if tc.reporter != nil {
				opts = append(opts, WithBackupReporter(tc.reporter))
			}
			svc := NewStatusService(fakeStatusMembers{}, fakeStatusStore{}, "/d", "n", "v", discardStatusLogger(), opts...)
			resp, err := svc.GetStatus(context.Background(), &statusv1.GetStatusRequest{})
			if err != nil {
				t.Fatalf("GetStatus: %v", err)
			}
			tc.check(t, resp.GetBackup())
		})
	}
}
