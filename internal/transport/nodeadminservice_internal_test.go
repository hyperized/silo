package transport

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/hyperized/silo/api/proto/silo/node/v1"
	"github.com/hyperized/silo/internal/backup"
)

type fakeBackupRunner struct {
	run   backup.Run
	err   error
	calls int
}

func (f *fakeBackupRunner) RunNow(context.Context) (backup.Run, error) {
	f.calls++
	return f.run, f.err
}

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

func TestNodeAdminService_RunBackup(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	okRun := backup.Run{
		StartedAt: started, FinishedAt: started.Add(time.Minute), OK: true,
		ChunksUploaded: 4, ChunksSkipped: 2, BytesUploaded: 4096, Manifest: "silo-a/manifest-1.json",
	}
	failedRun := backup.Run{StartedAt: started, FinishedAt: started, Error: "target unreachable; check credentials"}

	cases := []struct {
		name     string
		runner   *fakeBackupRunner // nil means no runner configured
		wantCode codes.Code
		wantMsg  string
		wantRun  backup.Run
	}{
		{name: "not configured", wantCode: codes.FailedPrecondition, wantMsg: "SILO_BACKUP_TARGET"},
		{name: "run in progress", runner: &fakeBackupRunner{err: backup.ErrRunInProgress}, wantCode: codes.FailedPrecondition, wantMsg: "already running"},
		{name: "wrapped run in progress", runner: &fakeBackupRunner{err: fmt.Errorf("x: %w", backup.ErrRunInProgress)}, wantCode: codes.FailedPrecondition, wantMsg: "already running"},
		{name: "stopped", runner: &fakeBackupRunner{err: backup.ErrStopped}, wantCode: codes.FailedPrecondition, wantMsg: "shutting down"},
		{name: "deadline exceeded", runner: &fakeBackupRunner{err: context.DeadlineExceeded}, wantCode: codes.DeadlineExceeded},
		{name: "canceled", runner: &fakeBackupRunner{err: context.Canceled}, wantCode: codes.Canceled},
		{name: "unrelated error", runner: &fakeBackupRunner{err: errors.New("boom")}, wantCode: codes.Unknown},
		{name: "success", runner: &fakeBackupRunner{run: okRun}, wantCode: codes.OK, wantRun: okRun},
		{name: "failed run is still a successful call", runner: &fakeBackupRunner{run: failedRun}, wantCode: codes.OK, wantRun: failedRun},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opts []NodeAdminOption
			if tc.runner != nil {
				opts = append(opts, WithBackupRunner(tc.runner))
			}
			svc := NewNodeAdminService(&fakeDrainer{}, "silo-a", discardStatusLogger(), opts...)
			resp, err := svc.RunBackup(context.Background(), &nodev1.RunBackupRequest{})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("code = %v (err %v), want %v", got, err, tc.wantCode)
			}
			if tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error %q lacks %q", err, tc.wantMsg)
			}
			if tc.wantCode != codes.OK {
				if resp != nil {
					t.Errorf("response = %+v on error, want nil", resp)
				}
				return
			}
			if resp.GetNodeId() != "silo-a" {
				t.Errorf("node id = %q, want silo-a", resp.GetNodeId())
			}
			r := resp.GetRun()
			if r.GetOk() != tc.wantRun.OK || r.GetError() != tc.wantRun.Error || r.GetManifest() != tc.wantRun.Manifest ||
				r.GetChunksUploaded() != tc.wantRun.ChunksUploaded || r.GetChunksSkipped() != tc.wantRun.ChunksSkipped ||
				r.GetBytesUploaded() != tc.wantRun.BytesUploaded || r.GetStartedUnix() != tc.wantRun.StartedAt.Unix() ||
				r.GetFinishedUnix() != tc.wantRun.FinishedAt.Unix() {
				t.Errorf("run = %+v, want %+v", r, tc.wantRun)
			}
			if tc.runner.calls != 1 {
				t.Errorf("runner called %d times, want 1", tc.runner.calls)
			}
		})
	}
}
