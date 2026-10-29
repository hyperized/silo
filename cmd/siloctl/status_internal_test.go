package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
)

// errStatusClient fails GetStatus so the reportRPC path can be exercised.
type errStatusClient struct{}

func (errStatusClient) GetStatus(context.Context, *statusv1.GetStatusRequest, ...grpc.CallOption) (*statusv1.GetStatusResponse, error) {
	return nil, status.Error(codes.Unavailable, "silod is down")
}

func TestStatus_Succeeds(t *testing.T) {
	addr, teardown := newTestServer(t)
	defer teardown()

	// Pin the clock so the rendered age is deterministic.
	prev := statusNow
	t.Cleanup(func() { statusNow = prev })
	statusNow = func() time.Time { return time.Unix(1_700_000_000, 0).Add(90 * time.Second) }

	var out, errBuf bytes.Buffer
	if code := runStatus([]string{"--server=" + addr}, &out, &errBuf); code != 0 {
		t.Fatalf("status code = %d, err = %s", code, errBuf.String())
	}
	got := out.String()
	for _, want := range []string{
		"Cluster: 2 nodes — 1 alive, 1 suspect",
		"Queried silo-a (silo test)",
		"silo-a", "alive", "silo-a:7100",
		"silo-b", "suspect",
		"256.0 MiB/1.0 GiB (25%)", // silo-a's advertised capacity
		"1m ago",
		"Storage on silo-a (/var/lib/silo):",
		"chunk",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("status output missing %q.\n--- got ---\n%s", want, got)
		}
	}
}

func TestStatus_UsageError(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := runStatus([]string{"extra-arg"}, &out, &errBuf); code != 2 {
		t.Errorf("trailing-arg code = %d, want 2", code)
	}
	errBuf.Reset()
	if code := runStatus([]string{"--bogus"}, &out, &errBuf); code != 2 {
		t.Errorf("bad-flag code = %d, want 2", code)
	}
}

func TestStatus_DialError(t *testing.T) {
	prev := dialer
	t.Cleanup(func() { dialer = prev })
	dialer = func(string) (*grpc.ClientConn, error) { return nil, errors.New("dial boom") }

	var out, errBuf bytes.Buffer
	if code := runStatus([]string{"--server=x"}, &out, &errBuf); code != 1 {
		t.Errorf("dial-error code = %d, want 1", code)
	}
	if !strings.Contains(errBuf.String(), "could not dial") {
		t.Errorf("stderr = %q, want a dial failure", errBuf.String())
	}
}

func TestStatus_RPCError(t *testing.T) {
	prevDial, prevClient := dialer, newStatusClient
	t.Cleanup(func() { dialer = prevDial; newStatusClient = prevClient })
	dialer = func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///x", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	newStatusClient = func(*grpc.ClientConn) statusv1.ClusterStatusClient { return errStatusClient{} }

	var out, errBuf bytes.Buffer
	if code := runStatus([]string{"--server=x"}, &out, &errBuf); code == 0 {
		t.Error("an RPC failure should be a non-zero exit")
	}
	if errBuf.Len() == 0 {
		t.Error("an RPC failure should print to stderr")
	}
}

func TestHumanizeBytes(t *testing.T) {
	cases := map[int64]string{
		0:             "0 B",
		512:           "512 B",
		1024:          "1.0 KiB",
		1536:          "1.5 KiB",
		1 << 20:       "1.0 MiB",
		1 << 30:       "1.0 GiB",
		5 * (1 << 40): "5.0 TiB",
	}
	for in, want := range cases {
		if got := humanizeBytes(in); got != want {
			t.Errorf("humanizeBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestHumanizeAge(t *testing.T) {
	prev := statusNow
	t.Cleanup(func() { statusNow = prev })
	base := time.Unix(2_000_000_000, 0)
	statusNow = func() time.Time { return base }

	cases := []struct {
		unix int64
		want string
	}{
		{0, "—"},
		{base.Add(10 * time.Second).Unix(), "just now"}, // future -> just now
		{base.Add(-30 * time.Second).Unix(), "30s ago"},
		{base.Add(-5 * time.Minute).Unix(), "5m ago"},
		{base.Add(-3 * time.Hour).Unix(), "3h ago"},
		{base.Add(-50 * time.Hour).Unix(), "2d ago"},
	}
	for _, tc := range cases {
		if got := humanizeAge(tc.unix); got != tc.want {
			t.Errorf("humanizeAge(%d) = %q, want %q", tc.unix, got, tc.want)
		}
	}
}

func TestPrintStatus_AllStatesAndEmpty(t *testing.T) {
	// Dead/left/unspecified states render, and a nil storage block is omitted.
	resp := &statusv1.GetStatusResponse{
		RespondingNodeId: "n",
		Version:          "v",
		Nodes: []*statusv1.NodeStatus{
			{Id: "d", State: statusv1.NodeState_NODE_STATE_DEAD},
			{Id: "l", State: statusv1.NodeState_NODE_STATE_LEFT},
			{Id: "u", State: statusv1.NodeState_NODE_STATE_UNSPECIFIED},
		},
	}
	var buf bytes.Buffer
	printStatus(&buf, resp)
	got := buf.String()
	for _, want := range []string{"dead", "left", "unknown", "1 dead, 1 left"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Storage on") {
		t.Error("a nil storage block should be omitted")
	}

	// Zero nodes: no health summary suffix, no panic.
	buf.Reset()
	printStatus(&buf, &statusv1.GetStatusResponse{RespondingNodeId: "n", Version: "v"})
	if !strings.Contains(buf.String(), "Cluster: 0 nodes\n") {
		t.Errorf("zero-node header = %q", buf.String())
	}
}

func TestCapacitySummary(t *testing.T) {
	if got := capacitySummary(0, 0); got != "—" {
		t.Errorf("no capacity = %q, want dash", got)
	}
	if got := capacitySummary(256<<20, 1<<30); got != "256.0 MiB/1.0 GiB (25%)" {
		t.Errorf("summary = %q", got)
	}
}

func TestNodeStateStringAndHelpers(t *testing.T) {
	if orDash("") != "—" || orDash("x") != "x" {
		t.Error("orDash")
	}
	if plural(1) != "" || plural(2) != "s" || plural(0) != "s" {
		t.Error("plural")
	}
}

func TestPrintStatus_Volumes(t *testing.T) {
	for _, tc := range []struct {
		name string
		vols []*statusv1.VolumeStatus
		want []string
	}{
		{name: "empty", want: []string{"Volumes: none yet", "siloctl volume create"}},
		{
			name: "table",
			vols: []*statusv1.VolumeStatus{
				{Path: "/db", SizeBytes: 1 << 30, ExtentSizeBytes: 64 << 10, ExtentsWritten: 16, LeaseHolder: "silo-a", Replicas: []string{"silo-a", "silo-b"}, AttachedHere: true},
				{Path: "/unsized", ExtentSizeBytes: 4096},
			},
			want: []string{"Volumes: 2", "PATH", "ALLOCATED", "WRITER", "REPLICAS", "NOTES", "/db", "1.0 GiB", "1.0 MiB", "silo-a", "silo-a,silo-b", "attached here", "/unsized", "0 B"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			printStatus(&buf, &statusv1.GetStatusResponse{RespondingNodeId: "n", Version: "v", Volumes: tc.vols})
			for _, want := range tc.want {
				if !strings.Contains(buf.String(), want) {
					t.Errorf("missing %q in:\n%s", want, buf.String())
				}
			}
		})
	}
}

func TestVolumeSize(t *testing.T) {
	if got := volumeSize(0); got != "—" {
		t.Errorf("volumeSize(0) = %q, want a dash", got)
	}
	if got := volumeSize(2048); got != "2.0 KiB" {
		t.Errorf("volumeSize(2048) = %q", got)
	}
}

func TestPrintBackupStatus(t *testing.T) {
	prev := statusNow
	t.Cleanup(func() { statusNow = prev })
	now := time.Unix(2_000_000_000, 0)
	statusNow = func() time.Time { return now }
	ago := func(d time.Duration) int64 { return now.Add(-d).Unix() }
	in := func(d time.Duration) int64 { return now.Add(d).Unix() }

	tests := []struct {
		name string
		b    *statusv1.BackupStatus
		want []string
		not  []string
	}{
		{"nil", nil, []string{"Backup: not configured on silo-a (set SILO_BACKUP_TARGET to enable)"}, []string{"last run"}},
		{"not configured", &statusv1.BackupStatus{}, []string{"not configured on silo-a"}, nil},
		{"ok", &statusv1.BackupStatus{
			Configured: true, Target: "s3://b/p", IntervalSeconds: 6 * 3600, NextRunUnix: in(3 * time.Hour),
			LastRun: &statusv1.BackupRun{Ok: true, FinishedUnix: ago(2 * time.Hour), ChunksUploaded: 3, ChunksSkipped: 10},
		}, []string{"Backup: s3://b/p, every 6h0m0s", "  last run: ok 2h ago (3 uploaded, 10 already stored)", "  next run: in 3h"}, []string{"running now"}},
		{"failed and running", &statusv1.BackupStatus{
			Configured: true, Target: "s3://b/p", IntervalSeconds: 3600, Running: true, NextRunUnix: in(30 * time.Second),
			LastRun: &statusv1.BackupRun{FinishedUnix: ago(5 * time.Minute), Error: "target unreachable"},
		}, []string{"  running now", "  last run: FAILED 5m ago: target unreachable", "  next run: in 30s"}, nil},
		{"control characters in run error and target", &statusv1.BackupStatus{
			Configured: true, Target: "s3://b/\x1b[2Jp", IntervalSeconds: 3600,
			LastRun: &statusv1.BackupRun{FinishedUnix: ago(time.Minute), Error: "\x1b[31mboom\x07"},
		}, []string{"Backup: s3://b/?[2Jp, every", "FAILED 1m ago: ?[31mboom?"}, []string{"\x1b", "\x07"}},
		{"none yet, unscheduled", &statusv1.BackupStatus{Configured: true, Target: "/mnt/b", IntervalSeconds: 60}, []string{"  last run: none yet", "  next run: not scheduled"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			printStatus(&buf, &statusv1.GetStatusResponse{RespondingNodeId: "silo-a", Backup: tt.b})
			got := buf.String()
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q in:\n%s", w, got)
				}
			}
			for _, w := range tt.not {
				if strings.Contains(got, w) {
					t.Errorf("unexpected %q in:\n%s", w, got)
				}
			}
		})
	}
}

func TestHumanizeUntil(t *testing.T) {
	prev := statusNow
	t.Cleanup(func() { statusNow = prev })
	base := time.Unix(2_000_000_000, 0)
	statusNow = func() time.Time { return base }

	for _, tt := range []struct {
		d    time.Duration
		want string
	}{
		{-time.Minute, "due now"},
		{0, "due now"},
		{45 * time.Second, "in 45s"},
		{90 * time.Minute / 2, "in 45m"},
		{5 * time.Hour, "in 5h"},
		{49 * time.Hour, "in 2d"},
	} {
		if got := humanizeUntil(base.Add(tt.d).Unix()); got != tt.want {
			t.Errorf("humanizeUntil(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestPrintStatus_SectionOrder(t *testing.T) {
	var buf bytes.Buffer
	printStatus(&buf, &statusv1.GetStatusResponse{
		RespondingNodeId: "silo-a", Version: "v",
		Nodes:   []*statusv1.NodeStatus{{Id: "silo-a", State: statusv1.NodeState_NODE_STATE_ALIVE}},
		Storage: &statusv1.StorageStatus{DataDir: "/d"},
		Volumes: []*statusv1.VolumeStatus{{Path: "/db"}},
		Backup:  &statusv1.BackupStatus{Configured: true, Target: "s3://b/p", IntervalSeconds: 3600},
		Health:  &statusv1.ClusterHealth{ReplicationFactor: 1, AliveNodes: 1},
		Gc:      &statusv1.GCStatus{Enabled: true},
	})
	got := buf.String()
	last := -1
	for _, heading := range []string{"Cluster:", "Health:", "NODE", "\nStorage on", "\nChunk GC:", "\nVolumes:", "\nBackup:"} {
		i := strings.Index(got, heading)
		if i <= last {
			t.Fatalf("%q at %d, want after the previous section (%d) in:\n%s", heading, i, last, got)
		}
		last = i
	}
}

func TestPrintHealth(t *testing.T) {
	prev := statusNow
	t.Cleanup(func() { statusNow = prev })
	now := time.Unix(2_000_000_000, 0)
	statusNow = func() time.Time { return now }

	for _, tc := range []struct {
		name    string
		h       *statusv1.ClusterHealth
		want    []string
		notWant []string
	}{
		{name: "older server sends none", h: nil, notWant: []string{"Health:"}},
		{
			name:    "healthy",
			h:       &statusv1.ClusterHealth{ReplicationFactor: 3, AliveNodes: 3, ClockSkewMs: 12, StartedUnix: now.Add(-3 * time.Hour).Unix()},
			want:    []string{"Health: replication 3, 3 alive, max clock skew 12ms, up 3h"},
			notWant: []string{"WARNING"},
		},
		{
			name: "isolated, under-replicated, skewed",
			h:    &statusv1.ClusterHealth{ReplicationFactor: 3, AliveNodes: 1, Isolated: true, RejoinAttempts: 1, ClockSkewMs: 1500, ClockSkewAlerts: 2},
			want: []string{
				"max clock skew 1.5s\n",
				"node-a sees fewer alive peers than it needs and is retrying its seeds (1 rejoin attempt)",
				"fewer alive nodes than the replication factor",
				"2 clock-skew alerts since start",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			printHealth(&buf, "node-a", tc.h)
			for _, want := range tc.want {
				if !strings.Contains(buf.String(), want) {
					t.Errorf("missing %q in:\n%s", want, buf.String())
				}
			}
			for _, nw := range tc.notWant {
				if strings.Contains(buf.String(), nw) {
					t.Errorf("unexpected %q in:\n%s", nw, buf.String())
				}
			}
		})
	}
}

func TestPrintGC(t *testing.T) {
	prev := statusNow
	t.Cleanup(func() { statusNow = prev })
	now := time.Unix(2_000_000_000, 0)
	statusNow = func() time.Time { return now }
	fiveMinAgo := now.Add(-5 * time.Minute).Unix()

	for _, tc := range []struct {
		name string
		gc   *statusv1.GCStatus
		want []string
	}{
		{name: "older server sends none", gc: nil, want: nil},
		{name: "dry run, never swept", gc: &statusv1.GCStatus{}, want: []string{"Chunk GC: dry run (SILO_CHUNK_GC_ENABLE is off)", "last sweep: none since start"}},
		{name: "running", gc: &statusv1.GCStatus{Enabled: true, Running: true}, want: []string{"Chunk GC: reclaiming", "sweeping now"}},
		{
			name: "skipped",
			gc:   &statusv1.GCStatus{Enabled: true, LastRunUnix: fiveMinAgo, LastError: "skipped: maps disagree"},
			want: []string{"last sweep: 5m ago, did not complete: skipped: maps disagree"},
		},
		{
			name: "completed",
			gc:   &statusv1.GCStatus{Enabled: true, LastRunUnix: fiveMinAgo, LastDurationMs: 1200, LastOrphansDeleted: 1, LastBytesFreed: 2048},
			want: []string{"last sweep: 5m ago in 1.2s, 1 chunk reclaimed (2.0 KiB)"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			printGC(&buf, tc.gc)
			if tc.want == nil && buf.Len() != 0 {
				t.Errorf("want no output, got:\n%s", buf.String())
			}
			for _, want := range tc.want {
				if !strings.Contains(buf.String(), want) {
					t.Errorf("missing %q in:\n%s", want, buf.String())
				}
			}
		})
	}
}

func TestNodeColumnsAndVolumeNotes(t *testing.T) {
	if got := nodeCapacity(&statusv1.NodeStatus{CapacityBytes: 100, UsedBytes: 95, Pressured: true}); !strings.HasSuffix(got, "(95%) PRESSURE") {
		t.Errorf("pressured capacity = %q", got)
	}
	if got := nodeSkew(&statusv1.NodeStatus{Id: "node-a", ClockSkewMs: 5}, "node-a"); got != "—" {
		t.Errorf("self skew = %q, want a dash", got)
	}
	if got := nodeSkew(&statusv1.NodeStatus{Id: "node-b", ClockSkewMs: -40}, "node-a"); got != "-40ms" {
		t.Errorf("peer skew = %q", got)
	}
	for _, tc := range []struct {
		v    *statusv1.VolumeStatus
		want string
	}{
		{&statusv1.VolumeStatus{}, "—"},
		{&statusv1.VolumeStatus{AttachedHere: true}, "attached here"},
		{&statusv1.VolumeStatus{AttachedHere: true, MapDamaged: true}, "attached here, MAP DAMAGED"},
	} {
		if got := volumeNotes(tc.v); got != tc.want {
			t.Errorf("volumeNotes(%+v) = %q, want %q", tc.v, got, tc.want)
		}
	}
	if pluralCount(int64(1)) != "" || pluralCount(uint64(0)) != "s" {
		t.Error("pluralCount")
	}
}

func TestHumanizeDuration(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{{-time.Second, "0s"}, {30 * time.Second, "30s"}, {5 * time.Minute, "5m"}, {2 * time.Hour, "2h"}, {72 * time.Hour, "3d"}} {
		if got := humanizeDuration(tc.d); got != tc.want {
			t.Errorf("humanizeDuration(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}
