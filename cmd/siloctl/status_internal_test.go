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

func TestNodeStateStringAndHelpers(t *testing.T) {
	if orDash("") != "—" || orDash("x") != "x" {
		t.Error("orDash")
	}
	if plural(1) != "" || plural(2) != "s" || plural(0) != "s" {
		t.Error("plural")
	}
}
