package clusterstatus_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/backup"
	"github.com/hyperized/silo/internal/clusterstatus"
	"github.com/hyperized/silo/internal/diskusage"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/namespace"
	"github.com/hyperized/silo/internal/replication"
	"github.com/hyperized/silo/internal/transport"
)

type fakeMembers struct{ nodes []membership.Node }

func (f fakeMembers) Members() []membership.Node { return f.nodes }

type fakeChunks struct {
	ids []string
	err error
}

func (f fakeChunks) List(context.Context) ([]string, error) { return f.ids, f.err }

type fakeVolumes []namespace.VolumeInfo

func (f fakeVolumes) Volumes() []namespace.VolumeInfo { return f }

type fakeExtents map[string]int

func (f fakeExtents) Has(id string) bool { _, ok := f[id]; return ok }
func (f fakeExtents) Len(id string) int  { return f[id] }

type fakeBackup struct{ st backup.Status }

func (f fakeBackup) Status() backup.Status { return f.st }

type fakeSkew struct {
	peers  map[string]time.Duration
	alerts uint64
}

func (f fakeSkew) PeerSkews() map[string]time.Duration { return f.peers }
func (f fakeSkew) Alerts() uint64                      { return f.alerts }

type fakeGossip struct {
	isolated bool
	attempts uint64
}

func (f fakeGossip) Isolated() bool         { return f.isolated }
func (f fakeGossip) RejoinAttempts() uint64 { return f.attempts }

type fakeGC struct{ st replication.ChunkGCStatus }

func (f fakeGC) Status() replication.ChunkGCStatus { return f.st }

type fakeDamage map[string]bool

func (f fakeDamage) Lost(id string) bool { return f[id] }

// fakeReplicas returns the first n of a fixed node list, prefixed by the
// volume id so the test can see which volume a set was resolved for.
type fakeReplicas []string

func (f fakeReplicas) MetaReplicas(volumeID string, n int) []string {
	return append([]string{volumeID}, f[:n-1]...)
}

type fakeAttached []string

func (f fakeAttached) AttachedExports() []string { return f }

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var changed = time.Unix(1_700_000_000, 0)

func fullBuilder() *clusterstatus.Builder {
	members := fakeMembers{nodes: []membership.Node{
		{ID: "silo-a", Address: "silo-a:7100", DataAddress: "silo-a:7000", State: membership.StateAlive, Incarnation: 3, LastChange: changed, CapacityBytes: 2000, UsedBytes: 500},
		{ID: "silo-b", Address: "silo-b:7100", DataAddress: "silo-b:7000", State: membership.StateSuspect, Incarnation: 1, LastChange: changed, CapacityBytes: 4000, UsedBytes: 3900, Pressured: true},
		{ID: "silo-c", Address: "silo-c:7100", DataAddress: "silo-c:7000", State: membership.StateAlive, Incarnation: 2, LastChange: changed},
	}}
	vols := fakeVolumes{
		{Path: "/legacy", Inode: "inode-1", Size: 1 << 20, ExtentSize: 4096, InlineExtents: 5},
		{Path: "/vols/db", Inode: "inode-2", Size: 1 << 30, ExtentSize: 65536, LeaseHolder: "silo-a"},
		{Path: "/vols/elsewhere", Inode: "inode-3", Size: 1 << 30, ExtentSize: 65536},
	}
	return clusterstatus.New(members, fakeChunks{ids: []string{"c0", "c1", "c2"}}, "/var/lib/silo", "silo-a", "v1.2.3", discard(),
		clusterstatus.WithDiskUsage(func(string) (diskusage.Usage, error) {
			return diskusage.Usage{CapacityBytes: 1000, UsedBytes: 600, AvailableBytes: 400}, nil
		}),
		clusterstatus.WithVolumes(vols),
		clusterstatus.WithExtentCounts(fakeExtents{"inode-2": 7, "inode-1": 2}),
		clusterstatus.WithBackup(fakeBackup{backup.Status{
			Configured: true, Target: "s3://bucket/prefix", Interval: time.Hour,
			LastRun: &backup.Run{StartedAt: changed, FinishedAt: changed.Add(time.Minute), OK: true, ChunksUploaded: 4, Manifest: "manifests/silo-a/latest.json"},
		}}),
		// silo-a's own entry is never reported with a skew, even if the
		// monitor somehow held one for it.
		clusterstatus.WithClockSkew(fakeSkew{peers: map[string]time.Duration{"silo-a": time.Hour, "silo-b": -1500 * time.Millisecond, "silo-c": 40 * time.Millisecond}, alerts: 2}),
		clusterstatus.WithGossipHealth(fakeGossip{isolated: true, attempts: 5}),
		clusterstatus.WithGC(fakeGC{replication.ChunkGCStatus{Enabled: true, LastRun: changed, LastDuration: 1500 * time.Millisecond, LastDeleted: 3, LastBytesFreed: 4096, LastError: "skipped", ConsecutiveSkips: 2}}),
		clusterstatus.WithMapDamage(fakeDamage{"inode-2": true}),
		clusterstatus.WithReplicaSet(fakeReplicas{"silo-b", "silo-c"}),
		clusterstatus.WithAttached(fakeAttached{"vols//db"}),
		clusterstatus.WithReplicationFactor(3),
		clusterstatus.WithStartTime(changed.Add(-time.Hour)),
	)
}

func TestBuilder_Build(t *testing.T) {
	resp := fullBuilder().Build(context.Background())
	if resp.GetRespondingNodeId() != "silo-a" || resp.GetVersion() != "v1.2.3" {
		t.Errorf("identity = (%q, %q)", resp.GetRespondingNodeId(), resp.GetVersion())
	}
	if len(resp.GetNodes()) != 3 {
		t.Fatalf("nodes = %d, want 3", len(resp.GetNodes()))
	}
	a := resp.GetNodes()[0]
	if a.GetId() != "silo-a" || a.GetGossipAddress() != "silo-a:7100" || a.GetDataAddress() != "silo-a:7000" ||
		a.GetState() != statusv1.NodeState_NODE_STATE_ALIVE || a.GetIncarnation() != 3 || a.GetLastChangeUnix() != changed.Unix() ||
		a.GetCapacityBytes() != 2000 || a.GetUsedBytes() != 500 {
		t.Errorf("node a = %+v", a)
	}
	if a.GetPressured() || a.GetClockSkewMs() != 0 {
		t.Errorf("self entry: pressured=%v skew=%d, want false/0", a.GetPressured(), a.GetClockSkewMs())
	}
	if nb := resp.GetNodes()[1]; nb.GetState() != statusv1.NodeState_NODE_STATE_SUSPECT || !nb.GetPressured() ||
		nb.GetCapacityBytes() != 4000 || nb.GetClockSkewMs() != -1500 {
		t.Errorf("node b = %+v, want SUSPECT, pressured, capacity 4000, skew -1500ms", nb)
	}
	if nc := resp.GetNodes()[2]; nc.GetClockSkewMs() != 40 || nc.GetCapacityBytes() != 0 {
		t.Errorf("node c = %+v, want skew 40ms and unreported capacity", nc)
	}
	if h := resp.GetHealth(); h.GetReplicationFactor() != 3 || h.GetAliveNodes() != 2 || !h.GetIsolated() || h.GetRejoinAttempts() != 5 ||
		h.GetClockSkewMs() != 3_600_000 || h.GetClockSkewAlerts() != 2 || h.GetStartedUnix() != changed.Add(-time.Hour).Unix() {
		t.Errorf("health = %+v", h)
	}
	if gc := resp.GetGc(); !gc.GetEnabled() || gc.GetRunning() || gc.GetLastRunUnix() != changed.Unix() || gc.GetLastDurationMs() != 1500 ||
		gc.GetLastOrphansDeleted() != 3 || gc.GetLastBytesFreed() != 4096 || gc.GetLastError() != "skipped" || gc.GetConsecutiveSkips() != 2 {
		t.Errorf("gc = %+v", gc)
	}
	st := resp.GetStorage()
	if st.GetDataDir() != "/var/lib/silo" || st.GetCapacityBytes() != 1000 || st.GetUsedBytes() != 600 || st.GetAvailableBytes() != 400 || st.GetChunkCount() != 3 {
		t.Errorf("storage = %+v", st)
	}

	for _, tc := range []struct {
		name, path, holder string
		written            int64
		damaged, attached  bool
		replicas           []string
	}{
		{name: "inline count wins when larger", path: "/legacy", written: 5, replicas: []string{"inode-1", "silo-b", "silo-c"}},
		{name: "out-of-band count, damaged, attached under a sloppy export name", path: "/vols/db", holder: "silo-a", written: 7, damaged: true, attached: true, replicas: []string{"inode-2", "silo-b", "silo-c"}},
		{name: "not held on this node", path: "/vols/elsewhere", written: 0, replicas: []string{"inode-3", "silo-b", "silo-c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got *statusv1.VolumeStatus
			for _, v := range resp.GetVolumes() {
				if v.GetPath() == tc.path {
					got = v
				}
			}
			if got == nil {
				t.Fatalf("volume %q missing from %v", tc.path, resp.GetVolumes())
			}
			if got.GetExtentsWritten() != tc.written || got.GetLeaseHolder() != tc.holder {
				t.Errorf("volume %q = %+v, want written=%d holder=%q", tc.path, got, tc.written, tc.holder)
			}
			if got.GetMapDamaged() != tc.damaged || got.GetAttachedHere() != tc.attached || !slices.Equal(got.GetReplicas(), tc.replicas) {
				t.Errorf("volume %q: damaged=%v attached=%v replicas=%v, want %v/%v/%v", tc.path, got.GetMapDamaged(), got.GetAttachedHere(), got.GetReplicas(), tc.damaged, tc.attached, tc.replicas)
			}
		})
	}
	if v := resp.GetVolumes()[1]; v.GetInode() != "inode-2" || v.GetSizeBytes() != 1<<30 || v.GetExtentSizeBytes() != 65536 {
		t.Errorf("volume fields = %+v", v)
	}
	if bk := resp.GetBackup(); !bk.GetConfigured() || bk.GetTarget() != "s3://bucket/prefix" || bk.GetIntervalSeconds() != 3600 ||
		bk.GetLastRun().GetManifest() != "manifests/silo-a/latest.json" || !bk.GetLastRun().GetOk() {
		t.Errorf("backup = %+v", bk)
	}
}

func TestBuilder_DegradesAndOptionalSources(t *testing.T) {
	b := clusterstatus.New(
		fakeMembers{nodes: []membership.Node{{ID: "n", State: membership.StateAlive}}},
		fakeChunks{err: errors.New("disk gone")},
		"/data", "n", "v1", discard(),
		clusterstatus.WithDiskUsage(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, errors.New("statfs failed") }),
	)
	resp := b.Build(context.Background())
	if len(resp.GetNodes()) != 1 {
		t.Errorf("nodes = %d, want 1", len(resp.GetNodes()))
	}
	st := resp.GetStorage()
	if st.GetDataDir() != "/data" || st.GetCapacityBytes() != 0 || st.GetChunkCount() != 0 {
		t.Errorf("storage on error = %+v, want zeroed figures with the dir set", st)
	}
	if resp.GetVolumes() != nil {
		t.Errorf("volumes without a lister = %v, want none", resp.GetVolumes())
	}
	if bk := resp.GetBackup(); bk == nil || bk.GetConfigured() {
		t.Errorf("backup without a reporter = %+v, want present with configured=false", bk)
	}
	// Health and GC are always present; without their sources only what the
	// membership table knows is filled in.
	if h := resp.GetHealth(); h == nil || h.GetAliveNodes() != 1 || h.GetReplicationFactor() != 0 || h.GetIsolated() ||
		h.GetClockSkewMs() != 0 || h.GetClockSkewAlerts() != 0 || h.GetStartedUnix() != 0 {
		t.Errorf("health without sources = %+v", h)
	}
	if gc := resp.GetGc(); gc == nil || gc.GetEnabled() || gc.GetLastRunUnix() != 0 {
		t.Errorf("gc without a reporter = %+v, want present and zero", gc)
	}
	// A GC that has not finished a sweep reports last_run_unix 0, not year 1.
	neverRan := clusterstatus.New(fakeMembers{}, fakeChunks{}, "/d", "n", "v", discard(),
		clusterstatus.WithDiskUsage(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, nil }),
		clusterstatus.WithGC(fakeGC{replication.ChunkGCStatus{Enabled: true, Running: true}}))
	if gc := neverRan.Build(context.Background()).GetGc(); gc.GetLastRunUnix() != 0 || !gc.GetRunning() {
		t.Errorf("gc before its first sweep = %+v", gc)
	}
	nilReporter := clusterstatus.New(fakeMembers{}, fakeChunks{}, "/d", "n", "v", discard(),
		clusterstatus.WithDiskUsage(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, nil }),
		clusterstatus.WithBackup(nil))
	if bk := nilReporter.Build(context.Background()).GetBackup(); bk == nil || bk.GetConfigured() {
		t.Errorf("backup with WithBackup(nil) = %+v, want present with configured=false", bk)
	}

	// A volume lister but no extent store: inline counts only.
	b = clusterstatus.New(fakeMembers{}, fakeChunks{}, "/d", "n", "v", discard(),
		clusterstatus.WithDiskUsage(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, nil }),
		clusterstatus.WithVolumes(fakeVolumes{{Path: "/v", Inode: "i", InlineExtents: 4}}))
	v := b.Build(context.Background()).GetVolumes()[0]
	if v.GetExtentsWritten() != 4 {
		t.Errorf("extents written without an extent store = %d, want 4", v.GetExtentsWritten())
	}
	if v.GetReplicas() != nil || v.GetMapDamaged() || v.GetAttachedHere() {
		t.Errorf("volume without replica/damage/NBD sources = %+v", v)
	}

	// A resolver without a replication factor lists no replica set.
	b = clusterstatus.New(fakeMembers{}, fakeChunks{}, "/d", "n", "v", discard(),
		clusterstatus.WithDiskUsage(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, nil }),
		clusterstatus.WithVolumes(fakeVolumes{{Path: "/v", Inode: "i"}}),
		clusterstatus.WithReplicaSet(fakeReplicas{"x"}))
	if got := b.Build(context.Background()).GetVolumes()[0].GetReplicas(); got != nil {
		t.Errorf("replicas without a replication factor = %v, want none", got)
	}
}

func TestHandler_JSONContract(t *testing.T) {
	b := fullBuilder()
	srv := httptest.NewServer(clusterstatus.Handler(b))
	defer srv.Close()

	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	for h, want := range map[string]string{
		"Content-Type":           "application/json; charset=utf-8",
		"Cache-Control":          "no-store",
		"X-Content-Type-Options": "nosniff",
	} {
		if got := res.Header.Get(h); got != want {
			t.Errorf("%s = %q, want %q", h, got, want)
		}
	}

	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"respondingNodeId", "version", "nodes", "storage", "volumes", "backup", "health", "gc"} {
		if _, ok := body[key]; !ok {
			t.Errorf("key %q missing from %v", key, body)
		}
	}
	node := body["nodes"].([]any)[0].(map[string]any)
	if node["state"] != "NODE_STATE_ALIVE" {
		t.Errorf("state = %#v, want the enum name", node["state"])
	}
	if node["capacityBytes"] != "2000" || node["incarnation"] != "3" {
		t.Errorf("64-bit fields must be JSON strings, got capacityBytes=%#v incarnation=%#v", node["capacityBytes"], node["incarnation"])
	}
	if node["pressured"] != false || node["clockSkewMs"] != "0" {
		t.Errorf("node health keys = pressured %#v clockSkewMs %#v", node["pressured"], node["clockSkewMs"])
	}
	h := body["health"].(map[string]any)
	if h["replicationFactor"] != float64(3) || h["aliveNodes"] != float64(2) || h["isolated"] != true ||
		h["rejoinAttempts"] != "5" || h["clockSkewMs"] != "3600000" || h["clockSkewAlerts"] != "2" {
		t.Errorf("HTTP health = %#v", h)
	}
	gc := body["gc"].(map[string]any)
	for _, k := range []string{"enabled", "running", "lastRunUnix", "lastDurationMs", "lastOrphansDeleted", "lastBytesFreed", "lastError", "consecutiveSkips"} {
		if _, ok := gc[k]; !ok {
			t.Errorf("HTTP gc is missing %q: %#v", k, gc)
		}
	}
	vol := body["volumes"].([]any)[2].(map[string]any)
	// EmitUnpopulated: a vacant, empty volume still carries every key.
	if vol["leaseHolder"] != "" || vol["extentsWritten"] != "0" || vol["sizeBytes"] != "1073741824" ||
		vol["mapDamaged"] != false || vol["attachedHere"] != false || len(vol["replicas"].([]any)) != 3 {
		t.Errorf("volume = %#v, want empty holder, string-encoded numbers and the health keys", vol)
	}

	// The HTTP projection blanks the data directory and inode ids (keys stay,
	// values empty) while the gRPC path keeps them.
	if dir := body["storage"].(map[string]any)["dataDir"]; dir != "" {
		t.Errorf("HTTP storage.dataDir = %#v, want empty string", dir)
	}
	for _, v := range body["volumes"].([]any) {
		if inode := v.(map[string]any)["inode"]; inode != "" {
			t.Errorf("HTTP volume inode = %#v, want empty string", inode)
		}
	}
	// The projection leaves the backup block alone: target is credential-free
	// by construction, and the page needs it to say where backups go.
	bk := body["backup"].(map[string]any)
	if bk["configured"] != true || bk["target"] != "s3://bucket/prefix" || bk["intervalSeconds"] != "3600" {
		t.Errorf("HTTP backup = %#v", bk)
	}
	if run := bk["lastRun"].(map[string]any); run["manifest"] != "manifests/silo-a/latest.json" || run["chunksUploaded"] != "4" {
		t.Errorf("HTTP backup.lastRun = %#v", run)
	}

	full, err := transport.NewStatusService(b).GetStatus(context.Background(), &statusv1.GetStatusRequest{})
	if err != nil {
		t.Fatalf("gRPC GetStatus: %v", err)
	}
	if full.GetBackup().GetTarget() != "s3://bucket/prefix" || full.GetBackup().GetLastRun().GetManifest() == "" {
		t.Errorf("gRPC backup = %+v, want the same block as HTTP", full.GetBackup())
	}
	if full.GetStorage().GetDataDir() != "/var/lib/silo" {
		t.Errorf("gRPC dataDir = %q, want it populated", full.GetStorage().GetDataDir())
	}
	for _, v := range full.GetVolumes() {
		if v.GetInode() == "" {
			t.Errorf("gRPC volume %q lost its inode", v.GetPath())
		}
	}
}

func TestHandler_Methods(t *testing.T) {
	h := clusterstatus.Handler(fullBuilder())
	for _, tc := range []struct {
		method   string
		code     int
		wantBody bool
	}{
		{method: http.MethodHead, code: http.StatusOK},
		{method: http.MethodPost, code: http.StatusMethodNotAllowed, wantBody: true},
		{method: http.MethodDelete, code: http.StatusMethodNotAllowed, wantBody: true},
	} {
		t.Run(tc.method, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, "/api/v1/status", nil))
			if rec.Code != tc.code {
				t.Fatalf("code = %d, want %d", rec.Code, tc.code)
			}
			if !tc.wantBody {
				if rec.Body.Len() != 0 {
					t.Errorf("HEAD body = %q, want empty", rec.Body.String())
				}
				return
			}
			if rec.Header().Get("Allow") != "GET, HEAD" {
				t.Errorf("Allow = %q", rec.Header().Get("Allow"))
			}
			var e struct{ Error string }
			if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || !strings.Contains(e.Error, "GET") {
				t.Errorf("error body = %q (%v), want an instruction to use GET", rec.Body.String(), err)
			}
		})
	}
}

// extents_written counts mapped extents only, so trimming a volume brings the
// number down, from either the out-of-band store or the inline map.
func TestBuilder_ExtentsWrittenDropsAfterTrim(t *testing.T) {
	store := extentmap.New(discard())
	if err := store.SetBatch("inode-ext", []uint64{0, 1, 2}, []string{"c0", "c1", "c2"}, hlc.Timestamp{Wall: 10}); err != nil {
		t.Fatalf("SetBatch: %v", err)
	}
	if err := store.SetBatch("inode-ext", []uint64{1, 2}, []string{extentmap.Unmapped, extentmap.Unmapped}, hlc.Timestamp{Wall: 20}); err != nil {
		t.Fatalf("unmap: %v", err)
	}

	clk := int64(100)
	ns := namespace.New(hlc.New("n", hlc.WithNow(func() time.Time { clk++; return time.Unix(0, clk) })))
	if _, err := ns.CreateVolume("/inline", 4096); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if _, err := ns.AcquireLease("/inline", "w"); err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if err := ns.WriteExtents("/inline", []uint64{0, 1}, []string{"c0", "c1"}, "w"); err != nil {
		t.Fatalf("WriteExtents: %v", err)
	}
	if err := ns.WriteExtent("/inline", 0, "", "w"); err != nil {
		t.Fatalf("unmap: %v", err)
	}
	vols := append(fakeVolumes{{Path: "/ext", Inode: "inode-ext"}}, ns.Volumes()...)

	b := clusterstatus.New(fakeMembers{}, fakeChunks{}, "/d", "n", "v", discard(),
		clusterstatus.WithDiskUsage(func(string) (diskusage.Usage, error) { return diskusage.Usage{}, nil }),
		clusterstatus.WithVolumes(vols),
		clusterstatus.WithExtentCounts(store))
	got := map[string]int64{}
	for _, v := range b.Build(context.Background()).GetVolumes() {
		got[v.GetPath()] = v.GetExtentsWritten()
	}
	if got["/ext"] != 1 || got["/inline"] != 1 {
		t.Errorf("extents written = %v, want 1 mapped extent on each volume", got)
	}
}
