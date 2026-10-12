package replication

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/crdt"
	"github.com/hyperized/silo/internal/extentmap"
	"github.com/hyperized/silo/internal/hlc"
)

// openLostStore returns a disk-backed store whose map of vol was truncated
// before Open, the state a node is in after an unclean reboot.
func openLostStore(t *testing.T, vol string) *extentmap.Store {
	t.Helper()
	dir := t.TempDir()
	name := base64.RawURLEncoding.EncodeToString([]byte(vol)) + ".emap.json"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(`{"volume_id":"`+vol+`","entr`), 0o600); err != nil {
		t.Fatalf("seed damaged map: %v", err)
	}
	s, err := extentmap.Open(dir, quietLog())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !s.Lost(vol) {
		t.Fatal("the seeded map should have been flagged lost")
	}
	return s
}

func stamp(wall int64, node string) hlc.Timestamp { return hlc.Timestamp{Wall: wall, Node: node} }

// Every holder is merged, and per extent the binding with the newest hybrid
// logical clock wins regardless of which replica answered first.
func TestExtentCoordinator_WarmMergesEveryHolderNewestWins(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true
	peers.statHas["c:7000"] = true
	// b answers first but is behind on extent 0; c is behind on extent 1.
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{
		{Key: 0, Value: "old", TS: stamp(5, "x")},
		{Key: 1, Value: "b1-new", TS: stamp(8, "x")},
	}
	peers.fetchRes["c:7000"] = []crdt.MapEntry[uint64, string]{
		{Key: 0, Value: "new", TS: stamp(9, "x")},
		{Key: 1, Value: "c1-old", TS: stamp(2, "x")},
	}
	store := extentmap.New(quietLog())
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.Warm(context.Background(), "vol"); err != nil {
		t.Fatalf("Warm: %v", err)
	}
	for idx, want := range map[uint64]string{0: "new", 1: "b1-new"} {
		if got, _ := store.Get("vol", idx); got != want {
			t.Errorf("extent %d = %q, want %q (the newest binding across replicas)", idx, got, want)
		}
	}
}

func TestExtentCoordinator_WarmLostMap(t *testing.T) {
	newest := []crdt.MapEntry[uint64, string]{{Key: 0, Value: "newest", TS: stamp(9, "x")}}
	older := []crdt.MapEntry[uint64, string]{{Key: 0, Value: "older", TS: stamp(3, "x")}}

	for _, tc := range []struct {
		name      string
		setup     func(*fakeMetaPlace, *fakeEpeers)
		wantErr   string // empty means Warm must succeed
		wantValue string
	}{
		{
			name: "one replica unreachable is refused",
			setup: func(_ *fakeMetaPlace, p *fakeEpeers) {
				p.statHas["b:7000"] = true
				p.fetchRes["b:7000"] = older
				p.statErr["c:7000"] = errors.New("connection refused")
			},
			wantErr: "roll the volume back",
		},
		{
			name: "replica without an address is refused",
			setup: func(pl *fakeMetaPlace, p *fakeEpeers) {
				p.statHas["b:7000"] = true
				p.fetchRes["b:7000"] = older
				pl.noAddr = map[string]bool{"c": true}
			},
			wantErr: "1 of its 3 replicas could not be reached",
		},
		{
			name: "every replica answered and merges the newest",
			setup: func(_ *fakeMetaPlace, p *fakeEpeers) {
				p.statHas["b:7000"] = true
				p.statHas["c:7000"] = true
				p.fetchRes["b:7000"] = older
				p.fetchRes["c:7000"] = newest
			},
			wantValue: "newest",
		},
		{
			name: "no replica holds a copy is refused",
			setup: func(_ *fakeMetaPlace, _ *fakeEpeers) {
				// every replica answers, none holds the map
			},
			wantErr: "nothing to rebuild it from",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
			peers := newFakeEpeers()
			tc.setup(place, peers)
			store := openLostStore(t, "vol")
			c := NewExtentCoordinator(place, store, peers, 3, quietLog())

			err := c.Warm(context.Background(), "vol")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to mention %q", err, tc.wantErr)
				}
				if !store.Lost("vol") {
					t.Error("a refused rebuild must leave the volume flagged lost")
				}
				return
			}
			if err != nil {
				t.Fatalf("Warm: %v", err)
			}
			if store.Lost("vol") {
				t.Error("a complete rebuild should clear the lost flag")
			}
			if got, _ := store.Get("vol", 0); got != tc.wantValue {
				t.Errorf("extent 0 = %q, want %q", got, tc.wantValue)
			}
		})
	}
}

// The scrubber or a peer's push can merge one replica's (older) copy into a
// lost volume before anyone attaches it. That makes Has true, and Warm must
// still refuse rather than take the early return for an already-held map.
func TestExtentCoordinator_WarmLostMapAfterSinglePeerMerge(t *testing.T) {
	store := openLostStore(t, "vol")
	if err := store.Merge("vol", []crdt.MapEntry[uint64, string]{{Key: 0, Value: "older", TS: stamp(3, "x")}}); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if !store.Has("vol") {
		t.Fatal("the merge should have created the map")
	}

	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 0, Value: "older", TS: stamp(3, "x")}}
	peers.statErr["c:7000"] = errors.New("isolated by a gossip problem")
	c := NewExtentCoordinator(place, store, peers, 3, quietLog())
	if err := c.Warm(context.Background(), "vol"); err == nil {
		t.Fatal("a lost map holding a single peer's copy must not be served while a replica is unreachable")
	}

	// Once c is back, Warm merges it and the newer binding wins.
	delete(peers.statErr, "c:7000")
	peers.statHas["c:7000"] = true
	peers.fetchRes["c:7000"] = []crdt.MapEntry[uint64, string]{{Key: 0, Value: "newest", TS: stamp(9, "x")}}
	if err := c.Warm(context.Background(), "vol"); err != nil {
		t.Fatalf("Warm after the replica returned: %v", err)
	}
	if got, _ := store.Get("vol", 0); got != "newest" || store.Lost("vol") {
		t.Errorf("extent 0 = %q lost=%v, want newest and the flag cleared", got, store.Lost("vol"))
	}
}

// A restart between the quarantine and the first attach must not reset the
// rule: the reopened store still reports the volume lost, so Warm refuses while
// a replica is unreachable instead of serving the one that answered.
func TestExtentCoordinator_WarmLostMapSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	name := base64.RawURLEncoding.EncodeToString([]byte("vol")) + ".emap.json"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("{"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := extentmap.Open(dir, quietLog()); err != nil {
		t.Fatalf("first Open: %v", err)
	}
	store, err := extentmap.Open(dir, quietLog())
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}

	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}
	peers := newFakeEpeers()
	peers.statHas["b:7000"] = true
	peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 0, Value: "older", TS: stamp(3, "x")}}
	peers.statErr["c:7000"] = errors.New("unreachable")
	if err := NewExtentCoordinator(place, store, peers, 3, quietLog()).Warm(context.Background(), "vol"); err == nil {
		t.Fatal("after a restart the damaged volume must still be refused while a replica is unreachable")
	}
}

// A local save failure while warming is logged; the merged map serves from
// memory because the replicas still hold it.
func TestExtentCoordinator_WarmLocalSaveFailureStillServes(t *testing.T) {
	place := &fakeMetaPlace{replicas: []string{"a", "b", "c"}, self: "a"}

	t.Run("merge", func(t *testing.T) {
		peers := newFakeEpeers()
		peers.statHas["b:7000"] = true
		peers.fetchRes["b:7000"] = []crdt.MapEntry[uint64, string]{{Key: 0, Value: "c0", TS: at(1)}}
		store := newFakeStore()
		store.mergeErr = errors.New("disk full")
		if err := NewExtentCoordinator(place, store, peers, 3, quietLog()).Warm(context.Background(), "vol"); err != nil {
			t.Fatalf("Warm: %v", err)
		}
		if v, ok := store.Get("vol", 0); !ok || v != "c0" {
			t.Errorf("merged map should serve from memory: (%q,%v)", v, ok)
		}
	})

	t.Run("ensure of a never-written volume", func(t *testing.T) {
		store := newFakeStore()
		store.ensureErr = errors.New("disk full")
		if err := NewExtentCoordinator(place, store, newFakeEpeers(), 3, quietLog()).Warm(context.Background(), "vol"); err != nil {
			t.Fatalf("Warm: %v", err)
		}
		if !store.Has("vol") {
			t.Error("the empty map should exist in memory")
		}
	})
}
