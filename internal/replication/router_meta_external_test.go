package replication_test

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/hyperized/silo/internal/membership"
	"github.com/hyperized/silo/internal/replication"
)

// MetaReplicas resolves a volume's extent-map replica set over the plain ring,
// unaffected by pressure steering: with a pressured node present, MetaReplicas
// returns exactly the un-steered placement (matching a non-steering Router's
// Replicas), while the steering Router's Replicas diverges by shedding the
// pressured node — proving the extent-map replica set stays stable.
func TestRouter_MetaReplicasIgnoresPressureSteering(t *testing.T) {
	m, err := membership.New("a", "a:7100", "a:7000")
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	m.Apply(membership.Event{ID: "b", Address: "b:7100", DataAddress: "b:7000", State: membership.StateAlive, Incarnation: 1, CapacityBytes: 1000})
	m.Apply(membership.Event{ID: "c", Address: "c:7100", DataAddress: "c:7000", State: membership.StateAlive, Incarnation: 1, CapacityBytes: 1000, Pressured: true})
	m.Apply(membership.Event{ID: "d", Address: "d:7100", DataAddress: "d:7000", State: membership.StateAlive, Incarnation: 1, CapacityBytes: 1000})

	steered := replication.NewRouter(m, replication.WithPressureSteering(true))
	plain := replication.NewRouter(m)

	diverged := false
	for i := 0; i < 200; i++ {
		k := "vol-" + strconv.Itoa(i)
		meta := steered.MetaReplicas(k, 3)
		if want := plain.Replicas(k, 3); !reflect.DeepEqual(meta, want) {
			t.Fatalf("MetaReplicas(%q) = %v, want un-steered placement %v", k, meta, want)
		}
		if len(meta) != 3 {
			t.Fatalf("MetaReplicas(%q) returned %d ids, want 3", k, len(meta))
		}
		if !reflect.DeepEqual(steered.Replicas(k, 3), meta) {
			diverged = true // steering shed the pressured node for this key; MetaReplicas did not
		}
	}
	if !diverged {
		t.Error("expected steering to diverge from MetaReplicas for at least one key when a node is pressured")
	}
}

// MetaReplicas caps at the cluster size and is deterministic.
func TestRouter_MetaReplicasCapsAtClusterSize(t *testing.T) {
	m, _ := membership.New("a", "a:7100", "a:7000")
	m.Apply(membership.Event{ID: "b", Address: "b:7100", DataAddress: "b:7000", State: membership.StateAlive, Incarnation: 1})

	r := replication.NewRouter(m)
	got := r.MetaReplicas("vol-x", 5)
	if len(got) != 2 {
		t.Fatalf("MetaReplicas over a 2-node cluster = %v, want 2 ids", got)
	}
	if !reflect.DeepEqual(got, r.MetaReplicas("vol-x", 5)) {
		t.Error("MetaReplicas must be deterministic for the same key and membership")
	}
}
