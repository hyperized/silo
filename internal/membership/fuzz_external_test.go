package membership_test

import (
	"testing"

	"github.com/hyperized/silo/internal/membership"
)

// FuzzMerge drives the SWIM merge with an arbitrary sequence of events and
// checks the invariants convergence depends on: a stored incarnation never
// goes backwards, the local node can never be pushed below Alive (the
// refutation rule), the table never holds an empty id, and nothing panics.
// These are the subtle properties unit tests tend to under-cover.
func FuzzMerge(f *testing.F) {
	f.Add([]byte{0, 2, 9, 1, 1, 2, 2, 2, 5, 0, 0, 0})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := membership.New("self", "self:7100", "self:7000")
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ids := []string{"self", "a", "b", "c"}
		lastIncarnation := map[string]uint64{}

		// Each 3-byte group is one event: [node index, state, incarnation].
		for i := 0; i+2 < len(data); i += 3 {
			m.Apply(membership.Event{
				ID:          ids[int(data[i])%len(ids)],
				State:       membership.State(data[i+1] % 4),
				Incarnation: uint64(data[i+2]),
			})

			if self := m.Self(); self.State != membership.StateAlive {
				t.Fatalf("local node pushed to %v; refutation must keep self Alive", self.State)
			}
			for _, n := range m.Members() {
				if n.ID == "" {
					t.Fatal("table holds a node with an empty id")
				}
				if n.Incarnation < lastIncarnation[n.ID] {
					t.Fatalf("incarnation for %s went backwards: %d -> %d", n.ID, lastIncarnation[n.ID], n.Incarnation)
				}
				lastIncarnation[n.ID] = n.Incarnation
			}
		}
	})
}
