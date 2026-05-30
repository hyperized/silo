package clusterproto_test

import (
	"testing"

	"github.com/hyperized/silo/internal/clusterproto"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		peer uint32
		want clusterproto.Compatibility
	}{
		{"unversioned peer treated as v1", 0, clusterproto.Compatible},
		{"same protocol", clusterproto.Protocol, clusterproto.Compatible},
		{"at min compatible", clusterproto.MinCompatible, clusterproto.Compatible},
		{"newer peer", clusterproto.Protocol + 1, clusterproto.PeerNewer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clusterproto.Classify(tc.peer); got != tc.want {
				t.Errorf("Classify(%d) = %v, want %v", tc.peer, got, tc.want)
			}
		})
	}
}

func TestCompatibilityString(t *testing.T) {
	cases := map[clusterproto.Compatibility]string{
		clusterproto.Compatible:        "compatible",
		clusterproto.PeerNewer:         "peer-newer",
		clusterproto.PeerTooOld:        "peer-too-old",
		clusterproto.Compatibility(99): "unknown",
	}
	for c, want := range cases {
		if got := c.String(); got != want {
			t.Errorf("Compatibility(%d).String() = %q, want %q", c, got, want)
		}
	}
}

func TestVersionInvariants(t *testing.T) {
	if clusterproto.MinCompatible > clusterproto.Protocol {
		t.Fatalf("MinCompatible (%d) must not exceed Protocol (%d)", clusterproto.MinCompatible, clusterproto.Protocol)
	}
	if clusterproto.Protocol == 0 {
		t.Fatal("Protocol must be >= 1; 0 is reserved for unversioned peers")
	}
}
