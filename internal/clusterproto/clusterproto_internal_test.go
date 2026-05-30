package clusterproto

import "testing"

// TestClassifyWindow exercises the floor/ceiling logic directly with an
// injected support window, which is the only way to reach PeerTooOld while the
// package's MinCompatible is still 1. It also documents the behaviour after the
// first MinCompatible bump: an unversioned (0) or below-floor peer is fenced.
func TestClassifyWindow(t *testing.T) {
	cases := []struct {
		name                     string
		peer, minCompatible, cur uint32
		want                     Compatibility
	}{
		{"in window", 2, 1, 3, Compatible},
		{"at floor", 2, 2, 3, Compatible},
		{"at ceiling", 3, 1, 3, Compatible},
		{"below floor is fenced", 1, 2, 3, PeerTooOld},
		{"unversioned below raised floor is fenced", 0, 2, 3, PeerTooOld},
		{"above ceiling is newer", 4, 1, 3, PeerNewer},
		{"unversioned is v1", 0, 1, 3, Compatible},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classify(tc.peer, tc.minCompatible, tc.cur); got != tc.want {
				t.Errorf("classify(%d, min=%d, cur=%d) = %v, want %v",
					tc.peer, tc.minCompatible, tc.cur, got, tc.want)
			}
		})
	}
}
