package writer

import (
	"errors"
	"strings"
	"testing"
)

func TestSanitize(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"allowlist passes through", "silo-a_1", "silo-a_1"},
		{"non-allowlist replaced", "silo/a b.c", "silo_a_b_c"},
		{"empty falls back", "", "node"},
		{"capped at max length", strings.Repeat("x", 100), strings.Repeat("x", maxWriterPrefix)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitize(tc.in); got != tc.want {
				t.Errorf("sanitize(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNewWriterID_EntropyFailure(t *testing.T) {
	prev := randRead
	t.Cleanup(func() { randRead = prev })
	randRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }

	if _, err := NewWriterID("silo-a"); err == nil || !strings.Contains(err.Error(), "entropy") {
		t.Fatalf("got %v, want an entropy error", err)
	}
}
