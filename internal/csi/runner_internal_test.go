package csi

import (
	"context"
	"strings"
	"testing"
)

func TestExecRunner(t *testing.T) {
	// The production seam actually runs a command; echo is universally present.
	out, err := execRunner(context.Background(), "echo", "hello")
	if err != nil || strings.TrimSpace(string(out)) != "hello" {
		t.Errorf("execRunner echo = (%q, %v), want hello", out, err)
	}
	if _, err := execRunner(context.Background(), "this-binary-does-not-exist-silo"); err == nil {
		t.Error("execRunner of a missing binary should error")
	}
}

// recordingRunner records every command and returns canned output keyed by the
// binary name (args[0] of the underlying command).
type recordingRunner struct {
	calls    [][]string
	out      map[string][]byte
	err      map[string]error
	errOnArg map[string]error // fail any call whose args contain this substring
}

func newRecorder() *recordingRunner {
	return &recordingRunner{out: map[string][]byte{}, err: map[string]error{}, errOnArg: map[string]error{}}
}

func (r *recordingRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, args...))
	for sub, err := range r.errOnArg {
		for _, a := range args {
			if strings.Contains(a, sub) {
				return nil, err
			}
		}
	}
	return r.out[name], r.err[name]
}

func (r *recordingRunner) lastFor(name string) []string {
	for i := len(r.calls) - 1; i >= 0; i-- {
		if r.calls[i][0] == name {
			return r.calls[i]
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
