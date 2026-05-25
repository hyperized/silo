package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestRunMain_ConfigErrorIsActionable verifies the daemon refuses to start
// without SILO_ENCRYPTION_KEY and prints the operator the exact fix.
func TestRunMain_ConfigErrorIsActionable(t *testing.T) {
	// Clear required fields so config.LoadFromEnv fails.
	t.Setenv("SILO_NODE_ID", "test-runmain")
	t.Setenv("SILO_ENCRYPTION_KEY", "")
	t.Setenv("SILO_ENCRYPTION_KEY_SOURCE", "static")

	var stdout, stderr bytes.Buffer
	code := runMain(&stdout, &stderr)

	if code != 1 {
		t.Errorf("exit code: got %d, want 1", code)
	}
	msg := stderr.String()
	if !strings.Contains(msg, "invalid configuration") {
		t.Errorf("stderr should explain it is a configuration problem; got: %q", msg)
	}
	if !strings.Contains(msg, "SILO_ENCRYPTION_KEY") {
		t.Errorf("stderr should name the offending variable; got: %q", msg)
	}
	if !strings.Contains(msg, "openssl rand -base64 32") {
		t.Errorf("stderr should tell the operator how to generate the key; got: %q", msg)
	}
}

// TestRunMain_ValidationErrorIsActionable hits a config that parses but
// fails validation; the operator should still get an actionable message.
func TestRunMain_ValidationErrorIsActionable(t *testing.T) {
	t.Setenv("SILO_NODE_ID", "test-logger-err")
	t.Setenv("SILO_ENCRYPTION_KEY", "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=") // 32 raw bytes b64
	t.Setenv("SILO_LOG_FORMAT", "yaml")

	var stdout, stderr bytes.Buffer
	code := runMain(&stdout, &stderr)

	if code != 1 {
		t.Errorf("exit code: got %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "SILO_LOG_FORMAT") {
		t.Errorf("stderr should mention SILO_LOG_FORMAT; got: %q", stderr.String())
	}
}
