package observability

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLogger_Levels(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"DEBUG": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			got, err := parseLevel(in)
			if err != nil {
				t.Fatalf("parseLevel: %v", err)
			}
			if got != want {
				t.Errorf("parseLevel(%q): got %v, want %v", in, got, want)
			}
		})
	}
}

func TestNewLogger_BadLevel(t *testing.T) {
	_, err := NewLogger(&bytes.Buffer{}, "loud", "text")
	if err == nil || !strings.Contains(err.Error(), "log level") {
		t.Errorf("expected log-level error, got %v", err)
	}
}

func TestNewLogger_BadFormat(t *testing.T) {
	_, err := NewLogger(&bytes.Buffer{}, "info", "yaml")
	if err == nil || !strings.Contains(err.Error(), "log format") {
		t.Errorf("expected log-format error, got %v", err)
	}
}

func TestNewLogger_TextWritesMessage(t *testing.T) {
	buf := &bytes.Buffer{}
	lg, err := NewLogger(buf, "info", "text")
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	lg.Info("hello", "k", "v")
	if !strings.Contains(buf.String(), "hello") {
		t.Errorf("text output missing message: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "k=v") {
		t.Errorf("text output missing attrs: %q", buf.String())
	}
}

func TestNewLogger_JSONWritesMessage(t *testing.T) {
	buf := &bytes.Buffer{}
	lg, err := NewLogger(buf, "info", "JSON")
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	lg.Info("hello", "k", "v")
	out := buf.String()
	if !strings.Contains(out, `"msg":"hello"`) {
		t.Errorf("json output missing message: %q", out)
	}
	if !strings.Contains(out, `"k":"v"`) {
		t.Errorf("json output missing attrs: %q", out)
	}
}

func TestNewLogger_RespectsLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	lg, err := NewLogger(buf, "warn", "text")
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}
	lg.Info("ignored")
	if buf.Len() != 0 {
		t.Errorf("info at warn level should be suppressed, got %q", buf.String())
	}
	lg.Warn("kept")
	if !strings.Contains(buf.String(), "kept") {
		t.Errorf("warn message should appear: %q", buf.String())
	}
}
