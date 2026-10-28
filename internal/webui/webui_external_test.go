package webui_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hyperized/silo/internal/webui"
)

func assertSecurityHeaders(t *testing.T, h http.Header) {
	t.Helper()
	want := map[string]string{
		"Content-Security-Policy": "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'; form-action 'none'",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "no-referrer",
	}
	for k, v := range want {
		if got := h.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
}

func do(method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	webui.Handler().ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

// TestHandlerServesIndex must hold both with and without a UI build in dist/,
// since CI runs the Go tests in both states.
func TestHandlerServesIndex(t *testing.T) {
	root := do(http.MethodGet, "/")
	tests := []struct {
		name   string
		target string
	}{
		{"root", "/"},
		{"spa route", "/volumes/some-volume"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(http.MethodGet, tt.target)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
				t.Errorf("Content-Type = %q", ct)
			}
			if !strings.Contains(rec.Body.String(), "<title>silo") {
				t.Errorf("body is neither the built UI nor the placeholder")
			}
			if rec.Body.String() != root.Body.String() {
				t.Errorf("body differs from root")
			}
			if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
				t.Errorf("Cache-Control = %q", cc)
			}
			assertSecurityHeaders(t, rec.Header())
		})
	}
}

func TestHandlerErrors(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
		wantAllow  string
	}{
		{"missing file", http.MethodGet, "/missing.js", http.StatusNotFound, ""},
		{"post", http.MethodPost, "/", http.StatusMethodNotAllowed, "GET, HEAD"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(tt.method, tt.target)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if got := rec.Header().Get("Allow"); got != tt.wantAllow {
				t.Errorf("Allow = %q, want %q", got, tt.wantAllow)
			}
			if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
				t.Errorf("Cache-Control = %q", cc)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
				t.Errorf("Content-Type = %q", ct)
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Errorf("body = %q, err = %v", rec.Body.String(), err)
			}
			assertSecurityHeaders(t, rec.Header())
		})
	}
}

func TestHandlerHead(t *testing.T) {
	rec := do(http.MethodHead, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD body length = %d", rec.Body.Len())
	}
	assertSecurityHeaders(t, rec.Header())
}
