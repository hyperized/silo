package webui

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
)

const indexBody = "<!doctype html><title>test</title>"

func testFS() fstest.MapFS {
	return fstest.MapFS{
		"index.html":             {Data: []byte(indexBody)},
		"assets/app-abc123.js":   {Data: []byte("console.log(1)")},
		"assets/app-abc123.css":  {Data: []byte("body{}")},
		"favicon.svg":            {Data: []byte("<svg xmlns=\"http://www.w3.org/2000/svg\"/>")},
		"assets/nested/keep.txt": {Data: []byte("x")},
	}
}

func get(h http.Handler, target string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestServeFiles(t *testing.T) {
	h := newHandler(testFS())
	tests := []struct {
		name     string
		target   string
		wantCode int
		wantType string
		wantCC   string
		wantBody string
	}{
		{"js asset", "/assets/app-abc123.js", 200, "javascript", cacheImmutable, "console.log(1)"},
		{"css asset", "/assets/app-abc123.css", 200, "text/css", cacheImmutable, "body{}"},
		{"favicon", "/favicon.svg", 200, "image/svg+xml", "no-cache", ""},
		{"index", "/index.html", 200, "text/html", "no-cache", indexBody},
		{"assets dir no slash", "/assets", 200, "text/html", "no-cache", indexBody},
		{"assets dir slash", "/assets/", 200, "text/html", "no-cache", indexBody},
		{"traversal", "/../../etc/passwd", 200, "text/html", "no-cache", indexBody},
		{"encoded traversal", "/%2e%2e/%2e%2e/etc/passwd", 200, "text/html", "no-cache", indexBody},
		{"spa route", "/volumes/foo", 200, "text/html", "no-cache", indexBody},
		{"missing asset", "/assets/old-deadbeef.js", 404, "application/json", "no-store", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := get(h, tt.target)
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			if ct := rec.Header().Get("Content-Type"); !strings.Contains(strings.ToLower(ct), tt.wantType) {
				t.Errorf("Content-Type = %q, want containing %q", ct, tt.wantType)
			}
			if cc := rec.Header().Get("Cache-Control"); cc != tt.wantCC {
				t.Errorf("Cache-Control = %q, want %q", cc, tt.wantCC)
			}
			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Errorf("body = %q, want %q", rec.Body.String(), tt.wantBody)
			}
			if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Errorf("missing security headers")
			}
		})
	}
}

func TestConditionalRequest(t *testing.T) {
	h := newHandler(testFS())
	first := get(h, "/assets/app-abc123.js")
	etag := first.Header().Get("ETag")
	if etag == "" {
		t.Fatal("no ETag")
	}
	for _, name := range []string{"first revalidation", "cached tag"} {
		t.Run(name, func(t *testing.T) {
			rec := get(h, "/assets/app-abc123.js", "If-None-Match", etag)
			if rec.Code != http.StatusNotModified {
				t.Errorf("status = %d, want 304", rec.Code)
			}
		})
	}
}

func TestMissingIndexServesPlaceholder(t *testing.T) {
	h := newHandler(fstest.MapFS{"favicon.svg": {Data: []byte("x")}})
	for _, target := range []string{"/", "/volumes"} {
		rec := get(h, target)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d", target, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Errorf("%s: Content-Type = %q", target, ct)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != cacheRevalidate {
			t.Errorf("%s: Cache-Control = %q", target, cc)
		}
		if !strings.Contains(rec.Body.String(), "make ui") {
			t.Errorf("%s: placeholder does not say how to build the UI", target)
		}
	}
}

func TestDotFilesAreHidden(t *testing.T) {
	h := newHandler(fstest.MapFS{
		"index.html":           {Data: []byte(indexBody)},
		".gitkeep":             {Data: []byte("")},
		".env":                 {Data: []byte("SECRET=1")},
		".vite/manifest.json":  {Data: []byte(`{"SECRET":1}`)},
		"assets/.cache/x.js":   {Data: []byte("SECRET")},
		".git/HEAD":            {Data: []byte("SECRET")},
		"assets/app-abc123.js": {Data: []byte("ok")},
	})
	for _, target := range []string{"/.gitkeep", "/.env", "/.vite/manifest.json", "/assets/.cache/x.js"} {
		rec := get(h, target)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404", target, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "SECRET") {
			t.Errorf("%s: dot file content was served", target)
		}
	}
	// No extension: falls back to the app shell, never the file.
	if rec := get(h, "/.git/HEAD"); strings.Contains(rec.Body.String(), "SECRET") {
		t.Error("/.git/HEAD: dot directory content was served")
	}
	if rec := get(h, "/assets/app-abc123.js"); rec.Code != http.StatusOK {
		t.Errorf("a normal asset next to a dot directory: status = %d, want 200", rec.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := newHandler(testFS())
	for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(m, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(m, "/", nil))
			if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
				t.Errorf("status = %d, Allow = %q", rec.Code, rec.Header().Get("Allow"))
			}
		})
	}
}

// brokenFS stats fine but fails on open, which is how a read error surfaces.
type brokenFS struct{ fs.FS }

func (b brokenFS) Open(name string) (fs.File, error) {
	if name == "index.html" {
		return nil, fs.ErrPermission
	}
	return b.FS.Open(name)
}

func (b brokenFS) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(b.FS, name)
}

func TestReadFailure(t *testing.T) {
	rec := get(newHandler(brokenFS{testFS()}), "/")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "restart") {
		t.Errorf("body = %q", rec.Body.String())
	}
}

func TestConcurrentRequests(t *testing.T) {
	h := newHandler(testFS())
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, target := range []string{"/", "/assets/app-abc123.js", "/favicon.svg"} {
				if rec := get(h, target); rec.Code != http.StatusOK {
					t.Errorf("%s status = %d", target, rec.Code)
				}
			}
		}()
	}
	wg.Wait()
}
