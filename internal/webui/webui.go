// Package webui serves the embedded web UI. Build output under dist/ is
// git-ignored, so a plain `go build` without Node.js embeds an empty dist and
// serves placeholder.html instead, which tells the operator how to build the UI.
package webui

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"
)

//go:embed all:dist
var embedded embed.FS

//go:embed placeholder.html
var placeholder []byte

const (
	indexName = "index.html"

	// No 'unsafe-inline' for styles: the Vite build emits only linked CSS and
	// Vue applies bound styles through the CSSOM, which CSP does not police.
	contentSecurityPolicy = "default-src 'self'; img-src 'self' data:; style-src 'self'; " +
		"script-src 'self'; connect-src 'self'; frame-ancestors 'none'; " +
		"base-uri 'none'; object-src 'none'; form-action 'none'"

	cacheImmutable  = "public, max-age=31536000, immutable"
	cacheRevalidate = "no-cache"
	cacheNever      = "no-store"
)

type errorBody struct {
	Error string `json:"error"`
}

// Handler returns the handler for the embedded web UI.
//
// It answers GET and HEAD only. Paths that match no file and have no file
// extension get index.html so client-side routes survive a reload. Files under
// assets/ are content-hashed and cached as immutable; everything else must be
// revalidated. Every response carries a restrictive Content-Security-Policy.
// The handler is safe for concurrent use.
func Handler() http.Handler {
	// "dist" is a constant, valid name, so Sub cannot fail.
	sub, _ := fs.Sub(embedded, "dist")
	return newHandler(sub)
}

type handler struct {
	fsys fs.FS
	// etags avoids rehashing immutable embedded content on every request.
	etags sync.Map
}

func newHandler(fsys fs.FS) http.Handler {
	return &handler{fsys: fsys}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, http.StatusMethodNotAllowed, "the web UI is read-only; use GET to load it")
		return
	}
	name := requestName(r)
	if isRegularFile(h.fsys, name) {
		h.serveFile(w, r, name)
		return
	}
	if name != indexName && path.Ext(name) != "" {
		writeError(w, http.StatusNotFound,
			"this file is not part of the running web UI build; reload the page to pick up the current version")
		return
	}
	if !isRegularFile(h.fsys, indexName) {
		h.serveBytes(w, r, indexName, placeholder)
		return
	}
	h.serveFile(w, r, indexName)
}

func setSecurityHeaders(w http.ResponseWriter) {
	hdr := w.Header()
	hdr.Set("Content-Security-Policy", contentSecurityPolicy)
	hdr.Set("X-Content-Type-Options", "nosniff")
	hdr.Set("Referrer-Policy", "no-referrer")
}

// requestName cleans the URL path first so ".." segments cannot leave fsys.
func requestName(r *http.Request) string {
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if name == "" {
		return indexName
	}
	return name
}

// isRegularFile rejects directories so the server never lists them, and any
// path with a segment starting with "." so neither the .gitkeep that keeps
// dist/ in the repository nor anything inside a dot directory a build tool
// left behind is ever served.
func isRegularFile(fsys fs.FS, name string) bool {
	if hasDotSegment(name) {
		return false
	}
	info, err := fs.Stat(fsys, name)
	return err == nil && info.Mode().IsRegular()
}

func hasDotSegment(name string) bool {
	for seg := range strings.SplitSeq(name, "/") {
		if strings.HasPrefix(seg, ".") {
			return true
		}
	}
	return false
}

func cacheControlFor(name string) string {
	if strings.HasPrefix(name, "assets/") {
		return cacheImmutable
	}
	return cacheRevalidate
}

func (h *handler) serveFile(w http.ResponseWriter, r *http.Request, name string) {
	data, err := fs.ReadFile(h.fsys, name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "the web UI file could not be read; restart silod and try again")
		return
	}
	h.serveBytes(w, r, name, data)
}

func (h *handler) serveBytes(w http.ResponseWriter, r *http.Request, name string, data []byte) {
	w.Header().Set("Cache-Control", cacheControlFor(name))
	w.Header().Set("ETag", h.etagFor(name, data))
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}

// etagFor is needed because embedded files have no modification time, so
// ServeContent would otherwise have no validator for conditional requests.
func (h *handler) etagFor(name string, data []byte) string {
	if v, ok := h.etags.Load(name); ok {
		return v.(string)
	}
	sum := sha256.Sum256(data)
	tag := `"` + hex.EncodeToString(sum[:16]) + `"`
	h.etags.Store(name, tag)
	return tag
}

func writeError(w http.ResponseWriter, status int, msg string) {
	hdr := w.Header()
	hdr.Set("Content-Type", "application/json; charset=utf-8")
	hdr.Set("Cache-Control", cacheNever)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: msg})
}
