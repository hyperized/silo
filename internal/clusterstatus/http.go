package clusterstatus

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
)

// jsonOptions fix the wire contract the web UI is built against: lowerCamelCase
// keys, every field present even when zero (so the UI never has to guess a
// default), 64-bit integers as strings and enums by name, which is the standard
// protobuf JSON mapping.
var jsonOptions = protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: false}

// marshalJSON is a seam so tests can exercise the encoding failure path.
var marshalJSON = func(m proto.Message) ([]byte, error) { return jsonOptions.Marshal(m) }

// Building a snapshot takes the namespace's exclusive lock for a full tree walk
// and reads the chunk directory, so an unauthenticated endpoint must not do it
// once per request. These bound the cost of a request burst.
const (
	// cacheTTL is short enough that a page polling every few seconds still
	// sees fresh data, and long enough that a burst collapses to one build.
	cacheTTL = 2 * time.Second
	// requestTimeout caps how long a client waits behind a slow build.
	requestTimeout = 5 * time.Second
	// maxInFlight is how many requests may wait at once; the UI polls one at a
	// time per tab, so this only bites on scripted floods.
	maxInFlight = 8
)

const (
	msgMethod  = "this endpoint is read-only; send a GET request to read the node status"
	msgEncode  = "silod could not encode its status; retry, and if it keeps failing check the silod logs and report it as a bug"
	msgTimeout = `{"error":"silod took too long to collect its status; retry in a few seconds, and if it keeps happening check the silod logs for a stuck disk or namespace"}` + "\n"
	msgBusy    = "too many status requests are in progress on this node; retry in a second, and poll less often if a script is calling this endpoint"
)

type handlerConfig struct {
	ttl         time.Duration
	timeout     time.Duration
	maxInFlight int
	now         func() time.Time
}

// Handler serves b's snapshot as JSON at whatever path it is mounted on.
//
// It answers GET and HEAD only and never lets clients cache. The projection
// leaves out the node's data directory and volume inode ids, which the browser
// does not need. Responses are shared for two seconds and built by at most one
// request at a time; a request waits at most five seconds, and more than eight
// concurrent requests get 503 with Retry-After. Failures are JSON
// {"error": "..."} bodies that say what to do next. Safe for concurrent use.
func Handler(b *Builder) http.Handler {
	return newHandler(b, handlerConfig{ttl: cacheTTL, timeout: requestTimeout, maxInFlight: maxInFlight, now: time.Now})
}

func newHandler(b *Builder, cfg handlerConfig) http.Handler {
	h := &statusHandler{cache: &snapshotCache{build: projected(b), ttl: cfg.ttl, now: cfg.now}}
	limited := &limiter{slots: make(chan struct{}, cfg.maxInFlight), next: http.TimeoutHandler(h, cfg.timeout, msgTimeout)}
	return limited
}

// projected renders the HTTP view of a snapshot. It clones so the gRPC path,
// which serves operators holding a client certificate, keeps every field.
func projected(b *Builder) func(context.Context) ([]byte, error) {
	return func(ctx context.Context) ([]byte, error) {
		resp, _ := proto.Clone(b.Build(ctx)).(*statusv1.GetStatusResponse)
		if resp.GetStorage() != nil {
			resp.Storage.DataDir = ""
		}
		for _, v := range resp.GetVolumes() {
			v.Inode = ""
		}
		return marshalJSON(resp)
	}
}

// limiter rejects instead of queueing so a flood cannot pile up goroutines
// behind the cache.
type limiter struct {
	slots chan struct{}
	next  http.Handler
}

func (l *limiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set before TimeoutHandler takes over: on a timeout it writes its body to
	// this writer with whatever headers are already here.
	setJSONHeaders(w)
	select {
	case l.slots <- struct{}{}:
		defer func() { <-l.slots }()
		l.next.ServeHTTP(w, r)
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, msgBusy)
	}
}

type statusHandler struct {
	cache *snapshotCache
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeError(w, http.StatusMethodNotAllowed, msgMethod)
		return
	}
	body, err := h.cache.get(r.Context())
	switch {
	case r.Context().Err() != nil:
		// The client left or TimeoutHandler already answered; nothing to write.
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, msgEncode)
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

// snapshotCache keeps the last rendered body for ttl and lets only one caller
// build at a time; everyone who arrives during a build waits for that result.
type snapshotCache struct {
	build func(context.Context) ([]byte, error)
	ttl   time.Duration
	now   func() time.Time

	mu       sync.Mutex
	body     []byte
	at       time.Time
	inflight *buildCall
}

type buildCall struct {
	done chan struct{}
	body []byte
	err  error
}

func (c *snapshotCache) get(ctx context.Context) ([]byte, error) {
	c.mu.Lock()
	if c.body != nil && c.now().Sub(c.at) < c.ttl {
		body := c.body
		c.mu.Unlock()
		return body, nil
	}
	call := c.inflight
	if call == nil {
		call = &buildCall{done: make(chan struct{})}
		c.inflight = call
		go c.run(ctx, call)
	}
	c.mu.Unlock()

	select {
	case <-call.done:
		return call.body, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// run builds on its own goroutine, detached from the caller's cancellation, so
// the first requester timing out does not fail everyone queued behind it. The
// goroutine ends when the build returns.
func (c *snapshotCache) run(ctx context.Context, call *buildCall) {
	body, err := c.build(context.WithoutCancel(ctx))
	if err == nil {
		body = append(body, '\n')
	}
	c.mu.Lock()
	if err == nil {
		c.body, c.at = body, c.now()
	}
	c.inflight = nil
	c.mu.Unlock()
	call.body, call.err = body, err
	close(call.done)
}

func setJSONHeaders(w http.ResponseWriter) {
	hdr := w.Header()
	hdr.Set("Content-Type", "application/json; charset=utf-8")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("X-Content-Type-Options", "nosniff")
}

func writeError(w http.ResponseWriter, code int, msg string) {
	setJSONHeaders(w)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{msg})
}
