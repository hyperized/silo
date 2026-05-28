package replication_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/hyperized/silo/internal/chunkstore"
	"github.com/hyperized/silo/internal/replication"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// recordingPlace is a deterministic Placement fake. replicas/addrs/self are
// set up once before use and only read concurrently; lastN is guarded
// because the fan-out reads it from goroutines.
type recordingPlace struct {
	self     string
	replicas []string
	addrs    map[string]string

	mu    sync.Mutex
	lastN int
}

func (p *recordingPlace) Replicas(_ string, n int) []string {
	p.mu.Lock()
	p.lastN = n
	p.mu.Unlock()
	if n > len(p.replicas) {
		n = len(p.replicas)
	}
	out := make([]string, n)
	copy(out, p.replicas[:n])
	return out
}

func (p *recordingPlace) DataAddr(id string) (string, bool) {
	a, ok := p.addrs[id]
	return a, ok
}

func (p *recordingPlace) SelfID() string { return p.self }

func (p *recordingPlace) sawN() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastN
}

type fakeLocal struct {
	putInfo chunkstore.Info
	putErr  error
	getData []byte
	getInfo chunkstore.Info
	getErr  error

	mu   sync.Mutex
	puts int
}

func (l *fakeLocal) Put(_ context.Context, _ string, _ []byte) (chunkstore.Info, error) {
	l.mu.Lock()
	l.puts++
	l.mu.Unlock()
	return l.putInfo, l.putErr
}

func (l *fakeLocal) Get(_ context.Context, _ string) ([]byte, chunkstore.Info, error) {
	return l.getData, l.getInfo, l.getErr
}

type fakePeers struct {
	storeInfo chunkstore.Info
	storeErr  map[string]error // keyed by addr; nil/absent means success
	fetchData []byte
	fetchInfo chunkstore.Info
	fetchErr  map[string]error // keyed by addr; nil/absent means success

	mu      sync.Mutex
	fetched []string
}

func (p *fakePeers) Store(_ context.Context, addr, _ string, _ []byte) (chunkstore.Info, error) {
	return p.storeInfo, p.storeErr[addr]
}

func (p *fakePeers) Fetch(_ context.Context, addr, _ string) ([]byte, chunkstore.Info, error) {
	p.mu.Lock()
	p.fetched = append(p.fetched, addr)
	p.mu.Unlock()
	if err := p.fetchErr[addr]; err != nil {
		return nil, chunkstore.Info{}, err
	}
	return p.fetchData, p.fetchInfo, nil
}

func (p *fakePeers) fetchCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.fetched)
}

func TestWrite_QuorumSuccess(t *testing.T) {
	want := chunkstore.Info{ID: "c1", PlainBytes: 10, StoredBytes: 106}
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b", "c"},
		addrs:    map[string]string{"b": "b:7000", "c": "c:7000"},
	}
	local := &fakeLocal{putInfo: want}
	peers := &fakePeers{storeInfo: want, storeErr: map[string]error{}}
	c := replication.New(place, local, peers, 3, discardLogger())

	got, err := c.Write(context.Background(), "c1", []byte("payload"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got != want {
		t.Errorf("info: got %+v, want %+v", got, want)
	}
}

func TestWrite_NoLiveNodes(t *testing.T) {
	place := &recordingPlace{self: "self"} // empty replica set
	c := replication.New(place, &fakeLocal{}, &fakePeers{}, 3, discardLogger())

	_, err := c.Write(context.Background(), "c1", []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "no live node") {
		t.Fatalf("got %v, want a no-live-node error", err)
	}
}

func TestWrite_QuorumNotReached(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b", "c"},
		addrs:    map[string]string{"b": "b:7000", "c": "c:7000"},
	}
	// self succeeds, both peers fail -> 1 ack, quorum is 2.
	local := &fakeLocal{}
	peers := &fakePeers{storeErr: map[string]error{
		"b:7000": context.DeadlineExceeded,
		"c:7000": context.DeadlineExceeded,
	}}
	c := replication.New(place, local, peers, 3, discardLogger())

	_, err := c.Write(context.Background(), "c1", []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "quorum") {
		t.Fatalf("got %v, want a quorum-shortfall error", err)
	}
}

func TestWrite_ReplicaMissingDataAddress(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b"}, // b has no entry in addrs
		addrs:    map[string]string{},
	}
	c := replication.New(place, &fakeLocal{}, &fakePeers{}, 3, discardLogger())

	// quorum of 2 targets is 2; self acks but b cannot be addressed.
	_, err := c.Write(context.Background(), "c1", []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "data address") {
		t.Fatalf("got %v, want a missing-data-address error", err)
	}
}

func TestWrite_ClampsReplicationFactorBelowOne(t *testing.T) {
	place := &recordingPlace{self: "self", replicas: []string{"self"}}
	c := replication.New(place, &fakeLocal{}, &fakePeers{}, 0, discardLogger())

	if _, err := c.Write(context.Background(), "c1", []byte("x")); err != nil {
		t.Fatalf("Write with clamped rf: %v", err)
	}
	if n := place.sawN(); n != 1 {
		t.Errorf("replication factor not clamped to 1: Replicas asked for n=%d", n)
	}
}

func TestRead_PrefersLocal(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b"},
		addrs:    map[string]string{"b": "b:7000"},
	}
	local := &fakeLocal{getData: []byte("local-bytes"), getInfo: chunkstore.Info{ID: "c1"}}
	peers := &fakePeers{}
	c := replication.New(place, local, peers, 3, discardLogger())

	data, _, err := c.Read(context.Background(), "c1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "local-bytes" {
		t.Errorf("data: got %q, want local-bytes", data)
	}
	if peers.fetchCount() != 0 {
		t.Errorf("Read hit a peer despite a healthy local replica (%d fetches)", peers.fetchCount())
	}
}

func TestRead_FallsBackToPeerWhenLocalFails(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b"},
		addrs:    map[string]string{"b": "b:7000"},
	}
	local := &fakeLocal{getErr: chunkstore.ErrNotFound}
	peers := &fakePeers{fetchData: []byte("peer-bytes"), fetchErr: map[string]error{}}
	c := replication.New(place, local, peers, 3, discardLogger())

	data, _, err := c.Read(context.Background(), "c1")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "peer-bytes" {
		t.Errorf("data: got %q, want peer-bytes", data)
	}
}

func TestRead_NoLiveNodes(t *testing.T) {
	place := &recordingPlace{self: "self"}
	c := replication.New(place, &fakeLocal{}, &fakePeers{}, 3, discardLogger())

	if _, _, err := c.Read(context.Background(), "c1"); err == nil || !strings.Contains(err.Error(), "no live node") {
		t.Fatalf("got %v, want a no-live-node error", err)
	}
}

func TestRead_AllReplicasUnreachable(t *testing.T) {
	// self is not a replica, so the local fast-path is skipped entirely;
	// one peer has no address and the other fails its fetch.
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"b", "c"},
		addrs:    map[string]string{"c": "c:7000"},
	}
	peers := &fakePeers{fetchErr: map[string]error{"c:7000": chunkstore.ErrNotFound}}
	c := replication.New(place, &fakeLocal{}, peers, 3, discardLogger())

	_, _, err := c.Read(context.Background(), "c1")
	if err == nil || !strings.Contains(err.Error(), "could not be read") {
		t.Fatalf("got %v, want an all-replicas-unreachable error", err)
	}
}
