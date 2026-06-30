package replication_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

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

	delErr   error
	statInfo chunkstore.Info
	statErr  error

	mu      sync.Mutex
	puts    int
	deletes int
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

func (l *fakeLocal) Delete(_ context.Context, _ string) error {
	l.mu.Lock()
	l.deletes++
	l.mu.Unlock()
	return l.delErr
}

func (l *fakeLocal) Stat(_ context.Context, _ string) (chunkstore.Info, error) {
	return l.statInfo, l.statErr
}

type fakePeers struct {
	storeInfo chunkstore.Info
	storeErr  map[string]error // keyed by addr; nil/absent means success
	fetchData []byte
	fetchInfo chunkstore.Info
	fetchErr  map[string]error // keyed by addr; nil/absent means success
	deleteErr map[string]error // keyed by addr; nil/absent means success
	statInfo  chunkstore.Info
	statErr   map[string]error // keyed by addr; nil/absent means present

	mu      sync.Mutex
	fetched []string
	deleted []string
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

func (p *fakePeers) Delete(_ context.Context, addr, _ string) error {
	p.mu.Lock()
	p.deleted = append(p.deleted, addr)
	p.mu.Unlock()
	return p.deleteErr[addr]
}

func (p *fakePeers) Stat(_ context.Context, addr, _ string) (chunkstore.Info, error) {
	if err := p.statErr[addr]; err != nil {
		return chunkstore.Info{}, err
	}
	return p.statInfo, nil
}

func (p *fakePeers) fetchCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.fetched)
}

func (p *fakePeers) deletedAddrs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.deleted...)
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

func TestDelete_FansOutToAllReplicas(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b", "c"},
		addrs:    map[string]string{"b": "b:7000", "c": "c:7000"},
	}
	local := &fakeLocal{}
	peers := &fakePeers{deleteErr: map[string]error{}}
	c := replication.New(place, local, peers, 3, discardLogger())

	if err := c.Delete(context.Background(), "c1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if local.deletes != 1 {
		t.Errorf("local replica not deleted (deletes=%d)", local.deletes)
	}
	if got := peers.deletedAddrs(); len(got) != 2 {
		t.Errorf("expected deletes to b and c, got %v", got)
	}
}

func TestDelete_TreatsMissingReplicaAsDeleted(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b"},
		addrs:    map[string]string{"b": "b:7000"},
	}
	local := &fakeLocal{delErr: chunkstore.ErrNotFound}
	peers := &fakePeers{deleteErr: map[string]error{"b:7000": chunkstore.ErrNotFound}}
	c := replication.New(place, local, peers, 2, discardLogger())

	if err := c.Delete(context.Background(), "c1"); err != nil {
		t.Fatalf("a replica that already lacks the chunk must count as deleted: %v", err)
	}
}

func TestDelete_FailsWhenAReplicaErrors(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b"},
		addrs:    map[string]string{"b": "b:7000"},
	}
	peers := &fakePeers{deleteErr: map[string]error{"b:7000": context.DeadlineExceeded}}
	c := replication.New(place, &fakeLocal{}, peers, 2, discardLogger())

	if err := c.Delete(context.Background(), "c1"); err == nil || !strings.Contains(err.Error(), "did not reach every replica") {
		t.Fatalf("got %v, want a partial-delete error", err)
	}
}

func TestDelete_NoLiveNodes(t *testing.T) {
	c := replication.New(&recordingPlace{self: "self"}, &fakeLocal{}, &fakePeers{}, 3, discardLogger())
	if err := c.Delete(context.Background(), "c1"); err == nil || !strings.Contains(err.Error(), "no live node") {
		t.Fatalf("got %v, want a no-live-node error", err)
	}
}

func TestDelete_ReplicaMissingDataAddress(t *testing.T) {
	place := &recordingPlace{self: "self", replicas: []string{"self", "b"}, addrs: map[string]string{}}
	c := replication.New(place, &fakeLocal{}, &fakePeers{}, 2, discardLogger())
	if err := c.Delete(context.Background(), "c1"); err == nil || !strings.Contains(err.Error(), "data address") {
		t.Fatalf("got %v, want a missing-data-address error", err)
	}
}

func TestStat_PrefersLocal(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b"},
		addrs:    map[string]string{"b": "b:7000"},
	}
	local := &fakeLocal{statInfo: chunkstore.Info{ID: "c1", PlainBytes: 7}}
	peers := &fakePeers{statErr: map[string]error{"b:7000": context.DeadlineExceeded}}
	c := replication.New(place, local, peers, 3, discardLogger())

	info, err := c.Stat(context.Background(), "c1")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.PlainBytes != 7 {
		t.Errorf("Stat info: got %+v, want PlainBytes=7 from local", info)
	}
}

func TestStat_FallsBackToPeer(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b"},
		addrs:    map[string]string{"b": "b:7000"},
	}
	local := &fakeLocal{statErr: chunkstore.ErrNotFound}
	peers := &fakePeers{statInfo: chunkstore.Info{ID: "c1", PlainBytes: 9}, statErr: map[string]error{}}
	c := replication.New(place, local, peers, 3, discardLogger())

	info, err := c.Stat(context.Background(), "c1")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.PlainBytes != 9 {
		t.Errorf("Stat info: got %+v, want PlainBytes=9 from peer", info)
	}
}

func TestStat_NoLiveNodes(t *testing.T) {
	c := replication.New(&recordingPlace{self: "self"}, &fakeLocal{}, &fakePeers{}, 3, discardLogger())
	if _, err := c.Stat(context.Background(), "c1"); err == nil || !strings.Contains(err.Error(), "no live node") {
		t.Fatalf("got %v, want a no-live-node error", err)
	}
}

func TestStat_AllReplicasMissing(t *testing.T) {
	// self is not a replica, so the local fast-path is skipped; one peer
	// has no address, the other reports the chunk absent.
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"b", "c"},
		addrs:    map[string]string{"c": "c:7000"},
	}
	peers := &fakePeers{statErr: map[string]error{"c:7000": chunkstore.ErrNotFound}}
	c := replication.New(place, &fakeLocal{}, peers, 3, discardLogger())

	if _, err := c.Stat(context.Background(), "c1"); err == nil || !strings.Contains(err.Error(), "not found on any") {
		t.Fatalf("got %v, want an all-replicas-missing error", err)
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

// blockingPeers is a Peers whose Store blocks on release after announcing its
// entry (by addr) on entered, so a test can hold a replica send in flight and
// observe whether a second send is admitted under the concurrency bound.
type blockingPeers struct {
	entered chan string
	release chan struct{}
	info    chunkstore.Info
}

func (p *blockingPeers) Store(_ context.Context, addr, _ string, _ []byte) (chunkstore.Info, error) {
	p.entered <- addr
	<-p.release
	return p.info, nil
}

func (p *blockingPeers) Fetch(context.Context, string, string) ([]byte, chunkstore.Info, error) {
	return nil, chunkstore.Info{}, nil
}
func (p *blockingPeers) Delete(context.Context, string, string) error { return nil }
func (p *blockingPeers) Stat(context.Context, string, string) (chunkstore.Info, error) {
	return chunkstore.Info{}, nil
}

// With WithMaxConcurrentWrites(1) only one peer send runs at a time: the second
// replica's send must wait for the first to release its slot. This is the cap
// that stops grpc's send-buffer pool growing without limit under a write storm.
func TestCoordinator_BoundsConcurrentPeerSends(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b", "c"},
		addrs:    map[string]string{"b": "b:7000", "c": "c:7000"},
	}
	peers := &blockingPeers{entered: make(chan string, 2), release: make(chan struct{})}
	c := replication.New(place, &fakeLocal{}, peers, 3, discardLogger(), replication.WithMaxConcurrentWrites(1))

	done := make(chan error, 1)
	go func() { _, err := c.Write(context.Background(), "c1", []byte("x")); done <- err }()

	var first string
	select {
	case first = <-peers.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no peer send started")
	}
	select {
	case s := <-peers.entered:
		t.Fatalf("a second peer send (%s) started while the one-slot limit was held by %s", s, first)
	case <-time.After(150 * time.Millisecond):
		// good: the second send is parked on the semaphore
	}

	close(peers.release) // first send finishes and frees its slot
	select {
	case second := <-peers.entered:
		if second == first {
			t.Errorf("second send went to the same addr %q", second)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second peer send never started after the slot was freed")
	}
	if err := <-done; err != nil {
		t.Errorf("Write: %v", err)
	}
}

// A send that cannot get a slot fails on its own context (here it sinks the
// write below quorum), rather than parking forever.
func TestCoordinator_PeerSendSlotContextCancel(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"b", "c"}, // self is not a replica, so both acks must come from peers
		addrs:    map[string]string{"b": "b:7000", "c": "c:7000"},
	}
	peers := &blockingPeers{entered: make(chan string, 2), release: make(chan struct{})}
	c := replication.New(place, &fakeLocal{}, peers, 2, discardLogger(), replication.WithMaxConcurrentWrites(1))

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := c.Write(ctx, "c1", nil); errc <- err }()

	<-peers.entered // one send holds the only slot
	cancel()        // the other send is parked on the slot -> its context fires
	close(peers.release)

	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "cancelled") {
			t.Errorf("got %v, want a quorum error mentioning the cancelled send", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Write did not return after the parked send was cancelled")
	}
}

// WithMaxConcurrentWrites(0) leaves peer sends unbounded: both replicas send at
// once, neither waiting on the other.
func TestCoordinator_UnboundedPeerSendsWhenZero(t *testing.T) {
	place := &recordingPlace{
		self:     "self",
		replicas: []string{"self", "b", "c"},
		addrs:    map[string]string{"b": "b:7000", "c": "c:7000"},
	}
	peers := &blockingPeers{entered: make(chan string, 2), release: make(chan struct{})}
	c := replication.New(place, &fakeLocal{}, peers, 3, discardLogger(), replication.WithMaxConcurrentWrites(0))

	go func() { _, _ = c.Write(context.Background(), "c1", nil) }()
	for i := 0; i < 2; i++ {
		select {
		case <-peers.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("not all peer sends started concurrently — unexpected bound at n=0")
		}
	}
	close(peers.release)
}
