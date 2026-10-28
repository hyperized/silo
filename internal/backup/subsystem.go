package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperized/silo/internal/blobstore"
	"github.com/hyperized/silo/internal/metrics"
)

// DefaultInterval is how often the backup subsystem exports when none is set.
const DefaultInterval = 6 * time.Hour

// DefaultRetain is how many manifests per node retention keeps by default.
const DefaultRetain = 7

var (
	// ErrRunInProgress is returned by RunNow while another run is going.
	ErrRunInProgress = errors.New("backup: a backup is already running on this node")
	// ErrStopped is returned by RunNow once the subsystem is shutting down.
	ErrStopped = errors.New("backup: the backup subsystem is shutting down")
)

// Run is the outcome of one backup run.
type Run struct {
	StartedAt  time.Time
	FinishedAt time.Time
	OK         bool
	// Error is empty when OK; otherwise it says what to do next.
	Error          string
	ChunksUploaded int64
	ChunksSkipped  int64
	BytesUploaded  int64
	// Manifest is the object name of the manifest the run wrote.
	Manifest string
}

// Status is a point-in-time view of the subsystem for the status API.
type Status struct {
	Configured bool
	Target     string
	Interval   time.Duration
	// NextRun is zero while a scheduled run is executing or before Start.
	NextRun time.Time
	Running bool
	// LastRun is nil until a run has finished.
	LastRun *Run
}

// Subsystem runs the exporter on a timer, writes the node's state to the
// configured target and applies retention afterwards. It fits silod's
// subsystem interface (Name/Start/Shutdown), serves Prometheus metrics, and
// accepts manual runs through RunNow. The first scheduled run fires one
// interval after Start, not at startup, so a restart loop cannot cause a
// backup storm.
//
// All methods are safe for concurrent use. At most one run executes at a time.
type Subsystem struct {
	exporter  *Exporter
	target    blobstore.Target
	interval  time.Duration
	retain    int
	retainFor time.Duration
	logger    *slog.Logger
	now       func() time.Time

	// ctx bounds every run; Shutdown cancels it so an in-flight upload stops.
	ctx    context.Context
	cancel context.CancelFunc
	// runs tracks manual runs, which execute outside the Start goroutine.
	runs     sync.WaitGroup
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}

	mu      sync.Mutex
	stopped bool
	running bool
	nextRun time.Time
	lastRun *Run

	runsTotal      atomic.Int64
	failures       atomic.Int64
	lastChunks     atomic.Int64
	lastSuccess    atomic.Int64 // unix seconds
	lastDurationMs atomic.Int64
	uploaded       atomic.Int64
	skipped        atomic.Int64
	bytesUploaded  atomic.Int64
	pruned         atomic.Int64
	pruneFailures  atomic.Int64
}

// SubsystemOption configures a Subsystem.
type SubsystemOption func(*Subsystem)

// WithRetain keeps the newest n manifests of this node and prunes the rest
// after every successful run. Zero switches pruning off; negative values keep
// DefaultRetain.
func WithRetain(n int) SubsystemOption {
	return func(s *Subsystem) {
		if n >= 0 {
			s.retain = n
		}
	}
}

// WithRetainFor never prunes a manifest younger than d, whatever the retention
// count says. Zero disables the floor; negative values keep DefaultRetainFor.
func WithRetainFor(d time.Duration) SubsystemOption {
	return func(s *Subsystem) {
		if d >= 0 {
			s.retainFor = d
		}
	}
}

// WithSubsystemClock replaces time.Now for run bookkeeping and the prune grace
// window.
func WithSubsystemClock(now func() time.Time) SubsystemOption {
	return func(s *Subsystem) { s.now = now }
}

// NewSubsystem builds the backup subsystem. A non-positive interval falls back
// to DefaultInterval.
func NewSubsystem(exporter *Exporter, target blobstore.Target, interval time.Duration, logger *slog.Logger, opts ...SubsystemOption) *Subsystem {
	if interval <= 0 {
		interval = DefaultInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Subsystem{
		exporter:  exporter,
		target:    target,
		interval:  interval,
		retain:    DefaultRetain,
		retainFor: DefaultRetainFor,
		logger:    logger,
		now:       time.Now,
		ctx:       ctx,
		cancel:    cancel,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Name identifies the subsystem in lifecycle logs.
func (s *Subsystem) Name() string { return "backup" }

// Start runs the schedule until Shutdown. A tick that arrives while a manual
// run is executing is skipped; the next one comes an interval later.
func (s *Subsystem) Start() error {
	defer close(s.done)
	timer := time.NewTimer(s.interval)
	defer timer.Stop()
	s.setNextRun(s.now().Add(s.interval))
	for {
		select {
		case <-s.stop:
			return nil
		case <-timer.C:
			if s.claim() {
				s.setNextRun(time.Time{})
				s.execute(true)
			}
			timer.Reset(s.interval)
			s.setNextRun(s.now().Add(s.interval))
		}
	}
}

// Shutdown stops the schedule, cancels any run in flight and waits for it to
// return.
func (s *Subsystem) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		// Set under mu so no manual run can join s.runs once Wait may
		// have begun.
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()
		close(s.stop)
		s.cancel()
	})
	finished := make(chan struct{})
	go func() {
		<-s.done
		s.runs.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("backup subsystem did not stop within the shutdown deadline (%w); an upload may be in flight, the next run will redo it", ctx.Err())
	}
}

// RunNow starts a run immediately and waits for its outcome. It returns
// ErrRunInProgress when a run is already going and ErrStopped during shutdown.
// Manual runs never prune: otherwise anyone allowed to trigger runs could push
// every older restore point out of retention in minutes. The next scheduled
// run applies retention as usual.
// The run uses the subsystem's own lifetime, not ctx: when ctx ends first,
// RunNow returns ctx's error and the run carries on; Status reports its result.
func (s *Subsystem) RunNow(ctx context.Context) (Run, error) {
	if err := s.claimManual(); err != nil {
		return Run{}, err
	}
	result := make(chan Run, 1)
	go func() {
		defer s.runs.Done()
		result <- s.execute(false)
	}()
	select {
	case r := <-result:
		return r, nil
	case <-ctx.Done():
		return Run{}, ctx.Err()
	}
}

// Status returns a copy of the subsystem's current state.
func (s *Subsystem) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Status{
		Configured: true,
		Target:     s.target.Name(),
		Interval:   s.interval,
		NextRun:    s.nextRun,
		Running:    s.running,
	}
	if s.lastRun != nil {
		r := *s.lastRun
		st.LastRun = &r
	}
	return st
}

// claim marks a run as started, or reports false when one already is.
func (s *Subsystem) claim() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return false
	}
	s.running = true
	return true
}

// claimManual is claim for RunNow: it also refuses during shutdown and
// registers the run with s.runs while still holding mu.
func (s *Subsystem) claimManual() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrStopped
	}
	if s.running {
		return ErrRunInProgress
	}
	s.running = true
	s.runs.Add(1)
	return nil
}

func (s *Subsystem) setNextRun(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextRun = t
}

// execute performs one claimed run, applies retention when asked and the run
// succeeded, and records the outcome. It releases the claim before returning.
func (s *Subsystem) execute(prune bool) Run {
	s.runsTotal.Add(1)
	started := s.now()
	res, err := s.exporter.Export(s.ctx, s.target)
	finished := s.now()
	run := Run{StartedAt: started, FinishedAt: finished, OK: err == nil}
	s.lastDurationMs.Store(finished.Sub(started).Milliseconds())

	if err != nil {
		s.failures.Add(1)
		run.Error = runError(err, s.target.Name())
		s.logger.Warn("backup failed; the next scheduled run will retry", "target", s.target.Name(), "error", err)
	} else {
		run.ChunksUploaded = int64(res.Uploaded)
		run.ChunksSkipped = int64(res.Skipped)
		run.BytesUploaded = res.BytesUploaded
		run.Manifest = res.Manifest
		s.lastChunks.Store(int64(res.Chunks))
		s.lastSuccess.Store(finished.Unix())
		s.uploaded.Add(int64(res.Uploaded))
		s.skipped.Add(int64(res.Skipped))
		s.bytesUploaded.Add(res.BytesUploaded)
		s.logger.Info("backup complete", "target", s.target.Name(), "manifest", res.Manifest,
			"chunks", res.Chunks, "uploaded", res.Uploaded, "skipped", res.Skipped, "bytes_uploaded", res.BytesUploaded)
		if prune {
			s.prune(res.Stamp)
		}
	}

	s.mu.Lock()
	s.lastRun = &run
	s.running = false
	s.mu.Unlock()
	return run
}

// prune applies retention. Its failures are logged and counted but never turn
// a successful backup into a failed one: the restore point already exists.
func (s *Subsystem) prune(stamp string) {
	if s.retain == 0 {
		return
	}
	res, err := Prune(s.ctx, s.target, PruneOptions{
		NodeID:       s.exporter.nodeID,
		CurrentStamp: stamp,
		Retain:       s.retain,
		RetainFor:    s.retainFor,
		Grace:        PruneGrace(s.interval),
		Now:          s.now(),
		Concurrency:  s.exporter.concurrency,
	})
	s.pruned.Add(int64(res.ManifestsDeleted + res.ObjectsDeleted))
	if err != nil {
		s.pruneFailures.Add(1)
		s.logger.Warn("backup retention could not run; old backups are kept until the next run", "target", s.target.Name(), "error", err)
		return
	}
	s.pruneFailures.Add(int64(res.Failures))
	if res.ChunksHeldBack != "" {
		s.logger.Info("backup retention kept unreferenced chunks for now; they are reconsidered after the next run",
			"target", s.target.Name(), "reason", res.ChunksHeldBack)
	}
	if res.Failures > 0 {
		s.logger.Warn("backup retention skipped some objects; they are retried on the next run",
			"target", s.target.Name(), "failures", res.Failures, "first_error", res.FirstError)
	}
	if res.ManifestsDeleted+res.ObjectsDeleted > 0 {
		s.logger.Info("backup retention pruned old objects", "target", s.target.Name(),
			"manifests", res.ManifestsDeleted, "objects", res.ObjectsDeleted)
	}
}

// runError turns an export failure into the operator-facing message stored in
// Run.Error and shown by 'siloctl status'.
func runError(err error, target string) string {
	if errors.Is(err, context.Canceled) {
		return "the backup was interrupted because silod is shutting down; it runs again on the next schedule, or start one with 'siloctl backup run'"
	}
	return fmt.Sprintf("%v; fix the cause, then start a new run with 'siloctl backup run' (target %s)", err, target)
}

// MetricPrefix namespaces the backup metrics.
func (s *Subsystem) MetricPrefix() string { return "silo_backup" }

// CollectMetrics reports backup activity. Every metric is a distinct,
// label-free family, so series stay adjacent in the exposition.
func (s *Subsystem) CollectMetrics() []metrics.Metric {
	return []metrics.Metric{
		{Name: "runs_total", Help: "Backup runs attempted.", Kind: metrics.Counter, Value: float64(s.runsTotal.Load())},
		{Name: "failures_total", Help: "Backup runs that failed.", Kind: metrics.Counter, Value: float64(s.failures.Load())},
		{Name: "last_chunks", Help: "Chunks listed in the manifest of the most recent successful backup.", Kind: metrics.Gauge, Value: float64(s.lastChunks.Load())},
		{Name: "last_success_timestamp_seconds", Help: "Unix time the most recent successful backup finished; 0 before the first.", Kind: metrics.Gauge, Value: float64(s.lastSuccess.Load())},
		{Name: "last_duration_seconds", Help: "Duration of the most recent backup run, successful or not.", Kind: metrics.Gauge, Value: float64(s.lastDurationMs.Load()) / 1000},
		{Name: "chunks_uploaded_total", Help: "Chunks uploaded to the backup target.", Kind: metrics.Counter, Value: float64(s.uploaded.Load())},
		{Name: "chunks_skipped_total", Help: "Chunks skipped because the backup target already had them.", Kind: metrics.Counter, Value: float64(s.skipped.Load())},
		{Name: "bytes_uploaded_total", Help: "Chunk bytes uploaded to the backup target.", Kind: metrics.Counter, Value: float64(s.bytesUploaded.Load())},
		{Name: "pruned_objects_total", Help: "Manifests, snapshots and chunks deleted by backup retention.", Kind: metrics.Counter, Value: float64(s.pruned.Load())},
		{Name: "prune_failures_total", Help: "Backup retention reads or deletes that failed.", Kind: metrics.Counter, Value: float64(s.pruneFailures.Load())},
	}
}
