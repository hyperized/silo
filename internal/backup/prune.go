package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hyperized/silo/internal/blobstore"
)

// MinPruneGrace is the shortest age an unreferenced object must reach before
// prune deletes it, whatever the backup interval.
const MinPruneGrace = 24 * time.Hour

// DefaultRetainFor is how long a manifest is kept regardless of the retention
// count.
const DefaultRetainFor = 7 * 24 * time.Hour

// staleRunAfter is how long a run marker may go without a heartbeat before it
// is treated as the leftover of a crashed run. Markers are refreshed every five
// minutes; an hour tolerates slow uploads, a stalled heartbeat or two, and
// clock differences between the node and the storage service, whose
// modification times decide freshness.
const staleRunAfter = time.Hour

// ErrListingIncomplete means the target's listing did not yet show the
// manifest the run just wrote, so it cannot be trusted for deletions.
var ErrListingIncomplete = errors.New("backup: the target's listing is incomplete")

// PruneGrace returns the grace window for a backup interval: twice the
// interval, but at least MinPruneGrace.
//
// The window protects objects whose referencing manifest has not been written
// yet: namespace and extent objects of a run in progress, and chunks uploaded
// by a node whose run marker has not appeared in the listing yet. Running
// exports are also protected directly by their run markers (see Prune).
func PruneGrace(interval time.Duration) time.Duration {
	return max(2*interval, MinPruneGrace)
}

// PruneOptions controls one prune pass.
type PruneOptions struct {
	// NodeID is the node whose manifests are thinned out. Other nodes'
	// manifests are only read, never deleted.
	NodeID string
	// CurrentStamp is the stamp of the manifest the caller just wrote. When
	// set and the listing does not show it, the pass is abandoned: a listing
	// that lags behind writes might also hide manifests that keep chunks
	// alive.
	CurrentStamp string
	// Retain is how many timestamped manifests of NodeID to keep. It must be
	// at least one; disabling pruning is the caller's decision.
	Retain int
	// RetainFor protects every manifest younger than this, whatever Retain
	// says, so a burst of runs cannot push all good restore points out.
	// Zero disables the floor.
	RetainFor time.Duration
	// Grace is the minimum age of an unreferenced object before it is
	// deleted (see PruneGrace).
	Grace time.Duration
	// Now is the reference time for Grace, RetainFor and run markers.
	Now time.Time
	// Concurrency bounds parallel deletes; values below one use
	// DefaultConcurrency.
	Concurrency int
}

// PruneResult counts what a prune pass did. Failures are deletes or reads that
// went wrong; they never stop the rest of the pass except where continuing
// could delete data that is still needed.
type PruneResult struct {
	ManifestsDeleted int
	ObjectsDeleted   int
	Failures         int
	// FirstError is the first failure, kept so the caller can log a cause
	// without the pass logging every object.
	FirstError error
	// ChunksHeldBack explains why chunk deletion was skipped this pass, or
	// is empty when it ran.
	ChunksHeldBack string
}

func (r *PruneResult) fail(err error) {
	r.Failures++
	if r.FirstError == nil {
		r.FirstError = err
	}
}

// Prune applies retention after a successful scheduled run:
//
//  1. delete NodeID's timestamped manifests beyond the newest Retain that are
//     also older than RetainFor (latest.json is never deleted);
//  2. delete NodeID's namespace and extent objects whose run no longer has a
//     manifest and that are older than Grace;
//  3. delete chunks that no manifest of any node references and that are
//     older than Grace, unless another node has a run in progress.
//
// Step 3 is skipped if any manifest cannot be read, because a chunk referenced
// only by that manifest would look unreferenced. It is also skipped while any
// other node's run marker is fresh: that run may have seen a chunk, decided
// not to upload it, and not yet written the manifest that references it. An
// object with no modification time is treated as new.
//
// The returned error is set only when the pass did not run (bad options, the
// manifest listing failed, or the listing lags behind CurrentStamp);
// per-object problems are counted in Failures.
func Prune(ctx context.Context, target blobstore.Target, opts PruneOptions) (PruneResult, error) {
	if err := ValidateNodeID(opts.NodeID); err != nil {
		return PruneResult{}, err
	}
	if opts.Retain < 1 {
		return PruneResult{}, fmt.Errorf("backup: prune needs to keep at least one manifest (got %d); set SILO_BACKUP_RETAIN to 1 or more, or 0 to switch pruning off", opts.Retain)
	}
	if opts.Concurrency < 1 {
		opts.Concurrency = DefaultConcurrency
	}
	cutoff := opts.Now.Add(-opts.Grace)
	var res PruneResult

	retained, err := pruneManifests(ctx, target, opts, &res)
	if err != nil {
		return res, err
	}
	pruneRunObjects(ctx, target, opts, retained, cutoff, &res)
	pruneChunks(ctx, target, opts, cutoff, &res)
	return res, nil
}

// pruneManifests deletes the node's old manifests and returns the run stamps
// that keep a manifest.
func pruneManifests(ctx context.Context, target blobstore.Target, opts PruneOptions, res *PruneResult) (map[string]bool, error) {
	objs, err := target.List(ctx, manifestsPrefix+opts.NodeID+"/")
	if err != nil {
		return nil, fmt.Errorf("backup: could not list this node's manifests on %s for pruning (%w)", target.Name(), err)
	}
	var stamps []string
	for _, o := range objs {
		node, name, ok := parseManifestKey(o.Name)
		if ok && node == opts.NodeID && name != LatestManifest {
			stamps = append(stamps, name)
		}
	}
	retained := map[string]bool{}
	for _, s := range stamps {
		retained[s] = true
	}
	if opts.CurrentStamp != "" && !retained[opts.CurrentStamp] {
		return nil, fmt.Errorf("%w: %s does not list the manifest this run just wrote (%s); retention is skipped this round and runs again after the next backup", ErrListingIncomplete, target.Name(), opts.CurrentStamp)
	}
	// List returns names sorted, and stamps sort in time order.
	cut := max(len(stamps)-opts.Retain, 0)
	for _, s := range stamps[:cut] {
		if !manifestExpired(s, opts) {
			continue
		}
		if err := target.Delete(ctx, ManifestKey(opts.NodeID, s)); err != nil {
			// Still a restore point; keep its objects.
			res.fail(err)
			continue
		}
		delete(retained, s)
		res.ManifestsDeleted++
	}
	return retained, nil
}

// manifestExpired reports whether a manifest beyond the retention count is
// also past the RetainFor floor.
func manifestExpired(stamp string, opts PruneOptions) bool {
	if opts.RetainFor <= 0 {
		return true
	}
	t, ok := stampTime(stamp)
	return ok && t.Before(opts.Now.Add(-opts.RetainFor))
}

// oldEnough reports whether an object may be deleted: its modification time
// is known and before the cutoff.
func oldEnough(o blobstore.ObjectInfo, cutoff time.Time) bool {
	return !o.ModTime.IsZero() && o.ModTime.Before(cutoff)
}

// pruneRunObjects removes namespace snapshots and extent maps left behind by
// pruned runs (or by runs that failed before writing a manifest).
func pruneRunObjects(ctx context.Context, target blobstore.Target, opts PruneOptions, retained map[string]bool, cutoff time.Time, res *PruneResult) {
	var doomed []string
	for _, dir := range []string{namespaceDir, extentsDir} {
		prefix := dir + "/" + opts.NodeID + "/"
		objs, err := target.List(ctx, prefix)
		if err != nil {
			res.fail(err)
			continue
		}
		for _, o := range objs {
			stamp := runStamp(strings.TrimPrefix(o.Name, prefix))
			if stamp == "" || retained[stamp] || !oldEnough(o, cutoff) {
				continue
			}
			doomed = append(doomed, o.Name)
		}
	}
	deleteAll(ctx, target, opts.Concurrency, doomed, res)
}

// runStamp extracts the run stamp from "<stamp>.json" or "<stamp>/<file>", or
// returns "" for names this package did not write.
func runStamp(rest string) string {
	if isStampJSON(rest) {
		return strings.TrimSuffix(rest, ".json")
	}
	if stamp, _, ok := strings.Cut(rest, "/"); ok && stampPattern.MatchString(stamp) {
		return stamp
	}
	return ""
}

// pruneChunks deletes chunks that no manifest references, once they are older
// than the cutoff and no other node is mid-run.
func pruneChunks(ctx context.Context, target blobstore.Target, opts PruneOptions, cutoff time.Time, res *PruneResult) {
	live, err := referencedChunks(ctx, target)
	if err != nil {
		res.fail(err)
		res.ChunksHeldBack = err.Error()
		return
	}
	objs, err := target.List(ctx, chunksPrefix)
	if err != nil {
		res.fail(err)
		res.ChunksHeldBack = err.Error()
		return
	}
	var doomed []string
	for _, o := range objs {
		id, ok := chunkIDFromKey(o.Name)
		if !ok || live[id] || !oldEnough(o, cutoff) {
			continue
		}
		doomed = append(doomed, o.Name)
	}
	if len(doomed) == 0 {
		return
	}
	// Checked last, right before deleting, to keep the window in which a
	// run can start unseen as short as possible. A run that starts inside
	// it anyway re-lists the chunks before writing its manifest and fails
	// rather than reference a deleted one.
	if busy := activeRuns(ctx, target, opts, res); busy != "" {
		res.ChunksHeldBack = busy
		return
	}
	deleteAll(ctx, target, opts.Concurrency, doomed, res)
}

// activeRuns returns a reason to hold back chunk deletion when another node
// has a fresh run marker (or the markers cannot be listed), and "" otherwise.
// Stale markers of other nodes belong to crashed runs and are deleted.
func activeRuns(ctx context.Context, target blobstore.Target, opts PruneOptions, res *PruneResult) string {
	objs, err := target.List(ctx, runsPrefix)
	if err != nil {
		res.fail(err)
		return fmt.Sprintf("could not list run markers (%v)", err)
	}
	var stale []string
	busy := ""
	for _, o := range objs {
		node, _, ok := strings.Cut(strings.TrimPrefix(o.Name, runsPrefix), "/")
		if !ok || node == opts.NodeID {
			continue
		}
		if !oldEnough(o, opts.Now.Add(-staleRunAfter)) {
			if busy == "" {
				busy = fmt.Sprintf("node %s has a backup in progress (%s)", node, o.Name)
			}
			continue
		}
		stale = append(stale, o.Name)
	}
	deleteAll(ctx, target, opts.Concurrency, stale, res)
	return busy
}

// referencedChunks is the union of the chunk lists of every manifest under the
// prefix, latest.json copies included. It fails when any manifest could not be
// read or decoded, or when there are none at all: in both cases the set is not
// trustworthy enough to delete against.
func referencedChunks(ctx context.Context, target blobstore.Target) (map[string]bool, error) {
	objs, err := target.List(ctx, manifestsPrefix)
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	seen := 0
	for _, o := range objs {
		if _, _, ok := parseManifestKey(o.Name); !ok {
			continue
		}
		raw, err := target.Get(ctx, o.Name, MaxManifestBytes)
		if err != nil {
			return nil, fmt.Errorf("backup: skipped deleting chunks because %s could not be read (%w)", o.Name, err)
		}
		// Only the chunk ids matter here; decoding into a narrow struct
		// keeps the rest of each manifest out of memory.
		var m struct {
			Chunks []struct {
				ID string `json:"id"`
			} `json:"chunks"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("backup: skipped deleting chunks because %s is damaged (%w); delete or repair that manifest", o.Name, err)
		}
		for _, c := range m.Chunks {
			live[c.ID] = true
		}
		seen++
	}
	if seen == 0 {
		return nil, fmt.Errorf("backup: skipped deleting chunks because no manifest was found on %s", target.Name())
	}
	return live, nil
}

// deleteAll deletes names in parallel, counting successes and failures. A
// failed delete does not stop the others.
func deleteAll(ctx context.Context, target blobstore.Target, n int, names []string, res *PruneResult) {
	var (
		mu      sync.Mutex
		deleted int
	)
	_ = forEach(ctx, n, names, func(ctx context.Context, name string) error {
		err := target.Delete(ctx, name)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			res.fail(err)
			return nil
		}
		deleted++
		return nil
	})
	res.ObjectsDeleted += deleted
}
