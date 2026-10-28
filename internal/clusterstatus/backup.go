package clusterstatus

import (
	statusv1 "github.com/hyperized/silo/api/proto/silo/status/v1"
	"github.com/hyperized/silo/internal/backup"
)

// BackupReporter is the slice of the backup subsystem the builder reads.
// *backup.Subsystem satisfies it.
type BackupReporter interface {
	Status() backup.Status
}

// WithBackup adds the node's backup state to the snapshot. Without it, or with
// a nil reporter, the snapshot says configured=false, which is what a node
// without SILO_BACKUP_TARGET is.
func WithBackup(r BackupReporter) Option {
	return func(b *Builder) { b.backup = r }
}

// backupStatus always returns a message, never nil, so clients can read
// configured=false instead of having to treat a missing field as "unknown".
func backupStatus(r BackupReporter) *statusv1.BackupStatus {
	if r == nil {
		return &statusv1.BackupStatus{}
	}
	st := r.Status()
	out := &statusv1.BackupStatus{
		Configured:      st.Configured,
		Target:          st.Target,
		IntervalSeconds: int64(st.Interval.Seconds()),
		Running:         st.Running,
		LastRun:         BackupRun(st.LastRun),
	}
	// time.Time{}.Unix() is a large negative number, not 0; the proto
	// contract is 0 for "nothing scheduled".
	if !st.NextRun.IsZero() {
		out.NextRunUnix = st.NextRun.Unix()
	}
	return out
}

// BackupRun converts one run outcome to its wire form; nil (no run yet) stays
// nil. The node-admin RunBackup RPC uses it too, so a triggered run and the
// status page describe the same run the same way.
func BackupRun(r *backup.Run) *statusv1.BackupRun {
	if r == nil {
		return nil
	}
	return &statusv1.BackupRun{
		StartedUnix:    r.StartedAt.Unix(),
		FinishedUnix:   r.FinishedAt.Unix(),
		Ok:             r.OK,
		Error:          r.Error,
		ChunksUploaded: r.ChunksUploaded,
		ChunksSkipped:  r.ChunksSkipped,
		BytesUploaded:  r.BytesUploaded,
		Manifest:       r.Manifest,
	}
}
