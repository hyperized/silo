// Package diskusage reports a filesystem's capacity accounting. silod uses it
// for the data directory backing the chunk store — both in the operator status
// view and in the Prometheus capacity gauges.
package diskusage

// Usage is a filesystem's capacity accounting, in bytes. It reports the whole
// filesystem's figures (not just silo's chunk files), which is what an operator
// planning capacity on a dedicated data volume wants.
type Usage struct {
	CapacityBytes  int64
	UsedBytes      int64
	AvailableBytes int64
}
