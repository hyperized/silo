// Package metrics is the contract between silo's instrumented components and
// the exporter that renders them. A component implements Source to expose its
// readings under a namespace it owns; the exporter — the only package that
// knows the Prometheus wire format — renders them. Keeping this contract free
// of the http-serving exporter lets domain packages declare metrics without
// depending on it.
package metrics

import (
	"math"
	"sort"
	"sync/atomic"
)

// Kind is the Prometheus metric type the exporter knows how to render.
type Kind int

const (
	// Gauge is a value that can rise and fall.
	Gauge Kind = iota
	// Counter is a value that only increases.
	Counter
	// Histogram is a distribution: the Metric carries cumulative Buckets plus
	// the observation Sum and Count.
	Histogram
)

// Bucket is one cumulative histogram bucket: the count of observations less than
// or equal to LE (use math.Inf(1) for the catch-all bucket).
type Bucket struct {
	LE    float64
	Count uint64
}

// Metric is a single reading. Name is unprefixed (e.g. "peer_clock_skew_seconds");
// the exporter joins it to the Source's prefix. Labels are optional. For a
// Histogram, Value is unused and Buckets/Sum/Count carry the distribution.
type Metric struct {
	Name    string
	Help    string
	Kind    Kind
	Value   float64
	Labels  [][2]string
	Buckets []Bucket
	Sum     float64
	Count   uint64
}

// Hist is a concurrency-safe latency/size histogram with fixed bucket bounds.
// Observe records a value; Collect snapshots it into a Histogram Metric. It is
// the recorder a Source embeds for distribution metrics.
type Hist struct {
	bounds  []float64       // sorted explicit upper bounds (no +Inf)
	counts  []atomic.Uint64 // per-bucket (non-cumulative) counts
	total   atomic.Uint64
	sumBits atomic.Uint64 // float64 bits of the running sum
}

// NewHist builds a histogram over the given bucket upper bounds (any order; a
// +Inf catch-all is always implied). With no bounds it records only sum/count.
func NewHist(bounds ...float64) *Hist {
	b := append([]float64(nil), bounds...)
	sort.Float64s(b)
	return &Hist{bounds: b, counts: make([]atomic.Uint64, len(b))}
}

// Observe records one value.
func (h *Hist) Observe(v float64) {
	h.total.Add(1)
	for {
		old := h.sumBits.Load()
		nv := math.Float64bits(math.Float64frombits(old) + v)
		if h.sumBits.CompareAndSwap(old, nv) {
			break
		}
	}
	// Increment the first bucket whose bound is >= v; values past the last
	// explicit bound live only in the implied +Inf bucket (total).
	i := sort.Search(len(h.bounds), func(i int) bool { return h.bounds[i] >= v })
	if i < len(h.bounds) {
		h.counts[i].Add(1)
	}
}

// Collect snapshots the histogram into a Metric under name with optional labels.
func (h *Hist) Collect(name, help string, labels [][2]string) Metric {
	buckets := make([]Bucket, 0, len(h.bounds)+1)
	var cumulative uint64
	for i, bound := range h.bounds {
		cumulative += h.counts[i].Load()
		buckets = append(buckets, Bucket{LE: bound, Count: cumulative})
	}
	total := h.total.Load()
	buckets = append(buckets, Bucket{LE: math.Inf(1), Count: total})
	return Metric{
		Name:    name,
		Help:    help,
		Kind:    Histogram,
		Labels:  labels,
		Buckets: buckets,
		Sum:     math.Float64frombits(h.sumBits.Load()),
		Count:   total,
	}
}

// Source contributes metrics under a namespace it owns. CollectMetrics is
// called on every scrape, so values are always current; implementations must
// be safe for concurrent use.
type Source interface {
	MetricPrefix() string
	CollectMetrics() []Metric
}

// Static returns a Source that always reports the given constant metrics under
// prefix — the shape build_info and other fixed-at-boot readings take.
func Static(prefix string, ms ...Metric) Source {
	return staticSource{prefix: prefix, metrics: ms}
}

type staticSource struct {
	prefix  string
	metrics []Metric
}

func (s staticSource) MetricPrefix() string     { return s.prefix }
func (s staticSource) CollectMetrics() []Metric { return s.metrics }
