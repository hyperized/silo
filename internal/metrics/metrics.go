// Package metrics is the contract between silo's instrumented components and
// the exporter that renders them. A component implements Source to expose its
// readings under a namespace it owns; the exporter — the only package that
// knows the Prometheus wire format — renders them. Keeping this contract free
// of the http-serving exporter lets domain packages declare metrics without
// depending on it.
package metrics

// Kind is the Prometheus metric type the exporter knows how to render.
type Kind int

const (
	// Gauge is a value that can rise and fall.
	Gauge Kind = iota
	// Counter is a value that only increases.
	Counter
)

// Metric is a single reading. Name is unprefixed (e.g. "peer_clock_skew_seconds");
// the exporter joins it to the Source's prefix. Labels are optional.
type Metric struct {
	Name   string
	Help   string
	Kind   Kind
	Value  float64
	Labels [][2]string
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
