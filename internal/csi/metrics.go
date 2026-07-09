package csi

import (
	"github.com/hyperized/silo/internal/metrics"
	"github.com/hyperized/silo/internal/nbdclient"
)

// MetricPrefix namespaces the node plugin's readings.
func (a *NBDAttacher) MetricPrefix() string { return "silo_csi" }

// CollectMetrics reports the attachment fleet: how many volumes are attached,
// how many are currently riding out a lost connection, and how many
// reconnections have completed — the series to alert on when silod restarts
// stop being the cause (a reconnect storm without a rollout means trouble).
func (a *NBDAttacher) CollectMetrics() []metrics.Metric {
	a.mu.Lock()
	defer a.mu.Unlock()
	var reconnecting float64
	reconnects := a.reconnectsBase
	for _, s := range a.sessions {
		if s.State() == nbdclient.StateReconnecting {
			reconnecting++
		}
		reconnects += s.Reconnects()
	}
	return []metrics.Metric{
		{
			Name: "nbd_attached_volumes",
			Help: "Volumes currently attached on this node as NBD devices.",
			Kind: metrics.Gauge, Value: float64(len(a.records)),
		},
		{
			Name: "nbd_reconnecting_volumes",
			Help: "Attached volumes whose connection to silod is down; their I/O is paused while the plugin reconnects.",
			Kind: metrics.Gauge, Value: reconnecting,
		},
		{
			Name: "nbd_reconnects_total",
			Help: "Completed reconnections of attached volumes to silod since the plugin started.",
			Kind: metrics.Counter, Value: float64(reconnects),
		},
	}
}

// Compile-time check: the attacher feeds the exporter.
var _ metrics.Source = (*NBDAttacher)(nil)
