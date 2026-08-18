package burst

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Burst metrics (FR-23, FR-25), served by the control plane's /metrics
// alongside the internal/api collectors.
var (
	instancesGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forge_burst_instances",
		Help: "Desired burst instance count (FR-21).",
	})

	capHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forge_burst_cap_hits_total",
		Help: "Scale-ups blocked by the max-instances or daily-hours cap (FR-23).",
	})

	applyFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forge_burst_apply_failures_total",
		Help: "Failed terraform applies; retried on the next window (tdd.md §6.7).",
	})
)
