package runner

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Agent-side operational metrics (FR-25): sandbox start time and
// warm-pool behavior. Served by forge-agent's -metrics-addr listener.
// Buckets bracket NFR-1's targets: warm start under 2s p95, cold under
// 5s p95.
var (
	sandboxStartSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "forge_agent_sandbox_start_seconds",
		Help:    "Time to a started sandbox: credential injection only (warm) or create plus start (cold). NFR-1.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 1.5, 2, 3, 5, 7.5, 10, 30},
	}, []string{"mode"})

	warmPoolHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forge_agent_warm_pool_hits_total",
		Help: "Claims served from the warm pool (FR-16).",
	})

	warmPoolMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forge_agent_warm_pool_misses_total",
		Help: "Claims that fell through to a cold sandbox create (FR-16).",
	})

	warmPoolIdle = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forge_agent_warm_pool_idle",
		Help: "Warm sandboxes currently idle in the pool (FR-16).",
	})
)

func (p *Pool) setIdleGauge() {
	warmPoolIdle.Set(float64(p.idleCount()))
}

func observeSandboxStart(warm bool, d time.Duration) {
	mode := "cold"
	if warm {
		mode = "warm"
	}
	sandboxStartSeconds.WithLabelValues(mode).Observe(d.Seconds())
}
