package api

import (
	"context"
	"math"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Control-plane operational metrics (FR-25), served at /metrics on the
// main listener. The NFR-1 benchmark reads queued-to-running latency
// from here (bench/, tdd.md §8).
var jobLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "forge_job_latency_seconds",
	Help:    "Job latency from creation: queued_to_running (NFR-1) and total (creation to terminal state).",
	Buckets: []float64{0.25, 0.5, 1, 2, 3, 5, 10, 30, 60, 120, 300, 600},
}, []string{"phase"})

// metricsHandler combines the package-level collectors with a
// per-handler queue depth gauge, which needs this handler's Store.
func (h *Handler) metricsHandler() http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "forge_queue_depth",
		Help: "Jobs currently in state queued (FR-25).",
	}, func() float64 {
		n, err := h.Store.CountQueued(context.Background())
		if err != nil {
			return math.NaN()
		}
		return float64(n)
	}))
	return promhttp.HandlerFor(
		prometheus.Gatherers{prometheus.DefaultGatherer, reg},
		promhttp.HandlerOpts{},
	)
}
