package api

// This file substantiates FR-11's reclaim claim with a reproducible
// number and a reproducible correctness check, per fs.md's "measured
// claims" rule: a job recovers from a dead worker via visibility-timeout
// reclaim, and claiming is idempotent so a reclaimed job never runs
// twice.
//
// BenchmarkReclaimSweep measures the control plane's own processing cost
// to detect and fully requeue one job abandoned by a dead worker: the
// mark-lost-worker + XAUTOCLAIM + transition + requeue + redeliver
// sequence in sweep.go. Setup (creating the job, claiming it, aging the
// worker past LostAfter and Visibility) runs with the benchmark timer
// stopped, so only the reclaim mechanism itself is measured. The
// configured LostAfter/Visibility wait is an operator-tunable dial, not
// something the code "achieves", so it is deliberately excluded. Run:
//
//	go test ./internal/api/ -run '^$' -bench BenchmarkReclaimSweep -benchtime 300x
//
// TestReclaimIsIdempotentUnderConcurrentClaims verifies the other half of
// FR-11: once a job is reclaimed and requeued, many workers racing to
// claim it at the same instant must produce exactly one winner. Run with
// -race; this is exactly the bug class the race detector exists for:
//
//	go test ./internal/api/ -run TestReclaimIsIdempotentUnderConcurrentClaims -race -v

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/store"
	"github.com/CharlesBai-blc/forge/internal/stream"
	"github.com/alicebob/miniredis/v2"
)

// newReclaimBenchHandler builds a single-worker control plane on a fresh
// SQLite file and an in-memory Redis (miniredis). It mirrors openAPI in
// api_test.go but is typed on testing.TB so it can also drive a
// *testing.B loop, which openAPI (typed on *testing.T) cannot.
func newReclaimBenchHandler(tb testing.TB) (*store.Store, *stream.Stream, *Client, *Handler) {
	tb.Helper()
	dir := tb.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(dir, "forge.db"))
	if err != nil {
		tb.Fatalf("store.Open: %v", err)
	}
	tb.Cleanup(func() { st.Close() })

	mr := miniredis.RunT(tb)
	jobs, err := stream.Open(context.Background(), mr.Addr())
	if err != nil {
		tb.Fatalf("stream.Open: %v", err)
	}
	tb.Cleanup(func() { jobs.Close() })

	h := &Handler{
		Stream:      jobs,
		Store:       st,
		Source:      &fakeSource{},
		Image:       "alpine:3.20",
		Command:     []string{"true"},
		CPU:         2,
		MemoryBytes: 4096 << 20,
		PIDs:        4096,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		ClaimWait:   50 * time.Millisecond,
		// 1ms is Redis's own floor for XAUTOCLAIM's min-idle-time (finer
		// values are silently truncated); a short, fixed sleep in the
		// benchmark loop crosses it with wide margin. 0 would fall back
		// to the 60s production default (see visibility() in sweep.go).
		Visibility: time.Millisecond,
	}
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	tb.Cleanup(srv.Close)

	id, tok := enrollWorkerTB(tb, st)
	c := &Client{BaseURL: srv.URL, Token: tok, WorkerID: id, HTTP: srv.Client()}
	return st, jobs, c, h
}

// enrollWorkerTB is enrollWorker (api_test.go) typed on testing.TB.
func enrollWorkerTB(tb testing.TB, st *store.Store) (id, token string) {
	tb.Helper()
	ctx := context.Background()
	enroll, err := st.IssueEnrollmentToken(ctx)
	if err != nil {
		tb.Fatalf("IssueEnrollmentToken: %v", err)
	}
	id, token, err = st.Enroll(ctx, enroll, "bench", "amd64", "test")
	if err != nil {
		tb.Fatalf("Enroll: %v", err)
	}
	return id, token
}

// BenchmarkReclaimSweep measures FR-11 reclaim latency. See the file doc
// comment for exactly what is, and is not, included in the timed region.
func BenchmarkReclaimSweep(b *testing.B) {
	st, jobs, base, h := newReclaimBenchHandler(b)
	ctx := context.Background()

	samples := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		id := fmt.Sprintf("bench-reclaim-%d", i)
		if err := st.CreateJob(ctx, testJob(id, int64(i))); err != nil {
			b.Fatalf("CreateJob: %v", err)
		}
		if err := jobs.Add(ctx, id); err != nil {
			b.Fatalf("Add: %v", err)
		}

		// A fresh worker per iteration keeps each sample independent: no
		// revival bookkeeping that could leak state between iterations.
		workerID, token := enrollWorkerTB(b, st)
		worker := &Client{BaseURL: base.BaseURL, HTTP: base.HTTP, Token: token, WorkerID: workerID}
		cl, err := worker.Claim(ctx)
		if err != nil {
			b.Fatalf("Claim: %v", err)
		}
		if cl.JobID != id {
			b.Fatalf("claimed job = %s, want %s", cl.JobID, id)
		}

		// Simulate the worker crashing: no more heartbeats, so last_seen
		// falls behind LostAfter without a real-time wait for it.
		if err := st.SetLastSeen(ctx, workerID, time.Now().Add(-2*time.Minute)); err != nil {
			b.Fatalf("SetLastSeen: %v", err)
		}
		// Cross the 1ms visibility window with a 5x margin so the sweep
		// is deterministically eligible to reclaim, independent of
		// scheduler jitter on the runner. This sleep is outside the
		// timed region, so it cannot inflate the measured latency.
		time.Sleep(5 * time.Millisecond)

		b.StartTimer()
		start := time.Now()
		err = h.Sweep(ctx)
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatalf("Sweep: %v", err)
		}
		samples = append(samples, elapsed)

		row, err := st.GetJob(ctx, id)
		if err != nil {
			b.Fatalf("GetJob: %v", err)
		}
		if row.State != job.JobQueued || row.WorkerID != "" || row.Attempt != 1 {
			b.Fatalf("job %s = %+v, want requeued at attempt 1", id, row)
		}

		// A successful reclaim puts the job's stream entry back into
		// circulation (correctly: that's the point of reclaiming it).
		// Drain it with a throwaway worker before the next iteration, or
		// it would sit in the stream and get delivered out of turn to a
		// later iteration's claim.
		drainWorkerID, drainToken := enrollWorkerTB(b, st)
		drainer := &Client{BaseURL: base.BaseURL, HTTP: base.HTTP, Token: drainToken, WorkerID: drainWorkerID}
		drained, err := drainer.Claim(ctx)
		if err != nil {
			b.Fatalf("drain Claim: %v", err)
		}
		if drained.JobID != id {
			b.Fatalf("drained job = %s, want %s", drained.JobID, id)
		}
		if err := drainer.Status(ctx, id, drained.Attempt, StatusReport{State: job.JobRunning}); err != nil {
			b.Fatalf("drain running: %v", err)
		}
		if err := drainer.Status(ctx, id, drained.Attempt, StatusReport{State: job.JobSucceeded}); err != nil {
			b.Fatalf("drain succeeded: %v", err)
		}
	}

	reportLatency(b, samples)
}

// reportLatency attaches p50/p95/max as readable, human-scale custom
// benchmark metrics (milliseconds). The default ns/op is a mean and
// hides tail latency, which matters more for a reclaim SLA than an
// average does.
func reportLatency(b *testing.B, samples []time.Duration) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	pct := func(p float64) float64 {
		idx := int(p * float64(len(samples)-1))
		return samples[idx].Seconds() * 1000
	}
	b.ReportMetric(pct(0.50), "p50_ms")
	b.ReportMetric(pct(0.95), "p95_ms")
	b.ReportMetric(samples[len(samples)-1].Seconds()*1000, "max_ms")
}

// racer is one claimant in the concurrent-claim race below. Named to
// avoid clashing with Handler.worker (handler.go), which resolves the
// authenticated caller of an HTTP request and means something different.
type racer struct {
	id string
	c  *Client
}

// claim is one successful claim result, recorded so the winner of a race
// can be resolved (finished) before the next trial reuses its worker.
type claim struct {
	r       racer
	jobID   string
	attempt int
}

// TestReclaimIsIdempotentUnderConcurrentClaims verifies the other half of
// the bullet: after a job is reclaimed and requeued, many workers racing
// to claim it at the same instant must produce exactly one winner. It
// runs many trials, rotating which worker "crashes", so a fencing bug
// that only shows up occasionally under real contention is likely to
// surface instead of getting lucky once.
func TestReclaimIsIdempotentUnderConcurrentClaims(t *testing.T) {
	const racers = 8
	const trials = 50

	st, jobs, _, base, _, h := openAPI(t, nil)
	h.Visibility = time.Millisecond
	ctx := context.Background()

	pool := make([]racer, racers)
	for i := range pool {
		id, tok := enrollWorker(t, st)
		pool[i] = racer{id: id, c: &Client{BaseURL: base.BaseURL, HTTP: base.HTTP, Token: tok, WorkerID: id}}
	}

	totalClaims := 0
	for trial := 0; trial < trials; trial++ {
		jobID := fmt.Sprintf("race-%d", trial)
		putQueued(t, st, jobs, testJob(jobID, int64(trial)))

		// One worker claims it and then "crashes": no status report, no
		// more heartbeats. The sweep reclaims it back to queued.
		crasher := pool[trial%racers]
		crashedClaim, err := crasher.c.Claim(ctx)
		if err != nil {
			t.Fatalf("trial %d: setup claim: %v", trial, err)
		}
		if crashedClaim.JobID != jobID {
			t.Fatalf("trial %d: claimed %s, want %s", trial, crashedClaim.JobID, jobID)
		}
		ageWorker(t, st, crasher.id)
		time.Sleep(2 * time.Millisecond)
		if err := h.Sweep(ctx); err != nil {
			t.Fatalf("trial %d: Sweep: %v", trial, err)
		}
		// Revive the crashed worker so it can legitimately compete below
		// instead of sitting out every remaining trial.
		if err := crasher.c.Heartbeat(ctx, Heartbeat{Capacity: 1, Healthy: true}); err != nil {
			t.Fatalf("trial %d: revive heartbeat: %v", trial, err)
		}

		// Every worker, including the just-revived one, lunges for the
		// requeued job at the same instant.
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []claim
			ready   = make(chan struct{})
		)
		for _, r := range pool {
			wg.Add(1)
			go func(r racer) {
				defer wg.Done()
				<-ready
				cl, err := r.c.Claim(ctx)
				if err != nil {
					if err != ErrNoJob {
						t.Errorf("trial %d: worker %s: Claim: %v", trial, r.id, err)
					}
					return
				}
				if cl.JobID != jobID {
					// Belongs to a different trial's redelivery, not a
					// duplicate of this trial's job.
					return
				}
				mu.Lock()
				winners = append(winners, claim{r: r, jobID: cl.JobID, attempt: cl.Attempt})
				mu.Unlock()
			}(r)
		}
		close(ready)
		wg.Wait()
		totalClaims += racers

		if len(winners) != 1 {
			ids := make([]string, len(winners))
			for i, w := range winners {
				ids[i] = w.r.id
			}
			t.Fatalf("trial %d: %d workers claimed job %s (want exactly 1): %v", trial, len(winners), jobID, ids)
		}

		// Finish the job so the winner's stream backlog is clear before
		// the next trial: otherwise that worker's next Claim would
		// redeliver this leftover attempt instead of racing fairly.
		winner := winners[0]
		if err := winner.r.c.Status(ctx, winner.jobID, winner.attempt, StatusReport{State: job.JobRunning}); err != nil {
			t.Fatalf("trial %d: finish winner (running): %v", trial, err)
		}
		if err := winner.r.c.Status(ctx, winner.jobID, winner.attempt, StatusReport{State: job.JobSucceeded}); err != nil {
			t.Fatalf("trial %d: finish winner (succeeded): %v", trial, err)
		}
	}

	t.Logf("idempotent claiming: exactly one winner in all %d trials (%d concurrent claim attempts, %d racers/trial)",
		trials, totalClaims, racers)
}
