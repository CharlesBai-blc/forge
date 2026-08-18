// Command soak is the NFR-4 harness: simulated workers drive sustained
// concurrent load through a real control plane (store, stream, claim,
// sweep) and the run fails on any lost, failed, or duplicated job.
//
// The acceptance run is 24h with 5 simulated machines x 10 agent slots
// (50 concurrent jobs); CI runs a short smoke with the same code path:
//
//	go run ./bench/soak -duration 30s
//	go run ./bench/soak -duration 24h -redis 127.0.0.1:6379
//
// An agent process runs one job at a time by design (tdd.md §4.6: a
// worker consumer re-reading its pending entries signals a restart),
// so a machine with N slots runs N agent processes. The harness
// mirrors that: each slot enrolls and claims as its own worker.
//
// Sandboxes are replaced by a timed sleep: NFR-4 targets scheduler and
// state-machine endurance, not Docker (that is FR-17/NFR-1 territory).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"net/http"

	"github.com/CharlesBai-blc/forge/internal/api"
	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/source"
	"github.com/CharlesBai-blc/forge/internal/store"
	"github.com/CharlesBai-blc/forge/internal/stream"
	"github.com/alicebob/miniredis/v2"
)

func main() {
	var (
		redisAddr    = flag.String("redis", "mini", `redis address, or "mini" for embedded miniredis`)
		duration     = flag.Duration("duration", 2*time.Minute, "load duration (24h for the NFR-4 acceptance run)")
		workers      = flag.Int("workers", 5, "simulated machines")
		capacity     = flag.Int("capacity", 10, "agent slots per machine")
		jobDuration  = flag.Duration("job", 2*time.Second, "simulated job runtime")
		minCompleted = flag.Int("min-completed", 1000, "minimum successful jobs required")
	)
	flag.Parse()
	if *duration <= 0 || *workers <= 0 || *capacity <= 0 || *jobDuration <= 0 || *minCompleted <= 0 {
		fmt.Fprintln(os.Stderr, "soak: duration, workers, capacity, job, and min-completed must be positive")
		os.Exit(2)
	}
	if err := run(*redisAddr, *duration, *workers, *capacity, *jobDuration, *minCompleted); err != nil {
		fmt.Fprintln(os.Stderr, "soak: FAIL:", err)
		os.Exit(1)
	}
}

// soakSource satisfies RunnerSource without GitHub: acquisition is
// simulated by the workers themselves.
type soakSource struct {
	mu   sync.Mutex
	next int64
}

func (s *soakSource) VerifyAndParse(*http.Request) ([]source.JobEvent, error) { return nil, nil }
func (s *soakSource) ListQueued(context.Context) ([]source.JobEvent, error)   { return nil, nil }
func (s *soakSource) Unregister(context.Context, int64) error                 { return nil }
func (s *soakSource) RegisterJIT(context.Context, *job.Job) (*source.JITConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	return &source.JITConfig{RunnerID: s.next, Encoded: "soak-jit"}, nil
}

type results struct {
	mu        sync.Mutex
	created   map[string]time.Time
	started   map[string]int // jobID -> times a worker ran it (duplicate detector)
	succeeded int
	latencies []time.Duration
}

func run(redisAddr string, duration time.Duration, workers, capacity int, jobDur time.Duration, minCompleted int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	if redisAddr == "mini" {
		mr, err := miniredis.Run()
		if err != nil {
			return err
		}
		defer mr.Close()
		redisAddr = mr.Addr()
	}

	dir, err := os.MkdirTemp("", "forge-soak")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	st, err := store.Open(ctx, filepath.Join(dir, "forge.db"))
	if err != nil {
		return err
	}
	defer st.Close()
	jobs, err := stream.Open(ctx, redisAddr)
	if err != nil {
		return err
	}
	defer jobs.Close()

	h := &api.Handler{
		Store:      st,
		Stream:     jobs,
		Source:     &soakSource{},
		Image:      "soak",
		Command:    []string{"true"},
		Log:        log,
		ClaimWait:  2 * time.Second,
		LostAfter:  15 * time.Second,
		Visibility: 30 * time.Second,
		SweepEvery: 2 * time.Second,
	}
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	go h.RunSweep(ctx)

	res := &results{created: map[string]time.Time{}, started: map[string]int{}}
	concurrency := workers * capacity

	// Generator: keep the queue topped up to the concurrency target so
	// every slot always has work.
	var wg sync.WaitGroup
	loadCtx, stopLoad := context.WithTimeout(ctx, duration)
	defer stopLoad()
	wg.Add(1)
	go func() {
		defer wg.Done()
		seq := int64(0)
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-loadCtx.Done():
				return
			case <-t.C:
			}
			depth, err := st.CountQueued(ctx)
			if err != nil {
				continue
			}
			for i := depth; i < concurrency; i++ {
				seq++
				j := &job.Job{
					ID:         fmt.Sprintf("soak-%d", seq),
					Source:     "soak",
					ExternalID: seq,
					Repo:       "forge/soak",
					RunID:      seq,
					State:      job.JobQueued,
				}
				if err := st.CreateJob(ctx, j); err != nil {
					log.Error("create", "err", err)
					continue
				}
				res.mu.Lock()
				res.created[j.ID] = time.Now()
				res.mu.Unlock()
				if err := jobs.Add(ctx, j.ID); err != nil {
					log.Error("stream add", "err", err)
				}
			}
		}
	}()

	// Each slot is one agent: enroll, heartbeat, and a sequential
	// claim-run-report loop, exactly like internal/runner.Run.
	for m := 0; m < workers; m++ {
		for s := 0; s < capacity; s++ {
			tok, err := st.IssueEnrollmentToken(ctx)
			if err != nil {
				return err
			}
			c := &api.Client{BaseURL: srv.URL, HTTP: srv.Client()}
			enr, err := c.Enroll(ctx, tok, fmt.Sprintf("soak-m%d-s%d", m, s), "arm64", "soak")
			if err != nil {
				return err
			}
			c.WorkerID, c.Token = enr.WorkerID, enr.Token
			var current sync.Map

			// Heartbeat loop (FR-18/FR-20).
			wg.Add(1)
			go func() {
				defer wg.Done()
				t := time.NewTicker(3 * time.Second)
				defer t.Stop()
				for {
					var ids []string
					current.Range(func(k, _ any) bool { ids = append(ids, k.(string)); return true })
					if err := c.Heartbeat(ctx, api.Heartbeat{Capacity: 1, Running: ids, Healthy: true}); err != nil && ctx.Err() == nil {
						log.Error("heartbeat", "err", err)
					}
					select {
					case <-loadCtx.Done():
						return
					case <-t.C:
					}
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				for loadCtx.Err() == nil {
					cl, err := c.Claim(loadCtx)
					if err != nil {
						continue // ErrNoJob or shutdown
					}
					res.mu.Lock()
					res.started[cl.JobID]++
					created := res.created[cl.JobID]
					res.mu.Unlock()
					current.Store(cl.JobID, true)
					if err := c.Status(ctx, cl.JobID, cl.Attempt, api.StatusReport{State: job.JobRunning}); err != nil {
						log.Error("status running", "job", cl.JobID, "err", err)
						current.Delete(cl.JobID)
						continue
					}
					if !created.IsZero() {
						res.mu.Lock()
						res.latencies = append(res.latencies, time.Since(created))
						res.mu.Unlock()
					}
					// Simulated job: jittered sleep around -job.
					time.Sleep(jobDur/2 + time.Duration(rand.Int63n(int64(jobDur))))
					if err := c.Status(ctx, cl.JobID, cl.Attempt, api.StatusReport{State: job.JobSucceeded}); err != nil {
						log.Error("status succeeded", "job", cl.JobID, "err", err)
						current.Delete(cl.JobID)
						continue
					}
					current.Delete(cl.JobID)
					res.mu.Lock()
					res.succeeded++
					res.mu.Unlock()
				}
			}()
		}
	}

	fmt.Printf("soak: %d machines x %d slots (%d agents), job %s, duration %s\n",
		workers, capacity, concurrency, jobDur, duration)

	wg.Wait()
	// Let the last in-flight status posts settle, then verify.
	time.Sleep(2 * jobDur)
	return report(ctx, st, res, minCompleted)
}

// report checks NFR-4 invariants against the store, which is the
// source of truth: no failed or dead-lettered jobs, no duplicate runs,
// and every job that started reached succeeded before the report.
func report(ctx context.Context, st *store.Store, res *results, minCompleted int) error {
	res.mu.Lock()
	defer res.mu.Unlock()

	all, err := st.ListJobs(ctx, len(res.created)+1)
	if err != nil {
		return err
	}
	counts := map[job.JobState]int{}
	var bad []string
	for _, j := range all {
		counts[j.State]++
		if j.State == job.JobFailed || j.DeadLettered {
			bad = append(bad, fmt.Sprintf("%s: %s %s", j.ID, j.State, j.Reason))
		}
		if res.started[j.ID] > 0 && j.State != job.JobSucceeded {
			bad = append(bad, fmt.Sprintf("%s: started but ended in %s", j.ID, j.State))
		}
	}
	dupes := 0
	for _, n := range res.started {
		if n > 1 {
			dupes++
		}
	}

	fmt.Printf("soak: created %d, succeeded %d, in flight at cutoff %d\n",
		len(res.created), counts[job.JobSucceeded],
		counts[job.JobQueued]+counts[job.JobAssigned]+counts[job.JobRunning])
	if len(res.latencies) > 0 {
		sort.Slice(res.latencies, func(i, k int) bool { return res.latencies[i] < res.latencies[k] })
		p := func(q float64) time.Duration {
			return res.latencies[int(float64(len(res.latencies)-1)*q)]
		}
		fmt.Printf("soak: queued-to-running p50 %s p95 %s (n=%d)\n",
			p(0.50).Round(time.Millisecond), p(0.95).Round(time.Millisecond), len(res.latencies))
	}

	if len(bad) > 0 {
		return fmt.Errorf("%d failed/dead-lettered jobs:\n%s", len(bad), strings.Join(bad, "\n"))
	}
	if dupes > 0 {
		return fmt.Errorf("%d jobs ran more than once", dupes)
	}
	if counts[job.JobSucceeded] < minCompleted {
		return fmt.Errorf("%d jobs completed, want at least %d", counts[job.JobSucceeded], minCompleted)
	}
	fmt.Println("soak: PASS")
	return nil
}
