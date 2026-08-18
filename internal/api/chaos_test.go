package api

// NFR-3 chaos suite: zero lost jobs across 50 or more forced worker
// kills, and a control-plane restart never loses accepted jobs.
//
// A "kill" here is what kill -9 looks like from the control plane: the
// worker claims (and maybe starts) a job, then goes silent forever, no
// status report, no heartbeat. The suite drives the real Handler, real
// SQLite store, and real Redis stream semantics (miniredis); only the
// sandbox and GitHub source are fakes, which is exactly the boundary
// NFR-3 is about, since jobs are lost or duplicated by the control
// plane's bookkeeping, not by what runs inside the container. Runs in
// CI with -race (tdd.md §8):
//
//	go test ./internal/api/ -run TestChaos -race -v

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
	"github.com/CharlesBai-blc/forge/internal/store"
)

// killedWorker enrolls a fresh worker, claims exactly jobID, optionally
// reports running (post-acquisition), then goes silent and is aged past
// LostAfter so the next sweep sees it dead.
func killedWorker(t *testing.T, env *chaosEnv, jobID string, acquired bool) {
	t.Helper()
	id, tok := enrollWorker(t, env.st)
	c := &Client{BaseURL: env.base.BaseURL, HTTP: env.base.HTTP, Token: tok, WorkerID: id}
	cl := claimExactly(t, c, jobID)
	if acquired {
		if err := c.Status(context.Background(), jobID, cl.Attempt, StatusReport{State: job.JobRunning}); err != nil {
			t.Fatalf("running report: %v", err)
		}
	}
	ageWorker(t, env.st, id)
}

func claimExactly(t *testing.T, c *Client, jobID string) *ClaimResponse {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cl, err := c.Claim(context.Background())
		if err == ErrNoJob {
			continue
		}
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if cl.JobID != jobID {
			t.Fatalf("claimed %s, want %s", cl.JobID, jobID)
		}
		return cl
	}
	t.Fatalf("job %s never claimable", jobID)
	return nil
}

func finishJob(t *testing.T, c *Client, cl *ClaimResponse) {
	t.Helper()
	ctx := context.Background()
	if err := c.Status(ctx, cl.JobID, cl.Attempt, StatusReport{State: job.JobRunning}); err != nil {
		t.Fatalf("running: %v", err)
	}
	if err := c.Status(ctx, cl.JobID, cl.Attempt, StatusReport{State: job.JobSucceeded}); err != nil {
		t.Fatalf("succeeded: %v", err)
	}
}

func sweepNow(t *testing.T, env *chaosEnv) {
	t.Helper()
	// Cross the 1ms visibility floor with margin so the abandoned
	// entry is deterministically idle enough to reclaim.
	time.Sleep(5 * time.Millisecond)
	if err := env.h.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
}

type chaosEnv struct {
	st   *store.Store
	base *Client
	h    *Handler
}

func newChaosEnv(t *testing.T) (*chaosEnv, func(id string, ext int64) string) {
	t.Helper()
	st, jobs, _, base, _, h := openAPI(t, nil)
	h.Visibility = time.Millisecond
	env := &chaosEnv{st: st, base: base, h: h}
	put := func(id string, ext int64) string {
		putQueued(t, st, jobs, testJob(id, ext))
		return id
	}
	return env, put
}

// TestChaosWorkerKillTrials forces 50+ worker kills across three
// reclamation flavors (FR-11, FR-12, NFR-3, tdd.md §6.1):
//
//   - pre-acquisition kill: the attempt never acquired the GitHub job,
//     so it is reclaimed and completes on another worker
//   - post-acquisition kill: GitHub bound the job to the dead runner,
//     so the job fails terminally
//   - repeated pre-acquisition kills: the job dead-letters at max
//     attempts with its history intact
//
// Every accepted job must reach exactly one terminal state: none lost,
// none run twice.
func TestChaosWorkerKillTrials(t *testing.T) {
	const trials = 51 // 17 per flavor; flavor 2 kills twice: 68 forced kills total

	env, put := newChaosEnv(t)
	ctx := context.Background()

	type expectation struct {
		id           string
		state        job.JobState
		deadLettered bool
	}
	var want []expectation
	kills := 0

	for trial := 0; trial < trials; trial++ {
		id := put(fmt.Sprintf("chaos-%d", trial), int64(trial))
		switch trial % 3 {
		case 0: // pre-acquisition kill, rescued elsewhere
			killedWorker(t, env, id, false)
			kills++
			sweepNow(t, env)
			rescuerID, rescuerTok := enrollWorker(t, env.st)
			rescuer := &Client{BaseURL: env.base.BaseURL, HTTP: env.base.HTTP, Token: rescuerTok, WorkerID: rescuerID}
			cl := claimExactly(t, rescuer, id)
			if cl.Attempt != 2 {
				t.Fatalf("trial %d: rescue attempt = %d, want 2", trial, cl.Attempt)
			}
			finishJob(t, rescuer, cl)
			want = append(want, expectation{id, job.JobSucceeded, false})

		case 1: // post-acquisition kill: terminal failure, never re-run
			killedWorker(t, env, id, true)
			kills++
			sweepNow(t, env)
			want = append(want, expectation{id, job.JobFailed, false})

		case 2: // two pre-acquisition kills: dead-letter at max attempts
			killedWorker(t, env, id, false)
			kills++
			sweepNow(t, env)
			killedWorker(t, env, id, false)
			kills++
			sweepNow(t, env)
			want = append(want, expectation{id, job.JobFailed, true})
		}
	}

	if kills < 50 {
		t.Fatalf("only %d forced kills, NFR-3 requires 50+", kills)
	}

	for _, w := range want {
		got, err := env.st.GetJob(ctx, w.id)
		if err != nil {
			t.Fatalf("job %s lost: %v", w.id, err)
		}
		if got.State != w.state || got.DeadLettered != w.deadLettered {
			t.Errorf("job %s = state %s deadlettered=%v, want %s deadlettered=%v",
				w.id, got.State, got.DeadLettered, w.state, w.deadLettered)
		}
		trs, err := env.st.ListTransitions(ctx, w.id)
		if err != nil || len(trs) == 0 {
			t.Errorf("job %s has no queryable attempt history (FR-9): %v", w.id, err)
		}
	}
	t.Logf("chaos: %d jobs, %d forced worker kills, zero lost", trials, kills)
}

// TestChaosControlPlaneRestart restarts the control plane at the
// critical-path steps and verifies no accepted job is lost (NFR-3,
// tdd.md §6.2): a crash between the SQLite insert and the stream XADD
// is repaired by the startup reconciler, and a restart that wipes the
// in-memory ack map still completes and acks in-flight jobs.
func TestChaosControlPlaneRestart(t *testing.T) {
	st, jobs, _, base, src, h := openAPI(t, nil)
	h.Visibility = time.Millisecond
	ctx := context.Background()

	// Crash between insert and XADD: the job exists only in SQLite.
	orphan := testJob("restart-orphan", 1)
	if err := st.CreateJob(ctx, orphan); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// A second job is claimed and in flight when the restart happens.
	inflight := testJob("restart-inflight", 2)
	putQueued(t, st, jobs, inflight)
	cl := claimExactly(t, base, inflight.ID)

	// "Restart": the startup reconciler re-enqueues queued jobs missing
	// from the stream (main.go newApp), and a fresh Handler loses the
	// in-memory jobID->msgID ack map.
	queued, err := st.QueuedIDs(ctx)
	if err != nil {
		t.Fatalf("QueuedIDs: %v", err)
	}
	if err := jobs.Reconcile(ctx, queued); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	h2 := &Handler{
		Stream:     jobs,
		Store:      st,
		Source:     src,
		Image:      h.Image,
		Command:    h.Command,
		LogDir:     h.LogDir,
		Log:        h.Log,
		ClaimWait:  h.ClaimWait,
		Visibility: h.Visibility,
	}
	base2, worker2 := restartServer(t, h2, st, base)

	// The in-flight job's worker survived the restart and finishes.
	finishJob(t, base2, cl)
	got, err := st.GetJob(ctx, inflight.ID)
	if err != nil || got.State != job.JobSucceeded {
		t.Fatalf("in-flight job after restart = %+v err=%v, want succeeded", got, err)
	}

	// The orphaned job became claimable after the reconciler ran.
	ocl := claimExactly(t, worker2, orphan.ID)
	finishJob(t, worker2, ocl)
	got, err = st.GetJob(ctx, orphan.ID)
	if err != nil || got.State != job.JobSucceeded {
		t.Fatalf("orphaned job after restart = %+v err=%v, want succeeded", got, err)
	}

	// Nothing left pending in the stream: the restarted handler acked
	// the in-flight entry via the PEL scan, not the lost map.
	pending, err := jobs.PendingAll(ctx)
	if err != nil {
		t.Fatalf("PendingAll: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("stream entries still pending after completion: %v", pending)
	}
}

// restartServer serves h2 on a fresh listener, simulating the restarted
// process. It returns the pre-restart worker's client pointed at the
// new server, plus a freshly enrolled worker.
func restartServer(t *testing.T, h2 *Handler, st *store.Store, old *Client) (survivor, fresh *Client) {
	t.Helper()
	mux := http.NewServeMux()
	h2.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	survivor = &Client{BaseURL: srv.URL, HTTP: srv.Client(), Token: old.Token, WorkerID: old.WorkerID}
	id, tok := enrollWorker(t, st)
	fresh = &Client{BaseURL: srv.URL, HTTP: srv.Client(), Token: tok, WorkerID: id}
	return survivor, fresh
}
