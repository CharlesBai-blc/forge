package store

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
)

func TestEnrollCreatesActiveWorker(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	tok, err := s.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatalf("IssueEnrollmentToken: %v", err)
	}
	id, machine, err := s.Enroll(ctx, tok, "host-a", "arm64", "test")
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	if id == "" || machine == "" {
		t.Fatal("empty worker id or token")
	}
	w, err := s.GetWorker(ctx, id)
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if w.Name != "host-a" || w.Arch != "arm64" || w.State != job.WorkerActive || w.Capacity != 1 {
		t.Fatalf("worker = %+v", w)
	}
	got, err := s.WorkerByToken(ctx, machine)
	if err != nil {
		t.Fatalf("WorkerByToken: %v", err)
	}
	if got.ID != id {
		t.Fatalf("WorkerByToken id = %s, want %s", got.ID, id)
	}
}

func TestEnrollSingleUse(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	tok, err := s.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Enroll(ctx, tok, "a", "amd64", "test"); err != nil {
		t.Fatalf("first Enroll: %v", err)
	}
	_, _, err = s.Enroll(ctx, tok, "b", "amd64", "test")
	if err != ErrEnrollmentUsed {
		t.Fatalf("second Enroll = %v, want ErrEnrollmentUsed", err)
	}
}

func TestEnrollExpired(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	tok, err := s.issueEnrollmentToken(ctx, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.Enroll(ctx, tok, "a", "amd64", "test")
	if err != ErrEnrollmentExpired {
		t.Fatalf("Enroll = %v, want ErrEnrollmentExpired", err)
	}
}

func TestEnrollInvalid(t *testing.T) {
	s := openTestStore(t)
	_, _, err := s.Enroll(context.Background(), "no-such-token", "a", "amd64", "test")
	if err != ErrEnrollmentInvalid {
		t.Fatalf("Enroll = %v, want ErrEnrollmentInvalid", err)
	}
}

func TestWorkerByTokenUnknown(t *testing.T) {
	s := openTestStore(t)
	_, err := s.WorkerByToken(context.Background(), "nope")
	if err != ErrWorkerNotFound {
		t.Fatalf("err = %v, want ErrWorkerNotFound", err)
	}
}

func TestEnrollmentAndMachineTokensNotPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forge.db")
	ctx := context.Background()
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	tok, err := s.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, machine, err := s.Enroll(ctx, tok, "a", "amd64", "test")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(tok)) || bytes.Contains(raw, []byte(machine)) {
		t.Fatal("plaintext token found in database file")
	}
}

func enrollTest(t *testing.T, s *Store) (id, token string) {
	t.Helper()
	ctx := context.Background()
	tok, err := s.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, token, err = s.Enroll(ctx, tok, "host", "amd64", "test")
	if err != nil {
		t.Fatal(err)
	}
	return id, token
}

func TestHeartbeatUpdatesAndRestoresLost(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	id, _ := enrollTest(t, s)
	if err := s.MarkLost(ctx, id); err != nil {
		t.Fatalf("MarkLost: %v", err)
	}
	if err := s.Heartbeat(ctx, id, 2, false); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	w, err := s.GetWorker(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != job.WorkerActive || w.Capacity != 2 || w.Healthy {
		t.Fatalf("worker = %+v", w)
	}
}

func TestMarkLostRejectsRemoved(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	id, _ := enrollTest(t, s)
	if err := s.SetWorkerState(ctx, id, job.WorkerCordoned); err != nil {
		t.Fatal(err)
	}
	if err := s.SetWorkerState(ctx, id, job.WorkerRemoved); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkLost(ctx, id); err == nil {
		t.Fatal("expected error marking removed worker lost")
	}
}

func TestRequeueAndDeadLetter(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateJob(ctx, testJob("job-r")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Assign(ctx, "job-r", "w1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, "job-r", job.JobLost, "visibility_timeout"); err != nil {
		t.Fatal(err)
	}
	if err := s.Requeue(ctx, "job-r"); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	got, err := s.GetJob(ctx, "job-r")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != job.JobQueued || got.WorkerID != "" || got.Attempt != 1 {
		t.Fatalf("requeued = %+v", got)
	}

	if _, err := s.Assign(ctx, "job-r", "w2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, "job-r", job.JobLost, "visibility_timeout"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeadLetter(ctx, "job-r", "max_attempts"); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	got, err = s.GetJob(ctx, "job-r")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != job.JobFailed || !got.DeadLettered || got.Reason != "max_attempts" {
		t.Fatalf("dead letter = %+v", got)
	}
}

func TestDrainRequeueDoesNotConsumeAttempt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateJob(ctx, testJob("job-d")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Assign(ctx, "job-d", "w1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DrainRequeue(ctx, "job-d"); err != nil {
		t.Fatalf("DrainRequeue: %v", err)
	}
	got, err := s.GetJob(ctx, "job-d")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != job.JobQueued || got.WorkerID != "" || got.Attempt != 0 || got.Reason != "drain" {
		t.Fatalf("drained = %+v", got)
	}
	assigned, err := s.Assign(ctx, "job-d", "w2")
	if err != nil {
		t.Fatal(err)
	}
	if assigned.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", assigned.Attempt)
	}
}

func TestCordonDrainRemove(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	id, tok := enrollTest(t, s)
	if err := s.TransitionWorker(ctx, id, job.WorkerCordoned); err != nil {
		t.Fatal(err)
	}
	if err := s.TransitionWorker(ctx, id, job.WorkerActive); err != nil {
		t.Fatal(err)
	}
	if err := s.TransitionWorker(ctx, id, job.WorkerDraining); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveWorker(ctx, id); err == nil {
		t.Fatal("expected error removing draining worker")
	}
	if err := s.TransitionWorker(ctx, id, job.WorkerCordoned); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveWorker(ctx, id); err != nil {
		t.Fatalf("RemoveWorker: %v", err)
	}
	w, err := s.GetWorker(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != job.WorkerRemoved {
		t.Fatalf("state = %s", w.State)
	}
	if _, err := s.WorkerByToken(ctx, tok); err != ErrWorkerNotFound {
		t.Fatalf("WorkerByToken after remove = %v, want ErrWorkerNotFound", err)
	}
}
