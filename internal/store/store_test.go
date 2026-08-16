package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/CharlesBai-blc/forge/internal/job"
)

func testJob(id string) *job.Job {
	return &job.Job{
		ID:         id,
		Source:     "github",
		ExternalID: 1,
		Repo:       "owner/name",
		RunID:      2,
		Labels:     []string{"self-hosted", "linux"},
		State:      job.JobQueued,
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "forge.db")
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpenAppliesMigration(t *testing.T) {
	s := openTestStore(t)

	var version int
	if err := s.db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if version != 1 {
		t.Errorf("schema_version = %d, want 1", version)
	}
}

func TestOpenReopenIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forge.db")
	ctx := context.Background()

	s1, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := s1.CreateJob(ctx, testJob("job-1")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Restart: reopening the same file must not fail or wipe data
	// (FR-9, tdd.md §6.2).
	s2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()

	got, err := s2.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob after restart: %v", err)
	}
	if got.State != job.JobQueued {
		t.Errorf("State = %s, want %s", got.State, job.JobQueued)
	}
}

func TestCreateAndGetJob(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := testJob("job-1")
	if err := s.CreateJob(ctx, want); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	got, err := s.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if got.ID != want.ID || got.Source != want.Source || got.Repo != want.Repo ||
		got.ExternalID != want.ExternalID || got.RunID != want.RunID || got.State != want.State {
		t.Errorf("GetJob = %+v, want fields matching %+v", got, want)
	}
	if len(got.Labels) != 2 || got.Labels[0] != "self-hosted" || got.Labels[1] != "linux" {
		t.Errorf("Labels = %v, want [self-hosted linux]", got.Labels)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Errorf("CreatedAt/UpdatedAt not set: %+v", got)
	}
}

func TestCreateJobRejectsWrongInitialState(t *testing.T) {
	s := openTestStore(t)

	j := testJob("job-1")
	j.State = job.JobRunning
	if err := s.CreateJob(context.Background(), j); err == nil {
		t.Fatal("expected error for non-queued initial state")
	}
}

func TestCreateJobWritesInitialTransition(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateJob(ctx, testJob("job-1")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	var fromState, toState string
	err := s.db.QueryRow(
		`SELECT from_state, to_state FROM transitions WHERE job_id = ?`, "job-1",
	).Scan(&fromState, &toState)
	if err != nil {
		t.Fatalf("read transitions: %v", err)
	}
	if fromState != "" || toState != string(job.JobQueued) {
		t.Errorf("transition = %q -> %q, want \"\" -> %q", fromState, toState, job.JobQueued)
	}
}

func TestTransitionUpdatesStateAndAppendsHistory(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateJob(ctx, testJob("job-1")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := s.Transition(ctx, "job-1", job.JobAssigned, ""); err != nil {
		t.Fatalf("Transition: %v", err)
	}

	got, err := s.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.State != job.JobAssigned {
		t.Errorf("State = %s, want %s", got.State, job.JobAssigned)
	}

	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM transitions WHERE job_id = ?`, "job-1").Scan(&count); err != nil {
		t.Fatalf("count transitions: %v", err)
	}
	if count != 2 {
		t.Errorf("transition count = %d, want 2", count)
	}
}

func TestTransitionRejectsIllegalEdge(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateJob(ctx, testJob("job-1")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// queued -> succeeded is not a legal edge (tdd.md §4.2).
	if err := s.Transition(ctx, "job-1", job.JobSucceeded, ""); err == nil {
		t.Fatal("expected error for illegal transition")
	}

	got, err := s.GetJob(ctx, "job-1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.State != job.JobQueued {
		t.Errorf("State = %s, want unchanged %s", got.State, job.JobQueued)
	}
}

func TestTransitionUnknownJob(t *testing.T) {
	s := openTestStore(t)
	if err := s.Transition(context.Background(), "missing", job.JobAssigned, ""); err == nil {
		t.Fatal("expected error for unknown job")
	}
}

func TestCreateJobDuplicateExternalIDRejected(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateJob(ctx, testJob("job-1")); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// Same source+external_id as job-1: webhook redelivery idempotency
	// (Appendix A UNIQUE constraint).
	dup := testJob("job-2")
	if err := s.CreateJob(ctx, dup); err == nil {
		t.Fatal("expected error for duplicate (source, external_id)")
	}
}
