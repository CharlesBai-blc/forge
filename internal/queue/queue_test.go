package queue

import (
	"context"
	"testing"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
)

func testJob(id string) *job.Job {
	return &job.Job{
		ID:    id,
		State: job.JobQueued,
	}
}

func TestEnqueueClaimFIFO(t *testing.T) {
	q := New()
	ctx := context.Background()

	if err := q.Enqueue(testJob("job-1")); err != nil {
		t.Fatalf("Enqueue job-1: %v", err)
	}
	if err := q.Enqueue(testJob("job-2")); err != nil {
		t.Fatalf("Enqueue job-2: %v", err)
	}

	first, err := q.Claim(ctx)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if first.ID != "job-1" {
		t.Errorf("first = %s, want job-1", first.ID)
	}

	second, err := q.Claim(ctx)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if second.ID != "job-2" {
		t.Errorf("second = %s, want job-2", second.ID)
	}
}

func TestClaimCancelled(t *testing.T) {
	q := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := q.Claim(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestEnqueueFull(t *testing.T) {
	q := newQueue(1)
	if err := q.Enqueue(testJob("job-1")); err != nil {
		t.Fatalf("Enqueue job-1: %v", err)
	}
	if err := q.Enqueue(testJob("job-2")); err == nil {
		t.Fatal("expected error when queue is full")
	}

	got, err := q.Claim(context.Background())
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got.ID != "job-1" {
		t.Errorf("Claim = %s, want job-1", got.ID)
	}
}

func TestClaimWaitsForEnqueue(t *testing.T) {
	q := New()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errc := make(chan error, 1)
	gotc := make(chan *job.Job, 1)
	go func() {
		j, err := q.Claim(ctx)
		if err != nil {
			errc <- err
			return
		}
		gotc <- j
	}()

	if err := q.Enqueue(testJob("job-1")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case err := <-errc:
		t.Fatalf("Claim: %v", err)
	case got := <-gotc:
		if got.ID != "job-1" {
			t.Errorf("Claim = %s, want job-1", got.ID)
		}
	}
}
