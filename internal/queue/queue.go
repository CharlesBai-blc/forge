// Package queue is the M1 in-process job queue (FR-10). Redis Streams
// replaces it at M3. This is a concrete type, not a queue interface.
package queue

import (
	"context"
	"fmt"

	"github.com/CharlesBai-blc/forge/internal/job"
)

const defaultCapacity = 256

// Queue is a bounded in-process FIFO of jobs.
type Queue struct {
	jobs chan *job.Job
}

// New returns a ready-to-use Queue.
func New() *Queue {
	return newQueue(defaultCapacity)
}

func newQueue(capacity int) *Queue {
	return &Queue{jobs: make(chan *job.Job, capacity)}
}

// Enqueue adds a job. Returns an error if the queue is full rather
// than blocking, so a webhook handler cannot stall behind a full buffer.
func (q *Queue) Enqueue(j *job.Job) error {
	select {
	case q.jobs <- j:
		return nil
	default:
		return fmt.Errorf("queue: full")
	}
}

// Claim blocks until a job is available or ctx is cancelled.
func (q *Queue) Claim(ctx context.Context) (*job.Job, error) {
	select {
	case j := <-q.jobs:
		return j, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
