package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
)

// BurstEvent is one desired-count change recorded by the burst
// controller (FR-21, FR-22). The ledger is the controller's persistent
// state: the latest event is the current desired instance count, and
// the FR-23 daily instance-hours cap is integrated from it.
type BurstEvent struct {
	Count int
	At    time.Time
}

// IssueBurstEnrollmentToken is IssueEnrollmentToken with the burst flag
// set, so the worker that enrolls with it carries Burst (FR-21).
func (s *Store) IssueBurstEnrollmentToken(ctx context.Context) (string, error) {
	tok, err := randomHex(32)
	if err != nil {
		return "", err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO enrollment_tokens (token_hash, expires_at, burst) VALUES (?, ?, 1)`,
		hashToken(tok), formatTime(time.Now().UTC().Add(EnrollmentTTL)))
	if err != nil {
		return "", fmt.Errorf("store: issue burst enrollment: %w", err)
	}
	return tok, nil
}

// AppendBurstEvent records a new desired instance count.
func (s *Store) AppendBurstEvent(ctx context.Context, count int) error {
	if count < 0 {
		return fmt.Errorf("store: burst event count %d", count)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO burst_events (count, created_at) VALUES (?, ?)`,
		count, formatTime(time.Now().UTC()))
	if err != nil {
		return fmt.Errorf("store: append burst event: %w", err)
	}
	return nil
}

// BurstCount returns the current desired instance count: the latest
// event's count, or 0 when no event exists.
func (s *Store) BurstCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT count FROM burst_events ORDER BY id DESC LIMIT 1`).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: burst count: %w", err)
	}
	return n, nil
}

// BurstEventsSince returns the count in effect at t (baseline) and the
// events at or after t, oldest first. The FR-23 daily cap integrates
// instance-hours from these.
func (s *Store) BurstEventsSince(ctx context.Context, t time.Time) (baseline int, events []BurstEvent, err error) {
	cut := formatTime(t.UTC())
	err = s.db.QueryRowContext(ctx,
		`SELECT count FROM burst_events WHERE created_at < ? ORDER BY id DESC LIMIT 1`, cut).Scan(&baseline)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, nil, fmt.Errorf("store: burst baseline: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT count, created_at FROM burst_events WHERE created_at >= ? ORDER BY id ASC`, cut)
	if err != nil {
		return 0, nil, fmt.Errorf("store: burst events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			e  BurstEvent
			at string
		)
		if err := rows.Scan(&e.Count, &at); err != nil {
			return 0, nil, fmt.Errorf("store: scan burst event: %w", err)
		}
		if e.At, err = parseTime(at); err != nil {
			return 0, nil, fmt.Errorf("store: parse burst event time: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("store: burst events: %w", err)
	}
	return baseline, events, nil
}

// NewestBurstWorker returns the most recently enrolled burst worker
// that has not been removed, so scale-down drains the newest first
// (FR-22). Rowid is insertion order, which is enrollment order. ok is
// false when no such worker exists, e.g. an instance that never
// enrolled (tdd.md §6.7).
func (s *Store) NewestBurstWorker(ctx context.Context) (*job.Worker, bool, error) {
	w, err := s.scanWorker(ctx, `
		SELECT `+workerCols+` FROM workers
		WHERE burst = 1 AND state != ?
		ORDER BY rowid DESC LIMIT 1`,
		string(job.WorkerRemoved))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return w, true, nil
}
