// Package store persists jobs and their state history in SQLite.
package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite" // no CGO

	"github.com/CharlesBai-blc/forge/internal/job"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store wraps the SQLite database.
type Store struct {
	db *sql.DB
}

// Open opens the database at path, creating it if needed, and applies
// pending migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: ping %s: %w", path, err)
	}
	// SQLite allows one writer. Without this, database/sql opens
	// extra connections and concurrent GetJob/Transition hit SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// migrate applies numbered SQL files in order. Each file is one
// transaction; already-applied versions are skipped. Failure rolls back.
func migrate(ctx context.Context, db *sql.DB) error {
	names, err := migrationNames()
	if err != nil {
		return err
	}

	current, err := currentSchemaVersion(ctx, db)
	if err != nil {
		return err
	}

	for _, name := range names {
		version, err := migrationVersion(name)
		if err != nil {
			return err
		}
		if version <= current {
			continue
		}

		sqlBytes, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("store: read migration %s: %w", name, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("store: begin migration %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, string(sqlBytes)); err != nil {
			tx.Rollback()
			return fmt.Errorf("store: apply migration %s: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: commit migration %s: %w", name, err)
		}
	}
	return nil
}

func migrationNames() ([]string, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("store: read migrations dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// migrationVersion is the leading number in "0001_init.sql".
func migrationVersion(name string) (int, error) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("store: malformed migration filename %q", name)
	}
	n, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("store: malformed migration filename %q: %w", name, err)
	}
	return n, nil
}

// currentSchemaVersion is 0 until schema_version exists.
func currentSchemaVersion(ctx context.Context, db *sql.DB) (int, error) {
	var exists int
	err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'`,
	).Scan(&exists)
	if err != nil {
		return 0, fmt.Errorf("store: check schema_version: %w", err)
	}
	if exists == 0 {
		return 0, nil
	}

	var version int
	if err := db.QueryRowContext(ctx, `SELECT version FROM schema_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("store: read schema_version: %w", err)
	}
	return version, nil
}

// CreateJob inserts a queued job and its first transition in one transaction.
func (s *Store) CreateJob(ctx context.Context, j *job.Job) error {
	if j.State != job.JobQueued {
		return fmt.Errorf("store: CreateJob expects state %s, got %s", job.JobQueued, j.State)
	}

	labels, err := json.Marshal(j.Labels)
	if err != nil {
		return fmt.Errorf("store: marshal labels: %w", err)
	}

	now := time.Now().UTC()
	j.CreatedAt = now
	j.UpdatedAt = now

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (id, source, external_id, repo, run_id, labels, state, attempt, worker_id, dead_lettered, reason, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?)`,
		j.ID, j.Source, j.ExternalID, j.Repo, j.RunID, string(labels), string(j.State),
		j.Attempt, j.WorkerID, boolToInt(j.DeadLettered), j.Reason, formatTime(now), formatTime(now))
	if err != nil {
		return fmt.Errorf("store: insert job: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO transitions (job_id, attempt, from_state, to_state, reason, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		j.ID, j.Attempt, "", string(job.JobQueued), j.Reason, formatTime(now))
	if err != nil {
		return fmt.Errorf("store: insert transition: %w", err)
	}

	return tx.Commit()
}

// Transition applies a legal state change and appends history in one transaction.
func (s *Store) Transition(ctx context.Context, jobID string, to job.JobState, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback()

	var (
		from    job.JobState
		attempt int
	)
	err = tx.QueryRowContext(ctx, `SELECT state, attempt FROM jobs WHERE id = ?`, jobID).Scan(&from, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("store: job %s not found", jobID)
	}
	if err != nil {
		return fmt.Errorf("store: select job: %w", err)
	}

	if err := job.ValidateTransition(from, to); err != nil {
		return err
	}

	now := formatTime(time.Now().UTC())

	if _, err := tx.ExecContext(ctx,
		`UPDATE jobs SET state = ?, reason = ?, updated_at = ? WHERE id = ?`,
		string(to), reason, now, jobID,
	); err != nil {
		return fmt.Errorf("store: update job: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO transitions (job_id, attempt, from_state, to_state, reason, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, attempt, string(from), string(to), reason, now,
	); err != nil {
		return fmt.Errorf("store: insert transition: %w", err)
	}

	return tx.Commit()
}

// Assign is claim delivery: increment attempt, set worker, queued -> assigned.
func (s *Store) Assign(ctx context.Context, jobID, workerID string) (*job.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback()

	var (
		from    job.JobState
		attempt int
	)
	err = tx.QueryRowContext(ctx, `SELECT state, attempt FROM jobs WHERE id = ?`, jobID).Scan(&from, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: job %s not found", jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: select job: %w", err)
	}
	if from != job.JobQueued {
		return nil, fmt.Errorf("store: assign %s: state %s, want %s", jobID, from, job.JobQueued)
	}

	attempt++
	now := formatTime(time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs SET state = ?, attempt = ?, worker_id = NULLIF(?, ''), updated_at = ?
		WHERE id = ?`,
		string(job.JobAssigned), attempt, workerID, now, jobID,
	); err != nil {
		return nil, fmt.Errorf("store: assign update: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO transitions (job_id, attempt, from_state, to_state, reason, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, attempt, string(job.JobQueued), string(job.JobAssigned), "", now,
	); err != nil {
		return nil, fmt.Errorf("store: assign transition: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.GetJob(ctx, jobID)
}

// Requeue moves a lost job back to queued and clears the worker (FR-11).
func (s *Store) Requeue(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin requeue: %w", err)
	}
	defer tx.Rollback()

	var (
		from    job.JobState
		attempt int
	)
	err = tx.QueryRowContext(ctx, `SELECT state, attempt FROM jobs WHERE id = ?`, jobID).Scan(&from, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("store: job %s not found", jobID)
	}
	if err != nil {
		return fmt.Errorf("store: select job: %w", err)
	}
	if err := job.ValidateTransition(from, job.JobQueued); err != nil {
		return err
	}
	now := formatTime(time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs SET state = ?, worker_id = NULL, reason = ?, updated_at = ? WHERE id = ?`,
		string(job.JobQueued), "", now, jobID,
	); err != nil {
		return fmt.Errorf("store: requeue update: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO transitions (job_id, attempt, from_state, to_state, reason, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, attempt, string(from), string(job.JobQueued), "", now,
	); err != nil {
		return fmt.Errorf("store: requeue transition: %w", err)
	}
	return tx.Commit()
}

// DrainRequeue returns an assigned-not-started job to queued without
// consuming an attempt (FR-19).
func (s *Store) DrainRequeue(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin drain requeue: %w", err)
	}
	defer tx.Rollback()

	var (
		from    job.JobState
		attempt int
	)
	err = tx.QueryRowContext(ctx, `SELECT state, attempt FROM jobs WHERE id = ?`, jobID).Scan(&from, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("store: job %s not found", jobID)
	}
	if err != nil {
		return fmt.Errorf("store: select job: %w", err)
	}
	if from != job.JobAssigned {
		return fmt.Errorf("store: drain requeue %s: state %s, want %s", jobID, from, job.JobAssigned)
	}
	if err := job.ValidateTransition(from, job.JobQueued); err != nil {
		return err
	}
	if attempt > 0 {
		attempt--
	}
	now := formatTime(time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs SET state = ?, attempt = ?, worker_id = NULL, reason = ?, updated_at = ? WHERE id = ?`,
		string(job.JobQueued), attempt, "drain", now, jobID,
	); err != nil {
		return fmt.Errorf("store: drain requeue update: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO transitions (job_id, attempt, from_state, to_state, reason, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, attempt, string(from), string(job.JobQueued), "drain", now,
	); err != nil {
		return fmt.Errorf("store: drain requeue transition: %w", err)
	}
	return tx.Commit()
}

// DeadLetter marks a lost job failed with DeadLettered set (FR-12).
func (s *Store) DeadLetter(ctx context.Context, jobID, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin dead letter: %w", err)
	}
	defer tx.Rollback()

	var (
		from    job.JobState
		attempt int
	)
	err = tx.QueryRowContext(ctx, `SELECT state, attempt FROM jobs WHERE id = ?`, jobID).Scan(&from, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("store: job %s not found", jobID)
	}
	if err != nil {
		return fmt.Errorf("store: select job: %w", err)
	}
	if err := job.ValidateTransition(from, job.JobFailed); err != nil {
		return err
	}
	now := formatTime(time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs SET state = ?, dead_lettered = 1, reason = ?, updated_at = ? WHERE id = ?`,
		string(job.JobFailed), reason, now, jobID,
	); err != nil {
		return fmt.Errorf("store: dead letter update: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO transitions (job_id, attempt, from_state, to_state, reason, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, attempt, string(from), string(job.JobFailed), reason, now,
	); err != nil {
		return fmt.Errorf("store: dead letter transition: %w", err)
	}
	return tx.Commit()
}

// QueuedIDs returns IDs of jobs still in queued (startup reconciler, tdd.md §6.2).
func (s *Store) QueuedIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM jobs WHERE state = ?`, string(job.JobQueued))
	if err != nil {
		return nil, fmt.Errorf("store: queued ids: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: queued ids scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: queued ids: %w", err)
	}
	return ids, nil
}

// GetJob returns a job by ID.
func (s *Store) GetJob(ctx context.Context, id string) (*job.Job, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: job %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("store: select job: %w", err)
	}
	return j, nil
}

// JobsByWorker returns jobs for workerID in state.
func (s *Store) JobsByWorker(ctx context.Context, workerID string, state job.JobState) ([]*job.Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE worker_id = ? AND state = ?`, workerID, string(state))
	if err != nil {
		return nil, fmt.Errorf("store: jobs by worker: %w", err)
	}
	defer rows.Close()
	var out []*job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: jobs by worker: %w", err)
	}
	return out, nil
}

// ListJobs returns jobs newest first (FR-24).
func (s *Store) ListJobs(ctx context.Context, limit int) ([]*job.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs ORDER BY created_at DESC, rowid DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs: %w", err)
	}
	defer rows.Close()
	out := []*job.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list jobs: %w", err)
	}
	return out, nil
}

// CountQueued is queue depth (FR-24).
func (s *Store) CountQueued(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM jobs WHERE state = ?`, string(job.JobQueued)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: count queued: %w", err)
	}
	return n, nil
}

// ListTransitions returns append-only history for a job (FR-9, FR-26).
func (s *Store) ListTransitions(ctx context.Context, jobID string) ([]job.Transition, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id, attempt, from_state, to_state, COALESCE(reason, ''), created_at
		FROM transitions WHERE job_id = ? ORDER BY id ASC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("store: list transitions: %w", err)
	}
	defer rows.Close()
	out := []job.Transition{}
	for rows.Next() {
		var (
			tr job.Transition
			at string
		)
		if err := rows.Scan(&tr.JobID, &tr.Attempt, &tr.From, &tr.To, &tr.Reason, &at); err != nil {
			return nil, fmt.Errorf("store: scan transition: %w", err)
		}
		t, err := parseTime(at)
		if err != nil {
			return nil, fmt.Errorf("store: parse transition time: %w", err)
		}
		tr.At = t
		out = append(out, tr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list transitions: %w", err)
	}
	return out, nil
}

const jobCols = `id, source, external_id, repo, run_id, labels, state, attempt, COALESCE(worker_id, ''), dead_lettered, COALESCE(reason, ''), created_at, updated_at`

func scanJob(row scanner) (*job.Job, error) {
	var (
		j                    job.Job
		labels               string
		deadLettered         int
		createdAt, updatedAt string
	)
	err := row.Scan(&j.ID, &j.Source, &j.ExternalID, &j.Repo, &j.RunID, &labels, &j.State, &j.Attempt,
		&j.WorkerID, &deadLettered, &j.Reason, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(labels), &j.Labels); err != nil {
		return nil, fmt.Errorf("store: unmarshal labels: %w", err)
	}
	j.DeadLettered = deadLettered != 0
	if j.CreatedAt, err = parseTime(createdAt); err != nil {
		return nil, fmt.Errorf("store: parse created_at: %w", err)
	}
	if j.UpdatedAt, err = parseTime(updatedAt); err != nil {
		return nil, fmt.Errorf("store: parse updated_at: %w", err)
	}
	return &j, nil
}

// ErrSecretNotFound is returned when GetSecret has no row for name.
var ErrSecretNotFound = errors.New("store: secret not found")

// PutSecret upserts an opaque ciphertext (FR-27).
func (s *Store) PutSecret(ctx context.Context, name string, ciphertext []byte) error {
	now := formatTime(time.Now().UTC())
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO secrets (name, ciphertext, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET ciphertext = excluded.ciphertext, updated_at = excluded.updated_at`,
		name, ciphertext, now)
	if err != nil {
		return fmt.Errorf("store: put secret %s: %w", name, err)
	}
	return nil
}

// GetSecret returns the ciphertext for name.
func (s *Store) GetSecret(ctx context.Context, name string) ([]byte, error) {
	var ciphertext []byte
	err := s.db.QueryRowContext(ctx, `SELECT ciphertext FROM secrets WHERE name = ?`, name).Scan(&ciphertext)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrSecretNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get secret %s: %w", name, err)
	}
	return ciphertext, nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

func formatTime(t time.Time) string {
	return t.Format(time.RFC3339Nano)
}

func parseTime(s string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
