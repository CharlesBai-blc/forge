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

// GetJob returns a job by ID.
func (s *Store) GetJob(ctx context.Context, id string) (*job.Job, error) {
	var (
		j                    job.Job
		labels               string
		deadLettered         int
		createdAt, updatedAt string
	)

	err := s.db.QueryRowContext(ctx, `
		SELECT id, source, external_id, repo, run_id, labels, state, attempt, COALESCE(worker_id, ''), dead_lettered, COALESCE(reason, ''), created_at, updated_at
		FROM jobs WHERE id = ?`, id,
	).Scan(&j.ID, &j.Source, &j.ExternalID, &j.Repo, &j.RunID, &labels, &j.State, &j.Attempt,
		&j.WorkerID, &deadLettered, &j.Reason, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: job %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("store: select job: %w", err)
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
