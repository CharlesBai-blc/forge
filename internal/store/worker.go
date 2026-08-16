package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/CharlesBai-blc/forge/internal/job"
)

const EnrollmentTTL = time.Hour

var (
	ErrEnrollmentInvalid = errors.New("store: enrollment token invalid")
	ErrEnrollmentUsed    = errors.New("store: enrollment token used")
	ErrEnrollmentExpired = errors.New("store: enrollment token expired")
	ErrWorkerNotFound    = errors.New("store: worker not found")
)

// IssueEnrollmentToken stores a one-time token hash with a 1h TTL and
// returns the plaintext once (FR-3, FR-27).
func (s *Store) IssueEnrollmentToken(ctx context.Context) (string, error) {
	return s.issueEnrollmentToken(ctx, time.Now().UTC().Add(EnrollmentTTL))
}

func (s *Store) issueEnrollmentToken(ctx context.Context, expires time.Time) (string, error) {
	tok, err := randomHex(32)
	if err != nil {
		return "", err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO enrollment_tokens (token_hash, expires_at) VALUES (?, ?)`,
		hashToken(tok), formatTime(expires))
	if err != nil {
		return "", fmt.Errorf("store: issue enrollment: %w", err)
	}
	return tok, nil
}

// Enroll consumes a one-time token and inserts an active worker (FR-3, FR-18).
func (s *Store) Enroll(ctx context.Context, token, name, arch, version string) (workerID, machineToken string, err error) {
	if token == "" {
		return "", "", ErrEnrollmentInvalid
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", fmt.Errorf("store: begin enroll: %w", err)
	}
	defer tx.Rollback()

	var expiresAt, usedAt sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT expires_at, used_at FROM enrollment_tokens WHERE token_hash = ?`,
		hashToken(token),
	).Scan(&expiresAt, &usedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrEnrollmentInvalid
	}
	if err != nil {
		return "", "", fmt.Errorf("store: select enrollment: %w", err)
	}
	if usedAt.Valid {
		return "", "", ErrEnrollmentUsed
	}
	exp, err := parseTime(expiresAt.String)
	if err != nil {
		return "", "", fmt.Errorf("store: parse enrollment expiry: %w", err)
	}
	if !exp.After(time.Now().UTC()) {
		return "", "", ErrEnrollmentExpired
	}

	workerID, err = randomHex(16)
	if err != nil {
		return "", "", err
	}
	machineToken, err = randomHex(32)
	if err != nil {
		return "", "", err
	}
	labels, err := json.Marshal([]string{})
	if err != nil {
		return "", "", fmt.Errorf("store: marshal labels: %w", err)
	}
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO workers (id, name, labels, capacity, state, burst, healthy, last_seen, token_hash, arch, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workerID, name, string(labels), 1, string(job.WorkerActive), 0, 1,
		formatTime(now), hashToken(machineToken), arch, version)
	if err != nil {
		return "", "", fmt.Errorf("store: insert worker: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE enrollment_tokens SET used_at = ? WHERE token_hash = ?`,
		formatTime(now), hashToken(token))
	if err != nil {
		return "", "", fmt.Errorf("store: consume enrollment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return workerID, machineToken, nil
}

// GetWorker returns a worker by ID.
func (s *Store) GetWorker(ctx context.Context, id string) (*job.Worker, error) {
	w, err := s.scanWorker(ctx, `SELECT `+workerCols+` FROM workers WHERE id = ?`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("store: worker %s not found", id)
	}
	if err != nil {
		return nil, err
	}
	return w, nil
}

// WorkerByToken looks up a worker by per-machine token (FR-27).
func (s *Store) WorkerByToken(ctx context.Context, token string) (*job.Worker, error) {
	if token == "" {
		return nil, ErrWorkerNotFound
	}
	w, err := s.scanWorker(ctx, `SELECT `+workerCols+` FROM workers WHERE token_hash = ?`, hashToken(token))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrWorkerNotFound
	}
	if err != nil {
		return nil, err
	}
	return w, nil
}

// SetWorkerState sets a worker's state. Transition checks are the caller's.
func (s *Store) SetWorkerState(ctx context.Context, id string, state job.WorkerState) error {
	res, err := s.db.ExecContext(ctx, `UPDATE workers SET state = ? WHERE id = ?`, string(state), id)
	if err != nil {
		return fmt.Errorf("store: set worker state: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: set worker state: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("store: worker %s not found", id)
	}
	return nil
}

const workerCols = `id, name, labels, capacity, state, burst, healthy, last_seen, token_hash, arch, version`

func (s *Store) scanWorker(ctx context.Context, q string, arg any) (*job.Worker, error) {
	var (
		w        job.Worker
		labels   string
		burst    int
		healthy  int
		lastSeen string
	)
	err := s.db.QueryRowContext(ctx, q, arg).Scan(
		&w.ID, &w.Name, &labels, &w.Capacity, &w.State, &burst, &healthy, &lastSeen, &w.TokenHash, &w.Arch, &w.Version,
	)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(labels), &w.Labels); err != nil {
		return nil, fmt.Errorf("store: unmarshal worker labels: %w", err)
	}
	w.Burst = burst != 0
	w.Healthy = healthy != 0
	if w.LastSeen, err = parseTime(lastSeen); err != nil {
		return nil, fmt.Errorf("store: parse last_seen: %w", err)
	}
	return &w, nil
}

func hashToken(tok string) []byte {
	sum := sha256.Sum256([]byte(tok))
	return sum[:]
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("store: random: %w", err)
	}
	return hex.EncodeToString(b), nil
}
