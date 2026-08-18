package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	ErrAdminExists     = errors.New("store: admin already exists")
	ErrAdminNotFound   = errors.New("store: admin not found")
	ErrSessionNotFound = errors.New("store: session not found")
)

// CreateAdmin inserts the single admin account (FR-2, tdd.md §7).
// Errors with ErrAdminExists if one is already configured.
func (s *Store) CreateAdmin(ctx context.Context, username string, passwordHash []byte) error {
	if username == "" || len(passwordHash) == 0 {
		return fmt.Errorf("store: create admin: username and password hash required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin create admin: %w", err)
	}
	defer tx.Rollback()
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM admin`).Scan(&n); err != nil {
		return fmt.Errorf("store: count admin: %w", err)
	}
	if n > 0 {
		return ErrAdminExists
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO admin (username, password_hash) VALUES (?, ?)`, username, passwordHash); err != nil {
		return fmt.Errorf("store: insert admin: %w", err)
	}
	return tx.Commit()
}

// GetAdmin returns the single admin row.
func (s *Store) GetAdmin(ctx context.Context) (username string, passwordHash []byte, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT username, password_hash FROM admin`).Scan(&username, &passwordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, ErrAdminNotFound
	}
	if err != nil {
		return "", nil, fmt.Errorf("store: get admin: %w", err)
	}
	return username, passwordHash, nil
}

// AdminExists reports whether setup has completed (FR-2).
func (s *Store) AdminExists(ctx context.Context) (bool, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM admin`).Scan(&n); err != nil {
		return false, fmt.Errorf("store: admin exists: %w", err)
	}
	return n > 0, nil
}

// CreateSession stores a session token hash with its expiry (tdd.md §7).
// Only the SHA-256 hash is stored, same pattern as Worker.TokenHash.
func (s *Store) CreateSession(ctx context.Context, tokenHash []byte, expiresAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sessions (token_hash, expires_at) VALUES (?, ?)`,
		tokenHash, formatTime(expiresAt.UTC()))
	if err != nil {
		return fmt.Errorf("store: create session: %w", err)
	}
	return nil
}

// SessionExpiresAt returns a session's expiry, or ErrSessionNotFound.
func (s *Store) SessionExpiresAt(ctx context.Context, tokenHash []byte) (time.Time, error) {
	var at string
	err := s.db.QueryRowContext(ctx,
		`SELECT expires_at FROM sessions WHERE token_hash = ?`, tokenHash).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, ErrSessionNotFound
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("store: session expiry: %w", err)
	}
	t, err := parseTime(at)
	if err != nil {
		return time.Time{}, fmt.Errorf("store: parse session expiry: %w", err)
	}
	return t, nil
}

// DeleteSession removes one session (logout). Idempotent.
func (s *Store) DeleteSession(ctx context.Context, tokenHash []byte) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash = ?`, tokenHash); err != nil {
		return fmt.Errorf("store: delete session: %w", err)
	}
	return nil
}

// DeleteExpiredSessions removes sessions past their expiry.
func (s *Store) DeleteExpiredSessions(ctx context.Context, now time.Time) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM sessions WHERE expires_at <= ?`, formatTime(now.UTC())); err != nil {
		return fmt.Errorf("store: delete expired sessions: %w", err)
	}
	return nil
}
