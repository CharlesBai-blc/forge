package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateGetAdmin(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	exists, err := s.AdminExists(ctx)
	if err != nil || exists {
		t.Fatalf("AdminExists = %v, %v; want false", exists, err)
	}
	if _, _, err := s.GetAdmin(ctx); !errors.Is(err, ErrAdminNotFound) {
		t.Fatalf("GetAdmin err = %v, want ErrAdminNotFound", err)
	}

	if err := s.CreateAdmin(ctx, "alice", []byte("hash-bytes")); err != nil {
		t.Fatalf("CreateAdmin: %v", err)
	}
	exists, err = s.AdminExists(ctx)
	if err != nil || !exists {
		t.Fatalf("AdminExists = %v, %v; want true", exists, err)
	}
	u, h, err := s.GetAdmin(ctx)
	if err != nil || u != "alice" || string(h) != "hash-bytes" {
		t.Fatalf("GetAdmin = %q, %q, %v", u, h, err)
	}

	// Single admin per install (tdd.md §7).
	if err := s.CreateAdmin(ctx, "bob", []byte("x")); !errors.Is(err, ErrAdminExists) {
		t.Fatalf("second CreateAdmin err = %v, want ErrAdminExists", err)
	}
}

func TestSessionLifecycle(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	hash := []byte("session-hash-0123456789abcdef")
	exp := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)

	if _, err := s.SessionExpiresAt(ctx, hash); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing session err = %v, want ErrSessionNotFound", err)
	}
	if err := s.CreateSession(ctx, hash, exp); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	got, err := s.SessionExpiresAt(ctx, hash)
	if err != nil || !got.Equal(exp) {
		t.Fatalf("SessionExpiresAt = %v, %v; want %v", got, err, exp)
	}
	if err := s.DeleteSession(ctx, hash); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if _, err := s.SessionExpiresAt(ctx, hash); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("deleted session err = %v, want ErrSessionNotFound", err)
	}
}

func TestDeleteExpiredSessions(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.CreateSession(ctx, []byte("old"), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateSession(ctx, []byte("live"), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteExpiredSessions(ctx, now); err != nil {
		t.Fatalf("DeleteExpiredSessions: %v", err)
	}
	if _, err := s.SessionExpiresAt(ctx, []byte("old")); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expired session survived: %v", err)
	}
	if _, err := s.SessionExpiresAt(ctx, []byte("live")); err != nil {
		t.Fatalf("live session deleted: %v", err)
	}
}

func TestBurstEnrollmentSetsWorkerBurst(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	tok, err := s.IssueBurstEnrollmentToken(ctx)
	if err != nil {
		t.Fatalf("IssueBurstEnrollmentToken: %v", err)
	}
	id, _, err := s.Enroll(ctx, tok, "burst-1", "arm64", "test")
	if err != nil {
		t.Fatalf("Enroll: %v", err)
	}
	w, err := s.GetWorker(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !w.Burst {
		t.Fatal("worker enrolled with burst token has Burst = false")
	}

	// A plain token still enrolls a non-burst worker.
	plain, err := s.IssueEnrollmentToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id2, _, err := s.Enroll(ctx, plain, "steady-1", "arm64", "test")
	if err != nil {
		t.Fatal(err)
	}
	w2, err := s.GetWorker(ctx, id2)
	if err != nil {
		t.Fatal(err)
	}
	if w2.Burst {
		t.Fatal("worker enrolled with plain token has Burst = true")
	}
}

func TestBurstEventsLedger(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	n, err := s.BurstCount(ctx)
	if err != nil || n != 0 {
		t.Fatalf("empty BurstCount = %d, %v", n, err)
	}
	for _, c := range []int{1, 2, 1} {
		if err := s.AppendBurstEvent(ctx, c); err != nil {
			t.Fatalf("AppendBurstEvent(%d): %v", c, err)
		}
	}
	n, err = s.BurstCount(ctx)
	if err != nil || n != 1 {
		t.Fatalf("BurstCount = %d, %v; want 1", n, err)
	}

	base, events, err := s.BurstEventsSince(ctx, time.Now().UTC().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if base != 0 || len(events) != 3 {
		t.Fatalf("baseline = %d, events = %d; want 0, 3", base, len(events))
	}
	base, events, err = s.BurstEventsSince(ctx, time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if base != 1 || len(events) != 0 {
		t.Fatalf("future baseline = %d, events = %d; want 1, 0", base, len(events))
	}
}

func TestNewestBurstWorker(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, ok, err := s.NewestBurstWorker(ctx); err != nil || ok {
		t.Fatalf("NewestBurstWorker on empty fleet = ok %v, %v", ok, err)
	}

	var ids []string
	for range 2 {
		tok, err := s.IssueBurstEnrollmentToken(ctx)
		if err != nil {
			t.Fatal(err)
		}
		id, _, err := s.Enroll(ctx, tok, "b", "arm64", "test")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	w, ok, err := s.NewestBurstWorker(ctx)
	if err != nil || !ok {
		t.Fatalf("NewestBurstWorker: ok %v, %v", ok, err)
	}
	if w.ID != ids[1] {
		t.Fatalf("newest = %s, want %s", w.ID, ids[1])
	}

	// Removed workers are skipped.
	if err := s.RemoveWorker(ctx, ids[1]); err == nil {
		t.Fatal("RemoveWorker from active should fail; cordon first")
	}
	if err := s.TransitionWorker(ctx, ids[1], "cordoned"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveWorker(ctx, ids[1]); err != nil {
		t.Fatal(err)
	}
	w, ok, err = s.NewestBurstWorker(ctx)
	if err != nil || !ok || w.ID != ids[0] {
		t.Fatalf("after remove: %+v, ok %v, %v; want %s", w, ok, err, ids[0])
	}
}

// TestMigrationUpgradeFromM4 builds an M4-era database (migrations
// 0001-0004) with data, then verifies Open applies 0005 and the old
// rows still work.
func TestMigrationUpgradeFromM4(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "forge.db")

	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{
		"0001_init.sql", "0002_secrets.sql", "0003_workers.sql", "0004_worker_prev_state_job_runner.sql",
	} {
		b, err := os.ReadFile(filepath.Join("migrations", m))
		if err != nil {
			t.Fatalf("read %s: %v", m, err)
		}
		if _, err := db.Exec(string(b)); err != nil {
			t.Fatalf("apply %s: %v", m, err)
		}
	}
	if _, err := db.Exec(`
		INSERT INTO workers (id, name, labels, capacity, state, burst, healthy, last_seen, token_hash, arch, version)
		VALUES ('w-old', 'm4-worker', '[]', 1, 'active', 0, 1, '2026-08-01T00:00:00Z', X'00', 'arm64', 'v0')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open after M4 data: %v", err)
	}
	defer s.Close()

	var version int
	if err := s.db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 5 {
		t.Fatalf("schema_version = %d, want 5", version)
	}
	// Pre-M5 worker (NULL created_at) still scans and is not burst.
	w, err := s.GetWorker(ctx, "w-old")
	if err != nil {
		t.Fatalf("GetWorker after upgrade: %v", err)
	}
	if w.Burst || w.Name != "m4-worker" {
		t.Fatalf("upgraded worker = %+v", w)
	}
	// New tables are usable.
	if err := s.CreateAdmin(ctx, "admin", []byte("h")); err != nil {
		t.Fatalf("CreateAdmin after upgrade: %v", err)
	}
}
