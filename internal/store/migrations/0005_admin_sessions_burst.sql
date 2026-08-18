-- 0005_admin_sessions_burst.sql: M5 additions (tdd.md Appendix A, §4.10, §7).
--
-- admin and sessions back the single-admin dashboard login (FR-2).
-- enrollment_tokens.burst marks tokens pre-issued by the burst
-- controller, so the worker that enrolls with one carries Burst (FR-21).
-- workers.created_at records enrollment time (NULL for pre-M5 rows);
-- scale-down picks the newest burst worker by insertion order (FR-22).
-- burst_events is the controller's persistent desired-count ledger;
-- the FR-23 daily instance-hours cap is integrated from it.

CREATE TABLE admin (
    username      TEXT PRIMARY KEY,
    password_hash BLOB NOT NULL             -- Argon2id, salt || key
);

CREATE TABLE sessions (
    token_hash BLOB PRIMARY KEY,            -- SHA-256 of the session token
    expires_at TEXT NOT NULL
);

ALTER TABLE enrollment_tokens ADD COLUMN burst INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN created_at TEXT;

CREATE TABLE burst_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    count      INTEGER NOT NULL,            -- desired instance count after the event
    created_at TEXT NOT NULL
);

UPDATE schema_version SET version = 5;
