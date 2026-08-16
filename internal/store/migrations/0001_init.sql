-- 0001_init.sql: M1 subset of docs/specs/tdd.md Appendix A.
--
-- workers, admin, sessions, secrets, and enrollment_tokens are not
-- created here; they ship as their own migrations at the milestones
-- that need them (M3, M5, M2, M5, M3 respectively), per tdd.md §6.2:
-- "a milestone's new tables or columns ship as a migration."

CREATE TABLE jobs (
    id            TEXT PRIMARY KEY,          -- random 128-bit hex
    source        TEXT NOT NULL,             -- 'github'
    external_id   INTEGER NOT NULL,
    repo          TEXT NOT NULL,
    run_id        INTEGER NOT NULL,
    labels        TEXT NOT NULL,             -- JSON array
    state         TEXT NOT NULL,
    attempt       INTEGER NOT NULL DEFAULT 0,
    worker_id     TEXT,
    dead_lettered INTEGER NOT NULL DEFAULT 0,
    reason        TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    UNIQUE (source, external_id)             -- webhook redelivery idempotency
);

CREATE TABLE transitions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     TEXT NOT NULL REFERENCES jobs(id),
    attempt    INTEGER NOT NULL,
    from_state TEXT NOT NULL,
    to_state   TEXT NOT NULL,
    reason     TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE schema_version (
    version INTEGER NOT NULL             -- single row, current migration number
);

INSERT INTO schema_version (version) VALUES (1);
