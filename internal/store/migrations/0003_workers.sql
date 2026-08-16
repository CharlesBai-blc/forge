-- 0003_workers.sql: enrolled machines and one-time enrollment tokens (FR-3, FR-18, FR-27).

CREATE TABLE workers (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    labels     TEXT NOT NULL,
    capacity   INTEGER NOT NULL,
    state      TEXT NOT NULL,
    burst      INTEGER NOT NULL DEFAULT 0,
    healthy    INTEGER NOT NULL DEFAULT 1,
    last_seen  TEXT NOT NULL,
    token_hash BLOB NOT NULL,
    arch       TEXT NOT NULL,
    version    TEXT NOT NULL
);

CREATE TABLE enrollment_tokens (
    token_hash BLOB PRIMARY KEY,
    expires_at TEXT NOT NULL,
    used_at    TEXT
);

UPDATE schema_version SET version = 3;
