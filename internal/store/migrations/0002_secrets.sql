-- 0002_secrets.sql: encrypted credentials at rest (FR-27, tdd.md §7).

CREATE TABLE secrets (
    name       TEXT PRIMARY KEY,
    ciphertext BLOB NOT NULL,
    updated_at TEXT NOT NULL
);

UPDATE schema_version SET version = 2;
