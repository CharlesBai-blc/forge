-- 0004_worker_prev_state_job_runner.sql: lost-worker revival state and
-- per-attempt JIT runner persistence (FR-5, FR-19, FR-20).
--
-- prev_state records the operational state a worker held before going
-- lost, so a resumed heartbeat restores cordon and drain intent.
-- runner_id persists the provider-side JIT registration for the job's
-- current attempt, so unregistration survives a control-plane restart.

ALTER TABLE workers ADD COLUMN prev_state TEXT;
ALTER TABLE jobs ADD COLUMN runner_id INTEGER;

UPDATE schema_version SET version = 4;
