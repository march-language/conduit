-- Conduit — complete Postgres schema for Conduit.Storage.Postgres
--
-- This is a single-file bootstrap script that creates all tables, indexes,
-- constraints, and triggers needed by the Conduit.Storage.Postgres backend.
--
-- Schema conventions
--   • Timestamps are stored as BIGINT epoch-milliseconds.  The March runtime
--     represents DateTime values as epoch-ms integers, so no conversion layer
--     is needed in the storage implementation.
--   • JSON lists (errors, tags, queues) are stored as TEXT containing a JSON
--     array (e.g. '["foo","bar"]').  Postgres validates them as TEXT; the
--     March side encodes/decodes with pencode_list / pdecode_list.
--   • All other fields are TEXT or INTEGER / BIGINT / DECIMAL as appropriate.
--
-- If you prefer to use the incremental migration DSL instead, run the
-- numbered *.march files in priv/migrations/ in order.  This script is an
-- alternative bootstrap path for fresh deployments using the Postgres backend.
--
-- Usage:
--   psql -d mydb -f priv/migrations/001_conduit_schema.sql

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Jobs
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_jobs (
  id                TEXT        PRIMARY KEY,
  queue             TEXT        NOT NULL DEFAULT 'default',
  job_type          TEXT        NOT NULL,
  payload           TEXT        NOT NULL DEFAULT '{}',
  status            TEXT        NOT NULL DEFAULT 'pending',
  attempt           INTEGER     NOT NULL DEFAULT 0,
  max_attempts      INTEGER     NOT NULL DEFAULT 3,
  priority          INTEGER     NOT NULL DEFAULT 0,
  backoff           TEXT        NOT NULL DEFAULT 'exponential',
  dead_letter_queue TEXT,
  inserted_at       BIGINT      NOT NULL,
  scheduled_at      BIGINT      NOT NULL,
  run_at            BIGINT      NOT NULL,
  started_at        BIGINT,
  completed_at      BIGINT,
  heartbeat_at      BIGINT,
  worker_id         TEXT,
  unique_key        TEXT,
  errors            TEXT        NOT NULL DEFAULT '[]',
  tags              TEXT        NOT NULL DEFAULT '[]'
);

-- Primary poll index: fetch_next uses (queue, status, priority DESC, run_at ASC)
-- with FOR UPDATE SKIP LOCKED.  The partial predicate keeps the index small.
CREATE INDEX IF NOT EXISTS idx_conduit_jobs_poll
  ON conduit_jobs (queue, status, priority DESC, run_at ASC)
  WHERE status IN ('pending', 'snoozed');

-- Rescue-stale index: find running jobs whose heartbeat has gone quiet.
CREATE INDEX IF NOT EXISTS idx_conduit_jobs_heartbeat
  ON conduit_jobs (heartbeat_at, status)
  WHERE status = 'running';

-- Node-reclaim index: find jobs owned by a dead worker node.
CREATE INDEX IF NOT EXISTS idx_conduit_jobs_worker
  ON conduit_jobs (worker_id, status)
  WHERE status = 'running';

-- Dashboard status filter.
CREATE INDEX IF NOT EXISTS idx_conduit_jobs_status
  ON conduit_jobs (status, inserted_at);

-- Unique-job fingerprint: one active job per key at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_conduit_jobs_unique_key
  ON conduit_jobs (unique_key)
  WHERE unique_key IS NOT NULL AND status NOT IN ('completed', 'dead');

-- LISTEN/NOTIFY trigger: wake waiting workers on every new job insert.
CREATE OR REPLACE FUNCTION conduit_notify_new_job()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('conduit_queue_' || NEW.queue, NEW.id);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS conduit_jobs_notify ON conduit_jobs;
CREATE TRIGGER conduit_jobs_notify
  AFTER INSERT ON conduit_jobs
  FOR EACH ROW EXECUTE FUNCTION conduit_notify_new_job();

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Dead letters
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_dead_letters (
  id                 TEXT     PRIMARY KEY,
  job_id             TEXT     NOT NULL UNIQUE,
  queue              TEXT     NOT NULL,
  job_type           TEXT     NOT NULL,
  payload            TEXT     NOT NULL DEFAULT '{}',
  errors             TEXT     NOT NULL DEFAULT '[]',
  attempts           INTEGER  NOT NULL DEFAULT 0,
  first_attempted_at BIGINT,
  last_attempted_at  BIGINT,
  discarded_at       BIGINT   NOT NULL,
  reviewed           BOOLEAN  NOT NULL DEFAULT FALSE,
  reviewed_by        TEXT,
  reviewed_at        BIGINT,
  retry_count        INTEGER  NOT NULL DEFAULT 0
);

-- List dead letters by queue, newest first.
CREATE INDEX IF NOT EXISTS idx_conduit_dead_letters_queue
  ON conduit_dead_letters (queue, discarded_at DESC);

-- Dashboard: unreviewed dead letters.
CREATE INDEX IF NOT EXISTS idx_conduit_dead_letters_unreviewed
  ON conduit_dead_letters (discarded_at DESC)
  WHERE reviewed = FALSE;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Cron schedules
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_cron_schedules (
  id          TEXT     PRIMARY KEY,
  job_type    TEXT     NOT NULL,
  schedule    TEXT     NOT NULL,
  timezone    TEXT     NOT NULL DEFAULT 'UTC',
  queue       TEXT     NOT NULL DEFAULT 'default',
  payload     TEXT     NOT NULL DEFAULT '{}',
  overlap     TEXT     NOT NULL DEFAULT 'skip',
  enabled     BOOLEAN  NOT NULL DEFAULT TRUE,
  last_run_at BIGINT,
  last_job_id TEXT,
  next_run_at BIGINT,
  tags        TEXT     NOT NULL DEFAULT '[]'
);

-- Fast lookup for the scheduler tick: all enabled schedules that are due.
CREATE INDEX IF NOT EXISTS idx_conduit_cron_due
  ON conduit_cron_schedules (next_run_at)
  WHERE enabled = TRUE;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Worker nodes
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_workers (
  id           TEXT     PRIMARY KEY,
  hostname     TEXT     NOT NULL DEFAULT '',
  pid          INTEGER  NOT NULL DEFAULT 0,
  queues       TEXT     NOT NULL DEFAULT '[]',
  pool_size    INTEGER  NOT NULL DEFAULT 0,
  started_at   BIGINT   NOT NULL,
  last_seen_at BIGINT   NOT NULL,
  status       TEXT     NOT NULL DEFAULT 'running',
  stopped_at   BIGINT
);

-- Heartbeat monitoring and stale-node detection.
CREATE INDEX IF NOT EXISTS idx_conduit_workers_active
  ON conduit_workers (last_seen_at)
  WHERE status = 'running';

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Workflows
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_workflows (
  id            TEXT   PRIMARY KEY,
  workflow_type TEXT   NOT NULL,
  parent_id     TEXT   REFERENCES conduit_workflows (id),
  input         TEXT   NOT NULL DEFAULT '{}',
  output        TEXT,
  status        TEXT   NOT NULL DEFAULT 'running',
  started_at    BIGINT NOT NULL,
  completed_at  BIGINT,
  timeout_at    BIGINT,
  error         TEXT,
  error_at      BIGINT,
  tags          TEXT   NOT NULL DEFAULT '[]',
  meta          TEXT   NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_conduit_workflows_status
  ON conduit_workflows (status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_conduit_workflows_type
  ON conduit_workflows (workflow_type, status);

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. Checkpoints
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_checkpoints (
  id          BIGSERIAL PRIMARY KEY,
  workflow_id TEXT      NOT NULL REFERENCES conduit_workflows (id),
  name        TEXT      NOT NULL,
  value       TEXT      NOT NULL,
  created_at  BIGINT    NOT NULL,
  duration_ms BIGINT,
  UNIQUE (workflow_id, name)
);

CREATE INDEX IF NOT EXISTS idx_conduit_checkpoints_workflow
  ON conduit_checkpoints (workflow_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. Workflow signals
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_workflow_signals (
  id           BIGSERIAL PRIMARY KEY,
  workflow_id  TEXT      NOT NULL REFERENCES conduit_workflows (id),
  signal_type  TEXT      NOT NULL,
  payload      TEXT      NOT NULL DEFAULT '{}',
  status       TEXT      NOT NULL DEFAULT 'pending',
  sent_at      BIGINT    NOT NULL,
  delivered_at BIGINT,
  expires_at   BIGINT,
  sent_by      TEXT
);

-- Look up pending signals for a specific workflow + type.
CREATE INDEX IF NOT EXISTS idx_conduit_signals_pending
  ON conduit_workflow_signals (workflow_id, signal_type)
  WHERE status = 'pending';

-- General signal listing by workflow.
CREATE INDEX IF NOT EXISTS idx_conduit_signals_workflow
  ON conduit_workflow_signals (workflow_id, status);

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. Workflow event store (deterministic replay)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_workflow_events (
  id          BIGSERIAL PRIMARY KEY,
  workflow_id TEXT      NOT NULL REFERENCES conduit_workflows (id),
  sequence    INTEGER   NOT NULL,
  event_type  TEXT      NOT NULL,
  payload     TEXT      NOT NULL DEFAULT '{}',
  recorded_at BIGINT    NOT NULL,
  UNIQUE (workflow_id, sequence)
);

-- Load events in replay order (covered by the unique index above, but an
-- explicit non-unique index lets the planner pick it for ORDER BY without
-- deduplication overhead on re-scans).
CREATE INDEX IF NOT EXISTS idx_conduit_events_workflow
  ON conduit_workflow_events (workflow_id, sequence ASC);

-- ─────────────────────────────────────────────────────────────────────────────
-- 9. Rate-limit token buckets
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conduit_rate_limit_buckets (
  key         TEXT    PRIMARY KEY,
  tokens      DECIMAL NOT NULL DEFAULT 0,
  last_refill BIGINT  NOT NULL
);

-- Allow periodic cleanup of buckets that haven't been touched in 24 h
-- (86_400_000 ms).  Conduit does not run this automatically; operators may
-- schedule it via a cron job or pg_cron extension.
CREATE INDEX IF NOT EXISTS idx_conduit_rate_limit_stale
  ON conduit_rate_limit_buckets (last_refill);
