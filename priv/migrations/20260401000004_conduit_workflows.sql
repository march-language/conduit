-- Migration: Phase 4 — Imperative Workflows & Checkpoints
-- Creates conduit_workflows, conduit_checkpoints, and conduit_workflow_signals tables.
-- Also adds workflow_id FK to conduit_jobs for associating jobs with workflows.

CREATE TABLE IF NOT EXISTS conduit_workflows (
  id            TEXT        NOT NULL,
  workflow_type TEXT        NOT NULL,
  parent_id     TEXT        REFERENCES conduit_workflows(id) ON DELETE SET NULL,
  input         JSONB       NOT NULL DEFAULT '{}',
  output        JSONB,
  status        TEXT        NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'completed', 'failed', 'cancelled', 'timed_out')),
  started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at  TIMESTAMPTZ,
  timeout_at    TIMESTAMPTZ,
  error         TEXT,
  error_at      TIMESTAMPTZ,
  tags          TEXT        NOT NULL DEFAULT '',   -- comma-separated tags
  meta          JSONB       NOT NULL DEFAULT '{}',
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS conduit_checkpoints (
  id          BIGSERIAL   NOT NULL,
  workflow_id TEXT        NOT NULL REFERENCES conduit_workflows(id) ON DELETE CASCADE,
  name        TEXT        NOT NULL,
  value       JSONB       NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  duration_ms INTEGER,
  PRIMARY KEY (id),
  UNIQUE (workflow_id, name)
);

CREATE TABLE IF NOT EXISTS conduit_workflow_signals (
  id           BIGSERIAL   NOT NULL,
  workflow_id  TEXT        NOT NULL REFERENCES conduit_workflows(id) ON DELETE CASCADE,
  signal_type  TEXT        NOT NULL,
  payload      JSONB       NOT NULL DEFAULT '{}',
  status       TEXT        NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending', 'delivered', 'expired')),
  sent_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  delivered_at TIMESTAMPTZ,
  expires_at   TIMESTAMPTZ,
  sent_by      TEXT,
  PRIMARY KEY (id)
);

-- Associate individual jobs with the workflow that spawned them (optional FK).
ALTER TABLE conduit_jobs
  ADD COLUMN IF NOT EXISTS workflow_id TEXT REFERENCES conduit_workflows(id) ON DELETE CASCADE;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_conduit_workflows_status
  ON conduit_workflows (status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_conduit_workflows_type
  ON conduit_workflows (workflow_type, status);

CREATE INDEX IF NOT EXISTS idx_conduit_checkpoints_workflow
  ON conduit_checkpoints (workflow_id);

CREATE INDEX IF NOT EXISTS idx_conduit_signals_workflow
  ON conduit_workflow_signals (workflow_id, status);

CREATE INDEX IF NOT EXISTS idx_conduit_signals_pending
  ON conduit_workflow_signals (workflow_id, signal_type)
  WHERE status = 'pending';

-- Expire stale signals: run periodically (dashboard or maintenance job).
CREATE OR REPLACE FUNCTION conduit_expire_signals()
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  cnt INTEGER;
BEGIN
  WITH expired AS (
    UPDATE conduit_workflow_signals
    SET status = 'expired'
    WHERE status = 'pending'
      AND expires_at IS NOT NULL
      AND expires_at < NOW()
    RETURNING id
  )
  SELECT COUNT(*) INTO cnt FROM expired;
  RETURN cnt;
END;
$$;
