-- Migration: Phase 3 — Cron Scheduling
-- Creates conduit_cron_schedules table for storing static and dynamic cron configs.

CREATE TABLE IF NOT EXISTS conduit_cron_schedules (
  id          TEXT        NOT NULL PRIMARY KEY,   -- cron_type name for static, UUID for dynamic
  job_type    TEXT        NOT NULL,               -- matches registered performer key
  schedule    TEXT        NOT NULL,               -- 5-field cron expression, e.g. "0 * * * *"
  timezone    TEXT        NOT NULL DEFAULT 'UTC', -- IANA timezone name
  queue       TEXT        NOT NULL DEFAULT 'default',
  payload     TEXT        NOT NULL DEFAULT '{}',  -- JSON payload for CronWithArgs
  overlap     TEXT        NOT NULL DEFAULT 'skip' CHECK (overlap IN ('skip', 'allow', 'replace')),
  enabled     BOOLEAN     NOT NULL DEFAULT TRUE,
  last_run_at TIMESTAMPTZ,
  last_job_id TEXT        REFERENCES conduit_jobs(id) ON DELETE SET NULL,
  next_run_at TIMESTAMPTZ,
  tags        TEXT        NOT NULL DEFAULT '',    -- comma-separated tags
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast lookup for the scheduler tick: all enabled schedules that are due.
CREATE INDEX IF NOT EXISTS idx_conduit_cron_due
  ON conduit_cron_schedules (next_run_at)
  WHERE enabled = TRUE;

-- Keep updated_at in sync automatically.
CREATE OR REPLACE FUNCTION conduit_cron_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS conduit_cron_updated_at ON conduit_cron_schedules;
CREATE TRIGGER conduit_cron_updated_at
  BEFORE UPDATE ON conduit_cron_schedules
  FOR EACH ROW EXECUTE FUNCTION conduit_cron_set_updated_at();
