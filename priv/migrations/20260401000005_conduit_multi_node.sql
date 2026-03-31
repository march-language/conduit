-- Migration: Phase 5 — Multi-Node Coordination
-- Creates conduit_workers table for node registration and heartbeating.
-- The leader advisory lock (ID 8473625) is session-level and needs no DDL.

CREATE TABLE IF NOT EXISTS conduit_workers (
  id           TEXT        NOT NULL,
  hostname     TEXT        NOT NULL DEFAULT '',
  pid          INTEGER     NOT NULL DEFAULT 0,
  queues       TEXT        NOT NULL DEFAULT '',  -- comma-separated queue names
  pool_size    INTEGER     NOT NULL DEFAULT 0,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status       TEXT        NOT NULL DEFAULT 'running'
                           CHECK (status IN ('running', 'stopping', 'stopped')),
  stopped_at   TIMESTAMPTZ,
  PRIMARY KEY (id)
);

-- Fast lookup for active nodes (heartbeat monitoring, stale detection).
CREATE INDEX IF NOT EXISTS idx_conduit_workers_active
  ON conduit_workers (last_seen_at)
  WHERE status = 'running';

-- ─────────────────────────────────────────────────────────────
-- Stored procedure: clean up stale workers and reclaim their jobs.
-- Call this from a maintenance job or from the application's stale detector.
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION conduit_cleanup_stale_workers(stale_threshold INTERVAL)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  cleaned INTEGER;
BEGIN
  -- Reclaim in-flight jobs from stale workers.
  UPDATE conduit_jobs
  SET status    = 'pending',
      worker_id = NULL,
      run_at    = NOW()
  WHERE status = 'running'
    AND worker_id IN (
      SELECT id FROM conduit_workers
      WHERE status = 'running'
        AND last_seen_at < NOW() - stale_threshold
    );

  -- Mark stale workers as stopped.
  WITH stale AS (
    UPDATE conduit_workers
    SET status = 'stopped', stopped_at = NOW()
    WHERE status = 'running'
      AND last_seen_at < NOW() - stale_threshold
    RETURNING id
  )
  SELECT COUNT(*) INTO cleaned FROM stale;

  RETURN cleaned;
END;
$$;

-- Convenience view: all currently active nodes with their job counts.
CREATE OR REPLACE VIEW conduit_cluster_status AS
SELECT
  w.id,
  w.hostname,
  w.pid,
  w.queues,
  w.pool_size,
  w.started_at,
  w.last_seen_at,
  w.status,
  COUNT(j.id) FILTER (WHERE j.status = 'running') AS running_jobs,
  COUNT(j.id) FILTER (WHERE j.status = 'pending') AS pending_jobs
FROM conduit_workers w
LEFT JOIN conduit_jobs j ON j.worker_id = w.id
WHERE w.status = 'running'
GROUP BY w.id, w.hostname, w.pid, w.queues, w.pool_size,
         w.started_at, w.last_seen_at, w.status;
