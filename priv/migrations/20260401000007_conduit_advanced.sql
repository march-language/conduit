-- Phase 7: Advanced Features — Unique Jobs & Rate Limiting
--
-- Unique job fingerprints: the unique_key column stores a SHA-256 hash of
-- (job_type + selected payload fields). A partial unique index ensures only
-- one active (non-completed, non-dead) job per fingerprint at a time.
-- When a job completes or dies the key is NULLed, freeing the slot.
--
-- Rate limit buckets: token bucket per key. Each row tracks the current token
-- count and last refill timestamp. The rate_limit_acquire storage method
-- atomically refills + consumes in a single upsert.

-- 1. Unique job fingerprints
ALTER TABLE conduit_jobs ADD COLUMN unique_key TEXT;

CREATE UNIQUE INDEX idx_conduit_jobs_unique_key
  ON conduit_jobs (unique_key)
  WHERE unique_key IS NOT NULL
    AND unique_key != ''
    AND status NOT IN ('completed', 'dead');

-- 2. Rate limit buckets (token bucket per key)
CREATE TABLE conduit_rate_limit_buckets (
  key         TEXT          NOT NULL,
  tokens      DECIMAL       NOT NULL DEFAULT 0,
  last_refill TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  PRIMARY KEY (key)
);

-- Index for cleanup of stale buckets (optional maintenance)
CREATE INDEX idx_conduit_rate_limit_stale
  ON conduit_rate_limit_buckets (last_refill)
  WHERE last_refill < NOW() - INTERVAL '24 hours';
