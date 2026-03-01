SET search_path TO nseml, public;

-- Backfill schema drift between ORM and bootstrap SQL.
ALTER TABLE job_run
ADD COLUMN IF NOT EXISTS idempotency_key text;

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_run_idempotency
ON job_run(idempotency_key);
