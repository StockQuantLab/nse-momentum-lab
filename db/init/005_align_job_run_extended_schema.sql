BEGIN;

SET search_path TO nseml, public;

ALTER TABLE job_run
  ADD COLUMN IF NOT EXISTS job_kind text NOT NULL DEFAULT 'GENERIC',
  ADD COLUMN IF NOT EXISTS inputs_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS outputs_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS partition_scope jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS code_hash text;

CREATE INDEX IF NOT EXISTS idx_job_run_kind ON job_run(job_kind);

COMMIT;
