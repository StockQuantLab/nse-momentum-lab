SET search_path TO nseml, public;

-- Backfill schema drift for legacy databases created before created_at columns.
ALTER TABLE md_ohlcv_raw
ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE md_ohlcv_adj
ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();
