-- nse-momentum-lab: Performance indexes for Phase 2 optimization
-- These indexes speed up common dashboard and API queries

-- Speed up experiment filtering by status and date
CREATE INDEX IF NOT EXISTS idx_exp_run_status_started
  ON nseml.exp_run (status, started_at DESC);

-- Speed up trade analytics queries by experiment and exit date
CREATE INDEX IF NOT EXISTS idx_bt_trade_exp_exit_date
  ON nseml.bt_trade (exp_run_id, exit_reason, entry_date DESC);

-- Speed up scan result filtering by run, pass status, and score
CREATE INDEX IF NOT EXISTS idx_scan_result_run_passed_score
  ON nseml.scan_result (scan_run_id, passed, score DESC);

-- Speed up job status polling for dashboard
CREATE INDEX IF NOT EXISTS idx_job_run_status_started
  ON nseml.job_run (status, started_at DESC);

-- Speed up symbol lookups in scans and trades
CREATE INDEX IF NOT EXISTS idx_ref_symbol_status_series
  ON nseml.ref_symbol (status, series);

-- Speed up experiment lookups by hash
CREATE INDEX IF NOT EXISTS idx_exp_run_hash
  ON nseml.exp_run (exp_hash);

-- Speed up scan run lookups by date
CREATE INDEX IF NOT EXISTS idx_scan_run_asof_date
  ON nseml.scan_run (asof_date DESC);

-- Speed up paper trading position queries
CREATE INDEX IF NOT EXISTS idx_paper_position_symbol
  ON nseml.paper_position (symbol_id);
CREATE INDEX IF NOT EXISTS idx_paper_position_state_opened
  ON nseml.paper_position (state, opened_at DESC);
