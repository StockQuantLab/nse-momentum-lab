-- nse-momentum-lab: initial schema bootstrap
-- NOTE: This is a starter DDL to unblock coding. Migrate to Alembic once app code exists.

BEGIN;

CREATE SCHEMA IF NOT EXISTS nseml;
SET search_path TO nseml, public;

-- Extensions (safe if already present)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ----------
-- Reference
-- ----------

CREATE TABLE IF NOT EXISTS ref_exchange_calendar (
  trading_date date PRIMARY KEY,
  is_trading_day boolean NOT NULL,
  notes text
);

CREATE TABLE IF NOT EXISTS ref_symbol (
  symbol_id bigserial PRIMARY KEY,
  symbol text NOT NULL,
  series text NOT NULL,
  isin text,
  name text,
  listing_date date,
  delisting_date date,
  status text NOT NULL DEFAULT 'ACTIVE',
  UNIQUE(symbol, series)
);

CREATE TABLE IF NOT EXISTS ref_symbol_alias (
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  vendor text NOT NULL,
  vendor_symbol text NOT NULL,
  valid_from date NOT NULL,
  valid_to date,
  PRIMARY KEY(symbol_id, vendor, vendor_symbol, valid_from)
);

-- ----------------
-- Market OHLCV data
-- ----------------

CREATE TABLE IF NOT EXISTS md_ohlcv_raw (
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  trading_date date NOT NULL,
  open_raw numeric(12,4) NOT NULL,
  high_raw numeric(12,4) NOT NULL,
  low_raw numeric(12,4) NOT NULL,
  close_raw numeric(12,4) NOT NULL,
  volume bigint NOT NULL,
  value_traded numeric(18,4),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(symbol_id, trading_date)
);

CREATE TABLE IF NOT EXISTS md_ohlcv_adj (
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  trading_date date NOT NULL,
  open_adj numeric(12,4) NOT NULL,
  high_adj numeric(12,4) NOT NULL,
  low_adj numeric(12,4) NOT NULL,
  close_adj numeric(12,4) NOT NULL,
  volume bigint NOT NULL,
  value_traded numeric(18,4),
  adj_factor numeric(12,6) NOT NULL DEFAULT 1.0,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(symbol_id, trading_date)
);

CREATE INDEX IF NOT EXISTS idx_md_ohlcv_raw_symbol_date ON md_ohlcv_raw(symbol_id, trading_date);
CREATE INDEX IF NOT EXISTS idx_md_ohlcv_adj_symbol_date ON md_ohlcv_adj(symbol_id, trading_date);

-- -----------------
-- Corporate actions
-- -----------------

CREATE TABLE IF NOT EXISTS ca_event (
  event_id bigserial PRIMARY KEY,
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  ex_date date NOT NULL,
  record_date date,
  action_type text NOT NULL, -- SPLIT | BONUS | RIGHTS | DIVIDEND
  ratio_num numeric,
  ratio_den numeric,
  cash_amount numeric,
  currency text NOT NULL DEFAULT 'INR',
  source_uri text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ca_event_symbol_ex_date ON ca_event(symbol_id, ex_date);
CREATE INDEX IF NOT EXISTS idx_ca_event_type_ex_date ON ca_event(action_type, ex_date);

-- -----
-- Scans
-- -----

CREATE TABLE IF NOT EXISTS scan_definition (
  scan_def_id bigserial PRIMARY KEY,
  name text NOT NULL,
  version text NOT NULL,
  config_json jsonb NOT NULL,
  code_sha text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS scan_run (
  scan_run_id bigserial PRIMARY KEY,
  scan_def_id bigint NOT NULL REFERENCES scan_definition(scan_def_id) ON DELETE CASCADE,
  asof_date date NOT NULL,
  dataset_hash text NOT NULL,
  status text NOT NULL,
  started_at timestamptz,
  finished_at timestamptz,
  logs_uri text,
  UNIQUE(scan_def_id, asof_date, dataset_hash)
);

CREATE TABLE IF NOT EXISTS scan_result (
  scan_run_id bigint NOT NULL REFERENCES scan_run(scan_run_id) ON DELETE CASCADE,
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  asof_date date NOT NULL,
  score numeric,
  passed boolean NOT NULL,
  reason_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(scan_run_id, symbol_id)
);

CREATE INDEX IF NOT EXISTS idx_scan_result_asof ON scan_result(asof_date);
CREATE INDEX IF NOT EXISTS idx_scan_run_asof_status ON scan_run(asof_date, status);

-- ----------------
-- Dataset manifest
-- ----------------

CREATE TABLE IF NOT EXISTS dataset_manifest (
  dataset_id bigserial PRIMARY KEY,
  dataset_kind text NOT NULL,
  dataset_hash text NOT NULL,
  code_hash text,
  params_hash text,
  source_uri text,
  row_count bigint,
  min_trading_date date,
  max_trading_date date,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(dataset_kind, dataset_hash, code_hash, params_hash)
);

CREATE INDEX IF NOT EXISTS idx_dataset_manifest_kind_created
  ON dataset_manifest(dataset_kind, created_at);

-- -------------------
-- Experiment registry
-- -------------------

CREATE TABLE IF NOT EXISTS exp_run (
  exp_run_id bigserial PRIMARY KEY,
  exp_hash text NOT NULL UNIQUE,
  strategy_name text NOT NULL,
  strategy_hash text NOT NULL,
  dataset_hash text NOT NULL,
  params_json jsonb NOT NULL,
  code_sha text,
  started_at timestamptz,
  finished_at timestamptz,
  status text NOT NULL
);

CREATE TABLE IF NOT EXISTS exp_metric (
  exp_run_id bigint NOT NULL REFERENCES exp_run(exp_run_id) ON DELETE CASCADE,
  metric_name text NOT NULL,
  metric_value numeric,
  PRIMARY KEY(exp_run_id, metric_name)
);

CREATE TABLE IF NOT EXISTS exp_artifact (
  exp_run_id bigint NOT NULL REFERENCES exp_run(exp_run_id) ON DELETE CASCADE,
  artifact_name text NOT NULL,
  uri text NOT NULL,
  sha256 text,
  PRIMARY KEY(exp_run_id, artifact_name)
);

CREATE INDEX IF NOT EXISTS idx_exp_run_strategy_dataset ON exp_run(strategy_hash, dataset_hash);

-- ----------------
-- Paper trading
-- ----------------

CREATE TABLE IF NOT EXISTS paper_session (
  session_id varchar(128) PRIMARY KEY,
  trade_date date,
  strategy_name text NOT NULL,
  experiment_id varchar(64),
  mode text NOT NULL,
  status text NOT NULL,
  symbols jsonb NOT NULL DEFAULT '[]'::jsonb,
  strategy_params jsonb NOT NULL DEFAULT '{}'::jsonb,
  risk_config jsonb NOT NULL DEFAULT '{}'::jsonb,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  finished_at timestamptz,
  archived_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_paper_session_status_trade_date
  ON paper_session(status, trade_date);

CREATE INDEX IF NOT EXISTS idx_paper_session_strategy_trade_date
  ON paper_session(strategy_name, trade_date);

CREATE TABLE IF NOT EXISTS signal (
  signal_id bigserial PRIMARY KEY,
  session_id varchar(128) REFERENCES paper_session(session_id) ON DELETE SET NULL,
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  asof_date date NOT NULL,
  strategy_hash varchar(64) NOT NULL,
  state text NOT NULL,
  entry_mode text NOT NULL, -- open|close
  planned_entry_date date,
  initial_stop numeric,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE signal ADD COLUMN IF NOT EXISTS session_id varchar(128);
CREATE INDEX IF NOT EXISTS idx_signal_session_state ON signal(session_id, state);
CREATE INDEX IF NOT EXISTS idx_signal_state_date ON signal(state, planned_entry_date);
CREATE INDEX IF NOT EXISTS idx_signal_session_state_entry_date
  ON signal(session_id, state, planned_entry_date);

CREATE TABLE IF NOT EXISTS paper_order (
  order_id bigserial PRIMARY KEY,
  session_id varchar(128) REFERENCES paper_session(session_id) ON DELETE SET NULL,
  broker_order_id varchar(64),
  signal_id bigint NOT NULL REFERENCES signal(signal_id) ON DELETE CASCADE,
  side text NOT NULL, -- BUY|SELL
  qty numeric NOT NULL,
  order_type text NOT NULL,
  limit_price numeric,
  status text NOT NULL,
  broker_status text,
  broker_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE paper_order ADD COLUMN IF NOT EXISTS session_id varchar(128);
ALTER TABLE paper_order ADD COLUMN IF NOT EXISTS broker_order_id varchar(64);
ALTER TABLE paper_order ADD COLUMN IF NOT EXISTS broker_status text;
ALTER TABLE paper_order ADD COLUMN IF NOT EXISTS broker_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX IF NOT EXISTS idx_paper_order_session_created
  ON paper_order(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_paper_order_broker_order_id
  ON paper_order(broker_order_id);

CREATE TABLE IF NOT EXISTS paper_fill (
  fill_id bigserial PRIMARY KEY,
  session_id varchar(128) REFERENCES paper_session(session_id) ON DELETE SET NULL,
  broker_trade_id varchar(64),
  broker_order_id varchar(64),
  order_id bigint NOT NULL REFERENCES paper_order(order_id) ON DELETE CASCADE,
  fill_time timestamptz NOT NULL,
  fill_price numeric NOT NULL,
  qty numeric NOT NULL,
  fees numeric,
  slippage_bps numeric,
  broker_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE paper_fill ADD COLUMN IF NOT EXISTS session_id varchar(128);
ALTER TABLE paper_fill ADD COLUMN IF NOT EXISTS broker_trade_id varchar(64);
ALTER TABLE paper_fill ADD COLUMN IF NOT EXISTS broker_order_id varchar(64);
ALTER TABLE paper_fill ADD COLUMN IF NOT EXISTS broker_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX IF NOT EXISTS idx_paper_fill_session_time
  ON paper_fill(session_id, fill_time);
CREATE INDEX IF NOT EXISTS idx_paper_fill_broker_trade_id
  ON paper_fill(broker_trade_id);
CREATE INDEX IF NOT EXISTS idx_paper_fill_broker_order_id
  ON paper_fill(broker_order_id);

CREATE TABLE IF NOT EXISTS paper_position (
  position_id bigserial PRIMARY KEY,
  session_id varchar(128) REFERENCES paper_session(session_id) ON DELETE SET NULL,
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  opened_at timestamptz NOT NULL,
  closed_at timestamptz,
  avg_entry numeric NOT NULL,
  avg_exit numeric,
  qty numeric NOT NULL,
  pnl numeric,
  state text NOT NULL,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS session_id varchar(128);
CREATE INDEX IF NOT EXISTS idx_paper_position_session_open
  ON paper_position(session_id, closed_at);

CREATE TABLE IF NOT EXISTS paper_session_signal (
  paper_session_signal_id bigserial PRIMARY KEY,
  session_id varchar(128) NOT NULL REFERENCES paper_session(session_id) ON DELETE CASCADE,
  signal_id bigint NOT NULL REFERENCES signal(signal_id) ON DELETE CASCADE,
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  asof_date date NOT NULL,
  rank integer,
  selection_score numeric,
  decision_status text NOT NULL,
  decision_reason text,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(session_id, signal_id)
);

CREATE INDEX IF NOT EXISTS idx_paper_session_signal_session_rank
  ON paper_session_signal(session_id, rank);

CREATE INDEX IF NOT EXISTS idx_paper_session_signal_session_status
  ON paper_session_signal(session_id, decision_status);

CREATE TABLE IF NOT EXISTS paper_order_event (
  event_id bigserial PRIMARY KEY,
  session_id varchar(128) NOT NULL REFERENCES paper_session(session_id) ON DELETE CASCADE,
  order_id bigint REFERENCES paper_order(order_id) ON DELETE SET NULL,
  signal_id bigint REFERENCES signal(signal_id) ON DELETE SET NULL,
  event_type text NOT NULL,
  event_status text NOT NULL,
  broker_order_id varchar(64),
  payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE paper_order_event ADD COLUMN IF NOT EXISTS broker_order_id varchar(64);
CREATE INDEX IF NOT EXISTS idx_paper_order_event_session_created
  ON paper_order_event(session_id, created_at);

CREATE INDEX IF NOT EXISTS idx_paper_order_event_session_type
  ON paper_order_event(session_id, event_type);
CREATE INDEX IF NOT EXISTS idx_paper_order_event_broker_order_id
  ON paper_order_event(broker_order_id);

CREATE TABLE IF NOT EXISTS paper_feed_state (
  session_id varchar(128) PRIMARY KEY REFERENCES paper_session(session_id) ON DELETE CASCADE,
  source text NOT NULL,
  mode text NOT NULL,
  status text NOT NULL,
  is_stale boolean NOT NULL DEFAULT false,
  subscription_count integer NOT NULL DEFAULT 0,
  heartbeat_at timestamptz,
  last_quote_at timestamptz,
  last_tick_at timestamptz,
  last_bar_at timestamptz,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_paper_feed_state_session_status
  ON paper_feed_state(session_id, status);

CREATE TABLE IF NOT EXISTS paper_bar_checkpoint (
  checkpoint_id bigserial PRIMARY KEY,
  session_id varchar(128) NOT NULL REFERENCES paper_session(session_id) ON DELETE CASCADE,
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  bar_interval text NOT NULL DEFAULT '5m',
  bar_start timestamptz NOT NULL,
  bar_end timestamptz,
  payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  processed boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(session_id, symbol_id, bar_interval, bar_start)
);

CREATE INDEX IF NOT EXISTS idx_paper_bar_checkpoint_session_time
  ON paper_bar_checkpoint(session_id, bar_start);

ALTER TABLE signal ADD COLUMN IF NOT EXISTS session_id varchar(128);
ALTER TABLE paper_order ADD COLUMN IF NOT EXISTS session_id varchar(128);
ALTER TABLE paper_order ADD COLUMN IF NOT EXISTS broker_order_id varchar(64);
ALTER TABLE paper_order ADD COLUMN IF NOT EXISTS broker_status text;
ALTER TABLE paper_order ADD COLUMN IF NOT EXISTS broker_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE paper_fill ADD COLUMN IF NOT EXISTS session_id varchar(128);
ALTER TABLE paper_fill ADD COLUMN IF NOT EXISTS broker_trade_id varchar(64);
ALTER TABLE paper_fill ADD COLUMN IF NOT EXISTS broker_order_id varchar(64);
ALTER TABLE paper_fill ADD COLUMN IF NOT EXISTS broker_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE paper_order_event ADD COLUMN IF NOT EXISTS broker_order_id varchar(64);
ALTER TABLE paper_position ADD COLUMN IF NOT EXISTS session_id varchar(128);
ALTER TABLE paper_session
  ALTER COLUMN session_id TYPE varchar(128),
  ALTER COLUMN experiment_id TYPE varchar(64);
ALTER TABLE signal
  ALTER COLUMN session_id TYPE varchar(128),
  ALTER COLUMN strategy_hash TYPE varchar(64);
ALTER TABLE paper_order
  ALTER COLUMN session_id TYPE varchar(128),
  ALTER COLUMN broker_order_id TYPE varchar(64);
ALTER TABLE paper_fill
  ALTER COLUMN session_id TYPE varchar(128),
  ALTER COLUMN broker_trade_id TYPE varchar(64),
  ALTER COLUMN broker_order_id TYPE varchar(64);
ALTER TABLE paper_position
  ALTER COLUMN session_id TYPE varchar(128);
ALTER TABLE paper_session_signal
  ALTER COLUMN session_id TYPE varchar(128);
ALTER TABLE paper_order_event
  ALTER COLUMN session_id TYPE varchar(128),
  ALTER COLUMN broker_order_id TYPE varchar(64);
ALTER TABLE paper_feed_state
  ALTER COLUMN session_id TYPE varchar(128);
ALTER TABLE paper_bar_checkpoint
  ALTER COLUMN session_id TYPE varchar(128);

-- -----------------------------
-- Job runs + backtest trade log
-- -----------------------------

CREATE TABLE IF NOT EXISTS job_run (
  job_run_id bigserial PRIMARY KEY,
  job_name text NOT NULL,
  job_kind text NOT NULL DEFAULT 'GENERIC',
  asof_date date NOT NULL,
  idempotency_key text,
  dataset_hash text,
  inputs_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  outputs_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  partition_scope jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  duration_ms bigint,
  logs_uri text,
  metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  code_hash text
);

ALTER TABLE job_run ADD COLUMN IF NOT EXISTS job_kind text NOT NULL DEFAULT 'GENERIC';
ALTER TABLE job_run ADD COLUMN IF NOT EXISTS partition_scope jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE job_run ADD COLUMN IF NOT EXISTS metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE job_run ADD COLUMN IF NOT EXISTS error_json jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE job_run ADD COLUMN IF NOT EXISTS code_hash text;
CREATE INDEX IF NOT EXISTS idx_job_run_name_date ON job_run(job_name, asof_date);
CREATE INDEX IF NOT EXISTS idx_job_run_status_date ON job_run(status, asof_date);
CREATE INDEX IF NOT EXISTS idx_job_run_kind ON job_run(job_kind);

CREATE TABLE IF NOT EXISTS bt_trade (
  trade_id bigserial PRIMARY KEY,
  exp_run_id bigint NOT NULL REFERENCES exp_run(exp_run_id) ON DELETE CASCADE,
  symbol_id bigint NOT NULL REFERENCES ref_symbol(symbol_id) ON DELETE CASCADE,
  entry_date date NOT NULL,
  entry_price numeric NOT NULL,
  entry_mode text NOT NULL, -- open|close
  qty numeric NOT NULL,
  initial_stop numeric,
  exit_date date,
  exit_price numeric,
  pnl numeric,
  pnl_r numeric,
  fees numeric,
  slippage_bps numeric,
  mfe_r numeric,
  mae_r numeric,
  exit_reason text,
  exit_rule_version text NOT NULL,
  scan_run_id bigint,
  reason_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bt_trade_exp_entry ON bt_trade(exp_run_id, entry_date);
CREATE INDEX IF NOT EXISTS idx_bt_trade_exit_reason ON bt_trade(exit_reason);

-- --------
-- Rollups
-- --------

CREATE TABLE IF NOT EXISTS rpt_scan_daily (
  asof_date date NOT NULL,
  scan_def_id bigint NOT NULL REFERENCES scan_definition(scan_def_id) ON DELETE CASCADE,
  dataset_hash text NOT NULL,
  total_universe int NOT NULL,
  passed_base_4p int NOT NULL,
  passed_2lynch int NOT NULL,
  passed_final int NOT NULL,
  by_fail_reason jsonb NOT NULL DEFAULT '{}'::jsonb,
  by_liquidity_bucket jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(asof_date, scan_def_id, dataset_hash)
);

CREATE TABLE IF NOT EXISTS rpt_bt_daily (
  asof_date date NOT NULL,
  strategy_name text NOT NULL,
  dataset_hash text NOT NULL,
  entry_mode text NOT NULL, -- open|close
  signals int NOT NULL,
  entries int NOT NULL,
  exits int NOT NULL,
  wins int NOT NULL,
  losses int NOT NULL,
  win_rate numeric,
  avg_r numeric,
  profit_factor numeric,
  max_dd numeric,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(asof_date, strategy_name, dataset_hash, entry_mode)
);

CREATE TABLE IF NOT EXISTS rpt_bt_failure_daily (
  asof_date date NOT NULL,
  strategy_name text NOT NULL,
  dataset_hash text NOT NULL,
  entry_mode text NOT NULL,
  exit_reason text NOT NULL,
  count int NOT NULL,
  avg_r numeric,
  median_r numeric,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(asof_date, strategy_name, dataset_hash, entry_mode, exit_reason)
);

COMMIT;
