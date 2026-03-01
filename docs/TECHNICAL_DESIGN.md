# nse-momentum-lab — Technical Design (Implementation-Ready)

This document expands the high-level overview + ADRs into a concrete technical design that a developer can implement quickly.

Scope: EOD-only (daily bars) ingestion, adjustment, scans, backtests, walk-forward evaluation, experiment registry, paper trading, monitoring, and a basic dashboard.

## 1) Principles and Constraints

### 1.0 Target platform versions

This project is local-only, but we still pin and record versions for reproducibility.

Target versions (as requested):
- Python: 3.14 (or latest stable available when implementation starts)
- PostgreSQL: 18 (or latest stable available when implementation starts)

Implementation note:
- If PostgreSQL 18 is not yet GA in your environment, use the latest GA (e.g. 17) and keep SQL/migrations compatible.
- Record actual runtime versions used in every experiment run (already required by §1.2).

### 1.1 Determinism
- All price math, indicators, and portfolio simulation must be deterministic and reproducible.
- LLMs are allowed for: reasoning about logs/results, summarization, orchestration suggestions.
- LLMs are *not* allowed to compute/transform price series or “decide” trades without storing a deterministic decision trace.

### 1.2 Reproducibility
Every run (scan/backtest/walk-forward/paper-trade evaluation) must be reproducible from:
- dataset version (hash)
- strategy definition (hash)
- parameters (serialized)
- code version (git SHA)
- execution environment info (package versions)

### 1.3 Survivorship-bias awareness
- Symbol lifecycle (listed → suspended → delisted) is tracked.
- Universe selection at time *t* must not peek into the future.

### 1.4 India-specific trading reality
- Liquidity is uneven; costs must be modeled conservatively.
- Corporate actions (splits/bonus/rights/dividends) materially change backtest results.
- Circuits/illiquidity can create unrealistic fills; simulation must guard against “fills you can’t get”.

### 1.5 Locked Phase 1 choices
- Universe: NSE cash equities only (NSE-EQ)
- Backtesting: EOD-only (daily bars)
- Live monitoring: later phase (will use broker feed)
- Broker integration target (later phase): Zerodha only for now
- Liquidity thresholds: implemented as optional parameters, not hard-coded until analysis

Additional Phase 1 locks (research protocol):
- Entry timing: run BOTH next-open and same-day-close entry variants in parallel
  - next-open is the primary, conservative baseline
  - same-day-close is a research control to measure overnight signal decay
- 2LYNCH `L` and `C`: start with STRICT numeric proxies (narrow bands, fewer trades)
- Dividends: stored as events only; do not apply dividend price adjustment in Phase 1 core series; TRI can be added later
- Slippage: configurable bps per fill by liquidity bucket (20-day rolling traded value)

## 2) System Architecture

### 2.1 Services (Docker Compose)
Minimum set:
1. **PostgreSQL**: system of record.
2. **MinIO**: raw + derived artifacts.
3. **Ingestion Worker**: processes vendor candle files (Zerodha), parses, writes to DB, archives raw to MinIO.
4. **Adjustment Worker**: applies corporate actions to build adjusted OHLCV tables.
5. **Scanner Worker**: runs breakout/anticipation scans and stores results.
6. **Backtest Worker**: runs vectorbt backtests and walk-forward sweeps.
7. **Experiment Registry Service**: library + API endpoints for querying prior results.
8. **Paper Trading Worker**: consumes signals → simulates fills → updates ledger → emits alerts.
9. **FastAPI Backend**: read APIs for dashboard and integrations.
10. **NiceGUI Dashboard**: shows pipeline status, candidates, registry, paper PnL, alarms.
11. **Scheduler**: APScheduler (or cron inside worker) triggers daily jobs.
12. **Monitoring**: Prometheus (and optional Grafana) + alert sender.

### 2.2 Data Flow (EOD)
1. Download vendor candle files from Zerodha (manual) → store raw CSV in MinIO.
2. Parse → store normalized raw OHLCV + reference tables in Postgres.
3. Validate quality checks → quarantine failing days.
4. Apply corporate actions → store adjusted OHLCV.
5. Compute features (returns, volatility, RS, trend context) → store feature tables.
6. Run scans (4% breakout + ANT/FHP) → store scan candidates with explanation columns.
7. Backtest candidate selection logic + exits → store results + artifacts.
8. Walk-forward evaluation (rolling windows) → store summary metrics.
9. If eligible → create paper-trade signals, simulate entries/exits.
10. Monitoring compares expected vs actual behavior; alerts on drift.

### 2.3 Key Boundaries
- Ingestion produces *raw* tables.
- Adjustment produces *adjusted* tables.
- Scans operate on adjusted tables by default (configurable).
- Backtests consume scan outputs OR operate directly on all symbols, depending on experiment.

### 2.4 UI framework choice (analysis dashboard)

Phase 1 implementation: **NiceGUI** (migrated from Streamlit on 2026-03-01).

Why NiceGUI was chosen:
- Persistent server-side state (no page re-runs on every interaction)
- DuckDB-friendly: Single connection works perfectly without threading issues
- Fast startup: <1s vs Streamlit's 5-10s
- Modern reactive UI with Vue.js frontend and WebSocket reactivity
- Python 3.14 compatible without compilation

See `docs/NICEGUI_MIGRATION_PLAN.md` for migration rationale and `docs/adr/ADR-011-dashboard-architecture.md` for current architecture.

## 3) Storage Design (Postgres + MinIO)

### 3.1 MinIO Layout
Follow ADR-005 + ADR-014.

Raw:
- `market-data/zerodha/{vendor}/{timeframe}/...`
- `market-data/nse/symbols/...`
- `market-data/nse/corp-actions/{type}/...`

Artifacts (content-addressed):
- `artifacts/experiments/{experiment_hash}/equity.parquet`
- `artifacts/experiments/{experiment_hash}/trades.parquet`
- `artifacts/experiments/{experiment_hash}/charts/...`
- `artifacts/datasets/{dataset_hash}/snapshot.parquet`

### 3.2 Postgres Schema (Core Tables)
Naming: snake_case, use `timestamptz` for event time, `date` for trading_date.

#### 3.2.1 Reference tables
- `ref_exchange_calendar(trading_date date primary key, is_trading_day bool, notes text)`
- `ref_symbol(symbol_id bigserial pk, symbol text, series text, isin text, name text, listing_date date, delisting_date date null, status text)`
- `ref_symbol_alias(symbol_id fk, vendor text, vendor_symbol text, valid_from date, valid_to date)`

#### 3.2.2 Raw market data
Partition monthly by trading_date:
- `md_ohlcv_raw(symbol_id, trading_date, open, high, low, close, volume, value_traded, trades_count, vwap, source_file_uri, ingest_run_id, primary key(symbol_id, trading_date))`

#### 3.2.3 Corporate actions
- `ca_event(event_id bigserial pk, symbol_id, ex_date date, record_date date null, action_type text, ratio_num numeric, ratio_den numeric, cash_amount numeric, currency text default 'INR', source_uri text, created_at timestamptz)`

#### 3.2.4 Adjusted OHLCV
- `md_ohlcv_adj(symbol_id, trading_date, open_adj, high_adj, low_adj, close_adj, volume, value_traded, adj_factor numeric, primary key(symbol_id, trading_date))`

Design note:
- Keep volume unadjusted unless you explicitly define volume adjustment rules; store `adj_factor` to reproduce prices.

#### 3.2.5 Derived features
Partition monthly:
- `feat_daily(symbol_id, trading_date, ret_1d, ret_5d, atr_20, range_pct, close_pos_in_range, ma_20, ma_65, rs_252, vol_20, dollar_vol_20, primary key(symbol_id, trading_date))`

#### 3.2.6 Scans
- `scan_definition(scan_def_id pk, name text, version text, config_json jsonb, code_sha text, created_at timestamptz)`
- `scan_run(scan_run_id pk, scan_def_id fk, asof_date date, dataset_hash text, status text, started_at timestamptz, finished_at timestamptz, logs_uri text)`
- `scan_result(scan_run_id fk, symbol_id fk, asof_date date, score numeric null, passed bool, reason_json jsonb, primary key(scan_run_id, symbol_id))`

#### 3.2.7 Experiment registry
- `exp_run(exp_run_id pk, exp_hash text unique, strategy_name text, strategy_hash text, dataset_hash text, params_json jsonb, code_sha text, started_at timestamptz, finished_at timestamptz, status text)`
- `exp_metric(exp_run_id fk, metric_name text, metric_value numeric, primary key(exp_run_id, metric_name))`
- `exp_artifact(exp_run_id fk, artifact_name text, uri text, sha256 text, primary key(exp_run_id, artifact_name))`

#### 3.2.8 Paper trading
State machine per ADR-015:
- `signal(signal_id pk, symbol_id, asof_date date, strategy_hash text, state text, entry_mode text, planned_entry_date date, initial_stop numeric, metadata_json jsonb, created_at timestamptz)`
- `paper_order(order_id pk, signal_id fk, side text, qty numeric, order_type text, limit_price numeric null, status text, created_at timestamptz)`
- `paper_fill(fill_id pk, order_id fk, fill_time timestamptz, fill_price numeric, qty numeric, fees numeric, slippage_bps numeric)`
- `paper_position(position_id pk, symbol_id, opened_at timestamptz, closed_at timestamptz null, avg_entry numeric, avg_exit numeric null, qty numeric, pnl numeric null, state text, metadata_json jsonb)`

### 3.3 Partitioning & Indexes
- Partition `md_ohlcv_raw`, `md_ohlcv_adj`, `feat_daily` monthly.
- Indexes:
  - `md_ohlcv_* (trading_date, symbol_id)`
  - `scan_result(asof_date)`
  - `exp_run(strategy_hash, dataset_hash)`
  - `signal(state, planned_entry_date)`

## 4) Ingestion Design (Vendor Candles)

### 4.1 Inputs
- Vendor candle files (Zerodha): OHLCV daily/minute data, downloaded from Jio Cloud.
- Corporate action files: splits/bonus/dividends/rights (from NSE).
- Symbol directories: metadata/ISIN.
- Delisted lists: symbol lifecycle.

Universe constraint:
- Only NSE-EQ rows are ingested into the research universe tables by default.
- Other series may be stored separately if desired, but they should not leak into scans/backtests.

### 4.2 Ingestion Worker Responsibilities
- Process vendor candle CSV files (manual download from Jio Cloud).
- Store raw file to MinIO with deterministic path.
- Parse into staging tables.
- Upsert into normalized tables with idempotency.
- Record an `ingest_run` row with checksums.

### 4.3 Idempotency
- Use `(symbol_id, trading_date)` as primary key; upsert by PK.
- Raw file checksum tracked; re-run allowed if checksum differs.

### 4.4 Data Quality Checks (ADR-017)
For each trading_date:
- Completeness: expected row count range.
- OHLC constraints: `low <= min(open,close) <= max(open,close) <= high`.
- No negative/zero prices.
- Volume/value sanity vs trailing median.
- Corporate action “suspicious” ratio checks.

If checks fail:
- Mark the day as quarantined.
- Do not update adjusted table for that day.
- Alert.

## 5) Corporate Action Adjustment

### 5.1 Objective
Build continuous adjusted series for correct returns and stop logic.

### 5.1.1 Dividend handling (what decision means)
Dividends are special because they are a cash distribution, not a change in the business value. On the ex-dividend date, the exchange price often drops mechanically by approximately the dividend amount.

There are two common ways to handle this depending on what you’re measuring:

1) **Price-adjusted series ("price continuity")**
  - Objective: remove mechanical jumps so price-based indicators (returns, breakouts, ATR) are smoother.
  - Typical approach: adjust prior prices by a factor that accounts for the dividend drop.
  - Risk: if dividend data is incomplete or misapplied, it can distort signals.

2) **Total return series ("performance with dividends")**
  - Objective: measure true investor return including dividends.
  - Typical approach: keep raw/price-adjusted close, and compute a total return index (TRI) by reinvesting dividends.
  - Benefit: more honest performance measurement for longer-horizon strategies.

Recommended Phase 1 default (practical + robust):
- **Adjust prices for splits/bonus/rights** (mandatory).
- **Store dividends as events** and compute **optional total-return metrics** later.

Rationale: this project is primarily a short-horizon (3–5 day burst) swing system where dividend impact is usually small relative to the momentum move, but corporate actions like splits/bonus can be huge and must be fixed. Keeping dividend handling explicit and optional reduces “silent” distortions.

ADR alignment note:
- ADR-006 describes full backward adjustment including dividends.
- Phase 1 refines this to “dividends as events; TRI later” to reduce error risk and keep treatment explicit.
- This refinement is captured in ADR-019.

### 5.2 Adjustment Method
Backward adjustment:
- For each symbol, compute a cumulative adjustment factor $f(t)$ such that adjusted close is continuous across ex-dates.
- For splits/bonus/rights: multiplicative factors.
- For cash dividends: choose either:
  - price adjustment via subtracting dividend (then scale), or
  - total return series separately.

Implementation detail:
- Store `adj_factor` per (symbol, date) so you can reproduce adjusted OHLC.

Implementation note for NSE:
- Prefer to source dividends from authoritative corporate action files.
- If dividends are missing for some symbols, do not partially apply dividend adjustments; record coverage metrics and treat dividend adjustment as a controlled feature flag.

### 5.3 Recompute strategy
- When new corporate action is discovered or corrected: recompute full history for that symbol.
- Keep versioned adjustment runs to compare.

## 6) Scan & Selection Engine

### 6.1 Base 4% Breakout Scan
Signal condition (canonical):
- `close / close_1 >= 1.04`
- `volume > volume_1`
- `volume >= vol_threshold`
- `close_pos_in_range >= 0.7` where `close_pos_in_range = (close - low) / (high - low)`

Daily classification (useful for analysis without intraday):
- Let $T = 1.04 \cdot C_{t-1}$ be the 4% threshold.
- **Touched intraday**: $H_t \ge T$.
- **Confirmed by close**: $C_t \ge T$.
- **Fizzled**: $H_t \ge T$ and $C_t < T$.

This allows post-trade analysis of “touch vs hold” even in a daily-only pipeline.

Also support “absolute move” variant for high-priced names:
- `close - open >= abs_move_threshold`

#### FEE (find and enter early) boundary

Stockbee’s “enter as early as possible on breakout day” is execution-path dependent.

Phase 1 constraint:
- The system is daily-only, so we do not attempt to model “first touch” fills or intraday stop ordering.

Phase 1 execution modes remain:
- next-open entry (primary)
- same-day close entry (control)

Intraday execution research (5-min/1-min, VWAP-based rules, intraday trailing) is explicitly deferred.

### 6.2 Deterministic proxies for 2LYNCH
We must encode each letter as numeric rules (configurable):

- `2 (not up 2 days in a row)`:
  - `not( ret_1d(t-1) > 0 and ret_1d(t-2) > 0 )`

- `N (narrow/negative day prior)`:
  - prior day range percentile in bottom X% over last 20 days OR prior day return <= 0.

- `H (close near high)`:
  - `close_pos_in_range >= 0.7` (already part of scan)

- `C (tight consolidation)`:
  - last 3–20 day realized volatility below threshold
  - and/or range contraction: average true range decreasing

- `L (linear/orderly move)`:
  - proxy: low drawdown during prior up-leg and low “zigzag”
  - e.g. `max_drawdown_20 <= dd_threshold` and `num_down_days <= k`

- `Y (1st–3rd breakout in trend)`:
  - define breakouts as close making a N-day high (e.g. 20-day)
  - count breakouts in last M days <= 3

All proxies should be recorded into `reason_json` for every candidate.

Indicator/timeframe note:
- ATR and CPR-style pivot calculations are naturally defined on **daily** OHLC.
- VWAP requires intraday candles unless a vendor provides a daily VWAP field; Phase 1 does not require VWAP.

#### Phase 1 v1 STRICT definitions (recommended defaults)

The goal of v1 is not to maximize trade count; it is to build a clean dataset of “textbook” setups to validate whether the edge exists in India.

All thresholds are configuration values with defaults. Use adjusted prices for splits/bonus/rights.

Common:
- Let `t` be the breakout day.
- `close_pos_in_range(t) = (close(t) - low(t)) / (high(t) - low(t))`.
- `range_pct(t) = (high(t) - low(t)) / close(t-1)`.
- `ATR_n(t)` is n-day ATR computed on adjusted OHLC.
- `VMA_n(t)` is n-day SMA of volume.

`H` (close near high):
- `close_pos_in_range(t) >= 0.70`

`N` (narrow/negative day prior):
- Require on `t-1`:
  - either `ret_1d(t-1) <= 0`
  - OR `true_range(t-1)` is in the bottom `nr_percentile` of `true_range(t-20..t-1)`

Default: `nr_percentile = 0.20`.

`2` (not up 2 days in a row):
- Reject if both `ret_1d(t-1) > 0` and `ret_1d(t-2) > 0`.

`Y` (young trend: current breakout is 1st–3rd):
- Define prior breakout days (excluding today):
  - `is_breakout_day(t') = close(t') >= max(close(t'-lookback_high .. t'-1))`
- Count prior breakouts in window `[t-lookback_y .. t-1]`:
  - `breakout_count = count(is_breakout_day(t'))`
- Require `breakout_count <= 2`.

Defaults:
- `lookback_high = 20`
- `lookback_y = 90`

`L` (linearity/orderliness via regression R²):

- Window: `W = [t-lookback_L .. t-1]`
- Fit OLS regression: `log(close(W)) = a + b * idx(W) + error`
- Compute `R2_L`.
- Require:
  - `R2_L >= min_R2_L`
  - `b > 0`

Defaults:
- `lookback_L = 20`
- `min_R2_L = 0.70`

Optional strictness (Phase 1 recommended ON):
- `down_days = count_{t' in W}(ret_1d(t') < 0)`
- require `down_days <= max_down_days_L` (default `max_down_days_L = 7`).

`C` (consolidation: ATR + range + volume compression):

- Window: `B = [t-lookback_C .. t-1]` (bounded 3–20)
- ATR compression:
  - `ATR_short = mean(ATR_5(B))`
  - `ATR_long = mean(ATR_20(B))`
  - require `ATR_short / ATR_long <= atr_compress_ratio`
- Range compression:
  - Require the median of `range_pct(B)` to be in the bottom `range_percentile` of the last `range_ref_window` days.
- Volume dry-up:
  - `VMA_5(t-1) <= VMA_20(t-1) * vol_dryup_ratio`
- “No more than one 4% day inside consolidation”:
  - `count_{t' in B}(ret_1d(t') >= 0.04) <= 1`

Defaults:
- `lookback_C = 15`
- `atr_compress_ratio = 0.80`
- `range_percentile = 0.20`
- `range_ref_window = 60`
- `vol_dryup_ratio = 0.80`

### 6.3 ANT / FHP scans
- ANT: volatility compression + volume dry-up + near resistance.
- FHP: high-priced bucket + strong RS vs benchmark.

### 6.4 Low Threshold Breakout (LTB) scans (reference definitions)

Phase 1 focuses on the 4% breakout + 2LYNCH pipeline. However, “LTB” scans are used operationally in the broader Stockbee workflow and are recorded here as reference expressions from project notes for future parity checks.

Notation:
- `c/o/h/l/v` = today close/open/high/low/volume
- `c1/c2/v1` = prior day(s)
- `avgc7/avgc65` = moving averages of close
- `minv3.1` = liquidity proxy (must be mapped explicitly when implementing; do not leave implicit)

LTB — Bullish (daily, reference):

`minv3.1>=300000 and c>=3 and avgc7/avgc65>=1.05`
`and c>o and c>c1 and c/c1>c1/c2 and c1/c2<1.011 and (c-l)/(h-l)>=.7`
`and CountTrue(c > 1.2 * c1 and (h-l) < 0.04 * c, 100) = 0`

LTB — Bearish (daily, reference only):

`c1>c2 and c<c1 AND c<o and minv3.1>=900000 and c>3 and avgc7/avgc65<.95`
`and CountTrue(c > 1.2 * c1 and (h-l) < 0.04 * c, 100) = 0`

## 7) Backtesting & Walk-Forward (vectorbt)

### 7.1 Data shape
vectorbt prefers wide matrices:
- rows = trading_date
- columns = symbols
- values = adjusted close (and other fields)

We will generate symbol subsets to keep memory bounded.

### 7.2 Execution model
Backtest modes:
1. **Signal-driven**: only enter if scan fires.
2. **Universe-driven**: compute indicators across universe, then apply scan.

### 7.3 Entry/Exit modeling
Run BOTH entry timings in parallel (same signals, different entry price rule):
- **Next open entry (primary)**: enter at next day open after signal day.
- **Same-day close entry (secondary/control)**: enter at close on signal day.

Phase 1 naming convention:
- `STRAT_4P_2LYNCH_v1_open`
- `STRAT_4P_2LYNCH_v1_close`

Phase 1 reporting requirement:
- Always show a side-by-side metrics table for Open vs Close variants (CAGR, Sharpe, Max DD, PF, Win%).

Stop rules:
- initial stop = low of entry day
- move to breakeven after +3–5% (modeled discretely unless intraday)
- trail after +8% (define trail method: ATR-based or pct-based)
- time stop after 3 days
- weak follow-through exit by day 3

### 7.4 Costs and slippage
At minimum:
- per-trade fixed fees + proportional fees
- slippage bps by liquidity bucket (based on rolling value traded)

Store gross + net metrics separately.

#### Slippage model options (what this means)

Slippage represents the difference between a theoretical fill price (e.g., next open) and the realized fill given spread + market impact + liquidity.

Option A — **Liquidity-bucket bps model (recommended Phase 1)**
- Compute rolling `value_traded` (e.g., median over 20 trading days).
- Assign symbols into buckets (e.g., top 20%, next 30%, etc.) or fixed INR thresholds (once decided).
- Apply a simple slippage in basis points per fill, e.g.:
  - very liquid: 5–10 bps
  - mid: 10–25 bps
  - illiquid: 25–75 bps
- Pros: simple, stable, easy to backtest at scale.
- Cons: not trade-size aware; approximates reality.

Option B — **Impact model (better realism, more complexity)**
- Use a size-aware model such as square-root impact:
  - impact $\propto \sigma \sqrt{\tfrac{Q}{ADV}}$
  - where $Q$ is order value, $ADV$ is average daily value traded, and $\sigma$ is daily volatility.
- Pros: more realistic when position sizing varies.
- Cons: requires careful calibration and can overfit if not disciplined.

Recommended Phase 1 approach:
- Implement **Option A** first (bps buckets), keep it configurable.
- Add Option B later once you have stable baseline results and a sizing model.

#### Phase 1 slippage buckets (locked defaults)

Bucket assignment uses rolling 20-day traded value (INR):
- `ADV_value_20(t) = median(value_traded(t-20..t-1))`

Buckets:
- Large: `ADV_value_20 > 100 Cr INR` → `slippage_bps = 5`
- Mid: `20–100 Cr INR` → `slippage_bps = 10`
- Small: `< 20 Cr INR` → `slippage_bps = 20`

Apply slippage on entry, exit, and stop fills.

Fill price function:
- BUY: `fill = theoretical * (1 + slippage_bps/10000)`
- SELL: `fill = theoretical * (1 - slippage_bps/10000)`

### 7.5 Walk-forward
Default framework:
- train window: 3 years
- test window: 6 months
- roll monthly

Train period is used for:
- parameter selection and risk sizing calibration
- (optional) regime thresholds

Test period is strictly out-of-sample.

## 8) Paper Trading Engine

### 8.1 Purpose
Validate live-like behavior without risking capital.

### 8.2 Inputs
- qualified signals from scanner/backtest selection logic
- broker feed for real-time/near-real-time prices (optional Phase 1)

Phase 1 constraint:
- Paper trading can be simulated using EOD bars (signal generation + next-day open fills) without any broker feed.
- Live monitoring and intraday alerts are explicitly deferred.

### 8.3 Execution
- Simulate entry: next open or close depending on mode.
- Simulate fills with conservative slippage.
- Enforce risk governance (ADR-016): daily loss cap, DD cap, pause.

### 8.4 State machine
NEW → QUALIFIED → ALERTED → ENTERED → MANAGED → EXITED → ARCHIVED

All transitions are recorded with timestamps and reasons.

## 9) APIs (FastAPI)

Read-only endpoints for dashboard and integrations:
- `GET /health`
- `GET /ingestion/status`
- `GET /symbols?status=...`
- `GET /scans/runs?date=...`
- `GET /scans/results?run_id=...`
- `GET /experiments?strategy_hash=...&dataset_hash=...`
- `GET /experiments/{exp_hash}`
- `GET /paper/signals?state=...`
- `GET /paper/positions?open=true`
- `GET /alerts/recent`

Write endpoints (optional; can be internal-only):
- `POST /experiments/run`
- `POST /paper/signals/{id}/ack`

## 10) Scheduling

Daily schedule (example, local time IST):
- 18:30 ingest + parse
- 19:00 validate
- 19:15 adjust
- 20:00 compute features
- 20:30 scan
- 21:00 backtest selected configs (or overnight)
- 22:00 generate report + alerts

All jobs must be idempotent and safe to re-run.

## 11) Monitoring & Alerting

### 11.1 Prometheus metrics
- ingest success/failure, latency
- row counts ingested
- quarantine count
- scan candidate count
- backtest durations
- paper PnL, drawdown, kill-switch state

### 11.2 Alerts
- ingestion failure
- quarantine detected
- corporate action anomaly
- kill-switch triggered
- strategy degradation (rolling hit rate / expectancy drop)

## 12) Developer Experience

### 12.1 Repo structure (proposed)
- `services/ingest/`
- `services/adjust/`
- `services/scan/`
- `services/backtest/`
- `services/paper/`
- `apps/api/`
- `apps/dashboard/`
- `libs/core/` (db models, MinIO client, logging, config)
- `libs/strategy/` (scan definitions, 2LYNCH proxies, exits)

Implementation preference:
- Use a modern `src/` layout once coding begins (e.g. `src/nse_momentum_lab/...`) to avoid import-path ambiguity.

### 12.2 Configuration
- No `.env` files (project runs locally but secrets must never be committed)
- Use Doppler for secrets injection
- Use typed config objects; validate at startup

### 12.2.1 Dependency management (uv only)

- Use **uv** for all dependency management and execution.
- Do not use `pip install ...` in docs/scripts.
- Use `pyproject.toml` as the single dependency source of truth.

Typical commands:
- `uv sync`
- `uv run pytest`
- `uv run ruff check .`

Run with secrets:
- `doppler run -- uv run <command>`

### 12.3 Logging
- structured JSON logs
- correlation IDs per job run
- store logs in MinIO and link from DB run tables

### 12.3.1 Async-first guideline

Use `async` for I/O-bound work end-to-end:
- HTTP downloads (NSE files): `httpx`
- Postgres: SQLAlchemy async engine + `psycopg` (async)
- MinIO/S3: async client (or isolate sync S3 calls behind a small threadpool wrapper)

Notes:
- Numeric computation (pandas/vectorbt) is typically CPU-bound and can remain synchronous; keep the boundary explicit.
- If a service mixes heavy CPU work and async I/O, isolate CPU work into dedicated functions and avoid blocking the event loop.

### 12.5 AI agents & chatbot

This project uses LLMs for assistance, not for trading math.

Modes:
- **Background agents**: scheduled operators (summarize runs, detect anomalies, emit alerts).
- **Interactive chatbot**: a local chat interface to ask questions and trigger safe actions.

Phase 1 decision:
- Implement a simple NiceGUI chat page (ADR-020).
- Keep tool permissions tight: read-only analysis + restricted safe actions (enqueue reruns, acknowledge alerts).

Hard boundary:
- The chatbot cannot mutate OHLCV datasets or compute indicators/trades.
- All computations remain deterministic and are executed by the pipeline services.

See: `docs/dev/AGENTS.md`.

## 12.4 Best practices (strongly suggested)

These practices keep research systems from becoming “unreproducible notebooks”.

### Configuration & secrets
- Do not use `.env` files; do not commit secrets.
- Use Doppler for local secrets management and injection.
- Commit only non-secret configuration like `doppler.yaml` (project/config names) if desired.
- Validate config at startup (fail fast) and print a redacted config summary.
- Separate configuration scopes:
  - **runtime**: DB URLs, S3 endpoints
  - **research defaults**: scan params, slippage defaults, walk-forward windows
  - **experiment params**: per-run overrides (must be stored in registry)

Suggested local execution pattern:
- Run compose with Doppler-injected environment variables (no secrets on disk), e.g.:
  - `doppler run -- docker compose up -d`
  - or `doppler run -- docker compose up` for foreground logs

### Migrations and schema evolution
- Use Alembic migrations for every schema change.
- Do not hand-edit schema in prod-like runs.
- For partitioned time-series tables, codify partition creation in migrations or a dedicated admin job.

### Data contracts
- Treat `md_ohlcv_raw` and `md_ohlcv_adj` as contracts:
  - no duplicate keys
  - monotonic trading_date per symbol
  - no negative/zero prices
- Add DB constraints where feasible (PKs, NOT NULL, CHECK constraints for basic OHLC validity).

### Idempotency and “safe reruns”
- Every job must be rerunnable without creating duplicates.
- Adopt a consistent job state pattern: `STARTED` → `SUCCEEDED` / `FAILED` / `QUARANTINED`.
- Store job inputs (file checksums, dataset hash) so replays are auditable.

### Versioning
- Record `code_sha` in every scan run and experiment run.
- Compute and store:
  - `dataset_hash` (derived from raw inputs + adjustment version)
  - `strategy_hash` (derived from scan + selection + exits)
- Never overwrite artifacts; content-address them and store SHA-256.

### Testing strategy (minimum)
- Unit tests for:
  - corporate action adjustment math
  - scan conditions and 2LYNCH proxy functions
  - trade simulation fills/stops
- Integration tests for:
  - run one scan and assert stable outputs

### Observability
- Prefer metrics for “counts and durations”:
  - rows ingested
  - quarantine count
  - scan candidate counts
  - experiment durations
- Alert on missing data and job failures first; performance alerts later.

### Research hygiene (quant best practices)
- Avoid look-ahead bias:
  - compute indicators using only history available at the decision time
  - ensure delisting info doesn’t leak into earlier dates
- Avoid survivorship bias:
  - universe membership should be time-aware
- Use walk-forward and keep a strict separation:
  - train window chooses params
  - test window evaluates
- Report gross and net performance (costs + slippage), side-by-side.

## 13) Open Questions (Need Your Input)

Confirmed from discussion:
- Broker feed target (later): Zerodha only
- Universe: NSE-EQ only
- Phase 1: EOD-only backtesting
- Entry timing: both next-open (primary) and same-day-close (control)
- `L` and `C`: strict numeric proxies in v1
- Dividends: events only; TRI later
- Slippage: bps per liquidity bucket (locked defaults)

Still to decide (can be deferred until after baseline results):
- Liquidity threshold(s): volume only vs value-traded thresholds
- Whether to keep the strict v1 thresholds as-is or relax systematically in Phase 2

## 14) Daily Summary + Failure Analysis (Chatbot Spec)

This section specifies what the NiceGUI dashboard/chatbot must be able to report *deterministically* every day.

Key design rule:
- The chatbot is a **reader and narrator** of deterministic outputs.
- All “crunching” (ingest/adjust/scan/backtest/rollups) runs in background workers.
- NiceGUI hosts:
  - dashboard views (tables/charts)
  - a chat UI that queries rollups and raw results

### 14.1 Division of labor: background vs UI

Background pipeline workers (deterministic):
- Download + parse + validate + quarantine
- Apply corporate actions + compute features
- Run scans and store candidates with `reason_json`
- Run backtests and store:
  - metrics
  - trades with explicit `exit_reason`
  - artifacts (equity curve, trades parquet)
- Compute daily rollups used by UI/chat

NiceGUI UI (read-heavy):
- Reads rollups and recent raw results from Postgres
- Fetches artifacts from MinIO on demand (charts, trade lists)
- Chatbot answers questions by:
  - running bounded queries
  - summarizing results
  - proposing *experiments* (never auto-changing strategy)

### 14.2 Required deterministic primitives (so “why failed” is real)

To explain success/failure reliably, every trade (paper or backtest) must have:

1) **Entry provenance**
- which scan run created it (or which universe-run strategy)
- which strategy variant (open vs close)
- which filters were passed/failed

2) **Exit provenance**
- an explicit `exit_reason` code (enum)
- an `exit_rule_version` (to handle future rule changes)

3) **Context fields** (not strictly necessary, but massively helps analysis)
- liquidity bucket at entry (`Large|Mid|Small`)
- gap info (e.g. next open vs prior close)
- MFE/MAE (max favorable/adverse excursion) computed from daily bars

### 14.3 Schema additions (Phase 1)

These are minimal additions to support daily summaries and failure analysis.

#### 14.3.1 Job run table (pipeline observability)

Add a generic job table so daily summaries can report pipeline health:

- `job_run(job_run_id pk, job_name text, asof_date date, dataset_hash text null, status text, started_at timestamptz, finished_at timestamptz, duration_ms bigint, logs_uri text null, metrics_json jsonb, error_json jsonb)`

Examples of `job_name`:
- `ingest_vendor_candles`
- `validate_day`
- `adjust_ohlcv`
- `compute_features`
- `scan_4p_2lynch`
- `backtest_strat_4p_2lynch_v1_open`
- `backtest_strat_4p_2lynch_v1_close`
- `rollup_daily_summary`

#### 14.3.2 Trade log tables (backtest + paper)

Backtest trades must be queryable in DB (not only parquet) for daily analysis.

Add (or extend) these tables:

- `bt_trade(trade_id pk, exp_run_id fk, symbol_id fk, entry_date date, entry_price numeric, entry_mode text, qty numeric, initial_stop numeric, exit_date date null, exit_price numeric null, pnl numeric null, pnl_r numeric null, fees numeric null, slippage_bps numeric null, mfe_r numeric null, mae_r numeric null, exit_reason text null, exit_rule_version text, scan_run_id bigint null, reason_json jsonb, created_at timestamptz)`

Notes:
- `entry_mode` is `open|close`.
- `reason_json` should embed letter scores/thresholds and which checks failed.

Paper trading already has orders/fills/positions; add (if not present) a consistent `exit_reason` on `paper_position` or a separate `paper_trade` view.

#### 14.3.3 Rollup tables (what the chatbot reads fast)

These are derived tables computed nightly and safe to query interactively.

1) Scan rollups:
- `rpt_scan_daily(asof_date date, scan_def_id fk, dataset_hash text, total_universe int, passed_base_4p int, passed_2lynch int, passed_final int, by_fail_reason jsonb, by_liquidity_bucket jsonb, created_at timestamptz, primary key(asof_date, scan_def_id, dataset_hash))`

2) Backtest rollups (by strategy variant):
- `rpt_bt_daily(asof_date date, strategy_name text, dataset_hash text, entry_mode text, signals int, entries int, exits int, wins int, losses int, win_rate numeric, avg_r numeric, profit_factor numeric, max_dd numeric, created_at timestamptz, primary key(asof_date, strategy_name, dataset_hash, entry_mode))`

3) Failure reason breakdown (loss cohorts):
- `rpt_bt_failure_daily(asof_date date, strategy_name text, dataset_hash text, entry_mode text, exit_reason text, count int, avg_r numeric, median_r numeric, created_at timestamptz, primary key(asof_date, strategy_name, dataset_hash, entry_mode, exit_reason))`

### 14.4 Exit reason enum (Phase 1)

The key to “why did it fail?” is a stable set of exit reason codes.

Phase 1 `exit_reason` codes (minimum):
- `STOP_INITIAL`
- `STOP_BREAKEVEN`
- `STOP_TRAIL`
- `TIME_STOP_DAY3`
- `WEAK_FOLLOW_THROUGH`
- `EXIT_EOD`
- `GAP_THROUGH_STOP` (EOD approximation; next open below stop)

Rule:
- Exactly one exit_reason per exit.
- If multiple rules trigger, define a deterministic priority order and record it.

### 14.5 Daily report contents (what the chatbot must output)

Every trading day, the assistant must be able to generate a deterministic report with:

1) Pipeline health
- Which jobs succeeded/failed/quarantined
- Total runtime and durations by job

2) Scan summary
- `total_universe`
- `passed_base_4p`, `passed_2lynch`, `passed_final`
- Top rejection reasons (e.g. `L_R2_TOO_LOW`, `C_NOT_COMPRESSED`, `LIQUIDITY_SMALL`)

3) Trade outcome summary (Open vs Close sibling variants)

Required table (must be shown side-by-side):

Metric | Open | Close
---|---:|---:
Signals | |
Entries | |
Win% | |
Profit Factor | |
Avg R | |
Max DD | |

4) Failure analysis
- Top 3 `exit_reason` causes of losses
- Loss concentration by liquidity bucket
- Any regime warnings (e.g. “weak follow-through dominating over last 10 days”)

5) Suggested experiments (not automatic changes)
- The chatbot can propose: “test relaxed `min_R2_L` from 0.70→0.65”, “exclude Small bucket”, “increase slippage for Small”, etc.
- Each suggestion must map to a new experiment run (new params → new exp_hash).

### 14.6 Chatbot question set (Phase 1)

The NiceGUI chat page should support these canonical questions (examples):

Daily operations:
- “Did the pipeline run successfully for {date}? What failed and why?”
- “How many candidates passed strict 2LYNCH today? Which letters filtered most?”

Strategy validation:
- “Over last 60 trading days, show Open vs Close metrics. Is Close materially better?”
- “For losing trades last month, summarize exit reasons and common pre-trade conditions.”

Learning loops:
- “Which liquidity bucket contributes most of the PnL? Which bucket contributes most losses?”
- “If we exclude Small bucket, what happens to PF and MaxDD?” (answer by pointing to existing experiments, or proposing a new run)

### 14.7 Security and safety

- The chatbot must not expose secrets (Doppler values) in responses.
- The chatbot must not execute arbitrary SQL from user text; implement a small set of parameterized query functions.
- Any write actions must be whitelisted and logged (enqueue rerun, acknowledge alert).
