# nse-momentum-lab — Roadmap (Phase 1 / 2 / 3)

This roadmap is the execution checklist for the project.

Design source of truth:
- Technical design: `docs/TECHNICAL_DESIGN.md`
- ADRs: `docs/adr/`
- Agents design (tools + determinism boundary): `docs/dev/AGENTS.md`

Golden rules (non-negotiable):
- No `.env` files; use Doppler runtime injection.
- LLMs never compute indicators/trades; they only summarize deterministic outputs.
- Importable Python code lives under `src/nse_momentum_lab/`.

---

## Phase 1 — Local-first, EOD pipeline + research + paper trading ✅ COMPLETED

Goal: A reproducible, deterministic EOD research system that can ingest NSE data, build adjusted series, run scans and backtests (two entry variants), produce daily summaries/failure analysis, and run a paper-trading ledger.

Scope constraints (locked in Phase 1):
- NSE cash equities only (NSE-EQ)
- EOD-only backtesting
- NiceGUI dashboard (migrated from Streamlit 2026-03-01)
- Dual sibling strategies: next-open (primary) + same-day-close (control)
- Dividends as events only (no dividend price adjustment in core series)
- Slippage model: bps-per-fill by liquidity bucket

### Phase 1.0 — Repo hygiene + developer workflow ✅ COMPLETED

Deliverables:
- Deterministic run commands and a reliable local stack.

TODO:
- [x] Add a small `Makefile`-equivalent for Windows: documented in `scripts/commands.ps1`.
- [x] Add VS Code tasks that run tests and lint (`.vscode/tasks.json`).
- [x] Enforce formatting/lint locally (Ruff) and in CI (Phase 1.9).

Acceptance:
- A new dev can go from clone → `uv sync` → `docker compose up -d` → tests pass.

### Phase 1.1 — Local stack + schema baseline ✅ COMPLETED

Deliverables:
- Postgres schema (core tables) and MinIO buckets initialized.

TODO:
- [x] Confirmed `db/init/001_init.sql` matches the tables in `docs/TECHNICAL_DESIGN.md`.
- [x] Added lightweight migration approach (Option A: init scripts + volume resets).

Acceptance:
- DB comes up cleanly; buckets exist; schema is queryable.

### Phase 1.2 — Data ingestion (Zerodha EOD) + raw persistence ✅ COMPLETED

Deliverables:
- Ingestion worker that writes normalized raw OHLCV into PostgreSQL and archives raw files into MinIO.

TODO:
- [x] Implemented data ingestion from Zerodha Parquet files.
- [x] Store raw inputs in MinIO with deterministic paths.

Acceptance:
- For a given trading date range, ingestion is repeatable and idempotent.

### Phase 1.3 — Corporate action adjustment (splits/bonus/rights) + dividends as events ✅ COMPLETED

Deliverables:
- Adjustment worker that builds `md_ohlcv_adj` from `md_ohlcv_raw`.

TODO:
- [x] Ingest corporate action events into `ca_event` (schema ready).
- [x] Implemented backward adjustment factors for splits/bonus/rights (`src/nse_momentum_lab/services/adjust/logic.py`).
- [x] Store dividends as events only; do not apply dividend price adjustment in Phase 1 adjusted series.
- [x] Persist `adj_factor` per row to support audit.
- [x] Add reconciliation checks: continuity around ex-dates, factor sanity.

Acceptance:
- Adjusted series is continuous across split/bonus/rights events.

### Phase 1.4 — Features + scan engine (4% breakout + strict 2LYNCH proxies) ✅ COMPLETED

Deliverables:
- Feature computation into `feat_daily` and scan results into `scan_run`/`scan_result`.

TODO:
- [x] Compute baseline features required by the scan rules (ATR, returns, dollar-vol, range metrics) (`src/nse_momentum_lab/services/scan/features.py`).
- [x] Implement strict numeric proxies for 2LYNCH `L` and `C` (`src/nse_momentum_lab/services/scan/rules.py`).
- [x] Implement 4% breakout scan logic and store `reason_json` explaining pass/fail.
- [x] Add parameterization (no hard-coded thresholds) + record config in `scan_definition`.

Acceptance:
- A scan run can be executed for an `asof_date` and produces deterministic, explainable outputs.

### Phase 1.5 — Backtest engine + experiment registry ✅ COMPLETED

Deliverables:
- Backtest worker producing trades + metrics for both sibling entry variants.

TODO:
- [x] Implement strategy definition hashing + dataset hashing (`src/nse_momentum_lab/services/backtest/registry.py`).
- [x] Implement backtest runner for:
  - next-open entry
  - same-day-close entry (control)
- [x] Implement the slippage model (liquidity-bucket bps per fill) + fees.
- [x] Store run metadata in `exp_run`, metrics in `exp_metric`, artifacts in MinIO.
- [x] Add walk-forward evaluation loop and summary metrics.

Acceptance:
- For a fixed dataset hash + params, rerunning produces identical results and artifacts.

### Phase 1.6 — Paper trading ledger + risk governance ✅ COMPLETED

Deliverables:
- A paper trading worker that consumes signals and produces a ledger, respecting the state machine and risk rules.

TODO:
- [x] Implement signal generation from scan results.
- [x] Implement paper order/fill simulation (EOD assumptions).
- [x] Implement position lifecycle + exit_reason enum.
- [x] Implement kill-switch / risk governance hooks (ADR-016).

Acceptance:
- Paper positions update deterministically from the signal stream; all exits are explainable.

### Phase 1.7 — Daily summary + failure analysis + chatbot integration boundary ✅ COMPLETED

Deliverables:
- Nightly job that writes rollups and generates "what happened today" summaries based on deterministic tables.

TODO:
- [x] Implement rollups and summary tables described in Technical Design §14 (`src/nse_momentum_lab/services/rollup/worker.py`).
- [x] Implement a minimal "Ops summary" endpoint that shows:
  - what ran
  - what failed
  - what to rerun
- [x] Wire the NiceGUI chat page to query rollups + explain results (no trading math).

Acceptance:
- You can ask: "how many trades today, winners/losers, why failures" and the answer is grounded in DB rows/artifacts.

### Phase 1.8 — Dashboard (NiceGUI) + FastAPI read APIs ✅ COMPLETED

Deliverables:
- A usable internal dashboard.

TODO:
- [x] Dashboard pages (minimal): pipeline status, scan candidates, backtest registry, paper ledger, daily summary.
- [x] FastAPI read-only endpoints for the dashboard (`src/nse_momentum_lab/api/app.py`).

Acceptance:
- You can browse candidates, run IDs, metrics, and recent failures without digging logs.

### Phase 1.9 — CI + packaging ✅ COMPLETED

Deliverables:
- Automated checks that keep the repo stable.

TODO:
- [x] Added GitHub Actions to run:
  - `uv sync --locked`
  - `uv run ruff check .`
  - `uv run pytest -q`
  - `uv run mypy src tests`

Acceptance:
- PRs cannot merge with failing tests/lint.

---

## Phase 2 — Live monitoring + broker integration + operational hardening (next)

Goal: Extend Phase 1 into a daily operational system with near-real-time monitoring (where feasible), broker integration (Zerodha target), and stronger governance.

### Phase 2.1 — Agentic Automation ✅ COMPLETED

Goal: Enable fully automated agentic research with LLM-powered chat and parameter optimization.

TODO:
- [x] **Strategy Parameter Optimization** - Added grid search (`optimizer.py`), sensitivity analysis (`sensitivity.py`), walk-forward validation.
- [x] **Data Quality Automation** - Added comprehensive quality validation (`data_quality.py`): OHLC checks, price anomalies, volume anomalies, date gaps, extreme moves.
- [x] **Survivorship Bias Handling** - Added delisting tracking in scan rules, force exit on delisting in backtest.
- [x] **Enhanced Metrics** - Added Calmar ratio, Sortino ratio, R-multiple distribution to backtest results.
- [ ] **Full Agent Chat Implementation** - Complete phidata agent setup with tool bindings, persistent conversation memory, LLM integration for research queries.

### Phase 2.2 — Broker Integration

TODO:
- [ ] Implement broker adapter interface (start with Zerodha) and a strict "paper vs live" separation.
- [ ] Add live price monitoring (where available) to track open positions and stop logic.

### Phase 2.3 — Alerting + Scheduler Hardening

TODO:
- [ ] Add alerting integrations (email/Telegram/etc.) driven by deterministic triggers.
- [ ] Add job scheduler hardening (retry policies, backoff, idempotency, alert on missed runs).
- [ ] Add performance profiling + caching where needed.

### Phase 2.4 — Reporting

TODO:
- [ ] Add optional TRI computation (dividends reinvested) as an alternate reporting series.
- [ ] Automated daily/weekly PDF reports.

Acceptance:
- System runs unattended daily; alerts are reliable; broker integration is safe and gated.

---

## Phase 3 — Scale-out + multi-user + production-grade governance

Goal: Make the system robust enough for long-term use: multiple strategies, larger datasets, multiple users, and strict operational controls.

TODO:
- [ ] Containerize workers/services fully (ingest/scan/backtest/paper/api/dashboard) with pinned images.
- [ ] Add role-based access controls for any write actions (especially those that can enqueue jobs).
- [ ] Add full observability: metrics, traces (OpenTelemetry), log retention.
- [ ] Add dataset snapshot/version management and long-horizon reproducibility guarantees.
- [ ] Add stronger research tooling: parameter sweeps at scale, artifact lineage, automated regression detection.
- [ ] Consider UI upgrades if NiceGUI becomes limiting.

Acceptance:
- Reproducible research at scale with auditable changes and multi-user safety.
