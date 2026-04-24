# Command Reference - nse-momentum-lab

**Version**: Multi-Strategy Platform (Phase 1–7 complete) + Paper Trading v2 Engine
**Last Updated**: 2026-04-24

---

## Table of Contents

1. [Infrastructure Commands](#infrastructure-commands)
2. [Application Commands](#application-commands)
3. [Pipeline Commands](#pipeline-commands)
4. [Testing Commands](#testing-commands)
5. [Monitoring & Debugging](#monitoring--debugging)
6. [AI Agent Commands](#ai-agent-commands) (Future)
7. [Common Workflows](#common-workflows)

---

## Infrastructure Commands

### Docker Services

#### Start all services
```bash
doppler run -- docker compose up -d
```
**What it does**: Starts Postgres and MinIO in background
**Ports**: Postgres 5434, MinIO API 9003, MinIO Console 9004

#### Stop all services
```bash
docker compose down
```
**What it does**: Stops and removes containers

#### View service status
```bash
docker compose ps
```
**What it does**: Shows running containers and their ports

#### View service logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f postgres
docker compose logs -f minio
```

#### Restart services
```bash
# Stop and restart
docker compose down
doppler run -- docker compose up -d

# Force recreate (use after port changes)
doppler run -- docker compose up -d --force-recreate
```

### Doppler Commands

#### Check environment variables
```bash
doppler run -- printenv | grep -E "POSTGRES|MINIO"
```

#### Update a secret
```bash
doppler secrets set MINIO_PORT 9003
doppler secrets set MINIO_CONSOLE_PORT 9004
```

#### List all secrets
```bash
doppler secrets list
```

---

## Application Commands

### Kite Token Refresh

#### Exchange the token and update Doppler
```bash
doppler run -- uv run nseml-kite-token --apply-doppler
```

#### If you already have the redirected callback URL
```bash
doppler run -- uv run nseml-kite-token --request-token "<full callback url>" --apply-doppler
```

### FastAPI Server

#### Start API server
```bash
doppler run -- uv run nseml-api
```
**What it does**: Starts FastAPI on port 8004 (configurable via `API_PORT` env var)
**Access**: http://127.0.0.1:8004
**Docs**: http://127.0.0.1:8004/docs

#### Test API health
```bash
curl http://127.0.0.1:8004/health
```

#### Test specific endpoint
```bash
# Summary
curl http://127.0.0.1:8004/api/dashboard/summary

# Symbols
curl http://127.0.0.1:8004/api/symbols

# Scan runs
curl http://127.0.0.1:8004/api/scans/runs?limit=10
```

### NiceGUI Dashboard

#### Start dashboard
```bash
doppler run -- uv run nseml-dashboard
```
**What it does**: Starts NiceGUI dashboard on port 8501
**Access**: http://localhost:8501

#### Available pages
```
http://localhost:8501/              # Home
http://localhost:8501/backtest      # Backtest Results
http://localhost:8501/trade_analytics
http://localhost:8501/compare       # Compare Experiments
http://localhost:8501/strategy      # Strategy Analysis
http://localhost:8501/scans         # Momentum Scans
http://localhost:8501/data_quality
http://localhost:8501/pipeline      # Pipeline Status
http://localhost:8501/paper_ledger   # Paper Trading v2 Ledger
http://localhost:8501/market_monitor
http://localhost:8501/daily_summary
```

---

## Pipeline Commands

### Data Layer

Market data is loaded from Zerodha Parquet files via DuckDB. See `nse_momentum_lab.db.market_db`.

#### Today-only ingest for 2026-03-30
```bash
doppler run -- uv run nseml-kite-ingest --date 2026-03-30
doppler run -- uv run nseml-kite-ingest --date 2026-03-30 --5min --resume
```

#### Full catch-up workflow (after a gap)
```bash
# 1. Refresh instrument master
doppler run -- uv run nseml-kite-ingest --refresh-instruments --exchange NSE

# 2. Daily OHLCV catch-up
doppler run -- uv run nseml-kite-ingest --from LAST_DATE --to TODAY

# 3. 5-min OHLCV catch-up (WAIT for daily to finish first)
doppler run -- uv run nseml-kite-ingest --from LAST_DATE --to TODAY --5min --resume

# 4. Incremental feature rebuild (WAIT for 5-min to finish — feat_intraday_core depends on it)
#    This step also force-syncs the market replica so the dashboard sees updated data.
doppler run -- uv run nseml-build-features --since LAST_DATE

# 5. Market monitor refresh
doppler run -- uv run nseml-market-monitor --incremental --since LAST_DATE

# 6. DQ scan (MANDATORY — do not skip)
doppler run -- uv run nseml-hygiene --refresh --full
doppler run -- uv run nseml-hygiene --report

# 7. Verify coverage
doppler run -- uv run nseml-db-verify
```

**Important**: Steps 2-3 are sequential. Step 4 must wait for step 3 to complete.
Step 6 (DQ scan) is mandatory — never skip it after ingestion.
The market replica is synced automatically by step 4 — the dashboard reads from the replica, not the source DB.

### Corporate Action Adjustment

#### Run adjustment
```bash
doppler run -- uv run python -m nse_momentum_lab.services.adjust.worker
```
**What it does**:
1. Reads corporate action events
2. Computes adjustment factors
3. Builds `md_ohlcv_adj` table
4. Handles splits, bonus, rights

### Feature Computation & Scanning

#### Run scan for a date
```bash
doppler run -- uv run python -m nse_momentum_lab.services.scan.worker YYYY-MM-DD
```
**Example**:
```bash
doppler run -- uv run python -m nse_momentum_lab.services.scan.worker 2025-02-07
```
**What it does**:
1. Computes features (ATR, returns, volume, etc.)
2. Applies 4% breakout filter
3. Applies 2LYNCH filters
4. Stores candidates with reason_json

#### Run scans across a date range (limited universe)

Use this to populate `scan_run` + `scan_result` across history for a small set of symbols (recommended before ingesting the full NSE universe).

```bash
# Dry-run (resolve symbols + show discovered trading dates)
doppler run -- uv run python scripts/run_scan_range_limited.py --start 2015-04-01 --end 2015-04-30 --symbols RELIANCE,TCS,INFY --dry-run

# Execute a short smoke test
doppler run -- uv run python scripts/run_scan_range_limited.py --start 2015-04-01 --end 2015-04-30 --symbols RELIANCE,TCS,INFY --max-dates 5 --progress-every 1

# Execute the full range (idempotent; use --force to recompute)
doppler run -- uv run python scripts/run_scan_range_limited.py --start 2015-04-01 --end 2015-04-30 --symbols RELIANCE,TCS,INFY
```

Notes:
- `--symbols` accepts either the exact `ref_symbol.symbol` value (e.g. `0001_RELIANCE`) or the base symbol (e.g. `RELIANCE`).

### Daily Rollup

#### Generate daily rollup
```bash
doppler run -- uv run python -c "import asyncio; from datetime import date; from nse_momentum_lab.services.rollup.worker import run_daily_rollup; asyncio.run(run_daily_rollup(date(2025,2,7)))"
```
**What it does**:
1. Aggregates daily metrics
2. Creates report tables
3. Summarizes failures
4. Updates dashboard summary

### Backtesting

#### Run canonical 4% breakout baseline
```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy thresholdbreakout \
  --breakout-threshold 0.04 \
  --universe-size 2000 \
  --start-year 2015 \
  --end-year 2026 \
  --start-date 2015-01-01 \
  --end-date 2026-04-22
```
Expected: matches the frozen canonical 4% breakout baseline in `docs/research/CANONICAL_REPORTING_RUNSET_2026-04-22.md`
Experiment ID: `d245816e1d89e196`

#### Run 2LYNCHBreakout (configurable threshold, LONG)
```bash
# 4% — should match the canonical 4% breakout baseline
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy thresholdbreakout --breakout-threshold 0.04 \
  --universe-size 2000 --start-year 2015 --end-year 2025

# 2% breakout
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy thresholdbreakout --breakout-threshold 0.02 \
  --universe-size 2000 --start-year 2015 --end-year 2025
```

#### Run 2LYNCHBreakdown (configurable threshold, SHORT)
```bash
# 4% breakdown (short)
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy 2lynchbreakdown --breakout-threshold 0.04 \
  --universe-size 2000 --start-year 2015 --end-year 2025

# 2% breakdown (short)
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy 2lynchbreakdown --breakout-threshold 0.02 \
  --universe-size 2000 --start-year 2015 --end-year 2025
```

#### List all strategies
```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest --list-strategies
```

#### Run single year (fast sanity check)
```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy thresholdbreakout --start-year 2023 --end-year 2023 \
  --universe-size 500
```

#### Refresh feature tables and market monitor
```bash
# Normal short-window refresh from a known catch-up date
doppler run -- uv run nseml-build-features --since YYYY-MM-DD

# Smart missing: check ALL feature tables, upsert only gaps
doppler run -- uv run nseml-build-features --missing

# Rebuild a specific table only
doppler run -- uv run nseml-build-features --feature-set intraday --force --allow-full-rebuild

# Explicit full rebuild only when necessary
doppler run -- uv run nseml-build-features --force --allow-full-rebuild

# Symbol-level feature rebuild
doppler run -- uv run nseml-build-features --symbols RELIANCE,TCS,INFY
doppler run -- uv run nseml-build-features --symbols-file data/missing_symbols.txt

# Incremental market monitor refresh
doppler run -- uv run nseml-market-monitor --incremental --since YYYY-MM-DD
```

Notes:
- `nseml-build-features --missing` checks each table independently (feat_daily_core, feat_intraday_core, feat_2lynch_derived) and upserts only what's missing per table.
- `nseml-build-features --since` is the normal operator path for short refreshes.
- `--force` is reserved for exceptional full rebuilds and requires `--allow-full-rebuild`.
- `--feature-set` rebuilds a single table without touching others.
- All build commands now log per-step elapsed time.
- DuckDB tuning: 36GB memory, 8 threads by default. Override via `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS` env vars.
- Manifest optimization: Intraday feature builds use a single parquet file manifest instead of per-symbol filesystem globs, reducing glob overhead for large universes.

#### Data quality report
```bash
# Quick CLI report: coverage, feature health, freshness, gaps, anomalies
doppler run -- uv run nseml-hygiene --report

# Preview dead (delisted) symbols
doppler run -- uv run nseml-hygiene --dry-run

# Purge dead symbols from parquet + DuckDB
doppler run -- uv run nseml-hygiene --purge --confirm
```

#### Database verification
```bash
doppler run -- uv run nseml-db-verify
```
**What it does**:
1. Verifies PostgreSQL runtime tables exist and are readable
2. Verifies DuckDB runtime coverage and `feat_daily`
3. Checks the 5-minute parquet lake for legacy `03:45` timestamp files
4. Fails fast if the timestamp regression reappears

**Intraday build note**:
- `feat_intraday_core` uses CPR-style symbol batches and symbol-specific parquet reads.
- Tune `INTRADAY_CORE_BATCH_SIZE` if you need smaller or larger batches.
- Keep DuckDB memory and spill limits set at launch time (`DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS`).
- The legacy yearly helper is hard-gated and should not be used in normal operations.

#### Symbol-scoped ingestion
```bash
# Ingest a symbol subset for one day
doppler run -- uv run nseml-kite-ingest --date 2026-03-27 --symbols RELIANCE,TCS,INFY

# Ingest from a file (no symbol limit)
doppler run -- uv run nseml-kite-ingest --symbols-file data/missing_symbols_daily.txt --from 2025-04-01 --to 2026-03-27

# Auto-detect missing symbols from tradeable master
doppler run -- uv run nseml-kite-ingest --missing

# Backfill against the full current Kite master universe
doppler run -- uv run nseml-kite-ingest --backfill --universe current-master
```

Notes:
- `--symbols` (CSV, max 50), `--symbols-file` (file, no limit), `--missing` (auto-detect from tradeable master).
- `--universe current-master` switches the universe resolver for full-master backfills.

**Important**: DuckDB is single-writer. Stop the dashboard before running a backtest.
Backtest results are stored in DuckDB (`bt_experiment`, `bt_trade`, `bt_yearly_metric`) and
PostgreSQL (`exp_run`, `exp_metric`), and artifacts in MinIO.

---

### Additional Backtest Commands

#### List strategies
```bash
doppler run -- uv run nseml-backtest --list-strategies
```

#### Check backtest status
```bash
doppler run -- uv run nseml-backtest-status --exp-id <EXP_ID>
```

#### Batch backtest run
```bash
doppler run -- uv run nseml-backtest-batch --params-json '{\"strategies\": [\"thresholdbreakout\", \"2lynchbreakdown\"]}'
```

---

### Paper Trading Commands

The v2 paper engine (`nseml-paper`) uses DuckDB-backed state and auto-resumes sessions — re-running `prepare` for the same strategy/date/mode returns the existing session rather than creating a duplicate.

Key behaviors:
- **Idempotent sessions**: `prepare` is safe to re-run after any interruption.
- **Atomic bar commits**: each 5-minute bar's DB writes are all-or-nothing; crash → rollback → clean resume.
- **Signal tracking**: each seeded symbol gets a `paper_session_signals` row; entries, exits, and `update_signal_state` are all linked by `signal_id`.
- **Crash recovery**: open positions are flattened at last-known mark prices; session marked FAILED; `live` resumes from last checkpoint.
- **Strategy overrides**: `--metadata '{"breakout_threshold":0.02}'` (or `--risk-config`) are persisted in the session and applied automatically on resume.

#### Create or resume a session

```bash
# Create (or resume) a replay session for a specific date
doppler run -- uv run nseml-paper prepare \
  --strategy thresholdbreakout \
  --mode replay \
  --trade-date 2026-03-25 \
  --portfolio-value 1000000

# With strategy param overrides (e.g. 2% threshold)
doppler run -- uv run nseml-paper prepare \
  --strategy thresholdbreakout \
  --mode live \
  --trade-date 2026-03-25 \
  --metadata '{"breakout_threshold":0.02}'
```

Output: `{"session_id": "...", "strategy": "...", "symbols": N, "resumed": false|true}`

#### Replay historical candles

```bash
# --session-id is optional; auto-discovers by --strategy + --trade-date
doppler run -- uv run nseml-paper replay --strategy thresholdbreakout --trade-date 2026-03-25
doppler run -- uv run nseml-paper replay --session-id <SESSION_ID> --trade-date 2026-03-25
```

#### Run a live session

```bash
doppler run -- uv run nseml-paper live --strategy thresholdbreakout --trade-date 2026-03-25
doppler run -- uv run nseml-paper live --session-id <SESSION_ID>
doppler run -- uv run nseml-paper live --session-id <SESSION_ID> --poll-interval 1.0 --max-cycles 100
```

#### Run multiple live sessions (one writer process)

```bash
# Preferred method: one shared writer for breakout + breakdown
doppler run -- uv run nseml-paper multi-live \
  --strategy 2lynchbreakout \
  --strategy 2lynchbreakdown \
  --trade-date 2026-04-27

# Or by explicit session IDs (repeatable)
doppler run -- uv run nseml-paper multi-live \
  --session-id <ID_1> --session-id <ID_2>

# Suppress Telegram/email alerts
doppler run -- uv run nseml-paper multi-live \
  --strategy 2lynchbreakout --strategy 2lynchbreakdown \
  --trade-date 2026-04-27 --no-alerts
```

**Why**: DuckDB is single-writer — running two separate `live` processes against the same `paper.duckdb` causes lock contention. `multi-live` runs all sessions inside one process, sharing one DB connection.

#### Session status

```bash
# List all sessions (tabular)
doppler run -- uv run nseml-paper status

# Filter by status
doppler run -- uv run nseml-paper status --status ACTIVE
doppler run -- uv run nseml-paper status --status PLANNED --limit 10

# Full session JSON for a specific session
doppler run -- uv run nseml-paper status --session-id <SESSION_ID>
```

#### Session lifecycle management

```bash
# Pause an active session
doppler run -- uv run nseml-paper pause --session-id <SESSION_ID>
# or auto-discover
doppler run -- uv run nseml-paper pause --strategy thresholdbreakout --trade-date 2026-03-25 --mode live

# Resume a paused session
doppler run -- uv run nseml-paper resume --session-id <SESSION_ID>

# Stop (mark COMPLETED)
doppler run -- uv run nseml-paper stop --session-id <SESSION_ID>

# Flatten open positions and pause session
doppler run -- uv run nseml-paper flatten --session-id <SESSION_ID>

# EMERGENCY: force-close all positions across all sessions for a trade date
doppler run -- uv run nseml-paper flatten-all --trade-date 2026-04-24

# Archive a completed session
doppler run -- uv run nseml-paper archive --session-id <SESSION_ID>
```

#### Multi-variant sessions

```bash
# Plan (dry-run preview only)
doppler run -- uv run nseml-paper plan \
  --strategy thresholdbreakout \
  --trade-date 2026-03-25 \
  --symbols RELIANCE,TCS,INFY \
  --variants 3 \
  --dry-run

# Create N variant sessions
doppler run -- uv run nseml-paper plan \
  --strategy thresholdbreakout \
  --trade-date 2026-03-25 \
  --symbols RELIANCE,TCS,INFY \
  --variants 3
```

#### Daily shortcuts (pre-fill today's date)

```bash
# Prepare today's session
doppler run -- uv run nseml-paper daily-prepare --strategy thresholdbreakout --mode replay

# Replay today's candles
doppler run -- uv run nseml-paper daily-replay --strategy thresholdbreakout

# Run live session for today
doppler run -- uv run nseml-paper daily-live --strategy thresholdbreakout

# Fast daily simulation (single-day replay, prints summary)
doppler run -- uv run nseml-paper daily-sim --session-id <SESSION_ID> --trade-date 2026-03-25
```

#### Post-market EOD carry

```bash
# Run after nseml-build-features completes — applies H-carry decisions and TIME_EXIT
doppler run -- uv run nseml-paper eod-carry --strategy 2lynchbreakout --trade-date 2026-04-24

# Or by session ID
doppler run -- uv run nseml-paper eod-carry --session-id <SESSION_ID> --trade-date 2026-04-24

# Suppress alerts
doppler run -- uv run nseml-paper eod-carry --strategy 2lynchbreakout --trade-date 2026-04-24 --no-alerts
```

**When to run**: After `nseml-build-features --since TODAY` completes. The H-carry rule checks daily
`close_pos_in_range` (the `filter_h` feature), which is only available after features are rebuilt.
This step carries positions overnight or triggers TIME_EXIT / WEAK_CLOSE_EXIT based on hold duration
and the H-filter signal.

See also: [`docs/operations/pre-open-live-paper.md`](../operations/pre-open-live-paper.md)

---

## Testing Commands

### Run All Tests
```bash
doppler run -- uv run pytest tests/unit/ -q
```
**Expected**: all unit tests pass; exact count varies with branch and local fixtures

### Run Specific Test File
```bash
doppler run -- uv run pytest tests/unit/test_config.py -v
```

### Run with Coverage
```bash
doppler run -- uv run pytest --cov=src/nse_momentum_lab --cov-report=html
```

### Run Specific Test
```bash
doppler run -- uv run pytest tests/unit/test_config.py::TestSettings::test_defaults -v
```

### Linting
```bash
# Check code
doppler run -- uv run ruff check .

# Fix auto-fixable issues
doppler run -- uv run ruff check . --fix
```

### Type Checking
```bash
doppler run -- uv run mypy src tests
```

---

## Monitoring & Debugging

### Check Database Connection
```bash
doppler run -- uv run python -c "from nse_momentum_lab.db import get_sessionmaker; print('DB connection OK')"
```

### View Database Tables
```bash
doppler run -- uv run python -c "
from nse_momentum_lab.db import get_sessionmaker
from sqlalchemy import text
async def check():
    sm = get_sessionmaker()
    async with sm() as session:
        result = await session.execute(text('SELECT COUNT(*) FROM nseml.ref_symbol'))
        print(f'Symbols: {result.scalar()}')
import asyncio
asyncio.run(check())
"
```

### Check MinIO Connection
```bash
doppler run -- uv run python -c "
from nse_momentum_lab.config import Settings
s = Settings()
print(f'MinIO: {s.minio_endpoint}')
print(f'Access Key: {s.minio_access_key[:8]}...')
"
```

### View API Logs
```bash
# API server logs (when running in foreground)
# Logs show in terminal where you ran: doppler run -- uv run nseml-api
```

### View Dashboard Logs
```bash
# Dashboard logs appear in the terminal where nseml-dashboard is running
```

### Debug Failed Ingestion
```bash
# Check job_run table
doppler run -- uv run python -c "
from nse_momentum_lab.db import get_sessionmaker
from sqlalchemy import select, text
async def check():
    sm = get_sessionmaker()
    async with sm() as session:
        result = await session.execute(text('SELECT * FROM nseml.job_run ORDER BY started_at DESC LIMIT 5'))
        for row in result:
            print(row)
import asyncio
asyncio.run(check())
"
```

---

## AI Agent Commands

### Chat Interface

The Chat page now supports natural language queries:

#### Available Commands
```
"What's today's summary?" - Pipeline overview
"Show me recent scans" - Latest scan results
"Show me the latest experiment" - Last backtest results
"Show my positions" - Paper trading positions
"help" - Show available commands
```

**Status**: Functional - queries database for deterministic data

---

## Common Workflows

### Workflow 1: First Time Setup

```bash
# 1. Install dependencies
uv sync

# 2. Start Docker services
doppler run -- docker compose up -d

# 3. Run tests to verify
doppler run -- uv run pytest -q

# 4. Start API
doppler run -- uv run nseml-api

# 5. Start Dashboard (in new terminal)
doppler run -- uv run nseml-dashboard

# 6. Access Dashboard
# Open http://localhost:8501 in browser
```

### Workflow 2: Using the Dashboard

The dashboard at http://localhost:8501 provides:
- Backtest results with equity curves, exit analysis, and trade tables
- Experiment comparison and strategy sensitivity analysis
- Data quality validation and pipeline status monitoring

Navigate using the persistent sidebar on the left.

### Workflow 2: Run Your First Scan

```bash
# 1. Ensure services are running
docker compose ps

# 2. Run scans (market data loaded from Parquet)
doppler run -- uv run python -m nse_momentum_lab.services.scan.worker 2025-02-07

# 3. Generate rollups
doppler run -- uv run python -c "import asyncio; from datetime import date; from nse_momentum_lab.services.rollup.worker import run_daily_rollup; asyncio.run(run_daily_rollup(date(2025,2,7)))"

# 4. Check Dashboard
# Refresh http://localhost:8501 to see data
```

### Workflow 3: Daily Operations

```bash
# Assuming API and Dashboard are already running

# 1. Run scans (after market close)
doppler run -- uv run python -m nse_momentum_lab.services.scan.worker $(date +%Y-%m-%d)

# 2. Check results in Dashboard
# Pipeline Status - see job runs
# Scans - see candidates
```

### Workflow 4: Reset & Start Fresh

```bash
# 1. Stop everything
docker compose down
# Stop API and Dashboard (Ctrl+C in their terminals)

# 2. Remove volumes (CAUTION: deletes all data)
docker compose down -v

# 3. Restart
doppler run -- docker compose up -d

# 4. Re-run tests
doppler run -- uv run pytest -q

# 5. Start applications
doppler run -- uv run nseml-api
doppler run -- uv run nseml-dashboard
```

### Workflow 5: Debug Pipeline Issues

```bash
# 1. Check if services are running
docker compose ps

# 2. Check API health
curl http://127.0.0.1:8004/health

# 3. Check recent job runs
curl http://127.0.0.1:8004/api/ingestion/status

# 4. Check database for errors
doppler run -- uv run python -c "
from nse_momentum_lab.db import get_sessionmaker
from sqlalchemy import select, text
async def check():
    sm = get_sessionmaker()
    async with sm() as session:
        result = await session.execute(text('SELECT job_name, status, error_json FROM nseml.job_run WHERE status = 'FAILED' ORDER BY started_at DESC LIMIT 5'))
        for row in result:
            print(f'{row.job_name}: {row.status}')
            if row.error_json:
                print(f'  Error: {row.error_json}')
import asyncio
asyncio.run(check())
"

# 5. Check application logs
# Review API and Dashboard terminal output
```

---

## Quick Reference Card

### Start Everything
```bash
doppler run -- docker compose up -d
doppler run -- uv run nseml-api
doppler run -- uv run nseml-dashboard
```

### Stop Everything
```bash
docker compose down
# Ctrl+C for API and Dashboard
```

### Run Pipeline for Date
```bash
DATE=2025-02-07
doppler run -- uv run python -m nse_momentum_lab.services.scan.worker $DATE
doppler run -- uv run python -c "import asyncio; from datetime import date; from nse_momentum_lab.services.rollup.worker import run_daily_rollup; asyncio.run(run_daily_rollup(date($DATE)))"
```

### Run Tests
```bash
doppler run -- uv run pytest -q
```

### Access Points
- Dashboard: http://localhost:8501
- API: http://127.0.0.1:8004
- API Docs: http://127.0.0.1:8004/docs
- MinIO: http://127.0.0.1:9004

---

## Troubleshooting

### Port Already in Use
```bash
# Find what's using the port
netstat -ano | findstr :8004

# Kill the process
taskkill /PID <pid> /F
```

### Database Connection Failed
```bash
# Check Postgres is running
docker compose ps postgres

# Check logs
docker compose logs postgres

# Restart Postgres
docker compose restart postgres
```

### MinIO Connection Failed
```bash
# Check MinIO is running
docker compose ps minio

# Check logs
docker compose logs minio

# Restart MinIO
docker compose restart minio
```

### Tests Failing
```bash
# Reinstall dependencies
uv sync

# Run with verbose output
doppler run -- uv run pytest -v

# Check Python version
python --version  # Should be >= 3.14
```

### Dashboard Not Loading
```bash
# Check if API is running
curl http://127.0.0.1:8004/health

# Restart Dashboard
# Ctrl+C in Dashboard terminal, then:
doppler run -- uv run nseml-dashboard
```

---

**Last Updated**: 2026-04-24
**For Issues**: Check [dev/AGENTS.md](../dev/AGENTS.md) runbook
