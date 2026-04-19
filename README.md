# nse-momentum-lab

Local-first NSE momentum research and backtesting platform.

## Core Principles

- No `.env` files. Secrets are injected via Doppler.
- Strategy math is deterministic Python code, not LLM logic.
- DuckDB is the compute/query engine for market data and backtests.
- PostgreSQL stores operational metadata; MinIO stores artifacts/snapshots.

## Quick Start (Windows/PowerShell)

```powershell
uv sync
doppler run -- docker compose up -d
doppler run -- uv run pytest -q
doppler run -- uv run nseml-api
doppler run -- uv run nseml-dashboard
```

Access:

- Dashboard: `http://localhost:8501`
- API docs: `http://127.0.0.1:8004/docs`
- MinIO console: `http://127.0.0.1:9004`

## Kite Ingestion

Token exchange:

```powershell
doppler run -- uv run nseml-kite-token
```

To exchange the token and write `KITE_ACCESS_TOKEN` back to Doppler in one step:

```powershell
doppler run -- uv run nseml-kite-token --apply-doppler
```

If you already have the redirected callback URL, you can avoid the prompt:

```powershell
doppler run -- uv run nseml-kite-token --request-token "<full callback url>" --apply-doppler
```

Equivalent wrapper scripts are also present:

```powershell
doppler run -- uv run python scripts/kite_get_token.py
doppler run -- uv run python scripts/kite_refresh_instruments.py --exchange NSE
```

Common ingestion commands:

```powershell
doppler run -- uv run nseml-kite-ingest --refresh-instruments --exchange NSE
doppler run -- uv run nseml-kite-ingest --today
doppler run -- uv run nseml-kite-ingest --today --update-features
doppler run -- uv run nseml-kite-ingest --from 2025-04-01 --to 2025-05-31 --5min --resume
doppler run -- uv run python scripts/kite_data_quality_report.py --start-date 2025-04-01 --end-date 2026-03-09
```

Short catch-up windows should stay incremental:

```powershell
doppler run -- uv run nseml-kite-ingest --from YYYY-MM-DD --to YYYY-MM-DD
doppler run -- uv run nseml-kite-ingest --from YYYY-MM-DD --to YYYY-MM-DD --5min --resume
doppler run -- uv run nseml-build-features --since YYYY-MM-DD
doppler run -- uv run nseml-market-monitor --incremental --since YYYY-MM-DD
doppler run -- uv run nseml-db-verify
```

Operational notes:

- The restored Kite ingestion files now live under `src/nse_momentum_lab/services/kite/` and `src/nse_momentum_lab/cli/`.
- Daily data writes to `data/parquet/daily/<SYMBOL>/kite.parquet`.
- Five-minute data writes to `data/parquet/5min/<SYMBOL>/<YEAR>.parquet`.
- Symbol-level resume checkpoints are stored in `data/raw/kite/checkpoints/`.
- `nseml-kite-ingest` loads only the requested date window; it does not backfill the full archive unless you ask it to.
- `nseml-db-verify` checks loaded runtime coverage and materialized tables; it does not ingest data.
- Any destructive full rebuild now requires an explicit `--allow-full-rebuild` acknowledgment.
- Use `docs/operations/TABLE_LOAD_MATRIX.md` for the table/load inventory instead of reading code.

## Storage Model

- DuckDB primary file: `data/market.duckdb`
- Parquet source data: `data/parquet/` (or MinIO S3 when `DATA_LAKE_MODE=minio`)
- Postgres: experiment lineage/metadata via `exp_run` tables
- MinIO: required backtest artifacts plus optional DuckDB snapshots (`--snapshot`)

This gives fast local analytics with reproducible artifacts and metadata history.

## Run Backtest

```powershell
doppler run -- uv run nseml-backtest --universe-size 500 --start-year 2015 --end-year 2025
```

Useful options:

- `--force` re-runs even if experiment hash exists
- `--snapshot` publishes a DuckDB snapshot artifact to MinIO
- `--entry-timeframe 5min` (default) uses 5-minute breakout entry timing with daily 2LYNCH setup filters
- `--progress-file data/progress/full_run.ndjson` appends compact progress heartbeats
- Postgres + MinIO persistence is enforced; run will fail fast if lineage/artifacts cannot be written

Monitor progress from another terminal:

```powershell
doppler run -- uv run nseml-backtest-status --watch --interval 15
```

## Paper Trading Workflow

The v2 paper engine uses DuckDB-backed state and auto-resumes existing sessions (no duplicate sessions on restart).

### Session lifecycle

```powershell
# 1. Create or resume a session (idempotent — returns existing if one exists for the same strategy/date/mode)
doppler run -- uv run nseml-paper prepare --strategy thresholdbreakout --mode replay --trade-date 2026-03-25

# 2a. Replay historical candles (--session-id optional; auto-discovers by --strategy + --trade-date)
doppler run -- uv run nseml-paper replay --strategy thresholdbreakout --trade-date 2026-03-25

# 2b. Run a live session
doppler run -- uv run nseml-paper live --strategy thresholdbreakout --trade-date 2026-03-25

# 3. Session management
doppler run -- uv run nseml-paper status                         # list all sessions
doppler run -- uv run nseml-paper status --session-id <ID>       # full session JSON
doppler run -- uv run nseml-paper status --status ACTIVE         # filter by status
doppler run -- uv run nseml-paper pause  --session-id <ID>
doppler run -- uv run nseml-paper resume --session-id <ID>
doppler run -- uv run nseml-paper stop   --session-id <ID>
doppler run -- uv run nseml-paper flatten --session-id <ID>      # close all open positions
doppler run -- uv run nseml-paper archive --session-id <ID>
```

### Daily shortcuts (today's date pre-filled)

```powershell
doppler run -- uv run nseml-paper daily-prepare --strategy thresholdbreakout --mode live
doppler run -- uv run nseml-paper daily-live    --strategy thresholdbreakout
doppler run -- uv run nseml-paper daily-replay  --strategy thresholdbreakout
doppler run -- uv run nseml-paper daily-sim     --session-id <ID> --trade-date 2026-03-25
```

### Multi-variant sessions

```powershell
# Plan (dry-run preview) or create N variant sessions at once
doppler run -- uv run nseml-paper plan --strategy thresholdbreakout --trade-date 2026-03-25 --symbols RELIANCE,TCS,INFY --variants 3 --dry-run
doppler run -- uv run nseml-paper plan --strategy thresholdbreakout --trade-date 2026-03-25 --symbols RELIANCE,TCS,INFY --variants 3
```

### Strategy params override

Pass `--metadata '{"breakout_threshold":0.02}'` to `prepare` to override strategy defaults. The engine reads those params back automatically when `replay` or `live` resumes the session.

```powershell
doppler run -- uv run nseml-paper prepare --strategy thresholdbreakout --mode live --trade-date 2026-03-25 --metadata '{"breakout_threshold":0.02}'
doppler run -- uv run nseml-paper live --strategy thresholdbreakout --trade-date 2026-03-25
```

See [`docs/operations/pre-open-live-paper.md`](docs/operations/pre-open-live-paper.md) for the full pre-open checklist.

The dashboard `/paper_ledger` page shows session state, open positions, fills, and alerts driven by the DuckDB paper store.

## Dashboard UX Model

Dashboard at `http://localhost:8501` — pages available:

| URL | Description |
|-----|-------------|
| `/` | Home — daily summary tiles |
| `/backtest` | Backtest results, equity curves, execution audit |
| `/trade_analytics` | Trade analytics per experiment |
| `/compare` | Experiment comparison |
| `/strategy` | Strategy analysis and sensitivity |
| `/scans` | Latest scan results |
| `/data_quality` | Data quality report and DQ runner |
| `/pipeline` | Pipeline status |
| `/paper_ledger` | Paper trading — sessions, positions, fills, alerts |
| `/daily_summary` | Daily P&L summary |
| `/market_monitor` | Market regime monitor |

## Local Development Workflow

Install hooks once:

```powershell
uv run pre-commit install --hook-type pre-commit --hook-type pre-push
```

Run gates before push:

```powershell
# Infrastructure required for integration/full tests
doppler run -- docker compose up -d

doppler run -- uv run python scripts/quality_gate.py --with-format-check --with-full
```

## Docs

- Runbook: [agents.md](agents.md)
- Quick start: [docs/guides/QUICK_START.md](docs/guides/QUICK_START.md)
- Dashboard guide: [docs/adr/ADR-011-dashboard-architecture.md](docs/adr/ADR-011-dashboard-architecture.md)
- Paper trading plan: [docs/architecture/PAPER_TRADING_WALK_FORWARD_PLAN.md](docs/architecture/PAPER_TRADING_WALK_FORWARD_PLAN.md)
- Kite ingestion plan: [docs/architecture/KITE_INGESTION_PLAN.md](docs/architecture/KITE_INGESTION_PLAN.md)
- ADRs: [docs/adr/ADR-INDEX.md](docs/adr/ADR-INDEX.md)
