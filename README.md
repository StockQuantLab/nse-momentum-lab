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

Operational notes:

- The restored Kite ingestion files now live under `src/nse_momentum_lab/services/kite/` and `src/nse_momentum_lab/cli/`.
- Daily data writes to `data/parquet/daily/<SYMBOL>/kite.parquet`.
- Five-minute data writes to `data/parquet/5min/<SYMBOL>/<YEAR>.parquet`.
- Symbol-level resume checkpoints are stored in `data/raw/kite/checkpoints/`.

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

Walk-forward is the promotion gate for replay and live paper sessions.

```powershell
doppler run -- uv run nseml-paper cleanup-walk-forward --yes
doppler run -- uv run nseml-paper walk-forward --strategy thresholdbreakout --start-date 2025-04-01 --end-date 2026-03-09
doppler run -- uv run nseml-paper replay-day --session-id <SESSION_ID> --trade-date 2026-03-09 --skip-gate
doppler run -- uv run nseml-paper live --session-id <SESSION_ID> --trade-date 2026-03-22 --execute --run
doppler run -- uv run nseml-paper qualify --session-id <SESSION_ID> --max-rank 10
doppler run -- uv run nseml-paper alert --session-id <SESSION_ID> --signal-ids <ID1,ID2>
```

The `/paper_ledger` page now emphasizes:

- walk-forward decision and fold history
- a session summary that explains whether you are looking at a walk-forward, replay, or live session
- a trade watchlist instead of a generic queue label
- recent activity and feed details for live sessions

## Dashboard UX Model

- Stable pages are shown by default:
  - Home
  - Backtest Results
  - Paper Ledger
  - Run Pipeline
- API-dependent legacy pages are hidden under "Legacy Pages".

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
