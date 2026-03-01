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

## Dashboard UX Model

- Stable pages are shown by default:
  - Home
  - Backtest Results
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
- ADRs: [docs/adr/ADR-INDEX.md](docs/adr/ADR-INDEX.md)
