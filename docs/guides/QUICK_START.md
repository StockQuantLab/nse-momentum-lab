# Quick Start

## Prerequisites

- Docker Desktop
- Doppler CLI
- Python 3.14+
- `uv`

## Setup

```powershell
uv sync
doppler run -- docker compose up -d
doppler run -- uv run pytest -q
```

## Run Services

Terminal 1:

```powershell
doppler run -- uv run nseml-api
```

Terminal 2:

```powershell
doppler run -- uv run nseml-dashboard
```

## First Backtest

```powershell
doppler run -- uv run nseml-backtest --universe-size 50 --start-year 2024 --end-year 2024 --force
```

Then open `Backtest Results` in the dashboard.

## Quality Gates Before Push

```powershell
doppler run -- uv run python scripts/quality_gate.py --with-format-check --with-full
```

## Common URLs

- Dashboard: `http://localhost:8501`
- API docs: `http://127.0.0.1:8004/docs`
- MinIO console: `http://127.0.0.1:9004`
