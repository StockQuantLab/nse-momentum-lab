# Command Reference - nse-momentum-lab

**Version**: Phase 1
**Last Updated**: 2025-02-08

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
http://localhost:8501/paper_ledger
http://localhost:8501/daily_summary
```

---

## Pipeline Commands

### Data Layer

Market data is loaded from Zerodha Parquet files via DuckDB. See `nse_momentum_lab.db.market_db`.

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

### Backtesting (Not Fully Tested)

#### Run backtest
```bash
# Note: This service exists but hasn't been tested with real data yet
doppler run -- uv run python -m nse_momentum_lab.services.backtest.worker ...
```
**Status**: Framework exists, needs real data testing

---

## Testing Commands

### Run All Tests
```bash
doppler run -- uv run pytest -q
```
**Expected**: ~149 passed (may vary by environment)

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

**Last Updated**: 2025-02-13
**For Issues**: Check `agents.md` runbook
