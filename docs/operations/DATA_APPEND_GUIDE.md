# Data Append Guide

This guide explains how to append new market data to NSE Momentum Lab and trigger incremental rebuilds of features and backtests.

---

## Overview

NSE Momentum Lab uses a layered data architecture:

| Layer | Storage | Format | Update Pattern |
|-------|---------|--------|----------------|
| Bronze | MinIO / local | Raw CSV/JSON | Append-only |
| Silver | MinIO / local | Parquet | Append or replace partition |
| Gold | DuckDB | Materialized tables | Incremental refresh |

**Key principle**: Adding new data should NOT require a full historical rebuild.

---

## Adding New Daily Data

### 1. Prepare Raw Data Files

Place new daily OHLCV CSV files in the bronze layer:

```bash
# Bronze data location (adjust based on DATA_LAKE_MODE)
data/bronze/daily/<SYMBOL>/YYYY/<SYMBOL>_YYYY_MM_DD.csv
```

Example file structure:
```csv
symbol,date,open,high,low,close,volume,value_traded
RELIANCE,2026-01-02,2345.50,2380.00,2330.00,2375.00,1250000,2950000000
```

### 2. Run the Ingestion Pipeline

```bash
# Ingest new daily data
doppler run -- uv run python -m nse_momentum_lab.cli.ingest \
  --dataset daily \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

The ingestion job will:
- Validate data quality (OHLC constraints, price ranges)
- Normalize to adjusted prices
- Publish to Silver layer as partitioned Parquet
- Update dataset manifest in Postgres

### 3. Verify Ingestion

```bash
# Check latest available date
doppler run -- uv run python -c "
from nse_momentum_lab.db.market_db import get_market_db
db = get_market_db()
latest = db.con.execute('SELECT MAX(date) FROM v_daily').fetchone()
print(f'Latest daily date: {latest[0]}')
"
```

### 4. Incremental Feature Rebuild

For daily features with 252-day rolling windows, only rebuild:
- New partitions (2026 data)
- Late-2025 overlap (for rolling window warmup)

```bash
# Rebuild affected feature sets
doppler run -- uv run python -m nse_momentum_lab.cli.build_features \
  --feature-set feat_daily_core \
  --incremental \
  --start-date 2025-10-01  # Warmup period
```

The materializer will:
1. Check current materialization state
2. Determine affected date ranges
3. Build only affected partitions
4. Update `bt_materialization_state`

### 5. Backtest Rerun Strategy

**Option A: Rerun only affected date ranges**

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy thresholdbreakout \
  --start-year 2026 \
  --end-year 2026
```

**Option B: Full rerun (recommended if features changed)**

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy thresholdbreakout \
  --start-year 2015 \
  --end-year 2026
```

---

## Adding New 5-Minute Data

### 1. Prepare 5-Minute Files

```bash
data/bronze/5min/<SYMBOL>/YYYY/MM/<SYMBOL>_YYYY_MM_DD_5min.csv
```

### 2. Ingest with Correct Grain

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.ingest \
  --dataset 5min \
  --start-date 2026-03-01 \
  --end-date 2026-03-31
```

### 3. Rebuild Intraday Features

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.build_features \
  --feature-set feat_intraday_core \
  --incremental \
  --start-date 2026-02-15  # Short lookback for intraday
```

---

## Detecting Stale Runs

After data updates, identify experiments that need rerun:

```python
from nse_momentum_lab.services.research import (
    DatasetVersionTracker,
    list_stale_runs,
)
from nse_momentum_lab.db.market_db import get_market_db

db = get_market_db()
tracker = DatasetVersionTracker(db.con)

# Find all stale runs
stale = list_stale_runs(tracker, limit=50)

for run in stale:
    print(f"{run.exp_hash}: {run.stale_reasons}")
```

### CLI Command (Planned)

```bash
# List stale experiments
doppler run -- uv run python -m nse_momentum_lab.cli.research_status \
  --stale-only

# Rerun specific stale experiment
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --rerun <exp_hash>
```

---

## Repair Mode

When data corrections are needed (not appends):

### Repair a Specific Date Range

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.ingest \
  --dataset daily \
  --start-date 2024-03-01 \
  --end-date 2024-03-31 \
  --repair
```

### Repair a Single Symbol

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.ingest \
  --dataset daily \
  --symbol RELIANCE \
  --repair
```

### Full Rebuild (Last Resort)

```bash
# CAUTION: This rebuilds everything
doppler run -- uv run python -m nse_momentum_lab.cli.build_features \
  --force-all
```

---

## Manifest Tracking

Dataset manifests track lineage for reproducibility:

```sql
-- Check current dataset version
SELECT * FROM nseml.dataset_manifest
WHERE dataset_kind = 'daily'
ORDER BY created_at DESC
LIMIT 1;

-- Check feature materialization state
SELECT * FROM bt_materialization_state
WHERE table_name LIKE 'feat_%'
ORDER BY updated_at DESC;
```

---

## Troubleshooting

### "Feature rebuild is slow"

1. Check if rebuild is truly incremental:
   ```sql
   SELECT table_name, row_count, updated_at
   FROM bt_materialization_state;
   ```

2. For 5-year rolling features, ~1.4 years of overlap is normal

3. Consider increasing `partition_grain` if using monthly partitions

### "New data not appearing in backtest"

1. Verify ingestion completed:
   ```sql
   SELECT MAX(date) FROM v_daily;
   ```

2. Check feature materialization includes new dates:
   ```sql
   SELECT MAX(trading_date) FROM feat_daily_core;
   ```

3. Rebuild features with `--force` if state is inconsistent

### "Stale run detection not working"

1. Check hashes are being computed correctly:
   ```python
   from nse_momentum_lab.services.research import DatasetVersionTracker
   tracker = DatasetVersionTracker(db.con)
   current = tracker.get_current_dataset_version()
   print(f"Current hash: {current.hash}")
   ```

2. Verify `bt_experiment` table has `dataset_hash` populated:
   ```sql
   SELECT exp_hash, dataset_hash FROM bt_experiment LIMIT 5;
   ```

---

## Best Practices

1. **Test on single symbol first**: Before full ingestion, test with `--symbol RELIANCE`

2. **Monitor manifest changes**: Always check manifests after ingestion completes

3. **Keep bronze files**: Never delete raw bronze files; they are source of truth

4. **Version control schema changes**: When feature schemas change, bump `FEAT_DAILY_QUERY_VERSION`

5. **Document repair reasons**: When using `--repair`, add notes to `nseml.dataset_manifest.metadata_json`

6. **Incremental > Full**: Always prefer incremental rebuilds unless schema changed

---

## Append Performance Expectations

| Operation | Expected Duration | Notes |
|-----------|-------------------|-------|
| Ingest 1 month daily data | ~30 seconds | ~1,832 symbols |
| Incremental feat_daily_core rebuild | ~2-3 minutes | With 1-year overlap |
| Incremental feat_intraday_core rebuild | ~5-10 minutes | More data |
| Full backtest (1 year, 500 stocks) | ~1-2 minutes | Depends on signal density |
