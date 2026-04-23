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

### Today-only example: 2026-03-30

Use this when you want to ingest only `2026-03-30` for the full current universe.

```bash
# Daily OHLCV for 2026-03-30 only
doppler run -- uv run nseml-kite-ingest --date 2026-03-30

# 5-minute OHLCV for 2026-03-30 only
doppler run -- uv run nseml-kite-ingest --date 2026-03-30 --5min --resume
```

Recommended follow-up after both runs complete:

```bash
doppler run -- uv run nseml-build-features --since 2026-03-30
doppler run -- uv run nseml-market-monitor --incremental --since 2026-03-30
doppler run -- uv run nseml-db-verify
```

### 1. Ingest the date window

Use the current CLI directly. There is no `--dataset` flag.

```bash
# Today
doppler run -- uv run nseml-kite-ingest --today

# Specific date
doppler run -- uv run nseml-kite-ingest --date 2026-03-27

# Range
doppler run -- uv run nseml-kite-ingest --from 2026-03-24 --to 2026-03-27

# Optional raw CSV snapshots
doppler run -- uv run nseml-kite-ingest --today --save-raw
```

### Symbol-scoped daily ingest

If you want to ingest only a specific symbol list, pass `--symbols` to the Kite ingest CLI.
This is the supported symbol-level control for ingestion.

```bash
# Ingest one day for a symbol subset
doppler run -- uv run nseml-kite-ingest --date 2026-03-27 --symbols RELIANCE,TCS,INFY

# Backfill a date window for a symbol subset
doppler run -- uv run nseml-kite-ingest --from 2026-03-24 --to 2026-03-27 --symbols RELIANCE,TCS,INFY

# Force current Kite master resolution instead of local-first universe selection
doppler run -- uv run nseml-kite-ingest --backfill --universe current-master
```

Notes:
- `--symbols` is exact symbol-level filtering for ingestion.
- `--universe current-master` changes the default resolver; it does not override an explicit `--symbols` list.
- `--update-features` still rebuilds the dependent daily feature tables after ingest; it does not create a symbol-only feature pack.

### 2. Refresh dependent tables

```bash
doppler run -- uv run nseml-build-features --since 2026-03-27
doppler run -- uv run nseml-market-monitor --incremental --since 2026-03-27
doppler run -- uv run nseml-db-verify
```

Note: `nseml-build-features` automatically force-syncs the market replica after every build. The
dashboard reads from the replica (`data/market_replica/`), not the source DB — so the market
monitor, DQ, and other dashboard pages will reflect the latest data after this step.

If you need to refresh data for only a small symbol list, the supported route is:

1. Run `nseml-kite-ingest --symbols ... --update-features` for the affected date window.
2. Rebuild the relevant feature sets with `nseml-build-features --since YYYY-MM-DD`.

`nseml-build-features` is feature-set scoped, not symbol scoped. It rebuilds the selected feature
tables for the requested date range; it does not accept a `--symbols` flag.

### 3. Verify latest coverage

```bash
doppler run -- uv run python -c "
from nse_momentum_lab.db.market_db import get_market_db
db = get_market_db()
latest = db.con.execute('SELECT MAX(date) FROM v_daily').fetchone()
print(f'Latest daily date: {latest[0]}')
"
```

Replay and live paper sessions consume the same loaded runtime coverage. If readiness
fails on stale `market_day_state`, `strategy_day_state`, or `intraday_day_pack` tables,
refresh the runtime tables first and rerun the session.

---

## Adding New 5-Minute Data

### 1. Run the 5-minute catch-up

```bash
# Current day
doppler run -- uv run nseml-kite-ingest --today --5min --resume

# Short catch-up window
doppler run -- uv run nseml-kite-ingest --from 2026-03-24 --to 2026-03-27 --5min --resume
```

### 2. Refresh intraday features

```bash
doppler run -- uv run nseml-build-features --since 2026-03-27
doppler run -- uv run nseml-market-monitor --incremental --since 2026-03-27
```

`feat_intraday_core` now rebuilds with CPR-style symbol batches and symbol-specific parquet reads.
Use `INTRADAY_CORE_BATCH_SIZE` to tune the batch size for low-RAM hosts.
Keep DuckDB memory and temp spill limits set at launch time.
The legacy yearly helper is disabled by default and should not be used for normal rebuilds.

### 3. Verify 5-minute coverage

```bash
doppler run -- uv run python -c "
from nse_momentum_lab.db.market_db import get_market_db
db = get_market_db()
latest = db.con.execute('SELECT MAX(date) FROM v_5min').fetchone()
print(f'Latest 5-min date: {latest[0]}')
"
```

As of `2026-03-27`, the local lake is caught up through that date for both daily and 5-minute
data. Future runs should be incremental catch-up only unless you intentionally need a historical
backfill.

For the `2026-03-30` catch-up, use the explicit `--date 2026-03-30` form instead of `--today`
when you want the runbook and logs to stay pinned to that specific trade date.

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
doppler run -- uv run nseml-backtest \
  --rerun <exp_hash>
```

---

## Corrections and Re-runs

The current CLI does not expose a separate `--repair` mode.

When you need to correct a date window:

1. Re-run the affected date range with `nseml-kite-ingest`.
2. Use `--no-resume` only when you intentionally want to overwrite the same window from scratch.
3. Refresh dependent tables with `nseml-build-features --since <YYYY-MM-DD>` and `nseml-market-monitor --incremental --since <YYYY-MM-DD>`.
   The feature build step auto-syncs the market replica — the dashboard will see updated data.

Example:

```bash
doppler run -- uv run nseml-kite-ingest --from 2026-03-24 --to 2026-03-27 --5min --resume
doppler run -- uv run nseml-build-features --since 2026-03-24
doppler run -- uv run nseml-market-monitor --incremental --since 2026-03-24
```

For the operator-facing table inventory and load modes, see `docs/operations/TABLE_LOAD_MATRIX.md`.

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

3. Rebuild features only with `--force --allow-full-rebuild` if state is inconsistent

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

1. **Test on a short date window first**: Before full ingestion, test with `--from YYYY-MM-DD --to YYYY-MM-DD` on a small window

2. **Monitor manifest changes**: Always check manifests after ingestion completes

3. **Keep bronze files**: Never delete raw bronze files; they are source of truth

4. **Version control schema changes**: When feature schemas change, bump `FEAT_DAILY_QUERY_VERSION`

5. **Incremental > Full**: Always prefer incremental rebuilds unless schema changed

6. **DuckDB performance tuning**: DuckDB connections use 36GB memory limit and 8 threads by default. Override via `DUCKDB_MEMORY_LIMIT` and `DUCKDB_THREADS` env vars if needed. Feature builds apply these settings automatically via `configure_duckdb_for_feature_build()`.

7. **Manifest optimization**: Intraday feature builds use a parquet file manifest (single pathlib walk) instead of per-symbol filesystem globs. This eliminates ~4000 glob calls for a 2000-symbol universe and provides explicit file paths to DuckDB (no glob resolution overhead). The manifest is built once per build session and passed through all batch operations.

---

## Append Performance Expectations

| Operation | Expected Duration | Notes |
|-----------|-------------------|-------|
| Ingest 1 month daily data | ~30 seconds | ~1,832 symbols |
| Incremental feat_daily_core rebuild | ~2-3 minutes | With 1-year overlap |
| Incremental feat_intraday_core rebuild | ~5-10 minutes | More data |
| Full backtest (1 year, 500 stocks) | ~1-2 minutes | Depends on signal density |
