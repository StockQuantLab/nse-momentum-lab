# Zerodha Kite API Ingestion Layer — Plan & Status

**Last updated**: 2026-03-10
**Goal**: Ingest daily OHLCV and 5-minute OHLCV from Kite Connect without touching existing 10-year baseline parquet.

---

## Data Safety Contract

Kite ingestion writes to dedicated targets and never mutates baseline files:

```text
data/parquet/daily/SYMBOL/all.parquet   # existing baseline (read-only)
data/parquet/daily/SYMBOL/kite.parquet  # Kite daily incremental data
data/parquet/5min/SYMBOL/YYYY.parquet   # Kite 5-min data
```

DuckDB daily view globs `data/parquet/daily/*/*.parquet`, so both baseline and kite files are automatically visible.

---

## Implemented Architecture

| Area | Implementation | Status |
|---|---|---|
| Auth flow | CLI token exchange via `scripts/kite_get_token.py` | ✅ |
| FastAPI auth routes | Removed (not required for local 2FA flow) | ✅ |
| Ingestion API routes | `POST /ingestion/kite/daily`, `POST /ingestion/kite/5min`, `GET /ingestion/kite/status` | ✅ |
| Scheduler lifecycle | Singleton scheduler (`get_kite_scheduler`) | ✅ |
| Symbol resolution | Local parquet universe first, Kite fallback | ✅ |
| Instrument master caching | `data/raw/kite/instruments/NSE.csv` + in-memory map | ✅ |
| Cache miss behavior | One per-exchange API refresh, then miss cache | ✅ |
| Retry/backoff | Exponential retry for transient Kite errors | ✅ |
| Resume/checkpoint | Symbol-level checkpoint JSON with resume default | ✅ |
| Checkpoint I/O | Batched checkpoint flush (`CHECKPOINT_FLUSH_EVERY=25`) | ✅ |
| Daily feature update | Decoupled; enabled via `--update-features` | ✅ |
| Materialization state update | Incremental `feat_daily` updates snapshot/hash state | ✅ |

---

## Runtime Behavior (Current)

### Auth

- `request_token` is one-time exchange code.
- `access_token` is valid until around `06:00 AM IST`.
- Renewing before expiry is allowed (for example `05:55 AM IST`), but still requires full login flow.
- New token must be persisted with printed Doppler command.

### Scheduler default path (`--symbols` omitted)

1. Resolve symbols from baseline daily universe (`data/parquet/daily/*/all.parquet`).
2. If local universe unavailable, resolve from Kite instrument master.
3. Resolve instrument token from memory cache, then `NSE.csv`, then API refresh-on-miss.
4. Fetch with retry/backoff and write parquet with dedup.
5. Persist checkpoint progress and resume on rerun.

### Result accounting

- `status: ok` with `records > 0` counts as success.
- `status: ok` with `records = 0` counts under failed counter (non-crash, usually missing token/no data).
- Hard exceptions are tracked as `status: error`.

---

## Operational Commands

### Token refresh

```bash
doppler run -- uv run python scripts/kite_get_token.py
```

### Instrument master refresh

```bash
doppler run -- uv run python scripts/kite_refresh_instruments.py --exchange NSE
# or
doppler run -- uv run nseml-kite-ingest --refresh-instruments --exchange NSE
```

### Daily ingestion

```bash
doppler run -- uv run nseml-kite-ingest --today
doppler run -- uv run nseml-kite-ingest --today --update-features
doppler run -- uv run nseml-kite-ingest --date 2026-03-06
doppler run -- uv run nseml-kite-ingest --from 2026-03-05 --to 2026-03-06 --save-raw
```

### 5-min ingestion (recommended chunked backfill)

```bash
doppler run -- uv run nseml-kite-ingest --from 2025-04-01 --to 2025-05-31 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2025-06-01 --to 2025-07-31 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2025-08-01 --to 2025-09-30 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2025-10-01 --to 2025-11-30 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2025-12-01 --to 2026-01-31 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2026-02-01 --to 2026-03-09 --5min --resume
```

Fallback one-shot command is still available:

```bash
doppler run -- uv run nseml-kite-ingest --backfill --5min
```

---

## Validation and Reporting

Data written by ingestion:

- Parquet: `data/parquet/daily/...` and `data/parquet/5min/...`
- Raw CSV snapshots (optional): `data/raw/kite/daily/...`, `data/raw/kite/5min/...`
- Checkpoints: `data/raw/kite/checkpoints/*.json`
- Optional quality report exports: `data/raw/kite/reports/*.csv`

Recommended quick checks after each chunk:

1. Confirm run result has `checkpoint_cleared: True` for that chunk.
2. Spot-check symbol files under `data/parquet/5min/<SYMBOL>/2025.parquet`.
3. Track missing-token and zero-row symbols from report CSVs.

---

## FastAPI Endpoints (Retained)

| Endpoint | Purpose |
|---|---|
| `POST /ingestion/kite/daily` | Trigger daily ingestion (`symbols_csv`, `trading_date`, `update_features`, `save_raw`, `resume`) |
| `POST /ingestion/kite/5min` | Trigger 5-min ingestion (`symbols_csv`, `start_date`, `end_date`, `save_raw`, `resume`) |
| `GET /ingestion/kite/status` | Auth status + daily min/max date snapshot |

---

## API Limits and Resilience

| Item | Value |
|---|---|
| Base request spacing | `0.35s` |
| Max retries | `5` |
| Backoff | Exponential (`1s` base, `30s` cap, jitter) |
| Daily chunk size | up to `2000` days per request |
| 5-min chunk size | up to `60` days per request |
| Checkpoint flush | every `25` symbols |

---

## Current Operational To-Dos (as of 2026-03-13)

1. Backfill campaign status:
   - Daily and 5-min historical backfill through `2026-03-09` is complete.
2. Next ingestion catch-up window:
   - Ingest from `2026-03-10` through current date (daily + 5-min) in the next ops run.
3. Post-catch-up validation:
   - Run focused DQ report on the catch-up window and reconcile any new missing-token / duplicate issues.
4. Scheduler hardening:
   - Configure Windows Task Scheduler for daily ingestion after 2-3 consecutive clean daily runs.
