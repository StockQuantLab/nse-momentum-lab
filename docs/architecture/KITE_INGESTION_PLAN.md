# Zerodha Kite API Ingestion Layer — Plan & Status

**Last updated**: 2026-03-27
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
| Symbol resolution | Local parquet universe first, Kite fallback; optional current-master backfill mode | ✅ |
| Instrument master caching | `data/raw/kite/instruments/NSE.csv` + in-memory map | ✅ |
| Cache miss behavior | One per-exchange API refresh, then miss cache | ✅ |
| Retry/backoff | Shared token-bucket pacing + exponential retry for transient Kite errors | ✅ |
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
3. Use `--universe current-master` to force a full current-master backfill instead of local-first resolution.
4. Resolve instrument token from memory cache, then `NSE.csv`, then API refresh-on-miss.
5. Fetch with retry/backoff and write parquet with dedup.
6. Persist checkpoint progress and resume on rerun.

The daily ingest command loads only the requested date window. It does not backfill the full
archive unless you pass a broad range or `--backfill`. Use `--universe current-master` when you
want to ingest the full current Kite master rather than the local parquet universe.

### Symbol-scoped ingest

The ingest CLI also supports a direct symbol list:

```bash
doppler run -- uv run nseml-kite-ingest --date 2026-03-27 --symbols RELIANCE,TCS,INFY
doppler run -- uv run nseml-kite-ingest --from 2026-03-24 --to 2026-03-27 --symbols RELIANCE,TCS,INFY
```

Notes:
- `--symbols` bypasses universe resolution and uses the explicit list.
- This is the supported way to limit ingestion to a specific subset of symbols.
- `nseml-build-features` is not symbol-scoped; it rebuilds feature tables by feature set and date window.

Current operational state as of `2026-03-27`:
- Daily ingestion is caught up through `2026-03-27`.
- 5-minute ingestion is caught up through `2026-03-27`.
- Runtime feature tables and market monitor are refreshed through the same date.
- Future runs should be incremental catch-up only unless you intentionally need a historical backfill.

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
doppler run -- uv run nseml-kite-ingest --today --5min --resume
doppler run -- uv run nseml-kite-ingest --date 2026-03-06
doppler run -- uv run nseml-kite-ingest --from 2026-03-05 --to 2026-03-06 --save-raw
doppler run -- uv run nseml-kite-ingest --backfill --universe current-master
doppler run -- uv run nseml-kite-ingest --backfill --5min --universe current-master --resume
```

Short catch-up windows should stay incremental:

```bash
doppler run -- uv run nseml-kite-ingest --from YYYY-MM-DD --to YYYY-MM-DD
doppler run -- uv run nseml-kite-ingest --from YYYY-MM-DD --to YYYY-MM-DD --5min --resume
doppler run -- uv run nseml-build-features
doppler run -- uv run nseml-market-monitor --incremental --since YYYY-MM-DD
doppler run -- uv run nseml-db-verify
```

### 5-min ingestion (recommended chunked backfill)

```bash
doppler run -- uv run nseml-kite-ingest --from 2025-04-01 --to 2025-05-31 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2025-06-01 --to 2025-07-31 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2025-08-01 --to 2025-09-30 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2025-10-01 --to 2025-11-30 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2025-12-01 --to 2026-01-31 --5min --resume
doppler run -- uv run nseml-kite-ingest --from 2026-02-01 --to 2026-03-27 --5min --resume
```

The historical 5-minute backfill is now complete through `2026-03-27`. Use chunked catch-up only
for future windows.

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

`nseml-db-verify` checks the loaded runtime coverage and materialized tables. It does not ingest
new Kite data.

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
| Base request spacing | Shared token bucket, target `2.85 req/sec` with burst `3` |
| Max retries | `5` |
| Backoff | Exponential (`1s` base, `30s` cap, jitter) |
| Daily chunk size | up to `2000` days per request |
| 5-min chunk size | up to `60` days per request |
| Checkpoint flush | every `25` symbols |

---

## Current Operational To-Dos (as of 2026-03-27)

1. Ingestion status:
   - Daily and 5-min historical coverage is caught up through `2026-03-27`.
2. Ongoing cadence:
   - Future Kite jobs should be incremental daily/5-minute catch-up only.
3. Post-ingest validation:
   - Run `nseml-build-features --since <YYYY-MM-DD>` and `nseml-market-monitor --incremental --since <YYYY-MM-DD>` after each new catch-up window.
4. Rate-limit tuning:
   - Keep the shared historical token bucket conservative enough to avoid Kite 429s while staying close to the 3 req/sec cap.
