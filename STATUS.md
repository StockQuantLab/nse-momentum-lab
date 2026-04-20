# NSE Momentum Lab — System Status

> Last updated: 2026-04-18 (update manually after each data catch-up)

---

## Data Coverage

| Source | Last Date | Files | Notes |
|--------|-----------|-------|-------|
| Kite daily OHLCV | 2026-04-17 | ~3,285 files | Catch-up to today before live session |
| Kite 5-min OHLCV | 2026-04-17 | ~14,093 files | Use `--5min --resume` |
| feat_daily | 2026-04-17 | 3,839,743 rows | `nseml-build-features --since` |
| market_monitor | 2026-04-17 | — | `nseml-market-monitor --incremental --since` |

---

## Canonical Experiment Baselines

Window: `2025-04-01 → 2026-03-10`, universe 2000.

| Strategy | Exp ID | Return | Max DD | Profit Factor | Trades |
|----------|--------|--------|--------|---------------|--------|
| Breakout 4% | `1716b78c208a90f3` | +136.4% | 2.26% | — | 991 |
| Breakout 2% | `87577645e9c99961` | +160.6% | 3.50% | — | 1842 |
| Breakdown 4% (Opt-B) | `84d9a58f3ad105be` | +3.36% | 1.24% | 2.49 | 37 |
| Breakdown 2% (canonical) | `c52e19a02db552d1` | +9.4% | 3.46% | 1.59 | 292 |

Frozen reporting runset: `docs/research/CANONICAL_REPORTING_RUNSET_2026-03-13.md`

---

## Infrastructure Health

| Service | Host Port | Container Port | Status |
|---------|-----------|----------------|--------|
| PostgreSQL (`nseml-postgres`) | 5434 | 5432 | — |
| MinIO API (`nseml-minio`) | 9003 | 9000 | — |
| MinIO Console | 9004 | 9001 | — |
| FastAPI (`nseml-api`) | 8004 | 8004 | — |
| NiceGUI dashboard | 8501 | 8501 | — |

Check infrastructure:
```bash
docker compose ps
doppler run -- uv run nseml-db-verify
```

---

## Kite Token Status

`KITE_ACCESS_TOKEN` expires at ~06:00 AM IST daily. Refresh before any ingestion run:
```bash
doppler run -- uv run nseml-kite-token --apply-doppler
doppler run -- uv run python scripts/kite_get_token.py
```

---

## Pending Daily Tasks

Run these before each live session in order:

```bash
# 1. Refresh Kite token
doppler run -- uv run nseml-kite-token --apply-doppler

# 2. Catch-up daily OHLCV
doppler run -- uv run nseml-kite-ingest --today

# 3. Catch-up 5-min (if needed)
doppler run -- uv run nseml-kite-ingest --today --5min --resume

# 4. Rebuild features incrementally
doppler run -- uv run nseml-build-features --since TODAY

# 5. Refresh runtime monitor tables
doppler run -- uv run nseml-market-monitor --incremental --since TODAY

# 6. Data quality check
doppler run -- uv run nseml-hygiene --refresh --full
doppler run -- uv run nseml-hygiene --report

# 7. Verify DB coverage
doppler run -- uv run nseml-db-verify
```

---

## Walk-Forward Gate Status

Before each paper session, confirm walk-forward coverage spans the intended trade date:
```bash
doppler run -- uv run nseml-paper status --status ACTIVE --summary
```

The promotion gate requires:
1. Completed `walk_forward` session for `thresholdbreakout` strategy
2. Trade date falls inside validated test coverage
3. (Optional) `--experiment-id` lineage check passes

---

## Known Constraints

- DuckDB is **single-writer**: stop the dashboard before running backtests or feature rebuilds.
- `KITE_ACCESS_TOKEN` is process-singleton — restart the process after a daily token refresh.
- `--force --allow-full-rebuild` is required for full feature rebuilds (safety guard against accidental rebuilds).
- Windows sandbox (Codex): run `doppler`, `docker`, `git commit`, and `git push` on the **host**, not inside the sandbox.
