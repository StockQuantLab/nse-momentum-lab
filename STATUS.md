# NSE Momentum Lab — System Status

> Last updated: 2026-04-21 (update manually after each data catch-up)

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

Window: `2015-01-01 → 2026-04-17`, universe 2000. Wave-1 fixes applied (H-carry, entry gate 09:20, filter direction parity, pnl_r guard).

| Strategy | Exp ID | Avg Annual | Max DD | Calmar | Profit Factor | Trades | Neg Years |
|----------|--------|-----------|--------|--------|---------------|--------|-----------|
| Breakout 4% | `0cd353d536dd6f91` | +54.1% | 3.16% | 17.1 | 22.98 | 2,211 | 0 |
| Breakout 2% | `f923e1a9517d9b2c` | +121.8% | 2.73% | 44.6 | 19.06 | 7,078 | 0 |
| Breakdown 4% | `f6e7646ac932697d` | +3.1% | 0.74% | 4.2 | 6.65 | 258 | 2 |
| Breakdown 2% | `b769984bf6d0c5c7` | +8.1% | 1.99% | 4.1 | 6.52 | 790 | 0 |

Frozen reporting runset: `docs/research/CANONICAL_REPORTING_RUNSET_2026-04-21.md`

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

- DuckDB is **single-writer**: stop the dashboard before running backtests or feature rebuilds. The dashboard reads from a versioned replica so cleanup (`nseml-backtest-cleanup`) is safe while the dashboard runs.
- `KITE_ACCESS_TOKEN` is process-singleton — restart the process after a daily token refresh.
- `--force --allow-full-rebuild` is required for full feature rebuilds (safety guard against accidental rebuilds).
- Windows sandbox (Codex): run `doppler`, `docker`, `git commit`, and `git push` on the **host**, not inside the sandbox.
