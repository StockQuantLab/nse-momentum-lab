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

Window: `2015-01-01 → 2026-04-21`, universe 2000. Wave-1 fixes applied (H-carry, entry gate 09:20, filter direction parity, pnl_r guard).

| Strategy | Exp ID | Avg Annual | Max DD | Calmar | Profit Factor | Trades | Neg Years |
|----------|--------|-----------|--------|--------|---------------|--------|-----------|
| Breakout 4% | `f155489ee3422815` | +54.1% | 3.16% | 17.1 | 20.73 | 2,212 | 0 |
| Breakout 2% | `8e219692ea67b157` | +121.9% | 2.73% | 44.7 | 16.49 | 7,082 | 0 |
| Breakdown 4% | `f0cd849cf08f4fdc` | +3.1% | 0.74% | 4.2 | 5.51 | 258 | 2 |
| Breakdown 2% | `1f910e9069a508d2` | +8.2% | 1.90% | 4.3 | 5.47 | 790 | 0 |

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

Run these after market close to prep the next live session day, then re-run the pre-open
checks before launch:

```bash
# 1. Refresh Kite token
doppler run -- uv run nseml-kite-token --apply-doppler

# 2. Refresh NSE instrument master before today's ingest
doppler run -- uv run nseml-kite-ingest --refresh-instruments --exchange NSE

# 3. Catch-up daily OHLCV for today only
doppler run -- uv run nseml-kite-ingest --today

# 4. Catch-up 5-min for today only (if needed)
doppler run -- uv run nseml-kite-ingest --today --5min --resume

# 5. Rebuild features incrementally from today
doppler run -- uv run nseml-build-features --since TODAY

# 6. EOD H-carry decisions (post-market, before next-day prepare)
doppler run -- uv run nseml-paper eod-carry --strategy thresholdbreakout --trade-date TODAY
doppler run -- uv run nseml-paper eod-carry --strategy 2lynchbreakdown --trade-date TODAY

# 7. Refresh runtime monitor tables
doppler run -- uv run nseml-market-monitor --incremental --since TODAY

# 8. Data quality check
doppler run -- uv run nseml-hygiene --refresh --full
doppler run -- uv run nseml-hygiene --report

# 9. Verify DB coverage
doppler run -- uv run nseml-db-verify
```

For a normal live launch, ingest and feature rebuild are **today-only**. Use date-range backfill
commands only when recovering older gaps or repairing corrupted data.

## Live Monitoring

For long-running paper sessions, launch them in the background and tail the log from a second
terminal:

```bash
mkdir -p .tmp_logs
PYTHONUNBUFFERED=1 doppler run -- uv run nseml-paper multi-live \
  --strategy 2lynchbreakout \
  --strategy 2lynchbreakdown \
  --trade-date YYYY-MM-DD \
  >> .tmp_logs/live_YYYYMMDD.log 2>&1 &

tail -f .tmp_logs/live_YYYYMMDD.log \
  | grep --line-buffered -E "trade open|trade close|LIVE_BAR|TICKER_HEALTH|STALE|ERROR|Exception|Traceback|WARNING scripts.paper"
```

Use a tight pattern. Broad grep terms can match routine HTTP 200 responses and hide the actual
trading events.

---

## CPR Session Resilience Parity

Completed parity items for the live paper runtime:

- [x] Session lifecycle alerts are durable across retries/restarts via `alert_log` checks.
- [x] Feed stale/recovered alerts are transition-based and persisted in `paper_feed_state.raw_state.alert_state`.
- [x] Pause/resume commands skip no-op transitions so they do not spam Telegram.
- [x] `SESSION_COMPLETED` and `DAILY_PNL_SUMMARY` are deduped at the DB layer before dispatch.
- [x] `multi-live` runs breakout + breakdown in one writer process so both directions share the same
  websocket, DB lock, replica sync, and alert dispatcher.
- [x] `prepare --preset BREAKOUT_2PCT/BREAKDOWN_2PCT` persists the real 2% paper config instead of
  relying on ad hoc strategy metadata overrides.
- [x] Live candidate seeding uses the backtest candidate SQL with the session's configured threshold.
- [x] Feed stale detection is tick-age based, not “no 5-minute bar closed yet”, which removes false
  startup `FEED_STALE` alerts.
- [x] `FEED_STALE` / `FEED_RECOVERED` now follow the CPR operator model more closely:
  tick-age-based episodes, stale-alert cooldown, richer bodies, and `TICKER_HEALTH` in logs instead
  of Telegram.

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
