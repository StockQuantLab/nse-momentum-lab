# Pre-Open Live Paper Checklist

Use this checklist before starting a live paper session. It is the operational version of the
paper-trading readiness workflow for the v2 engine (`nseml-paper`).

> **v2 engine key behaviors**
> - Sessions are **idempotent**: `prepare` returns the existing session if one already exists for the same strategy/date/mode — safe to re-run after any interruption.
> - Each 5-minute bar's DB writes are **atomic**: a mid-bar crash rolls back the partial bar; on resume the bar is replayed cleanly.
> - Crash recovery **flattens open positions** at last-known mark prices before marking the session FAILED. The dashboard shows correct P&L after recovery.
> - Signal rows are written to `paper_session_signals` at seeding time, so the full signal identity is preserved through entry → hold → exit and is queryable for audit.

## Previous Day / EOD Prep

1. Refresh the Kite access token if it will expire before the next session.
2. Refresh the NSE instrument master before any fresh daily ingest.
   - Normal daily launch: `doppler run -- uv run nseml-kite-ingest --refresh-instruments --exchange NSE`
   - Do this first so delisted / newly listed symbols are resolved before the data pull.
3. Finish any pending Kite ingest for the latest missing date range.
   - Normal daily launch: ingest only today's data (`--today`).
   - Backfill / recovery: use `--from/--to` or `--backfill` only when repairing older gaps.
4. Refresh features and runtime tables after ingest.
   - Normal daily launch: rebuild incrementally from today's date, not full history.
   - `nseml-build-features` automatically force-syncs the market replica after every build, so the dashboard (market monitor, DQ) sees updated data without a manual step.
5. Run the runtime verifier:
   ```bash
   doppler run -- uv run nseml-db-verify
   ```

This EOD prep is what makes the next trading day live-ready. The next morning should only need
session creation and launch, not a full historical rebuild.

## Pre-Open

### Step 1: Ensure data freshness

For a normal live session, this step should already have been completed earlier in the day:

```bash
doppler run -- uv run nseml-kite-ingest --refresh-instruments --exchange NSE
doppler run -- uv run nseml-kite-ingest --today
doppler run -- uv run nseml-build-features --since YYYY-MM-DD
doppler run -- uv run nseml-market-monitor --incremental --since YYYY-MM-DD
doppler run -- uv run nseml-db-verify
```

### Step 2: Prepare paper sessions

`prepare` is idempotent — if a session already exists for the same strategy/date/mode, it returns that session. Safe to re-run on interruption.

```bash
# Canonical 2% breakout session
doppler run -- uv run nseml-paper prepare \
  --preset BREAKOUT_2PCT \
  --mode live \
  --trade-date YYYY-MM-DD \
  --portfolio-value 1000000

# Canonical 2% breakdown session
doppler run -- uv run nseml-paper prepare \
  --preset BREAKDOWN_2PCT \
  --mode live \
  --trade-date YYYY-MM-DD \
  --portfolio-value 1000000
```

Daily shortcut (auto-fills today's date):

```bash
doppler run -- uv run nseml-paper daily-prepare --preset BREAKOUT_2PCT --mode live
doppler run -- uv run nseml-paper daily-prepare --preset BREAKDOWN_2PCT --mode live
```

For four threshold variants, use `plan`:

```bash
doppler run -- uv run nseml-paper plan \
  --strategy thresholdbreakout \
  --trade-date YYYY-MM-DD \
  --symbols RELIANCE,TCS,INFY \
  --variants 4
```

### Step 3: Start live sessions

```bash
# Shared writer path for breakout + breakdown (preferred; CPR-style)
doppler run -- uv run nseml-paper multi-live \
  --session-id <BREAKOUT_SESSION_ID> \
  --session-id <BREAKDOWN_SESSION_ID>

# Auto-discovery by strategy + date also works, but keep both legs in one process:
doppler run -- uv run nseml-paper multi-live \
  --strategy 2lynchbreakout \
  --strategy 2lynchbreakdown \
  --trade-date YYYY-MM-DD
```

Daily shortcut:

```bash
mkdir -p .tmp_logs
PYTHONUNBUFFERED=1 doppler run -- uv run nseml-paper multi-live \
  --strategy 2lynchbreakout \
  --strategy 2lynchbreakdown \
  --trade-date YYYY-MM-DD \
  >> .tmp_logs/multi_live_YYYYMMDD.log 2>&1 &
```

## Live Checks

Use the dashboard + Telegram as the operator view, and logs for agent monitoring.

Monitor the background log with a tight grep:

```bash
tail -f .tmp_logs/multi_live_YYYYMMDD.log \
  | grep --line-buffered -E "trade open|trade close|TRADE|TARGET|SL_HIT|TRAIL|LIVE_BAR|TICKER_HEALTH|STALE|ERROR|Exception|Traceback|WARNING scripts.paper"
```

Feed-alert policy:
- Telegram should carry transitions only: session start/end, feed stale, feed recovered, risk breach, trade open/close.
- `TICKER_HEALTH` stays in logs, not Telegram.
- `FEED_STALE` is tick-age based, not “no closed 5-min bar yet”.

For DB/operator checks:

```bash
# List current sessions
doppler run -- uv run nseml-paper status

# Full JSON for a specific session
doppler run -- uv run nseml-paper status --session-id <SESSION_ID>
```

Fields to monitor in session JSON:

- `status = ACTIVE`
- `open_positions > 0` (after market open once positions are taken)
- `closed_positions` growing after exits
- `paper_feed_state.status = OK` with fresh `heartbeat_at` / `last_tick_at` once ticks are flowing

## Emergency Commands

```bash
# Pause an active session (stops entry but holds positions)
doppler run -- uv run nseml-paper pause --strategy thresholdbreakout --mode live --trade-date YYYY-MM-DD

# Resume a paused session
doppler run -- uv run nseml-paper resume --session-id <SESSION_ID>

# Flatten all open positions immediately
doppler run -- uv run nseml-paper flatten --session-id <SESSION_ID>

# Stop session (mark COMPLETED)
doppler run -- uv run nseml-paper stop --session-id <SESSION_ID>
```

## Required Readiness Signals

- Valid Kite credentials are present in Doppler (`KITE_ACCESS_TOKEN` fresh).
- `nseml-db-verify` passes.
- The selected universe has prior-day `v_daily` and `v_5min` coverage.

## What This Does Not Do

- It does not run a backtest.
- It does not guarantee that every intraday live feed will be non-empty after open.
- It does not check walk-forward gate (v2 engine does not require walk-forward validation).
- It does not rebuild the full historical dataset during a normal daily launch. Full-history rebuilds are only for recovery, backfill, or data correction.

## Crash Recovery

If a live session crashes mid-day:

1. Open positions are automatically flattened at last-known mark prices in the DB.
2. The session is marked `FAILED`.
3. Inspect the session:
   ```bash
   doppler run -- uv run nseml-paper status --session-id <SESSION_ID>
   ```
4. To resume, re-run `prepare` (returns the same session) then `multi-live`:
   ```bash
   doppler run -- uv run nseml-paper prepare --preset BREAKOUT_2PCT --mode live --trade-date YYYY-MM-DD
   doppler run -- uv run nseml-paper prepare --preset BREAKDOWN_2PCT --mode live --trade-date YYYY-MM-DD
   doppler run -- uv run nseml-paper multi-live --strategy 2lynchbreakout --strategy 2lynchbreakdown --trade-date YYYY-MM-DD
   ```
   The engine resumes from the last committed bar checkpoint — no bars are double-processed.

## Dashboard

The `/paper_ledger` page shows:
- Session summary (status, open/closed position counts, realized P&L)
- Signal ledger (`paper_session_signals`) with entry mode, selection rank/score
- Open and closed positions with full order/fill history
- Alert log (Telegram delivery status, errors redacted)
