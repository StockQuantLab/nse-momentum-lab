# Pre-Open Live Paper Checklist

Use this checklist before starting a live paper session. It is the operational version of the
paper-trading readiness workflow for the v2 engine (`nseml-paper`).

> **v2 engine key behaviors**
> - Sessions are **idempotent**: `prepare` returns the existing session if one already exists for the same strategy/date/mode — safe to re-run after any interruption.
> - Each 5-minute bar's DB writes are **atomic**: a mid-bar crash rolls back the partial bar; on resume the bar is replayed cleanly.
> - Crash recovery **flattens open positions** at last-known mark prices before marking the session FAILED. The dashboard shows correct P&L after recovery.
> - Signal rows are written to `paper_session_signals` at seeding time, so the full signal identity is preserved through entry → hold → exit and is queryable for audit.

## Previous Day

1. Refresh the Kite access token if it will expire before the next session.
2. Finish any pending Kite ingest for the latest missing date range.
3. Refresh features and runtime tables after ingest.
4. Run the runtime verifier:
   ```bash
   doppler run -- uv run nseml-db-verify
   ```

## Pre-Open

### Step 1: Ensure data freshness

```bash
doppler run -- uv run nseml-kite-ingest --today
doppler run -- uv run nseml-build-features --since YYYY-MM-DD
doppler run -- uv run nseml-market-monitor --incremental --since YYYY-MM-DD
doppler run -- uv run nseml-db-verify
```

### Step 2: Prepare paper sessions

`prepare` is idempotent — if a session already exists for the same strategy/date/mode, it returns that session. Safe to re-run on interruption.

```bash
# Single session (e.g. breakout 4%)
doppler run -- uv run nseml-paper prepare \
  --strategy thresholdbreakout \
  --mode live \
  --trade-date YYYY-MM-DD \
  --portfolio-value 1000000

# With threshold override (2%)
doppler run -- uv run nseml-paper prepare \
  --strategy thresholdbreakout \
  --mode live \
  --trade-date YYYY-MM-DD \
  --metadata '{"breakout_threshold":0.02}'
```

Daily shortcut (auto-fills today's date):

```bash
doppler run -- uv run nseml-paper daily-prepare --strategy thresholdbreakout --mode live
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
# Auto-discovers session by strategy + date
doppler run -- uv run nseml-paper live --strategy thresholdbreakout --trade-date YYYY-MM-DD

# Or with explicit session id
doppler run -- uv run nseml-paper live --session-id <SESSION_ID>
```

Daily shortcut:

```bash
doppler run -- uv run nseml-paper daily-live --strategy thresholdbreakout
```

## Live Checks

Use status for operator checks:

```bash
# List all active sessions
doppler run -- uv run nseml-paper status --status ACTIVE

# Full JSON for a specific session
doppler run -- uv run nseml-paper status --session-id <SESSION_ID>
```

Fields to monitor in session JSON:

- `status = ACTIVE`
- `open_positions > 0` (after market open once positions are taken)
- `closed_positions` growing after exits

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

## Crash Recovery

If a live session crashes mid-day:

1. Open positions are automatically flattened at last-known mark prices in the DB.
2. The session is marked `FAILED`.
3. Inspect the session:
   ```bash
   doppler run -- uv run nseml-paper status --session-id <SESSION_ID>
   ```
4. To resume, re-run `prepare` (returns the same session) then `live`:
   ```bash
   doppler run -- uv run nseml-paper prepare --strategy thresholdbreakout --mode live --trade-date YYYY-MM-DD
   doppler run -- uv run nseml-paper live --strategy thresholdbreakout --trade-date YYYY-MM-DD
   ```
   The engine resumes from the last committed bar checkpoint — no bars are double-processed.

## Dashboard

The `/paper_ledger` page shows:
- Session summary (status, open/closed position counts, realized P&L)
- Signal ledger (`paper_session_signals`) with entry mode, selection rank/score
- Open and closed positions with full order/fill history
- Alert log (Telegram delivery status, errors redacted)
