# Pre-Open Live Paper Checklist

Use this checklist before starting a live paper session. It is the operational version of the
paper-trading readiness workflow.

## Previous Day

1. Refresh the Kite access token if it will expire before the next session.
2. Finish any pending Kite ingest for the latest missing date range.
3. Refresh features and runtime tables after ingest.
4. Run the runtime verifier:
   ```bash
   doppler run -- uv run nseml-db-verify
   ```

## Pre-Open

1. Run the readiness check for the intended trade date:
   ```bash
   doppler run -- uv run nseml-paper daily-prepare --trade-date YYYY-MM-DD --mode live --all-symbols
   ```
2. Read the verdict:
   - `READY` means the selected universe has prior-day daily and 5-minute coverage.
   - `OBSERVE_ONLY` means partial coverage exists and the session can still start in
     observe-only mode.
   - `BLOCKED` means the universe is missing required prior-day coverage.
3. If `READY`, start the live paper session:
   ```bash
   doppler run -- uv run nseml-paper daily-live --trade-date YYYY-MM-DD --all-symbols --watchlist --run
   ```
   For the four threshold variants, use explicit versioned session ids so each stream is isolated:
   ```bash
   doppler run -- uv run nseml-paper daily-live --session-id paper-thresholdbreakout-thr-0p04-watchlist-YYYY-MM-DD-live-vN --trade-date YYYY-MM-DD --strategy thresholdbreakout --strategy-params '{"breakout_threshold":0.04}' --all-symbols --watchlist --run
   doppler run -- uv run nseml-paper daily-live --session-id paper-thresholdbreakout-thr-0p02-watchlist-YYYY-MM-DD-live-vN --trade-date YYYY-MM-DD --strategy thresholdbreakout --strategy-params '{"breakout_threshold":0.02}' --all-symbols --watchlist --run
   doppler run -- uv run nseml-paper daily-live --session-id paper-thresholdbreakdown-thr-0p04-watchlist-YYYY-MM-DD-live-vN --trade-date YYYY-MM-DD --strategy thresholdbreakdown --strategy-params '{"breakout_threshold":0.04}' --all-symbols --watchlist --run
   doppler run -- uv run nseml-paper daily-live --session-id paper-thresholdbreakdown-thr-0p02-watchlist-YYYY-MM-DD-live-vN --trade-date YYYY-MM-DD --strategy thresholdbreakdown --strategy-params '{"breakout_threshold":0.02}' --all-symbols --watchlist --run
   ```
4. If `OBSERVE_ONLY`, decide whether to narrow the symbol list or run the session in
   observe-only mode.

## Live Checks

Use compact status for operator checks instead of the full session payload:

```bash
doppler run -- uv run nseml-paper status --summary --status ACTIVE
doppler run -- uv run nseml-paper status --session-id <SESSION_ID> --summary
```

Healthy signals for a running session:

- `status = ACTIVE`
- `feed_state.status = CONNECTED`
- non-zero `symbol_count`
- non-zero `queue_signals`
- non-zero `token_count`

## Required Readiness Signals

- Valid Kite credentials are present.
- `nseml-db-verify` passes.
- The selected universe has prior-day `v_daily` and `v_5min` coverage.
- The watchlist query returns a non-zero candidate set when `--watchlist` is used.
- The live session can resolve a non-zero instrument token count.
- Walk-forward is not required for live paper or replay startup.

## What This Does Not Do

- It does not run a backtest.
- It does not guarantee that every intraday live feed will be non-empty after open.
