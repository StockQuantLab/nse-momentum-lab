# Paper Trading And Walk-Forward Reference

**Last updated**: 2026-03-27
**Goal**: document the implemented walk-forward, replay, and daily live paper workflow for NSE Momentum Lab.

---

## Current Repo State

The repo now has the core paper-trading foundation implemented; this page documents the
current operating model and the remaining operator caveats:

| Area | Current state | Assessment |
|---|---|---|
| Strategy/backtest engine | Mature breakout and breakdown research path with 5-minute entry-day execution | Operational |
| Walk-forward support | `src/nse_momentum_lab/services/backtest/walkforward.py` and optimizer fold loop exist | Operational promotion gate |
| Paper-trading schema | Session-aware paper tables and legacy compatibility tables exist in PostgreSQL | Operational |
| Paper runtime | `src/nse_momentum_lab/services/paper/runtime.py` manages daily prepare, replay, and live sessions | Operational |
| API | Paper session and feed-state endpoints exist | Operational |
| Dashboard | `/paper_ledger` is session-aware and hides archived clutter | Operational |
| Tests | unit coverage exists for walk-forward, replay/live bootstrap, runtime, and Kite pacing | Operational |
| Kite scaffold | `services/kite/` contains REST, websocket, pacing, and batch-planning helpers | Operational |

### Remaining caveats

1. Walk-forward is still the research/promotion gate, not the same thing as a daily live-readiness check.
2. Replay/live can still be misconfigured if the selected universe or runtime tables are stale.
3. The canonical operational threshold and session config should be changed deliberately, not ad hoc.
4. Older sessions can clutter the ledger if they are not archived.
5. Any new transport or queue change should be validated on the Windows host path, not only in unit tests.

---

## Reference Pattern From `cpr-pivot-lab`

The implementation here mirrors the useful parts of `cpr-pivot-lab`:

1. A session-oriented paper runtime with explicit commands for `prepare`, `walk-forward`, `replay`, `live`, `pause`, `resume`, `flatten`, and `stop`.
2. PostgreSQL ownership of mutable paper state such as sessions, open positions, order events, and feed status.
3. DuckDB ownership of immutable historical archive and reporting.
4. A daily workflow that treats walk-forward validation as an operational checkpoint before live paper execution.
5. A dual-mode ledger view: active live session state first, archived paper results second.
6. Zerodha Kite v4 should be the single broker API surface for paper/live market data and order routing.

This separation is the operating model now used in NSE Momentum Lab.

---

## Design Principles

1. Keep trading math deterministic and shared across backtest and paper execution.
2. Keep mutable operational state in PostgreSQL only.
3. Keep historical archive and analytics in DuckDB only.
4. Treat walk-forward as the promotion gate between research and paper execution.
5. Preserve every candidate, accepted signal, skipped signal, order event, and exit reason for audit.
6. Prefer a daily replayable workflow before attempting full live intraday automation.

---

## Target Operating Model

### Stage 1: Clean walk-forward gate

Use the current research corpus to run a rolling walk-forward protocol that produces:

- the selected operating configuration for the next session or next week
- fold-level metrics, not just a single aggregate
- drift diagnostics versus the current canonical baseline
- a promotion decision: `APPROVED`, `HOLD`, or `REJECTED`

### Stage 2: Daily paper workflow

For each trade date:

1. Freeze the operating configuration from the latest approved walk-forward output.
2. Build the advisory watchlist and full candidate ledger from DuckDB.
3. Load the chosen date and experiment into a PostgreSQL paper session.
4. Run either:
   - `replay` mode using historical 5-minute bars for validation, or
   - `live` mode using the broker feed or polled bars.
5. Persist session state, order events, fills, risk state, and exits in PostgreSQL.
6. Archive the completed session into DuckDB for unified reporting.

### Stage 3: Live paper runtime

The live paper loop should be narrow:

- subscribe or poll only the symbols in the day’s queue
- aggregate to 5-minute bars
- call shared decision logic
- update positions and stops
- enforce kill-switch and flatten policies

No custom strategy logic should live in the orchestration layer.

---

## Implemented Workflow

### 1. Walk-forward promotion gate

Walk-forward is implemented as the research/promotion gate:

- generates folds from actual trading sessions in `v_daily`
- persists fold outputs and lineage
- supports cleanup and rerun of stale sessions
- remains separate from daily paper bootstrapping

Current command examples:

```text
doppler run -- uv run nseml-paper walk-forward-cleanup --wf-run-id <SESSION_ID>
doppler run -- uv run nseml-paper walk-forward-cleanup --wf-run-id <SESSION_ID> --apply
doppler run -- uv run nseml-paper walk-forward --strategy thresholdbreakout --start-date YYYY-MM-DD --end-date YYYY-MM-DD
doppler run -- uv run nseml-paper walk-forward --strategy thresholdbreakout --start-date YYYY-MM-DD --end-date YYYY-MM-DD --train-days 5 --test-days 3 --roll-interval-days 1
```

### 2. Session-aware paper trading

The paper runtime now uses a session model:

- `paper_session`
- `paper_session_signal`
- `paper_order_event`
- `paper_feed_state`

Session state is persisted in PostgreSQL and mirrored into the dashboard. The operational queue is now built from runtime data for replay/live workflows instead of relying on backtest diagnostics for the normal path.

### 3. Shared execution decisions

Backtest and paper execution now share the same decision boundary where practical:

- candidate admission
- entry confirmation
- stop creation and progression
- exit classification
- reason-code generation

The paper runtime uses shared helpers rather than reimplementing strategy math in the websocket runner.

### 4. Kite Connect transport

The current live/broker transport uses Kite Connect v4 helpers under `src/nse_momentum_lab/services/kite/` and keeps request-token / access-token handling server-side only.

Operational constraints remain:

- websocket connections are token-bound
- subscriptions are batched
- snapshot requests are batched
- historical ingestion uses shared token-bucket pacing close to the documented cap

### 5. Paper session CLI

The operational CLI now includes:

```text
nseml-paper prepare
nseml-paper walk-forward
nseml-paper replay-day
nseml-paper daily-prepare
nseml-paper daily-replay
nseml-paper daily-live
nseml-paper status
nseml-paper pause
nseml-paper resume
nseml-paper flatten
nseml-paper archive
```

Current daily workflow examples:

```text
doppler run -- uv run nseml-paper daily-prepare --trade-date YYYY-MM-DD --mode replay --all-symbols
doppler run -- uv run nseml-paper daily-replay --trade-date YYYY-MM-DD --all-symbols
doppler run -- uv run nseml-paper daily-live --trade-date YYYY-MM-DD --all-symbols --execute
doppler run -- uv run nseml-paper status --session-id <SESSION_ID>
```

### 6. Dashboard

`/paper_ledger` is now session-aware and can show active and archived paper state without the old placeholder-only UX.

---

## Current Validation

### Required tests

1. Walk-forward fold persistence and approval-decision tests
2. Session repository tests for create/resume/complete/flatten
3. Replay-day workflow tests
4. Live feed stale-heartbeat tests
5. API tests for active session and archived session views
6. NiceGUI state tests for `/paper_ledger`

### Required manual checks

1. Replay one known date from a canonical experiment and compare results against the audit tools
2. Run `daily-prepare` and `daily-live` for the current trade date and confirm feed + queue state in `/paper_ledger`
3. Pause and resume a paper session mid-run
4. Force stale-feed handling and confirm safe session behavior
5. Confirm archived paper session is visible after completion

---

## Remaining Limits

The implemented workflow is usable, but the following are still operator concerns:

1. live paper depends on fresh daily and 5-minute runtime tables
2. the selected threshold/config should be deliberate, not ad hoc
3. stale sessions should be archived so the ledger stays readable
4. any transport or queue change should be validated on the Windows host path
