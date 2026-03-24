# Paper Trading And Walk-Forward Plan

**Last updated**: 2026-03-21
**Goal**: turn the current research-heavy stack into a clean, repeatable walk-forward validation loop and a crash-safe paper-trading runtime without contaminating backtest code or storage.

---

## Current Repo State

The repo already has part of the paper-trading foundation, but the pieces are uneven:

| Area | Current state | Assessment |
|---|---|---|
| Strategy/backtest engine | Mature breakout and breakdown research path with 5-minute entry-day execution | Strong base |
| Walk-forward support | `src/nse_momentum_lab/services/backtest/walkforward.py` and optimizer fold loop exist | Too generic and lightly integrated |
| Paper-trading schema | `signal`, `paper_order`, `paper_fill`, `paper_position` exist in PostgreSQL | Insufficient for live sessions and recovery |
| Paper engine | `src/nse_momentum_lab/services/paper/engine.py` processes signals and simulates fills | EOD-style, in-memory, not session-safe |
| API | `GET /api/paper/positions` exists | Too narrow for operator workflow |
| Dashboard | `/paper_ledger` route exists | Still placeholder copy |
| Tests | unit coverage for paper engine and basic walk-forward window generation | Missing workflow/runtime coverage |
| Kite scaffold | `services/kite/` now contains REST, websocket, and batch-planning helpers | Ready for live feed wiring |

### Concrete gaps

1. Walk-forward evaluation is not the operating gate for paper trading.
2. The paper engine has no durable session concept, no resume path, and no live candle loop.
3. PostgreSQL tables are centered on trades, not on a paper session lifecycle.
4. The dashboard does not expose actionable paper-trading state.
5. There is no CLI or scheduler path for `prepare -> walk-forward -> replay/live paper -> archive`.

---

## Reference Pattern From `cpr-pivot-lab`

The useful parts to copy from `cpr-pivot-lab` are architectural, not strategy-specific:

1. A session-oriented paper runtime with explicit commands for `prepare`, `walk-forward`, `replay`, `live`, `pause`, `resume`, `flatten`, and `stop`.
2. PostgreSQL ownership of mutable paper state such as sessions, open positions, order events, and feed status.
3. DuckDB ownership of immutable historical archive and reporting.
4. A daily workflow that treats walk-forward validation as an operational checkpoint before live paper execution.
5. A dual-mode ledger view: active live session state first, archived paper results second.
6. Zerodha Kite v4 should be the single broker API surface for paper/live market data and order routing.

This separation is the right fit here too.

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

## Required Changes In `nse-momentum-lab`

### 1. Replace generic walk-forward with an operating protocol

Current issue:

- `services/backtest/walkforward.py` only splits date windows and reruns backtests.
- Tests verify window mechanics but not the operating decision.
- Fold results are not persisted as a first-class promotion artifact.

Required outcome:

- add an operating walk-forward runner that persists:
  - fold windows
  - parameter set chosen on each fold
  - out-of-sample metrics
  - approval status
  - comparison against the current canonical experiment

Recommended location:

- `src/nse_momentum_lab/services/backtest/walkforward.py`
- `src/nse_momentum_lab/services/backtest/registry.py`
- `src/nse_momentum_lab/cli/` as a dedicated walk-forward entrypoint

### 2. Introduce a session model for paper trading

Current issue:

- `signal`, `paper_order`, `paper_fill`, and `paper_position` are not enough for:
  - multi-day recovery
  - live feed status
  - multiple sessions
  - explicit operator controls

Required PostgreSQL additions:

- `paper_session`
  - session id, strategy family, experiment id, trade date, mode, status, risk config, notes
- `paper_session_signal`
  - session-linked candidate ledger with dedupe key, ranking, advisory status, decision status
- `paper_order_event`
  - append-only lifecycle for requested, acknowledged, filled, cancelled, rejected
- `paper_feed_state`
  - last bar time, last quote time, stale flag, heartbeat status
- optional `paper_bar_checkpoint`
  - durable candle-builder recovery state if live mode needs restart safety

Current tables can remain, but they should either become session-linked or be superseded by session-aware tables.

### 3. Extract shared execution decisions

Current issue:

- backtest execution logic and paper execution logic are not aligned tightly enough.
- `services/paper/engine.py` uses simplified close-based behavior and in-memory positions.

Required shared logic boundary:

- candidate admission
- entry confirmation
- initial stop creation
- stop progression and trail rules
- exit classification
- reason-code generation

Recommended location:

- `src/nse_momentum_lab/services/backtest/intraday_execution.py`
- `src/nse_momentum_lab/services/backtest/signal_models.py`
- a new shared module under `src/nse_momentum_lab/services/paper/` only for orchestration and persistence

The paper runtime should call the same decision functions used in backtest execution.

### 3b. Use Kite Connect v4 as the live/broker transport

Official Kite Connect constraints to respect in the scaffold:

- websocket connections are token-bound and should be planned in batches
- instrument subscription batching should stay under the documented per-connection limit
- quote / ltp / ohlc snapshot requests should be batched, not brute-forced one symbol at a time
- request-token and access-token handling must stay server-side only

Recommended location:

- `src/nse_momentum_lab/services/kite/`
- `src/nse_momentum_lab/services/paper/runtime.py`

### 4. Add a paper session CLI

Current issue:

- there is no operational CLI comparable to the `cpr-pivot-lab` session controller.

Required commands:

```text
nseml-paper prepare
nseml-paper walk-forward
nseml-paper replay-day
nseml-paper live
nseml-paper status
nseml-paper pause
nseml-paper resume
nseml-paper flatten
nseml-paper archive
```

Minimum viable behavior:

- `prepare`: validate dataset availability, selected experiment, and symbol universe
- `walk-forward`: run the promotion gate and emit a machine-readable decision
- `replay-day`: execute one historical date through the paper runtime
- `live`: start the live session loop
- `status`: show active session, positions, orders, feed state, and kill-switch status

Current command examples:

```text
doppler run -- uv run nseml-paper cleanup-walk-forward --yes
doppler run -- uv run nseml-paper walk-forward --strategy thresholdbreakout --start-date 2025-04-01 --end-date 2026-03-09
doppler run -- uv run nseml-paper walk-forward --strategy thresholdbreakout --start-date 2026-03-01 --end-date 2026-03-09 --train-days 5 --test-days 3 --roll-interval-days 1
doppler run -- uv run nseml-paper replay-day --trade-date 2026-03-09 --experiment-id <EXP_ID> --execute
doppler run -- uv run nseml-paper live --trade-date 2026-03-23 --experiment-id <EXP_ID> --execute
doppler run -- uv run nseml-paper stream --trade-date 2026-03-23 --experiment-id <EXP_ID>
doppler run -- uv run nseml-paper status --session-id <SESSION_ID>
```

Operational note:

- the default rolling walk-forward window is `252` train days and `63` test days
- a short window like `2026-03-01` to `2026-03-09` requires explicit smaller overrides such as `--train-days 5 --test-days 3 --roll-interval-days 1`

### 5. Upgrade the dashboard from placeholder to operator console

Current issue:

- `/paper_ledger` is still a static placeholder page.

Required dashboard views:

- active session summary
- session state and risk guardrails
- advisory queue with rank and decision status
- open positions
- order/fill timeline
- realized and unrealized PnL
- archived session list
- replay vs live mode indicator

This page should not wait for full broker automation. It should become useful as soon as replay-day sessions exist.

---

## Recommended Delivery Phases

### Phase 1. Walk-forward promotion path

Scope:

- formalize rolling walk-forward runner
- persist fold outputs and approval decision
- add CLI entrypoint

Exit criteria:

- a selected operating configuration can be reproduced from saved fold artifacts
- paper trading can consume that saved selection without re-running research logic ad hoc

### Phase 2. Paper session schema and repositories

Scope:

- add `paper_session` and session-aware paper tables
- add repository helpers
- keep current trade tables readable during migration

Exit criteria:

- session state can be created, resumed, queried, and completed without touching DuckDB

### Phase 3. Replay-day paper runtime

Scope:

- implement session orchestrator for a single historical day
- reuse current 5-minute execution semantics
- persist queue, orders, fills, and exits

Exit criteria:

- one historical trade date can run end to end via the paper session path
- archived results can be compared against the source backtest experiment

### Phase 4. Live paper runtime

Scope:

- add Kite-backed or polled bar adapter
- add heartbeat and stale-feed policy
- add pause/flatten controls

Exit criteria:

- a live session survives disconnects safely
- stale-feed policy is explicit and test-covered

### Phase 5. Dashboard and API consolidation

Scope:

- replace placeholder `/paper_ledger`
- add active-session APIs
- unify archived paper reporting with backtest reporting where practical

Exit criteria:

- operators can inspect session health and trade state without direct DB access

---

## Verification Plan

### Required tests

1. Walk-forward fold persistence and approval-decision tests
2. Session repository tests for create/resume/complete/flatten
3. Replay-day workflow tests
4. Live feed stale-heartbeat tests
5. API tests for active session and archived session views
6. NiceGUI state tests for `/paper_ledger`

### Required manual checks

1. Replay one known date from a canonical experiment and compare results against the audit tools
2. Pause and resume a paper session mid-run
3. Force stale-feed handling and confirm safe session behavior
4. Confirm archived paper session is visible after completion

---

## Immediate Next Work

If this repo is the implementation target, the next three concrete tasks should be:

1. add the paper-session schema and repository layer
2. add an `nseml-paper` CLI with `prepare`, `walk-forward`, `replay-day`, and `status`
3. replace `/paper_ledger` with a session-aware operator page backed by active and archived session reads

This sequence gives a usable replayable paper path first, then live-mode hardening second.
