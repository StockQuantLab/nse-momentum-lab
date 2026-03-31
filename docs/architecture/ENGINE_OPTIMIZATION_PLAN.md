# NSE Momentum Lab — Engine Optimization Plan

Last updated: 2026-03-27

---

## Objective

Improve the paper/live engine in the right order:

1. Make paper and live behavior operationally correct before adding new strategy features
2. Remove hidden coupling between live paper, backtest artifacts, and old session state
3. Make live readiness measurable and reproducible on Windows + Kite
4. Tune strategy behavior only after the runtime can produce and execute the right queue

This plan complements the existing paper workflow design in
[`docs/architecture/PAPER_TRADING_WALK_FORWARD_PLAN.md`](./PAPER_TRADING_WALK_FORWARD_PLAN.md).
That document defines the target workflow. This plan defines the concrete engine work needed
to make the workflow truthful and reliable in production-like use.

---

## Guiding Principles & Validation Requirements

Engine changes should not be accepted because a command “runs.” Each material change should
satisfy these rules:

1. Runtime correctness before strategy tuning
   - A live paper engine that subscribes to zero tokens or generates zero candidates is not
     operational, even if the CLI exits cleanly.
   - Fix transport, queue generation, and state reconciliation first.

2. Replay/live parity before promotion
   - Historical replay and live paper should use the same runtime decision boundary wherever
     possible.
   - Backtest-only artifacts may be used for audit, but should not be required to start a
     normal daily paper session.

3. Daily readiness must be lighter than walk-forward
   - Walk-forward remains available for research validation, but it is not required to start
     a normal daily paper session.
   - Daily live readiness should verify fresh runtime coverage and prior-day parity, not
     demand a full backtest or a same-day daily feature row.

4. Live orchestration must stay thin
   - The live loop should subscribe to the right symbols, aggregate or consume bars, invoke
     shared decision logic, and persist state.
   - Strategy logic should not be reimplemented in the websocket runner.

5. PostgreSQL owns mutable paper state; DuckDB owns read models
   - PostgreSQL: sessions, positions, orders, fills, feed state
   - DuckDB/parquet: historical market data, prepared features, candidate universe
   - MinIO is not part of the critical path for daily paper execution

6. Windows host execution is a first-class constraint
   - Doppler, DuckDB locking, and Twisted/Kite websocket behavior must be tested from the
     actual host execution path, not only from unit tests.

### Minimum validation bundle for any live-paper engine change

At minimum, report these checks:

| Category | Required outputs |
|---|---|
| Bootstrap | session id, mode, requested symbol count, resolved token count |
| Feed | connection status, subscription count, last tick time, stale flag |
| Queue | queue size, actionable queue size, candidate source |
| Execution | orders placed, fills, open positions, rejection reasons |
| Recovery | session reuse behavior, lock conflicts, restart behavior |
| Parity | replay-vs-live candidate path differences called out explicitly |

---

## Current Findings That Change Priorities

### 1. Historical replay is already closer to the right model than live paper

Recent work moved replay toward a CPR-style runtime-driven flow:

- `daily-replay` can now build the queue directly from feature/runtime tables
- historical replay no longer requires `bt_execution_diagnostic` or an experiment id
- missing `ref_symbol` rows can be auto-created during session bootstrap
- replay can fall back to DuckDB `v_daily` prices when PostgreSQL market rows are absent

Important implication:

- The highest-priority gap is no longer replay bootstrapping
- The highest-priority gap is **live candidate generation + live feed correctness**

### 2. `thresholdbreakout` live paper is not operational yet

The live path currently fails in two distinct ways:

1. **Candidate-generation gap**
   - `thresholdbreakout` still depends on same-day daily-style fields for candidate creation
   - before or during market hours, those rows do not exist in `v_daily` / `feat_*`
   - result: live queue remains empty even when the session is otherwise healthy

2. **Feed-subscription gap**
   - the session bootstrap can prepare a feed plan, but the stream runner path still drops
     the prepared instrument tokens
   - result: feed state becomes `CONNECTED` with `subscription_count = 0`

This means a live paper session may look active in PostgreSQL and the dashboard while
receiving no ticks and producing no trades.

### 3. The current daily live workflow still mixes three different concerns

Today’s `daily-live` path is trying to do all of the following in one command:

- readiness validation
- session bootstrap
- candidate queue generation
- websocket startup
- optional live execution

That makes failures ambiguous. A clean paper engine should separate:

1. `daily-prepare`
2. queue build
3. stream bootstrap
4. live execution loop

### 4. `DuckDBBacktestRunner` is still leaking into live bootstrap paths

The runtime queue builder currently uses backtest-runner infrastructure for candidate
queries. That creates problems:

- lock sensitivity on `backtest.duckdb`
- assumptions intended for research runs, not daily live startup
- confusing failure modes when an old backtest or live process is still open

Live paper should use a dedicated read-only candidate builder, not a backtest runner with
writer and conflict assertions.

### 5. Session clutter and stale-state reuse are now operational problems

We already have:

- replay sessions for `2026-03-23`, `2026-03-24`, `2026-03-25`
- partial and stale live sessions for `2026-03-27`

This creates two issues:

1. operators cannot easily tell which session is current and valid
2. stale feed state or old session ids can hide whether a fresh patch actually worked

That makes cleanup and archival part of engine reliability, not just dashboard hygiene.

---

## Phase 0 — Make Live Paper Truthful

These items must happen before any further live-paper claims.

### 0.1 Separate Runtime Candidate Builder From Backtest Runner

**Status:** DONE (2026-03-27)
**Priority:** P0

**Problem:** Replay can now build runtime queues without experiments, but the current queue
builder still depends on `DuckDBBacktestRunner`, which brings write-lock and research-run
assumptions into daily paper startup.

**Implementation:**
1. Create a dedicated read-only candidate builder for paper workflows
2. Move shared candidate-query assembly into a reusable module
3. Keep ranking and entry-resolution logic callable without `backtest.duckdb` writer checks
4. Ensure daily live bootstrap never requires writable backtest storage

**Validation:**
- `daily-replay` and `daily-live` both work while `backtest.duckdb` is open elsewhere
- no live bootstrap path calls a write-conflict assertion

---

### 0.2 Fix Feed-Plan To Stream-Runner Token Handoff

**Status:** DONE (2026-03-27)
**Priority:** P0

**Problem:** The prepared feed plan resolves tokens, but the live stream path still ends up
with `subscription_count = 0` and `instrument_tokens = []` in `paper_feed_state`.

**Implementation:**
1. Make `daily-live --run` consume prepared `feed_plan.instrument_tokens`
2. Ensure `paper_feed_state.metadata_json.instrument_tokens` matches the prepared token list
3. Fail fast if a live session is `CONNECTED` with zero tokens unless explicitly allowed for
   an observe-only dry run
4. Add CLI output that shows resolved token count before starting the websocket loop

**Validation:**
- `daily-live --run` shows non-zero token count for a valid symbol universe
- `paper_feed_state.subscription_count` matches the prepared token count
- first tick updates `last_tick_at` and `last_quote_at`

---

### 0.3 Make Kite Websocket Startup Windows-Safe

**Status:** DONE (2026-03-27)
**Priority:** P0

**Problem:** The websocket runner hit Twisted’s `signal only works in main thread of the main
interpreter` path when started incorrectly from worker-thread execution.

**Implementation:**
1. Run Kite websocket startup in the main interpreter path
2. Use a stable threaded mode or event-loop integration that does not delegate signal
   registration to a worker thread
3. Add explicit logging for:
   - websocket connect start
   - subscription batches applied
   - first tick received
   - reconnect attempts

**Validation:**
- no Twisted signal crash on Windows host execution
- process stays alive beyond initial connection window
- reconnect path updates feed state correctly

---

### 0.4 Add Explicit Observe-Only Live Mode

**Status:** DONE (2026-03-27)
**Priority:** P0

**Problem:** We currently improvised an “observe-only” live mode operationally, but the CLI
and feed state do not model it explicitly.

**Implementation:**
1. Add a first-class `observe` or `--no-execute` live mode
2. Allow subscriptions with zero queue safely, but make that state explicit
3. Persist mode in session state and feed metadata
4. Show the mode clearly in `/paper_ledger`

**Validation:**
- observe-only sessions subscribe and receive ticks without pretending trades can fire
- operators can distinguish observe-only from trading-enabled live sessions

---

## Phase 1 — Make Live Candidate Generation Correct

### 1.1 Build A True Intraday Candidate Path For `thresholdbreakout`

**Status:** DONE (2026-03-27)
**Priority:** P0

**Problem:** `thresholdbreakout` live paper currently depends on same-day daily-style fields
that are only known after the session. That makes pre-open and intraday live queue
generation invalid.

**Required outcome:**

- a live candidate/watchlist model based on information that actually exists before or during
  the session
- intraday trigger logic that promotes a watched symbol to an actionable signal when the live
  condition is met

**Implementation direction:**
1. Split the current strategy into two phases:
   - pre-open watchlist selection from prior-day features
   - intraday trigger confirmation from live 5-minute bars / quotes
2. Define which current filters are truly prior-day and which are breakout-day confirmation
3. Build a candidate state machine such as:
   - `WATCH`
   - `ARMED`
   - `TRIGGERED`
   - `REJECTED`
4. Persist every transition for audit

**Validation:**
- same-day live queue is non-empty when legitimate watchlist names exist
- live entries can occur without requiring end-of-day-only fields
- historical replay can simulate the same intraday promotion logic

---

### 1.2 Keep Replay/Live Decision Logic Aligned

**Status:** DONE (2026-03-27)
**Priority:** P0

**Problem:** Replay and live should differ only in data source and timing, not in signal
semantics. Right now replay is closer to correct than live.

**Implementation:**
1. Centralize the candidate-state transition logic
2. Centralize stop initialization and progression
3. Reuse the same intraday trigger evaluator in:
   - historical replay
   - live websocket/polling loop
4. Keep transport-specific code out of the decision module

**Validation:**
- a replay of a live date produces the same candidate transitions given the same bars
- decision module tests do not depend on websocket code

---

### 1.3 Tighten The Symbol Universe For Daily Live Sessions

**Status:** DONE (2026-03-27)
**Priority:** P1

**Problem:** `--all-symbols` currently resolves from the broad local parquet universe,
including stale or dormant names. That is too loose for a daily live watchlist.

**Implementation:**
1. Define an operational universe based on:
   - prior trading-day daily coverage
   - prior trading-day 5-minute coverage
   - current Kite instrument master intersection
2. Make that the default for daily live unless a narrower symbol list is provided
3. Keep broad historical symbol resolution available for replay and audits

**Validation:**
- live requested-symbol count is materially smaller and operationally relevant
- missing-token / stale-symbol churn drops

---

## Phase 2 — Reduce Operational Drag

### 2.1 Add A First-Class Daily Readiness Report

**Status:** DONE (2026-03-27)
**Priority:** P1

**Problem:** `daily-prepare` exists, but the readiness payload still mixes multiple concerns
and does not clearly separate:

- prior-day parity
- runtime coverage
- queue readiness
- feed readiness

**Implementation:**
1. Split readiness into explicit sections:
   - data readiness
   - queue readiness
   - feed readiness
   - gate lineage
2. Add a single overall decision:
   - `READY`
   - `OBSERVE_ONLY`
   - `BLOCKED`
3. Include operator remediation guidance in the payload

**Validation:**
- the CLI explains exactly why a session is blocked or downgraded to observe-only

---

### 2.2 Add Session Cleanup And Archive Workflow

**Status:** DONE (2026-03-27)
**Priority:** P1

**Problem:** Stale paper sessions are accumulating and polluting dashboard interpretation.

**Implementation:**
1. Add `nseml-paper cleanup` or strengthen archive/cleanup commands
2. Support:
   - stale live session discovery
   - archive completed replay sessions
   - stop + mark stale sessions explicitly
3. Surface “current active session” separately from archived history in the dashboard

**Validation:**
- operators can cleanly identify the current live session without manual DB inspection

---

### 2.3 Add One Daily Orchestration Command

**Status:** DONE (2026-03-27)
**Priority:** P2

**Problem:** Daily operation still spans too many manual steps:

- token refresh
- data refresh
- feature refresh
- market monitor refresh
- readiness
- live start

**Implementation:**
1. Add a wrapper command for the normal daily sequence
2. Keep each sub-step independently runnable
3. Emit a final operator summary with absolute dates and current status

**Validation:**
- one command can safely execute no-op, partial refresh, and start-of-day workflows

---

## Phase 3 — Strategy / Runtime Promotion Model

### 3.1 Keep Walk-Forward As Research Gate, Not Daily Startup Burden

**Status:** DONE (2026-03-27)
**Priority:** P1

**Problem:** Full walk-forward is too heavy to serve as the daily operational gate, but it
still matters for promotion and lineage.

**Implementation:**
1. Preserve walk-forward as the approval path for strategy/config families
2. Add a lighter daily gate for paper operations
3. Record the link between:
   - approved walk-forward lineage
   - today’s live session
4. Do not require a fresh backtest experiment id just to start paper trading

**Validation:**
- daily live can start from runtime tables alone
- operators can still inspect the last approved lineage behind the session

---

### 3.2 Revisit Persisted ATR / Range Semantics After Runtime Stabilization

**Status:** DONE (2026-03-27)
**Priority:** P2

**Problem:** The persisted daily ATR path still uses simplified true range logic in the main
feature materialization path. That is a correctness issue, but changing it now would mix
engine stabilization with feature-definition drift.

**Implementation:**
1. fix persisted true range after live-paper engine stabilization
2. rebuild affected feature tables in a controlled migration
3. compare backtest and paper impacts separately from runtime fixes

**Validation:**
- feature-definition change is evaluated independently from engine/runtime changes

---

## Recommended Execution Sequence

### Phase 0 — Trust the live engine
1. Separate runtime candidate builder from backtest runner
2. Fix feed-plan to stream-runner token handoff
3. Make Kite websocket startup Windows-safe
4. Add explicit observe-only live mode

### Phase 1 — Make live signals real
5. Build a true intraday candidate path for `thresholdbreakout`
6. Align replay/live decision logic
7. Tighten the daily live symbol universe

### Phase 2 — Make operations usable
8. Add a first-class daily readiness report
9. Add session cleanup and archive workflow
10. Add one daily orchestration command

### Phase 3 — Harden promotion and research coupling
11. Keep walk-forward as research gate, not daily startup burden
12. Revisit persisted ATR / range semantics after runtime stabilization

### Why this ordering

- The current blocker is not strategy alpha. It is live runtime correctness.
- A live session that does not subscribe to tokens or cannot create intraday candidates is
  not ready for operator use.
- Daily usability improvements matter, but only after the engine can truthfully start and
  observe the market.

---

## Immediate Next 3 Actions

If work starts immediately, the recommended first sprint is:

1. **Finish the live feed subscription fix**
   - make `daily-live --run` prove a non-zero token count end-to-end
2. **Extract a read-only paper candidate builder**
   - remove `DuckDBBacktestRunner` lock sensitivity from daily live bootstrap
3. **Design and implement the intraday `thresholdbreakout` watchlist-to-trigger model**
   - prior-day watchlist, intraday trigger, persistent transition audit

That sequence removes the current blocker first, then makes the live queue truthful, then
moves the strategy into a form that can actually trade live.

---

## Cross-References

- Existing paper workflow plan:
  [`docs/architecture/PAPER_TRADING_WALK_FORWARD_PLAN.md`](./PAPER_TRADING_WALK_FORWARD_PLAN.md)
- Operator catch-up flow:
  [`docs/operations/DATA_APPEND_GUIDE.md`](../operations/DATA_APPEND_GUIDE.md)
- CLI reference:
  [`docs/reference/COMMANDS.md`](../reference/COMMANDS.md)
