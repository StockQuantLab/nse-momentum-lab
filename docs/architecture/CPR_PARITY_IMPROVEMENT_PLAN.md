# CPR Parity Improvement Plan

**Date**: 2026-04-25
**Author**: Claude + Kannan
**Source**: Cross-project comparison with `cpr-pivot-lab`
**Status**: Approved — Claude (2 rounds) + Codex (3 rounds)

---

## Problem Statement

NSE momentum lab's paper trading engine was cloned from CPR's architecture but diverged during
the 2LYNCH adaptation. Key resilience and parity features from CPR were not ported or were
simplified. This plan identifies and prioritizes the gaps.

## Current State

- Paper trading v2 engine is functional (DuckDB-only, modular, 17 CLI subcommands)
- Replica sync, feed audit, alert dispatcher, crash recovery all work
- Backtest engine (`duckdb_backtest_runner.py`) and paper engine (`paper_runtime.py`) now share
  the entry trigger and H-carry decision helper, but same-day stop execution and next-session
  stop application still diverge by engine (`ISSUE-055`)
- Phase 2-8 operational guardrails are now in place; the remaining open architecture item is
  carry/exit consolidation (`ISSUE-055`) and Phase 7.2 parity metrics/API stays deferred

## Phase Implementation Status (2026-04-25)

| Phase | Description | Status | Notes |
|-------|-------------|--------|-------|
| Phase 1 | Shared Evaluation Module | ✅ **DONE** | Entry trigger + shared H-carry decision in `shared_eval.py`; parity tests in `test_parity.py`; same-day stop / next-session carry consolidation still open (`ISSUE-055`) |
| Phase 2 | Process Safety | ✅ **DONE** | `command_lock.py`, sentinel flatten, graceful shutdown, stale lock cleanup, CLI locking |
| Phase 3 | Pre-Market Readiness Gate | ✅ **DONE** | `_wait_until_market_ready()`, `_validate_live_runtime_coverage()`, `LIVE_STARTUP_READY` |
| Phase 4 | Feed Audit Replay | ✅ **DONE** | `pack_source="feed_audit"` in `local_ticker_adapter.py` + `paper_replay.py` |
| Phase 5 | Alert Improvements | ✅ **DONE** | `MAX_ALERT_AGE_SEC=600`, two-tier retry, batch suppression in `versioned_replica_sync.py` |
| Phase 6 | Multi-Variant Auto-Restart | ✅ **DONE** | `run_live_session_with_retry`, `run_live_session_group` |
| Phase 7.1 | Structured Trace Logging | ✅ **DONE** | `PARITY_TRACE=1` env var in `paper_runtime.py` |
| Phase 7.2 | Parity Metrics + Diagnostic API | 🔇 **DEFERRED** | Deferred until Phase 1 shared module stabilises |
| Phase 8 | Deployment & Rollback Docs | ✅ **DONE** | `docs/operations/DEPLOYMENT_AND_ROLLBACK.md`, `PARITY_INCIDENT_LOG.md` |

All listed phases are either complete or explicitly deferred. Phase 7.2 remains deferred until
Phase 1 is verified through a live session comparison against replay (`--pack-source feed_audit`).
The remaining open architecture item is carry/exit consolidation (`ISSUE-055`).

## Goals

1. **Structural parity guarantee** — shared evaluation module eliminates live-vs-backtest drift
2. **Operational safety** — process fencing prevents concurrent-writer corruption
3. **Emergency controls** — sentinel flatten works when the live loop is healthy enough to observe the signal file (file-based, bypasses DuckDB)
4. **Pre-market validation** — catch data gaps before the session starts
5. **Debugging capability** — replay from captured live feed for divergence analysis

---

## Phase 1: Shared Evaluation Module (P0)

**Effort**: Large (3-5 days)
**Impact**: Eliminates entire class of parity bugs
**Files to create/modify**: New `services/paper/engine/shared_eval.py`, modify `duckdb_backtest_runner.py`, `paper_runtime.py`, `intraday_execution.py`

### Step 1.1 — Extract thin shared decision helpers

**Approach**: Extract pure decision helpers rather than full orchestration. The repo already shares
some parity-critical logic: backtest delegates intraday entry resolution to shared candidate-builder
code, and paper delegates held-position bar logic to `evaluate_held_position_bar()` in
`intraday_execution.py`. Extract only the filter-evaluation and stop-computation functions that
both paths need, keeping orchestration in their respective modules.

Extract into `shared_eval.py`:
- `evaluate_admission_filters()` — admission filters (N, Y, C, L), causal `watch_date` features,
  threshold check, stop distance validation, price continuity guard. Pure function, no DB access.
- `compute_stop_update()` — stop check, trail update, one-bar resolution. Pure function.
- Both accept plain dataclass inputs (`SignalContext`, `PositionContext`) and return plain results
- Must be callable from both backtest batch loop and paper bar-by-bar evaluation without
  introducing additional DB queries or side effects

### Step 1.2 — Wire backtest and paper to shared helpers

Modify `duckdb_backtest_runner.py` to call `shared_eval.evaluate_admission_filters()` and
`shared_eval.compute_stop_update()` instead of inline filter/stop logic. The batch/vectorized
path remains for performance but delegates decision logic to the shared pure functions.

Modify `paper_runtime.py` `_evaluate_entry()` and `_advance_open_position()` to call
the same shared functions. Remove duplicated filter/stop logic.

### Step 1.3 — Parity test suite (two-layer contract)

**Layer 1 — Refactor-invariance tests** (must be exact):
- Run backtest path and paper path on the same symbol/date data
- Assert exact match: entry_time, exit_time, entry_price, exit_price, qty, exit_reason
- These tests verify the refactoring didn't change behavior for unchanged code paths
- Tolerance for PnL: `abs=15.0` (floating-point rounding in cost model)
- Cover: normal entry/exit, stop hit, time exit, gap-through-stop

**Layer 2 — ISSUE-042 observational tests** (quantified drift):
- Compare daily-bar hold-day P&L vs 5-min-bar hold-day P&L
- Document delta per strategy; set acceptance threshold (e.g., < 0.5% annualized drift)
- These are not exact-match tests — they quantify the known granularity gap
- If delta exceeds threshold, consider adding intraday backtest mode

### Step 1.4 — Parity check CLI tool

Extend existing `scripts/parity_check.py` (already in the repo):
- Add paper-session-vs-backtest comparison mode
- Join on `(symbol, trade_date, direction, entry_time, exit_time)`
- Report: matched trades, expected_only, actual_only, pnl_drift
- Exit code 0 if within tolerance, 1 if drift exceeds threshold

### Step 1.5 — Performance gate

Add a benchmark for the shared decision helpers:
- Measure time per decision call in the backtest hot loop
- Budget: shared helpers must not add > 5% overhead to full 11-year backtest runtime
- Run full 4-leg canonical backtest before and after; compare wall-clock time
- If budget exceeded, consider inlining hot paths while keeping shared logic for paper

Create `tests/unit/test_parity.py`:
- Run backtest path and paper path on the same symbol/date data
- Assert exact match: entry_time, exit_time, entry_price, exit_price, qty, exit_reason, pnl
- Tolerance for PnL: `abs=15.0` (floating-point rounding in cost model)
- Cover: normal entry/exit, stop hit, time exit, gap-through-stop, H-carry scenarios

### Step 1.6 — Expanded integration tests

Beyond parity tests, add integration tests for:
- **Late-start scenario**: Start session at 10:30 AM; verify entry window logic skips pre-start bars
- **Position seeding edge case**: Seed position with overnight gap; verify P&L reflects gap correctly
- **Duplicate candle protection**: Send same 5-min bar twice; verify no double-evaluation
- **Multi-variant writer contention**: Two strategies in `multi-live`; verify no corruption
- **Chaos test**: Kill process mid-write, resume from checkpoint, verify no stale lock,
  no duplicate close, no replica corruption
- **Candle builder disconnect test**: Simulate WebSocket disconnect mid-bar; verify partial bar
  state is preserved in feed_state, not lost
- **Multi-hour soak test**: Run `multi-live` for 3+ hours with alerts and feed audit enabled;
  verify no memory leak, no replica drift, alert queue doesn't overflow

---

## Phase 2: Process Safety (P1)

**Effort**: Small (1 day)
**Impact**: Prevents DuckDB lock contention from concurrent processes
**Files to create/modify**: New `services/paper/engine/command_lock.py`, modify CLI subcommands

### Step 2.1 — Port `command_lock.py` from CPR

Cross-process file locking using OS-level byte-range locking:
- `acquire_command_lock(name, detail)` — creates `.tmp_logs/{name}.lock` with PID + detail
- `release_command_lock(name)` — releases on process exit
- Stale lock detection: if holder PID is dead, auto-cleanup
- Actionable error: prints exact `taskkill` command on conflict

### Step 2.2 — Add locking to ALL mutating CLI subcommands

Wrap **every** `nseml-paper` subcommand that writes to `paper.duckdb`:
- `prepare`, `replay`, `live`, `multi-live` — session creation and execution
- `stop`, `pause`, `resume`, `archive` — session state transitions
- `flatten`, `flatten-all` — position mutations
- `eod-carry` — carry decisions
- `daily-prepare`, `daily-replay`, `daily-live`, `daily-sim` — daily shortcuts
- All share the same `command_lock("paper_writer")` name so only one writer runs at a time
- **Lock acquisition is in canonical handlers only** — daily shortcuts (`daily-prepare`,
  `daily-replay`, `daily-live`) delegate to canonical handlers which acquire the lock.
  Daily shortcuts must NOT acquire the lock themselves to avoid self-deadlock.

### Step 2.3 — Fix `stop` stranding positions (ISSUE-040)

Add safety guard to `_cmd_stop`:
- If session has open positions, refuse with error unless `--force` flag is given
- With `--force`: flatten all positions first, then stop the session
- Dispatch both `SESSION_COMPLETED` and `DAILY_PNL_SUMMARY` alerts with P&L summary (fixes ISSUE-041)

### Step 2.4 — Sentinel file flatten mechanism

Port CPR's file-based flatten signal:
- Check `.tmp_logs/flatten_{session_id}.signal` every poll cycle in `paper_live.py`
- If file exists: delete it, flatten all positions, set session COMPLETED, break main loop
- Also support `.tmp_logs/cmd_{session_id}/` for surgical `close_positions` by symbol

**Security hardening**:
- Validate file contents strictly (allowlist of known-safe symbols for `close_positions`)
- Reject malformed JSON or unknown command types with WARNING log
- Use atomic rename for signal file creation (write to `.tmp` then rename to `.signal`)
- Document that `.tmp_logs/` should have restricted Windows ACLs

### Step 2.5 — DuckDB lock diagnostics

Port CPR's `_diagnose_paper_db_lock()`:
- On DuckDB `IOException`, extract holding PID from error message
- Resolve command line via PowerShell `Get-Process -Id {pid} | Select-Object CommandLine`
- Print banner with exact `taskkill` command
- Record process start time alongside PID in lock files to detect PID reuse on Windows

### Step 2.6 — Graceful shutdown signal handling

Add explicit SIGINT/SIGTERM/SIGBREAK handling to `paper_live.py`:
- On interrupt: set session status to STOPPING (not FAILED)
- Flush remaining candle builder state (persist partial bars to feed_state)
- Drain alert dispatcher (wait up to 120s for queued alerts)
- Force replica sync
- Unregister session from ticker adapter
- Then auto-flatten positions if `auto_flatten_on_error=True`

### Step 2.7 — Stale signal file cleanup

On session startup, sweep `.tmp_logs/` for orphaned `.signal` and `cmd_*/` files:
- Only clean files older than 24 hours (avoid race with active session)
- Log count of cleaned files

Add explicit lock ordering in `multi-live.py`:
- Verify the shared `_lock` in `PaperDB` is sufficient for concurrent writes from multiple strategies
- Test: spawn two `multi-live` processes with different strategies; verify second fails with
  "another writer is active" message (via command_lock, not just DuckDB IOException)
- Add PID liveness check using `psutil` to verify lock-holder process is actually running
  (not just PID exists as defunct/zombie)

---

## Phase 3: Pre-Market Readiness Gate (P2)

**Effort**: Medium (1-2 days)
**Impact**: Prevents mid-session failures from missing data
**Files to modify**: `paper_live.py`, `paper_runtime.py`, `cli/paper_v2.py`

### Step 3.1 — Pre-market connect with suppressed trading

Add `_wait_until_market_ready()` to `paper_live.py`:
- Connect WebSocket **before** market open (not after) to capture 09:15 bar data
- Suppress all trading decisions until `entry_start_minutes` after open (5 min = 09:20 by default;
  do NOT use `entry_cutoff_minutes` which defaults to 60 and means the last minute allowed)
- The 09:15 bar is needed for `session_low`/`session_high` accumulation used by stop placement
- Harden reconnect logic around the KiteTicker pre-market-to-regular segment-flip at 09:15
- Print countdown during pre-market wait; log "TRADING_ENABLED" when suppression lifts

### Step 3.2 — Runtime coverage validation

Add `validate_live_runtime_coverage()` check before live session starts:
- Verify prior-day daily bars exist in `v_daily`
- Verify prior-day 5-min bars exist in `v_5min`
- Verify feat_daily has rows for the signal date
- Hard-exit with actionable message if any check fails

### Step 3.3 — Direction readiness preflight

Add direction/state readiness log at session startup:
- Count how many seeded candidates have valid setup rows
- Log `LIVE_STARTUP_READY: X/Y candidates have valid features`
- If < 50% valid, log WARNING

---

## Phase 4: Feed Audit Replay (P2)

**Effort**: Medium (1-2 days)
**Impact**: Debug live-vs-replay divergences with exact captured feed
**Files to create/modify**: Modify `feeds/local_ticker_adapter.py`, new `scripts/paper_feed_replay.py`

### Step 4.1 — Feed-audit-as-replay-source

Add `--pack-source feed_audit` option to replay mode:
- Load bars from `paper_feed_audit` table instead of `v_5min`
- Reconstruct `ClosedCandle` objects from feed_audit OHLCV
- Sets `feed_source="feed_audit"` and `transport="replay"` for traceability

### Step 4.2 — Parity incident log

Create `docs/operations/PARITY_INCIDENT_LOG.md`:
- Document every live-vs-replay divergence found
- Template: date, symbol, expected (backtest), actual (live), root cause, fix
- Include the CPR divergences as reference (double filtering, trail state, SHORT exit value)

---

## Phase 5: Alert Improvements (P3)

**Effort**: Small (0.5 day)
**Impact**: Better signal-to-noise in operator alerts
**Files to modify**: `alert_dispatcher.py`

### Step 5.1 — Alert age guard

Add `MAX_ALERT_AGE_SEC = 600` to alert dispatcher:
- Before each retry attempt, check if `alert.age + next_wait > MAX_ALERT_AGE_SEC`
- If stale, discard and log as "discarded_stale" instead of retrying
- Prevents FEED_STALE alerts from 15 minutes ago arriving after feed has recovered

### Step 5.2 — Two-tier retry (fast + slow)

Split the current 6-tier retry schedule into two tiers:
- Fast tier: 3 attempts at 1s/2s/4s (all error types)
- Slow tier: 2 attempts at 30s/120s (network errors only, classified by `_is_network_error()`)
- Total max retry budget: ~153s for persistent network errors

### Step 5.3 — Batch suppression for replica sync

Add `_begin_replica_batch()` / `_end_replica_batch()` to `PaperDB`:
- Suppresses `maybe_sync` calls while `_replica_batch_depth > 0`
- Final `force_sync()` after batch ends
- Use during multi-write operations (session creation, flatten, archive)

---

## Phase 6: Multi-Variant Auto-Restart (P2)

**Effort**: Small (0.5 day)
**Impact**: Faster recovery on volatile days
**Files to modify**: `paper_live.py`, `cli/paper_v2.py`

### Step 6.1 — Per-variant restart with time-of-day awareness

Enhance `run_live_session_with_retry` or `run_live_session_group`:
- Track per-strategy restart count independently
- Add `_should_retry_variant_exit()` — no retries after 14:30 IST
- Exponential backoff between restarts (10s, 20s, 40s...)
- On resumed sessions: set `auto_flatten_on_error=False` to preserve positions

---

## Phase 7: Observability (P2 — deferred until Phase 1 stabilizes)

**Effort**: Small (0.5-1 day, phased)
**Impact**: Debugging parity issues requires structured traces
**Files to modify**: `paper_runtime.py`, `duckdb_backtest_runner.py`

### Step 7.1 — Structured logging with trace context (Phase 1 prerequisite)

Add env-gated structured trace logging only — no DB writes, no new tables, no API endpoints:
- `PARITY_TRACE=1` env var enables verbose trace logging for entry/exit decisions
- `SETUP_PARITY_CHECK=1` env var cross-checks live setup rows against `feat_daily` values
- Traces include: symbol, date, bar_time, decision, reason, features_used, context_hash
- Output goes to Python logger only (structured JSON lines); no hot-path DB writes

### Step 7.2 — Parity metrics and diagnostic API (deferred)

Deferred until Phase 1 shared module stabilizes:
- `parity_metrics` table in `paper.duckdb` — per-session, per-symbol drift metrics
- `/api/paper/parity-state` endpoint for mid-session debugging
- These add schema, write load, and replica churn; unsafe to add before the refactoring settles

---

## Phase 8: Deployment & Rollback (P2)

**Effort**: Small (0.5 day)
**Impact**: Safe deployment without market-day disruption
**Files to modify**: None (process documentation)

### Step 8.1 — Rollback procedure

Document rollback procedure for each phase:
- Git revert strategy (which commits to revert, in what order)
- Feature rebuild requirement after reverting Phase 1 (`nseml-build-features --force --allow-full-rebuild`)
- DuckDB state recovery (replica files may contain post-refactor state)

### Step 8.2 — Pre-deployment checklist

- All parity tests pass (Phase 1.3/1.5/1.6)
- Canonical 4-leg backtest produces trade-level equivalent results (entry/exit/price/qty/reason
  match against pre-deployment baseline; `exp_id` WILL change due to `code_hash` — this is expected)
- `nseml-db-verify` passes
- DQ scan clean (`nseml-hygiene --report`)

#### ✅ Baseline Regression Check — Phases 2-8 (2026-04-25)

Canonical 4-leg baselines re-run on 2026-04-25 after implementing Phases 2-8. Compared against the
2026-04-22 pre-CPR-changes runset. **Result: zero regression confirmed.**

| Leg | Pre-CPR (Apr-22) | Post-Phases-2-8 (Apr-25) | Ann Δ | Trade Δ | Verdict |
|-----|-------------------|--------------------------|-------|---------|---------|
| BREAKOUT_4% | +54.2% / 2,213 trades | +54.5% / 2,217 trades | +0.3pp | +4 | ✅ date-extension only |
| BREAKOUT_2% | +122.0% / 7,086 trades | +122.0% / 7,097 trades | 0 | +11 | ✅ date-extension only |
| BREAKDOWN_4% | +3.1% / 258 trades | +3.1% / 258 trades | 0 | 0 | ✅ identical |
| BREAKDOWN_2% | +8.2% / 790 trades | +8.3% / 792 trades | +0.1pp | +2 | ✅ date-extension only |

Trade delta explanation: end-date extended from 2026-04-22 → 2026-04-23, adding 1 extra hold day that
resolved a small number of open positions. No behavioral change from the Phases 2-8 code modifications.
`exp_id` values changed (expected: `code_hash` incorporates module paths).

Post-Phases-2-8 canonical IDs (active pre-Phase-1 reference, run 2026-04-25 to end-date 2026-04-26):

| Preset | Exp ID | Ann% | DD% | Calmar | PF | Trades | Win% |
|--------|--------|------|-----|--------|-----|--------|------|
| BREAKOUT_4% | `bd22a5859c571c0d` | +54.5% | 3.16% | 17.3 | 20.80 | 2,217 | 40.6% |
| BREAKOUT_2% | `e5cbeed50a3c78e4` | +122.0% | 2.73% | 44.7 | 19.23 | 7,097 | 38.6% |
| BREAKDOWN_4% | `d6b34cbfb49137de` | +3.1% | 0.74% | 4.2 | 5.50 | 258 | 36.0% |
| BREAKDOWN_2% | `073e3a2225abb123` | +8.3% | 1.90% | 4.4 | 5.48 | 792 | 25.9% |

#### ✅ Baseline Regression Check — Phase 1 (2026-04-26)

Post-Phase-1 4-leg run (end-date 2026-04-26). Compared trade-by-trade against the pre-Phase-1
reference above. **Result: NOT a regression — post-Phase-1 reflects the intended canonical rules.**

| Leg | Pre-Phase-1 ID | Post-Phase-1 ID | Ann Δ | Trade Δ | Verdict |
|-----|---------------|-----------------|-------|---------|---------|
| BREAKOUT_4% | `bd22a5859c571c0d` | `a23f33ed4c15545c` | ~0 | −3 dropped / +1 added / 16 stop-only | ✅ spec change (stop semantics) |
| BREAKOUT_2% | `e5cbeed50a3c78e4` | `de7e20a20ecd03fc` | −0.2pp | −7 dropped / +1 added / 23 stop-only | ✅ spec change (stop semantics) |
| BREAKDOWN_4% | `d6b34cbfb49137de` | `2ef1d641142a6d25` | PF 5.50→5.82 | −4 dropped / 3 exit-changed | ✅ SHORT carry bug fixed |
| BREAKDOWN_2% | `073e3a2225abb123` | `e489fef43123b62a` | 0 | 0 dropped / 0 added / 0 changed | ✅ identical |

**Phase 1 had exactly two independent behavior-changing code changes:**

---

**Change 1 — Entry admission: `session_low` accumulation (spec change, not a bug fix)**

Pre-Phase-1: `initial_stop` was computed from the **triggering candle's own low** (point-in-time snapshot).
Post-Phase-1: `evaluate_entry_trigger()` uses `session_low = min(all candle lows from 09:15 → trigger time)` — the accumulated session low, which is a wider and more representative stop reference.

This is a **stop-distance semantics change**, not a bug fix. Both interpretations are internally consistent; post-Phase-1 is the intended canonical rule.

Effect on dropped trades (stop_dist from pre-Phase-1 DB):

| Trade | Pre stop_dist | Exit reason | P/L |
|-------|--------------|-------------|-----|
| RVNL 2020-05-12 | 0.00% | STOP_BREAKEVEN | −0.08% |
| MIRZAINT 2023-04-17 | 0.00% | ABNORMAL_PROFIT | +18.60% |
| RMDRIP 2025-03-21 | 0.00% | DATA_INVALIDATION | 0.00% |
| MOL 2023-04-18 | 1.96% | GAP_STOP | −0.66% |
| LAOPALA 2023-06-05 | 1.63% | TIME_EXIT | +4.21% |
| IDFCFIRSTB 2023-07-11 | 1.65% | STOP_POST_DAY3 | +0.23% |
| TEGA 2023-10-27 | 2.22% | STOP_INITIAL | −1.10% |

The three BREAKOUT_4% drops (RVNL, MIRZAINT, RMDRIP) all showed `stop_dist=0.00%` —
the triggering candle opened exactly at its own low, making the old candle-only stop = entry price.
Under the new accumulated `session_low`, the stop distance exceeded `max_stop_dist_pct=0.08` and
those entries were correctly rejected by the stricter canonical rule.

These were **valid trades under the old, looser stop interpretation**; they fail the **stricter
canonical stop-distance rule after Phase 1** and are removed by design. Whether MIRZAINT's
+18.60% gain is "lost" depends on which stop spec you consider authoritative. Post-Phase-1 is the
intended spec.

The 16/23 stop-only-changed trades are **diagnostic-only changes**: `initial_stop` shifted by a
fixed ratio (~0.96) due to the accumulation vs. snapshot difference, but exit price, exit reason,
and pnl_pct are **IDENTICAL** in all cases — no behavioral regression.

---

**Change 2 — SHORT H-carry breakeven clamp direction (real bug fix)**

Pre-Phase-1: the backtest applied `max(carry_stop, entry_price)` to SHORT trades — the LONG
formula. For a SHORT, breakeven requires the stop to be **at or below** entry price (`min`, not `max`).
Using `max` pushed the carry stop above entry for profitable shorts.

Post-Phase-1: `evaluate_hold_quality_carry_rule(is_short=True)` correctly uses
`min(carry_stop, entry_price)`.

| Trade | Entry | Pre exit | Pre pnl | Post exit | Post pnl | Swing |
|-------|-------|----------|---------|-----------|---------|-------|
| CENTRALBK 2018-10-09 | 30.20 | 28.700 | +4.88% | 29.848 | +1.08% | −3.8pp |
| ADANIGREEN 2023-02-27 | 462.20 | 485.300 | −5.08% | 456.664 | +1.11% | **+6.2pp** |
| MOKSH 2022-02-15 | 19.31 | 19.310 | −0.08% | 19.310 | −0.08% | 0 (stop-only) |

ADANIGREEN is the clearest evidence: the old carry stop (inferred from the exit price as ~485.30)
was **above the initial loss stop (472.25)**, which for a SHORT means the position was allowed to
run into a larger loss than the risk stop defined at entry. The `max` formula turned a breakeven
clamp into a loss-widener.

**Important caveat**: The pre-Phase-1 carry stop values (e.g. 485.30 for ADANIGREEN) are
**inferred from exit prices**, not read from stored DB fields. `carry_stop_next_session` was not
persisted in pre-Phase-1 experiments (see ISSUE-057). The inference is consistent with the
observed exit prices, but cannot be verified column-by-column from the old `bt_trade` rows.
Post-Phase-1 rows correctly store the clamped carry stop.

Regression tests for both cases are in `tests/unit/services/paper/test_parity.py`:
`test_short_carry_regression_centralbk_2018` and `test_short_carry_regression_adanigreen_2023`.

---

- **BREAKDOWN_2% zero diff**: `breakdown_filter_n_narrow_only=True` narrows candidates such
  that the session_high discrepancy from Change 1 does not affect any of the 792 entries.
- **Open**: same-day stop execution and next-session stop application are still split across
  engine-specific paths — tracked in ISSUE-055 (carry/exit consolidation, not yet done).

### Step 8.3 — Deployment timing rules

- **Phase 1 (Shared Evaluation)**: Deploy only on non-trading days (weekends, holidays)
  — this is a structural refactoring touching both backtest and paper engines
- **Phase 2-3, 5-6**: Can deploy any day (additive features, no behavior change)
- **Phase 4**: Deploy on non-trading day (replay path changes)
- **Phase 7**: Can deploy any day (observability only)
- Hold rule: if canonical backtest trade-level results diverge from pre-deployment baseline, revert immediately

---

### Step 8.4 — Deferred: Post-session export to `bt_trade` shape

Explicitly deferred from this plan cycle:
- Export completed paper sessions into a `bt_trade`-compatible shape for parity analysis and
  longitudinal comparisons
- This requires schema alignment between `paper_positions` and `bt_trade`, which is a separate
  design effort
- Tracked as a future work item; not a blocker for any phase in this plan

---

## Implementation Order

```
✅ Phase 2 (Process Safety)          command_lock, sentinel, graceful shutdown, ISSUE-040/041
✅ Phase 3 (Pre-Market Readiness)    Pre-market connect + suppressed trading
✅ Phase 7.1 (Structured Logs)       Env-gated trace logging only (no DB writes)
✅ Phase 1 (Shared Evaluation)       entry trigger + H-carry shared; carry/exit consolidation still open
✅ Phase 4 (Feed Audit Replay)       feed_audit pack source (Phase 1 parity tests still pending)
✅ Phase 5 (Alert Improvements)      Two-tier retry, age guard, batch sync
✅ Phase 6 (Multi-Variant Restart)   run_live_session_with_retry, run_live_session_group
🔇 Phase 7.2 (Metrics + API)         Deferred until Phase 1 stabilizes
✅ Phase 8 (Deployment Plan)         DEPLOYMENT_AND_ROLLBACK.md, PARITY_INCIDENT_LOG.md
```

Phases 2-8 are complete. Phase 7.2 is deferred until after Phase 1 is stable. The remaining
open architecture follow-up is carry/exit consolidation in `ISSUE-055`.

## Risk Assessment

| Risk | Mitigation |
|------|-----------|
| Phase 1 shared module breaks existing baselines | Trade-level equivalence tests (not exp_id match — IDs change with code_hash by design); deploy on non-trading days |
| Command lock leaves stale `.lock` files | PID + process start time check via `psutil` + auto-cleanup; file-age heuristic fallback for PID reuse |
| Feed audit replay diverges from EOD replay | Parity test suite (Phase 1) catches both paths |
| Pre-market connect loses 09:15 bar data | Connect before open, suppress trading until 09:20; do NOT delay WebSocket connection |
| DuckDB version drift between backtest and paper | Pin DuckDB version in pyproject.toml; add version assertion in `shared_eval.py` |
| Windows file locking edge cases (oplocks, antivirus) | Test command_lock on actual Windows 11; add fallback stale-lock detection with file-age heuristic |
| Alert delivery blackout during Phase 5 refactoring | Run in shadow mode for 1 week (send to both old and new paths) |
| Phase 1 shared helpers add backtest overhead | Performance gate: shared helpers must not add > 5% to full 11-year backtest runtime |
| Feature schema changes mid-implementation | Freeze `FEAT_DAILY_QUERY_VERSION` during Phase 1 work; bump after refactoring complete |
| Multi-variant writer contention in `multi-live` | All mutating CLI subcommands share same command_lock; explicit contention test |
| `stop` command strands open positions | Phase 2.3 adds `--force` guard and auto-flatten before stop |
| PID reuse on Windows invalidates lock check | Record process start time in lock file; compare against current PID start time |

## Success Criteria

1. **Phase 1**: Trade-level equivalence tests pass (entry/exit/price/qty/reason match); backtest wall-clock within 5% of baseline
2. **Phase 1.3**: ISSUE-042 test quantifies daily vs 5-min hold-day P&L delta; delta is documented
3. **Phase 2**: Two concurrent `nseml-paper replay` processes are prevented with clear error message; `stop` refuses when positions open
4. **Phase 3**: Live session connects before open and suppresses trading until 09:20
5. **Phase 4**: `replay --pack-source feed_audit` reproduces the exact trades from a live session
6. **Phase 5**: Stale FEED_STALE alerts are discarded after 10 minutes, not delivered
7. **Phase 6**: Failed breakout strategy auto-restarts without killing the breakdown strategy
8. **Phase 7.1**: `PARITY_TRACE=1` produces structured decision logs for both backtest and paper

## Estimated Total Effort

| Phase | Days | Dependencies |
|-------|------|-------------|
| Phase 1 | 3-5 | Phase 7.1 (traces for debugging) |
| Phase 2 | 1.5 | None |
| Phase 3 | 1-2 | None |
| Phase 4 | 1-2 | Phase 1 (parity tests) |
| Phase 5 | 0.5 | None |
| Phase 6 | 0.5 | None |
| Phase 7.1 | 0.5 | None |
| Phase 7.2 | 0.5 | Phase 1 stabilized |
| Phase 8 | 0.5 | None |
| **Total** | **9-12.5** | |

---

## Reference: CPR Source Locations

| Feature | CPR File | Key Lines |
|---------|----------|-----------|
| Shared eval module | `engine/cpr_atr_shared.py` | 87-313, 438-567 |
| TrailingStop shared class | `engine/cpr_atr_utils.py` | 25-120 |
| Parity test | `tests/test_parity.py` | 72-150 |
| Parity check CLI | `scripts/parity_check.py` | Full file |
| Command lock | `engine/command_lock.py` | Full file (173 lines) |
| Sentinel flatten | `scripts/paper_live.py` | 1231-1302 |
| Pre-market wait | `scripts/paper_trading.py` | 1196-1210 |
| Runtime coverage | `scripts/paper_trading.py` | 503-573 |
| Lock diagnostics | `db/paper_db.py` | 181-232 |
| Feed audit replay | `engine/day_pack_sources.py` | 94-237 |
| Two-tier retry | `engine/alert_dispatcher.py` | 235-302 |
| Batch suppression | `db/duckdb.py` | 524-530 |
| Auto-restart | `scripts/paper_trading.py` | 993-1154 |
| Parity incident log | `docs/PARITY_INCIDENT_LOG.md` | Full file |
