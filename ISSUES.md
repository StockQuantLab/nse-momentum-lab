# Known Issues

Track all open, fixed, and deferred issues here. Update status as issues are resolved.

Format: `[STATUS]` = `OPEN` | `FIXED` | `DEFERRED` | `INVESTIGATING`

---

## Data Quality

### ISSUE-001 — Daily/5-min price scale mismatch (corporate actions)

**Status**: ✅ RESOLVED — 2026-04-20
**Severity**: Critical (affects backtest P&L accuracy)
**Found**: 2026-04-19

**Problem**: Last week's 5-min re-ingest returned Kite-adjusted prices for all historical data.
Daily parquet was NOT re-ingested. For symbols with corporate actions (splits, bonus, reverse
consolidation), 5-min and daily prices are now on different price scales.

**Example**: FCL had a 10:1 reverse consolidation. 5-min entry ~34 (adjusted), daily exit ~327
(unadjusted) → -852% P&L on short trades. Root cause: entry price from 5-min, exit/stop from daily.

**Affected symbols**: 276 symbols with `EXTREME_MOVE_DAILY` flag in `data_quality_issues`.

**Resolution (2026-04-20)**:
- `all.parquet` trimmed to pre-2025-01-01 for all 276 symbols (removes wrong 2025 prices)
- `kite.parquet` deleted and re-ingested from 2025-01-01 to 2026-04-20 via Kite API
- 262/276 symbols succeeded; 14 skipped (delisted, no instrument token)
- Features rebuilt (`nseml-build-features --since 2025-01-01`)
- `EXTREME_MOVE_DAILY` count dropped: 276 → 181 (remaining are pre-2025 issues, outside backtest window)
- Clean 4-leg backtest completed — all 4 legs profitable (see CLAUDE.md canonical exp IDs)

---

### ISSUE-004 — Active DQ issues in market data

**Status**: OPEN
**Severity**: Medium
**Found**: 2026-04-19 (from DQ table scan)

**Active issue counts in `data_quality_issues` (market.duckdb)**:
- `TIMESTAMP_INVALID`: 1,628 rows
- `ZERO_PRICE`: 718 rows
- `DATE_GAP`: 319 rows
- `OHLC_VIOLATION`: 218 rows

These are pre-existing and were not introduced by recent re-ingest. Most affect illiquid symbols
not in the trading universe. Investigate before expanding universe beyond 2000 symbols.

---

## Backtest Engine

### ISSUE-002 — Duplicate signals (same symbol/date entered multiple times)

**Status**: FIXED — `duckdb_backtest_runner.py` (2026-04-20)
**Severity**: High (inflated trade count, duplicate losses)
**Found**: 2026-04-19 (FCL had 3 entries on 2025-02-06 in BREAKDOWN_4% run)

**Problem**: The candidate SQL query could return the same `(symbol, trading_date)` row multiple
times when a corporate action event appears in the data. The `vbt_signals` list got all duplicates
appended while `signal_context` dict only kept the last (silent overwrite).

**Fix**: Added `seen_signal_keys: set[tuple[int, date]]` before the signal loop. Any duplicate
`(symbol_id, sig_date)` is skipped with a WARNING log and `skipped_duplicate` counter incremented.
Counter is surfaced in year stats and experiment summary.

---

### ISSUE-006 — backtest_dashboard.duckdb snapshot skipped when dashboard is open

**Status**: ✅ RESOLVED — 2026-04-21 (versioned replica system)
**Severity**: Low
**Found**: 2026-03-12

**Problem**: After a backtest run completes, the engine attempted to snapshot `backtest.duckdb` →
`backtest_dashboard.duckdb`. If the NiceGUI dashboard was open and holding a read connection, the
snapshot could be skipped (DuckDB single-writer constraint).

**Resolution**: Replaced the single-file snapshot with `VersionedReplicaSync`. The backtest engine
writes to `data/backtest.duckdb`; after each run it creates a new versioned replica (e.g.
`data/backtest_replica/backtest_replica_v3.duckdb`) and atomically updates a pointer file.
The dashboard reads only the replica, so the writer and reader never contend on the same file.
`nseml-backtest-cleanup` also calls `force_sync()` after pruning, so the dashboard stays consistent.

---

## Dashboard

### ISSUE-003 — PNL% column sort incorrect in Trade Ledger

**Status**: FIXED — `apps/nicegui/pages/backtest_results.py` + `components/__init__.py` (2026-04-19)
**Severity**: Medium (usability)
**Found**: 2026-04-18

**Problem**: `pnl_pct` and `pnl_r` were stored as formatted strings (e.g. `"-852.97%"`) in the
Quasar table row data. Quasar string-sorts these lexicographically, making large negatives appear
out of order. Also, `float(r.get("pnl_pct", 0).replace("%",""))` crashed when the value was
already a float.

**Fix**:
- `_trade_rows()` returns raw floats for `pnl_pct` and `pnl_r`
- `_col_def()` adds a JS `format` function for display (e.g. `+12.3%`) and `sortable: True` only
  for numeric columns
- `components/__init__.py` filter uses `float(r.get("pnl_pct", 0) or 0)` (handles both string
  and float)

---

## Paper Trading Engine

### ISSUE-007 — Max-stop-distance filter not replicated in paper engine

**Status**: FIXED — `strategy_presets.py` + `paper_runtime.py` (2026-04-20)
**Severity**: Medium (paper vs backtest parity gap)
**Found**: 2026-04-18 (code review)

**Problem**: The backtest engine filtered out signals where the initial stop distance was too wide
(`max_stop_dist_pct = 0.08` default, `short_max_stop_dist_pct` for shorts). The paper engine
`_evaluate_entry` did not apply this filter, causing paper to enter trades that backtest skips.

**Fix**:
- Added `max_stop_dist_pct: float = 0.08` and `short_max_stop_dist_pct: float | None = None`
  to `PaperStrategyConfig` dataclass (matches `BacktestParams` defaults exactly)
- `get_paper_strategy_config()` fields dict and constructor updated to pass these through
- `_evaluate_entry` now checks after computing `initial_stop`:
  - LONG: skip if `initial_stop < entry_price * (1 - max_stop_dist_pct)`
  - SHORT: skip if `initial_stop > entry_price * (1 + effective_max_stop)`, where
    `effective_max_stop = short_max_stop_dist_pct ?? max_stop_dist_pct`
- Return value: `{"action": "SKIP", "reason": "stop_too_wide"}` (same semantics as backtest
  `skipped_stop_too_wide` diagnostic status)

---

## Strategy / Research

### ISSUE-005 — BREAKDOWN trade count too low for statistical significance

**Status**: INVESTIGATING
**Severity**: Medium
**Found**: 2026-04-19

**Current counts (Apr-19 run, 2025-01-01 → 2026-04-19)**:
- BREAKDOWN_4%: 81 trades, +0.84% total return
- BREAKDOWN_2%: 167 trades, +9.57% total return

**Root causes**:
1. Partial data contamination (ISSUE-001) may be excluding valid short candidates
2. Window is only ~15 months; short-side needs broader history
3. Short-side admission filters (N, Y, C, L) were adapted from long-side and may be too restrictive
4. Some prior profitable runs (pre-Mar-31) used lookahead bias — causal fix correctly
   reduced trade count but baseline comparison requires clean multi-year run

**Next steps**:
- Re-ingest 276 symbols (ISSUE-001) then re-run BREAKDOWN with full history (2020–2026)
- Investigate relaxing short-specific admission filters if trade count remains <200

---

## Operational

### ISSUE-008 — Causal admission fix reduces trade count vs pre-Mar-31 runs

**Status**: RESOLVED — expected behavior, not a bug
**Found**: 2026-04-19

**Context**: Mar-29 runs showed BREAKOUT_4% with +289% return and 1,402 trades. Apr-1 and later
runs show ~548 trades and +51.5%. This is because the Mar-31 commit (`4f2515b4`) fixed a
fundamental lookahead bias:

- **Old behavior**: Admission filters (N, Y, C, L) evaluated on the *breakout day's own features*
  (e.g. same-day `close_pos_in_range`, same-day volume). At 09:15 entry time, these are not
  known — you only know the prior day's data.
- **New behavior**: All admission filters use `watch_date` (prior-day) features. `filter_h`
  removed from admission; it is now a trade management signal only (carry/exit overnight).

The lower trade count in post-Mar-31 runs is correct. Pre-Mar-31 inflated returns are invalid.

---

## Strategy Enhancement Wave 1

### ISSUE-009 — WEAK_CLOSE_EXIT never triggers (H-carry rule disabled by default)

**Status**: ✅ FIXED — `duckdb_backtest_runner.py`, `backtest_presets.py` (2026-04-21)
**Severity**: High
**Found**: 2026-04-21 (deep analysis post Apr-21 baseline run)

**Problem**: Three compounding defects prevent WEAK_CLOSE_EXIT from ever firing:

1. `breakout_legacy_h_carry_rule: bool = False` default makes `hold_quality_cols = []`,
   so `hold_quality_passed = True` for every trade regardless of actual H-filter value.
2. `to_vbt_config()` sets `respect_same_day_exit_metadata = (direction == LONG and self.breakout_legacy_h_carry_rule)`.
   This is always `False` (rule disabled) AND only covers LONG trades — shorts never get WEAK_CLOSE_EXIT
   even when the rule would fire.
3. In `_apply_hold_quality_carry_rule`, the short branch at line 2026–2034 unconditionally exits at
   close when H=False for shorts, regardless of whether the trade is profitable. A short position
   where close < entry (in profit, but close not near day low) should carry overnight with a
   breakeven stop, not exit immediately.

**Result**: `min(holding_days) = 1` across all 12,930 trades in Apr-21 runs; zero same-day exits.
Stocks that reverse on entry day and close below entry carry overnight, amplifying losses.

**Fix**:
- Add `h_carry_enabled: bool = True` to `BacktestParams` (replaces `breakout_legacy_h_carry_rule`)
- Wire `hold_quality_cols = ["filter_h"]` when `h_carry_enabled=True` for both strategies
- Fix `to_vbt_config()`: `respect_same_day_exit_metadata = self.h_carry_enabled`
- Fix short branch: only WEAK_CLOSE_EXIT when `close >= entry` (losing/flat); carry with BE stop when `close < entry` (profitable)
- Add `"h_carry_enabled": True` to `_ENGINE_DEFAULTS` in `backtest_presets.py`

---

### ISSUE-010 — FilterChecker.check_n() uses wrong candle direction for shorts (paper parity gap)

**Status**: ✅ FIXED — `filters.py` (2026-04-21)
**Severity**: Medium
**Found**: 2026-04-21 (code review)

**Problem**: `FilterChecker.check_n()` in `filters.py` checks `prev_close < prev_open` (RED candle)
for ALL trades. For **breakdown (short)** strategies, a red prior-day candle is CONTINUATION
(stock already falling — not a rest before further decline). The correct signal for shorts is
a **GREEN prior day** (`prev_close > prev_open`): failed rally exhaustion creates the ideal setup.

The **backtest SQL** in `strategy_families.py` already has the correct direction-specific logic:
- Long path: `prev_close < prev_open` (red = compression)
- Short path (default): `prev_close > prev_open` (green = exhaustion)

But `FilterChecker.check_n()` is used by paper trading scan code and always applies the LONG
(red candle) logic, creating a backtest-vs-paper parity gap for BREAKDOWN strategies.

**Note**: `BREAKDOWN_2PCT` uses `breakdown_filter_n_narrow_only=True`, which skips the candle check
entirely (narrow only), so this gap only affects `BREAKDOWN_4PCT` paper trades.

**Fix**: Add `is_short: bool = False` parameter to `FilterChecker.check_n()` and `check_all()`.
When `is_short=True`, check `prev_close > prev_open` instead of `prev_close < prev_open`.
Update all callers in paper trading and scan code.

---

### ISSUE-011 — H-filter close_pos threshold hardcoded in strategy SQL (not configurable)

**Status**: ✅ FIXED — `strategy_families.py`, `BacktestParams` (2026-04-21)
**Severity**: Low
**Found**: 2026-04-21 (code review)

**Problem**: The "close near high" threshold is hardcoded as `>= 0.70` for longs and `<= 0.30` for
shorts in `strategy_families.py` lines 130–131 and 371–372. To test a threshold of e.g. 0.65 or
0.75, a code change is required rather than a preset parameter change.

**Fix**: Add `h_filter_close_pos_threshold: float = 0.70` to `BacktestParams`. Pass this into the
strategy SQL as a bound parameter. Short threshold becomes `1.0 - h_filter_close_pos_threshold`.
Also thread into `FilterChecker` as `close_pos_threshold` (already accepted as a constructor param
at line 174 of `filters.py`, so this is a wiring fix only).

---

### ISSUE-012 — pnl_r column has corrupt aggregate values (divide-by-zero)

**Status**: ✅ FIXED — `vectorbt_engine.py` (2026-04-21) — guard added: `abs(initial_risk) < 0.01` stores `pnl_r = None`
**Severity**: Medium
**Found**: 2026-04-21 (data analysis — `avg(pnl_r)` returns values like 1.7e+12)

**Problem**: `avg(pnl_r)` over experiment trades returns astronomically large values (1.7e+12),
indicating Inf or NaN values stored in the `pnl_r` column. Root cause is likely divide-by-zero
when `initial_risk = entry_price - initial_stop` is near zero (stock with extremely tight stop).

**Impact**: Any dashboard card showing "avg R-multiple" or "% trades at 3R+" shows meaningless
numbers. The underlying trade P&L is unaffected (pnl_pct is a separate column computed correctly).

**Fix**:
1. Identify where `pnl_r` is written (likely `vectorbt_engine.py` or trade row construction).
2. Add guard: if `abs(initial_risk) < 1e-6`: store `pnl_r = None` (NULL).
3. In dashboard queries: add `WHERE pnl_r IS NOT NULL AND ABS(pnl_r) < 100` guard when aggregating.

---

### ISSUE-013 — Entry quality degrades significantly after 9:30–9:40 IST

**Status**: OPEN — needs backtest experiment to validate cutoff change
**Severity**: Medium (late entries drag down per-trade expectancy)
**Found**: 2026-04-21 (data analysis on Apr-21 runs, binned by 5-min entry slot)

**Findings (avg PnL per trade by entry time, Apr-21 baseline, breakout strategies)**:

| Entry Time | BO 4% (bps) | BO 4% WR | BO 2% (bps) | BO 2% WR |
|------------|-------------|----------|-------------|----------|
| 09:20      | 176.6       | 39.4%    | 144.9       | 36.9%    |
| 09:25      | ~140        | ~38%     | ~120        | ~35%     |
| 10:05      | 28.1        | —        | 64.1        | —        |
| 10:15      | 98.9        | —        | 44.2        | —        |

Current default `entry_cutoff_minutes = 60` (10:15 IST). An earlier note in `BacktestParams`
says 60min gave Calmar 27.06 vs 19.74 at 30min — but that analysis predates the causal admission
fix (Mar-31, 2026) which eliminated lookahead bias. The comparison needs to be re-run.

**Next steps**: Run two experiments per leg (30min cutoff vs 45min cutoff) and compare Calmar ratio
and total return vs Apr-21 baselines before changing the default.

---

### ISSUE-014 — Long-side lacks per-side tuning overrides (asymmetric vs short side)

**Status**: OPEN — enhancement request
**Severity**: Low (current long-side params are adequate; short-specific overrides already exist)
**Found**: 2026-04-21 (code review)

**Problem**: Short strategies have 6 dedicated per-side override params:
`short_trail_activation_pct`, `short_time_stop_days`, `short_max_stop_dist_pct`,
`short_abnormal_profit_pct`, `short_same_day_r_ladder_start_r`, `short_post_day3_buffer_pct`.
The long side has no equivalent — any tuning of trail %, time stop, or max stop applies to both
strategies. This limits the ability to tune breakout and breakdown independently when both run
under a shared `_ENGINE_DEFAULTS`.

**Proposed fix** (low priority — needed only when multi-leg tuning conflicts arise):
Add `long_trail_activation_pct`, `long_time_stop_days`, `long_max_stop_dist_pct` mirroring
the short-side override pattern. When set, override base param for LONG direction in
`to_vbt_config()`.

---

*Last updated: 2026-04-22*

---

### ISSUE-015 — BREAKDOWN_2PCT time stop was 5D (now corrected to 3D)

**Status**: ✅ FIXED — `backtest_presets.py` (current session)
**Severity**: Medium (paper-backtest parity gap; BREAKDOWN_2PCT canonical IDs invalidated)
**Found**: Current session (user review of `TIME_EXIT` hold durations)

**Problem**: `BREAKDOWN_2PCT` preset was inheriting `time_stop_days=5` from `_ENGINE_DEFAULTS`
instead of using the short-side 3D. `BREAKDOWN_4PCT` already had `short_time_stop_days=3`
explicitly, but `BREAKDOWN_2PCT` did not.

**Impact**: All BREAKDOWN_2PCT canonical experiment IDs run before this fix are **invalidated**:
- `937dfce553f20956` (Apr-20 v3, 2015–2026) — **INVALIDATED**
- `b0840fc1dc510cbf` (Apr-20 v2, 2025–2026) — **INVALIDATED**
- `b769984bf6d0c5c7` (Apr-21, 2015–2026) — **INVALIDATED**

**Fix**: Added `"short_time_stop_days": 3` to `BREAKDOWN_2PCT` overrides dict in `backtest_presets.py`.

**Required action**: Completed. Re-run `scripts/run_full_operating_point.py` produced the new BREAKDOWN_2PCT baseline `1f910e9069a508d2`.

---

### ISSUE-016 — Config split: BacktestParams and PaperStrategyConfig were manually synced

**Status**: ✅ FIXED — `paper_backtest_bridge.py`, `strategy_presets.py`, `duckdb_backtest_runner.py` (current session)
**Severity**: Medium (parity divergences accumulate each time a new knob lands in only one system)
**Found**: Current session (code review comparing CPR-pivot-lab vs NSE config model)

**Problem**: `BacktestParams` (40+ fields) and `PaperStrategyConfig` (~15 fields) were maintained
independently. Known divergences at time of fix:
- `entry_cutoff_minutes` default: BacktestParams=60, PaperStrategyConfig=**30** (parity bug)
- `entry_start_minutes=5` present in BacktestParams, **absent** in PaperStrategyConfig
- `trail_activation_pct=0.08`, `trail_stop_pct=0.02` present in BacktestParams, **hardcoded** in paper_runtime
- `short_trail_activation_pct=0.04` (BREAKDOWN_4PCT), **absent** in PaperStrategyConfig
- `h_filter_close_pos_threshold` in BacktestParams vs `h_filter_threshold` (old name) in paper

**Fix**: Bridge method + neutral adapter module:
1. Added `BacktestParams.to_paper_config(direction)` — pure method, no structural change (hash-safe)
2. New `services/paper/paper_backtest_bridge.py` with `build_paper_config_from_preset(preset_name, direction)`
3. Added 4 fields to `PaperStrategyConfig`: `trail_activation_pct`, `trail_stop_pct`, `short_trail_activation_pct`, `entry_start_minutes`
4. Fixed `entry_cutoff_minutes` default 30→60 in `PaperStrategyConfig`
5. Renamed `h_filter_threshold` → `h_filter_close_pos_threshold` with compat alias
6. Wired trail params and `entry_start_minutes` gate into `paper_runtime.evaluate_candle()` / `execute_entry()`

---

### ISSUE-017 — Live paper websocket launch gaps in ops layer

**Status**: ✅ FIXED — durable session lifecycle + feed transition dedup added
**Severity**: High (affects correctness and observability of live paper sessions)
**Found**: 2026-04-22 (preflight review before Kite websocket launch)

**Problem**: The live paper runtime needed CPR-style durability for session-level alerts and feed transitions:
- `SESSION_STARTED` / `SESSION_COMPLETED` / `SESSION_ERROR` needed DB-backed dedup so retries and restarts could not repeat lifecycle alerts.
- `FEED_STALE` / `FEED_RECOVERED` needed a durable transition marker so the watchdog and quiet-bar paths could not flood Telegram.
- `paper_v2.py` pause/resume commands needed to ignore no-op state transitions.

**Fix**:
- Added durable alert-log checks for session lifecycle alerts.
- Persisted feed transition state in `paper_feed_state.raw_state.alert_state`.
- Suppressed duplicate pause/resume alerts on no-op state transitions.
- Added tests covering feed stale/recovered dedup and session alert lookups.

---

### ISSUE-018 — Live candidate seeding queried the wrong feat_daily date column

**Status**: ✅ FIXED — `paper_runtime.py` (current session)
**Severity**: High (live sessions failed to seed candidates and immediately retried)
**Found**: 2026-04-22 (during live Kite websocket launch)

**Problem**: `seed_candidates_from_market_db()` queried `feat_daily.trading_date`, but the
`feat_daily` table in this repo exposes the trading day as `date`.

**Impact**: Both live 2% test sessions launched successfully, but candidate seeding failed on the
first pass with a DuckDB binder error, so the sessions did not reach normal live evaluation.

**Fix**: Switched the live seeding query to `WHERE date = CAST(? AS DATE)`.

**Required action**: Restart the live sessions so they pick up the corrected seeding query.

---

### ISSUE-019 — Live monitor output was buffered and grep pattern was too broad

**Status**: ✅ FIXED — monitoring docs updated (`CLAUDE.md`, `STATUS.md`)
**Severity**: Low (operator workflow / observability only)
**Found**: 2026-04-22 (during the background live paper test)

**Problem**: The live monitor recipe was missing from the repo docs, which made it easy to launch
long-running jobs without unbuffered output or a line-buffered log tail. The first grep pattern
was also too broad and matched routine Telegram HTTP chatter instead of the actual trade / bar /
error lines.

**Fix**: Documented the background launch recipe:
- `PYTHONUNBUFFERED=1`
- append stdout/stderr to `.tmp_logs/<run>.log`
- monitor with `tail -f ... | grep --line-buffered -E "<tight pattern>"`

**Required action**: Use the documented background launch + monitor recipe for all future live
paper tests.

---

### ISSUE-020 — Concurrent live sessions contend on the single DuckDB writer

**Status**: FIXED — multi-live runs breakout + breakdown in one writer process
**Severity**: High (prevents two independent live sessions from running concurrently)
**Found**: 2026-04-22 (during fresh breakout/breakdown relaunch after cleanup)

**Problem**: Starting `2lynchbreakout` and `2lynchbreakdown` as separate live processes on the
same `paper.duckdb` caused the second runner to retry on the DuckDB writer lock. The first runner
started cleanly, but the second cannot become healthy until the first releases the writer.

**Impact**:
- Only one live paper process can hold the writer cleanly at a time.
- A second concurrent launch spins in retry loops and risks alert spam / stale dashboard state.

**Resolution**: Use `nseml-paper multi-live` so breakout and breakdown share one `PaperDB`,
one websocket/feed adapter, and one alert dispatcher inside the same process.

**Operator usage**:
```bash
doppler run -- uv run nseml-paper multi-live \
  --strategy 2lynchbreakout \
  --strategy 2lynchbreakdown \
  --trade-date 2026-04-22
```

The single-session `daily-live` path remains available for debugging one strategy at a time.

---

### ISSUE-021 — Live feed alerts were keyed off empty poll cycles instead of real market-data gaps

**Status**: ✅ FIXED — tick-age stale detection + CPR-style feed alert policy
**Severity**: High (operator-alert noise during live sessions)
**Found**: 2026-04-22 (during live 2% breakout / breakdown dry runs)

**Problem**: The NSE live loop treated “no closed bars for 3 poll cycles” as `FEED_STALE`.
In a 5-minute bar engine that is wrong: during normal operation there can be several poll cycles
with no closed bar, especially right after session start. The result was false `FEED_STALE`
followed by `FEED_RECOVERED` chatter in Telegram.

**Fix**:
- Switched stale detection to tick age / heartbeat age instead of closed-bar cadence.
- Added CPR-style stale cooldown so feed oscillation does not repeatedly re-page Telegram.
- Kept `TICKER_HEALTH` in logs for agent monitoring instead of pushing that telemetry into chat.
- Upgraded feed alert bodies to include operator-useful context, including manual SL guidance for
  open positions on stale alerts.

**Operator outcome**: Telegram now carries feed transitions only when the market-data stream is
actually stale, not just because a 5-minute bar has not closed yet.

---

## Canonical Experiment IDs (2026-04-21)

Wave-1 fixes applied: H-carry rule enabled, entry gate at 09:20, filter direction parity, pnl_r guard.
Full 11-year window: `2015-01-01 → 2026-04-21`, universe 2000.

| Leg | Exp ID | Avg Annual | Max DD | Calmar | PF | Trades |
|-----|--------|-----------|--------|--------|----|--------|
| Breakout 4% | `f155489ee3422815` | +54.1% | 3.16% | 17.1 | 20.73 | 2,212 |
| Breakout 2% | `8e219692ea67b157` | +121.9% | 2.73% | 44.7 | 16.49 | 7,082 |
| Breakdown 4% | `f0cd849cf08f4fdc` | +3.1% | 0.74% | 4.2 | 5.51 | 258 |
| Breakdown 2% | `1f910e9069a508d2` | +8.2% | 1.90% | 4.3 | 5.47 | 790 |

All prior experiment IDs have been pruned from DuckDB. These four are the only active baselines.
See `docs/research/CANONICAL_REPORTING_RUNSET_2026-04-21.md` for the frozen report.
