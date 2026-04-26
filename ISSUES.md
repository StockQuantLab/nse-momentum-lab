# Known Issues

Track all open, fixed, and deferred issues here. Update status as issues are resolved.

Format: `[STATUS]` = `OPEN` | `FIXED` | `DEFERRED` | `INVESTIGATING`

---

## Replica Sync & Dashboard Lag (ported from cpr-pivot-lab 2026-04-24)

### ISSUE-044 — CLI `stop`/`pause`/`resume`/`archive`/`prepare` left dashboard stale (no force_sync)

**Status**: ✅ FIXED — `cli/paper_v2.py` (2026-04-24)
**Severity**: Medium (dashboard showed wrong session status after every manual CLI operation)
**Found**: 2026-04-24 — after `nseml-paper stop` + `nseml-paper archive`, dashboard still showed ACTIVE/PLANNED

**Problem**: Only `flatten` and `flatten-all` called `_sync_paper_replica_after_cli_write()`.
`stop`, `pause`, `resume`, `archive`, and `prepare` all wrote to DuckDB but never triggered
a replica sync. Dashboard lagged until the next automatic maybe_sync cycle (≤5s when live,
indefinite when no session is running).

**Fix**: Added `_sync_paper_replica_after_cli_write()` after every status-changing write in:
- `_cmd_stop` — COMPLETED transition
- `_cmd_pause` — PAUSED transition
- `_cmd_resume` — ACTIVE transition
- `_cmd_archive` — ARCHIVED transition
- `_cmd_prepare` — PLANNED creation (so both LONG and SHORT appear before live starts)

---

### ISSUE-045 — Replica debounce 5s too long; write bursts at bar-open lose final state write

**Status**: ✅ FIXED — `paper_live.py` (2026-04-24)
**Severity**: Low-Medium (dashboard showed stale P&L/status during high-frequency open bursts)
**Found**: 2026-04-24 — observed via replica lag warning 8908s; ported from cpr-pivot-lab fix

**Problem**: `min_interval_sec=5.0` in both `VersionedReplicaSync` constructors in `paper_live.py`.
When 4–10 positions open/close in rapid succession at bar-close, the debounce window absorbs
the final writes. Dashboard shows stale values until the next full 5s window.

**Fix**: Reduced `min_interval_sec` from `5.0` to `2.0` in both replica constructors
(single-session at line 465, multi-live at line 1142).

---

### ISSUE-052 — Dashboard truncated experiment run IDs to 12 characters

**Status**: ✅ FIXED — `apps/nicegui/state/__init__.py`, `apps/nicegui/pages/home.py`, `apps/nicegui/pages/strategy_analysis.py` (2026-04-25)
**Severity**: Medium (dashboard showed run IDs that did not match the full experiment IDs)
**Found**: 2026-04-25 — dashboard labels and tables displayed only `exp_id[:12]`, which made the run IDs in the UI look different from the actual experiment IDs used in baseline comparisons

**Problem**: Several NiceGUI views truncated `exp_id` to the first 12 characters:
- `build_experiment_options()` in `apps/nicegui/state/__init__.py`
- home dashboard KPI and recent experiments table in `apps/nicegui/pages/home.py`
- strategy analysis experiment table in `apps/nicegui/pages/strategy_analysis.py`

This made the dashboard display a shortened run ID that did not match the full IDs pasted from backtest results.

**Fix**: Display the full `exp_id` everywhere in the dashboard and added a regression test covering a long experiment ID.

---

### ISSUE-046 — Sentinel file flatten mechanism not implemented (CPR pattern missing)

**Status**: OPEN — deferred, operator convenience feature
**Severity**: Low (workaround: use `nseml-paper flatten --session <id>`)
**Found**: 2026-04-24 ported from cpr-pivot-lab

**Problem**: CPR pivot lab has a sentinel file mechanism:
`touch .tmp_logs/flatten_{session_id}.signal` triggers graceful flatten and session
completion from the live process on its next polling cycle — no separate CLI call needed.
In NSE lab, flatten must be done via a separate terminal.

**Fix needed**: In `paper_live.py` main loop, after each bar cycle check for
`Path(f".tmp_logs/flatten_{session_id}.signal")`. If present: flatten all positions,
mark COMPLETED, delete the signal file, exit cleanly.

---

### ISSUE-047 — `paper_ledger_v2.py` crashes with UnboundLocalError when archived session has no closed positions

**Status**: ✅ FIXED — `apps/nicegui/pages/paper_ledger_v2.py` (2026-04-24)
**Severity**: High (dashboard crash on archived sessions tab)
**Found**: 2026-04-24 live — `UnboundLocalError: cannot access local variable 'sorted_closed'`

**Problem**: `sorted_closed` was defined inside `if closed_positions:` block but referenced
outside it in the trade ledger loop (line 897) and equity curve loop (line 861). When an
archived session has no closed positions (e.g. sessions stopped before any fills), the variable
is never assigned and the page crashes.

**Fix**: Moved `sorted_closed` assignment above the `if` block with an empty-list default:
`sorted_closed = sorted(closed_positions, ...) if closed_positions else []`

---

### ISSUE-048 — Market Monitor regime classification lags single-day distribution events

**Status**: OPEN — design improvement, deferred
**Severity**: Low-Medium (regime says "aggressive long" on heavy distribution days; operator may over-commit)
**Found**: 2026-04-24 — 29↑ / 139↓ 4% moves, yet regime showed Bullish / Long Favored / Aggressive (2.0)

**Problem**: The regime classification (`_market_monitor_select_sql` in `market_db.py:1866-1906`)
uses two inputs:
1. `up_25q_count` vs `down_25q_count` (stocks 25% above 65-day low vs 25% below 65-day high) —
   a cumulative position metric with a 1.1× dominance threshold
2. `ratio_10d` — 10-day rolling sum of 4%↑ / 4%↓ stocks — with a 2.0 threshold for "long_favored"

On April 24, the cumulative position was overwhelmingly bullish (MA40 at 84%, MA20 at 96%,
10D BR at 3.76). Today's 139↓ vs 29↑ (0.21 ratio) was absorbed by the 10-day window without
triggering `correction_watch` (which requires `ratio_10d < 2.0`).

**Impact**: The regime is intentionally smooth (filters noise), but this means single-day
distribution events are invisible until they persist for multiple sessions. On a day with 4.8×
more stocks down 4% than up 4%, the "Aggressive" posture may mislead operators into taking
full-sized positions.

**Proposed fix** (deferred):
1. Add a **distribution day alert** when daily 4%↓ count exceeds 4%↑ count by ≥3×, regardless
   of regime — surfaced as a dashboard flag and Telegram alert (not a regime change)
2. Consider adding a `daily_4pct_ratio` input to the tactical regime logic so that extreme
   single-day imbalances trigger `correction_watch` even when `ratio_10d` is still above 2.0
3. Add the daily 4% ratio as a visible column in the market monitor history table

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

**Status**: PARTIALLY ADDRESSED — live/backtest gate narrowed to tradability blockers; raw DQ cleanup still pending
**Severity**: Medium (was Critical during Apr-23 baseline regression)
**Found**: 2026-04-19 (from DQ table scan)
**Regression**: 2026-04-23 — commit `d68038a3` added a severity-only DQ filter to the backtest universe, excluding 1,626/2,097 symbols

**Active issue counts in `data_quality_issues` (market.duckdb)**:
- `TIMESTAMP_INVALID`: 1,628 rows (1,626 distinct symbols)
- `OHLC_VIOLATION`: 218 rows

These are pre-existing and were not introduced by recent re-ingest. Most are minor timestamp
formatting issues in older (2015-2019) data that don't materially affect backtest P&L.

**What happened**: Commit `d68038a3` ("Harden paper/live alert and parity paths") added a
severity-based `NOT EXISTS` subquery on `data_quality_issues` to the backtest universe selection SQL
in `duckdb_backtest_runner.py`. Because `TIMESTAMP_INVALID` is stored as `CRITICAL`, this excluded
1,626/2,097 symbols (78%), reducing BREAKOUT_4PCT from 2,213 trades to just 167 (only 2025-2026
data had trades). The backtest engine already has its own data quality guards (price continuity
guard, `DATA_INVALIDATION` exit reason), so the broad severity filter was redundant at this layer.

**Fix**: Replaced the severity filter with a live-tradability gate shared by backtest, paper, and
live bootstrap. Only structural corruption codes are blocked:
`OHLC_VIOLATION`, `NULL_PRICE`, `ZERO_PRICE`, and `DUPLICATE_CANDLE`.

**Mitigation**:
- paper live/replay session bootstrap applies the same tradability gate before seeding candidates
- the raw DQ rows remain in `data_quality_issues` for follow-up cleanup
- `TIMESTAMP_INVALID`, `DATE_GAP`, `EXTREME_CANDLE`, `EXTREME_MOVE_DAILY`, `SHORT_HISTORY`, and
  `ZERO_VOLUME_DAY` remain advisory until their data windows are cleaned up

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

**Status**: ✅ RESOLVED — 2026-04-24 (full 11-year run completed)
**Severity**: Medium
**Found**: 2026-04-19

**Original counts (Apr-19 run, 2025-01-01 → 2026-04-19, ~15 months only)**:
- BREAKDOWN_4%: 81 trades, +0.84% total return
- BREAKDOWN_2%: 167 trades, +9.57% total return

**Resolution**: Full 11-year canonical backtest (2015-01-01 → 2026-04-23) completed with clean
data (ISSUE-001 fixed, causal admission applied, tradability-only DQ gate):
- BREAKDOWN_4%: **258 trades** — +3.1% avg annual, Calmar 4.2, PF 5.50
- BREAKDOWN_2%: **790 trades** — +8.2% avg annual, Calmar 4.3, PF 5.46

Trade count is statistically workable over 11 years (~23/yr for 4%, ~72/yr for 2%).
Short strategies are genuinely lower frequency than long (fewer clean breakdown setups in NSE).
The short-side admission filters are directionally correct; no relaxation needed at this time.

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

**Status**: ✅ DEFERRED — 2026-04-24 (data analysis shows no benefit from tightening cutoff)
**Severity**: Low (original concern not supported by canonical 11-year data)
**Found**: 2026-04-21 (data analysis on Apr-21 runs, binned by 5-min entry slot)

**Original concern**: Entry quality drops sharply after 9:30–9:40; earlier cutoff (30–45min)
might improve Calmar by eliminating low-quality late entries.

**Evidence from Apr-23 canonical baselines (2015–2026, 7,091 and 2,214 trades):**

BREAKOUT_2PCT per-slot avg return and win rate:

| Entry Time | Trades | Avg Return | Win Rate | % of Total P&L |
|------------|--------|-----------|---------|----------------|
| 09:20      | 2,315  | **2.51%** | 42.7%   | 32.6%          |
| 09:25      | 733    | 1.94%     | 38.6%   | 8.0%           |
| 09:30      | 660    | 2.08%     | 38.8%   | 7.7%           |
| 09:35      | 555    | 2.04%     | 37.7%   | 6.4%           |
| 09:40      | 470    | 1.93%     | 37.7%   | 5.1%           |
| 09:45      | 439    | 1.98%     | 39.9%   | 4.9%           |
| 09:50–10:15| 1,919  | 1.73–1.91%| 32–35%  | 35.3%          |

Cutoff scenario impact (what we lose by cutting early):

| Cutoff     | BO2% trades | Avg/trade | P&L kept | BO4% trades | Avg/trade | P&L kept |
|------------|-------------|-----------|----------|-------------|-----------|----------|
| 9:40 (25m) | 4,733       | 2.25%     | 71.3%    | 1,333       | 3.20%     | 64.4%    |
| 9:45 (30m) | 5,172       | 2.22%     | 77.2%    | 1,502       | 3.15%     | 71.4%    |
| 10:00 (45m)| 6,210       | 2.15%     | 89.4%    | 1,898       | 3.11%     | 89.3%    |
| **10:15**  | **7,091**   | 2.10%     | 100%     | **2,214**   | 2.99%     | 100%     |

**Conclusion**: All time slots are profitable. The degradation is a gradual slope (2.51% → 1.7%),
not a cliff. Cutting off at 9:45 would surrender 22-28% of total P&L while reducing trade count
by ~27%. Avg per-trade return improves modestly (+0.12-0.15%) but at the cost of significant
absolute P&L and diversification. There is no data evidence that early cutoff improves
Calmar — the later slots have consistent win rates (32-40%) and positive EV.

**Decision**: Keep `entry_cutoff_minutes = 60` (10:15 IST). The original Apr-21 analysis predated
the causal admission fix and used a much smaller dataset. The 11-year canonical baseline
shows late entries contribute real, consistent alpha. Revisit only if intraday slippage data
shows late entries suffer worse fill quality in live trading.

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

*Last updated: 2026-04-24*

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

**Required action**: Completed. Re-run `scripts/run_full_operating_point.py` produced the current full-history canonical baseline `be7958b0f79c3c1c`.

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

### ISSUE-022 — Live writer crashed on first closed bar due to `last_bar_ts` / `last_bar_at` mismatch

**Status**: ✅ FIXED — `paper_live.py` (2026-04-23)
**Severity**: High (both live sessions failed and retried at the first 5-minute close)
**Found**: 2026-04-23 09:20 IST during live breakout + breakdown monitoring

**Problem**: `run_live_session()` called `_write_feed_state(..., last_bar_ts=...)` even though the
helper signature had already been renamed to `last_bar_at`. The live process launched cleanly,
connected to Kite, and only failed once the first closed bar tried to persist feed heartbeat state.

**Observed symptom**:
- `TypeError: _write_feed_state() got an unexpected keyword argument 'last_bar_ts'`
- both sessions marked `FAILED`, then entered retry loops

**Fix**: Switched the closed-bar heartbeat path in `paper_live.py` to pass `last_bar_at=...`.

---

### ISSUE-023 — Live seed path used same-day daily rows, producing empty intraday watchlists

**Status**: ✅ FIXED — `paper_runtime.py` (2026-04-23)
**Severity**: High (sessions stayed live but produced zero candidates / zero trades)
**Found**: 2026-04-23 09:11–09:36 IST during live session bring-up

**Problem**: `seed_candidates_from_market_db()` reused a backtest candidate-query shape and then
filtered to `trading_date == trade_day`. Intraday, that effectively asked the live engine to seed
from the breakout/breakdown day's own daily row instead of the prior-day watch-date features.

**Observed symptom**:
- `seed_candidates_from_market_db: 0/2034 symbols seeded from feat_daily`
- sessions were connected and receiving ticks, but `_evaluate_entry()` skipped because `prev_close`
  was missing from empty setup rows

**Fix**: Replaced the live seed query with a prior-day watchlist bootstrap:
- resolve the latest `watch_date < trade_date`
- load `prev_close`, `prev_high`, `prev_low`, `prev_open`, volume/value-traded, and ranking inputs
  from that watch date
- run the existing breakout / breakdown ranking transforms on those watch-date rows

**Result after fix**: both Apr 23 live sessions seeded `1079/2034` symbols and began trading at
the next bar cycle.

---

### ISSUE-024 — Paper Ledger session selector labels could stay stale after status transitions

**Status**: ✅ FIXED — `apps/nicegui/pages/paper_ledger_v2.py` (2026-04-23)
**Severity**: Low (dashboard inconsistency only)
**Found**: 2026-04-23 after live sessions moved `PLANNED → ACTIVE`

**Problem**: The active-session dropdown options were populated once on page load, while the detail
panel re-queried the latest replica rows on refresh. This allowed the card body to show `ACTIVE`
while the selector label still displayed `PLANNED`.

**Fix**: Refresh the select options from the latest replica snapshot on each render/auto-refresh and
update the selected value if the available session set changes.

---

### ISSUE-025 — Paper Ledger positions view rendered raw HTML and underreported open P&L

**Status**: ✅ FIXED — `apps/nicegui/pages/paper_ledger_v2.py` (2026-04-23)
**Severity**: Medium (operator view scrambled; unrealized P&L / current mark visibility incorrect)
**Found**: 2026-04-23 during live monitoring

**Problems**:
1. Direction cells returned raw `<span ...>` HTML strings, but the shared table component rendered
   them as plain text.
2. `_parse_metadata()` returned `{}` when `metadata_json` was already a dict, dropping
   `last_mark_price`.
3. `_compute_pnl()` used `paper_positions.pnl` for open positions, but open-trade unrealized P&L
   must be derived from `last_mark_price`, `avg_entry`, `qty`, and `direction`.

**Fix**:
- render direction as clean text instead of raw HTML markup
- accept both dict and JSON-string forms of `metadata_json`
- compute unrealized P&L from mark price for `OPEN` positions
- format table numerics as readable display strings instead of raw float internals

---

### ISSUE-026 — Live Telegram trade alerts bypassed the rich HTML formatter layer

**Status**: ✅ FIXED — `alert_dispatcher.py`, `paper_session_driver.py` (2026-04-23)
**Severity**: Medium (alerts delivered, but operator detail/scan quality was much worse than CPR)
**Found**: 2026-04-23 after comparing NSE alerts with CPR `stockquantlab` alerts

**Problem**: The notifier module already had richer `format_trade_opened_alert()` and
`format_trade_closed_alert()` helpers, but `paper_session_driver.py` bypassed them and enqueued
minimal plain-text bodies like `symbol=... direction=... entry=... qty=...`.

**Fix**:
- route `TRADE_OPENED` / `TRADE_CLOSED` through the HTML formatter helpers
- include CPR-style details: entry, SL, qty, rupee risk / realized P&L, reason, time, session context
- normalize close labels such as `STOP_BREAKEVEN → BREAKEVEN_SL`
- keep the TradingView `NSE:<SYMBOL>` chart link in the Telegram body/button

**Note**: The current 2LYNCH live engine does not populate a deterministic `target_price`, so the
rich alert includes a target only if that field is present in the trade result.

---

### ISSUE-027 — Windows restart flow can leave orphaned Python child processes holding `paper.duckdb`

**Status**: ✅ FIXED — CLI startup now clears stale writer processes on lock detection
**Severity**: Medium (restart friction; patched writer cannot relaunch until lock is cleared)
**Found**: 2026-04-23 during repeated live-writer restarts

**Problem**: Stopping the background `pwsh` parent for `nseml-paper multi-live` can leave an
orphaned `python.exe` child in `Not Responding` state. That child continues holding the DuckDB
writer lock, so the next launch fails with:

`IO Error: Cannot open file "data/paper.duckdb": The process cannot access the file because it is being used by another process.`

**Fix**: On CLI startup, if the paper DB is locked by a stale previous writer, the entrypoint now
uses a best-effort Windows process scan to terminate orphaned `nseml-paper` / `paper_live`
processes before retrying the DB open once.

**Tracking goal**: still worth replacing this with explicit writer ownership / PID-file semantics
later, but the manual orphan-cleanup loop is no longer required for normal restarts.

---

### ISSUE-028 — Paper session P&L is gross, while operators expect net-of-costs and Zerodha-style charges

**Status**: ✅ FIXED — session aggregates and alerts now use net-of-modeled-fees math
**Severity**: Medium (operator interpretation and paper-vs-live economics gap)
**Found**: 2026-04-23 after breakeven exits alerted as `₹0 / 0.00%`

**Problem**:
- `paper_positions.pnl` stores gross trade P&L for auditability.
- Session realized P&L used by dashboards, summaries, and risk logic was previously computed from
  gross price movement only.
- The current fill fee model is a simple `0.1%` per side approximation, not a true Zerodha brokerage +
  statutory charge model.

**Observed symptom**:
- breakeven exits alert as `₹0` gross even though the operator correctly expects a small rupee loss
  after costs
- dashboard/session P&L can overstate profitability relative to realistic broker net

**Fix**:
- `PaperDB.get_session_realized_pnl()` now returns net realized P&L using the modeled entry/exit
  fee approximation
- daily summary, risk controls, API session views, and EOD carry logic now inherit that net
  realized value
- trade-close alerts continue to display net rupee loss and net return percentage for fee-only exits

**Tracking goal**:
1. Decide on a canonical cost model for NSE paper/backtest (`flat approx` vs `Zerodha-equivalent`)
2. Thread that cost model into backtest and any future brokerage-equivalent regression pass
3. Keep gross move % and net rupee P&L distinct in operator messages

---

### ISSUE-029 — `GAP_THROUGH_STOP` could fire on same-day intraday bars instead of only overnight gaps

**Status**: ✅ FIXED — `paper_runtime.py` (2026-04-23)
**Severity**: Medium (exit-reason classification bug; can distort live diagnostics and alert semantics)
**Found**: 2026-04-23 after same-day trades were marked `GAP_THROUGH_STOP`

**Problem**: `_advance_open_position()` checked `open_px` vs stop on every processed 5-minute bar.
That means a position opened earlier the same day could later be classified as `GAP_THROUGH_STOP`
if a subsequent intraday candle opened beyond the stop. This is not an overnight gap; it is just
an intraday stop event.

**Why this is wrong**: For a position opened on the current trade date, a true gap-through-stop can
only happen on the next trading session's opening bar after the position is carried overnight.

**Fix**: Restrict `GAP_THROUGH_STOP` classification to:
- overnight-carried positions (`days_held > 0`)
- the first processed bar for that symbol in the current session

All other stop breaches now fall through to the normal stop-hit path (`STOP_INITIAL`,
`STOP_BREAKEVEN`, `STOP_TRAIL`) instead of being mislabeled as an overnight gap.

---

### ISSUE-030 — Manual `flatten` lacks CPR-style operator alerts and daily summary behavior

**Status**: ✅ FIXED — CLI flatten path patched on 2026-04-23
**Severity**: Medium (manual intervention works on DB state, but operator observability is incomplete)
**Found**: 2026-04-23 while testing manual flatten on active live sessions

**Problem**: `nseml-paper flatten` currently:
- closes open positions using the latest persisted marks
- marks the session `PAUSED`

but it does **not**:
- emit a CPR-style flatten/session alert
- dispatch `DAILY_PNL_SUMMARY`
- provide a clear operator message/contract that this is a manual liquidation event

**Impact**: During manual intervention, the database state changes correctly, but Telegram/email
operators do not get the same rich lifecycle/scorecard signals they would expect from CPR.

**Fix**:
1. `nseml-paper flatten` and `flatten-all` now dispatch a rich manual-flatten alert
2. the CLI path now also enqueues `DAILY_PNL_SUMMARY`
3. both paths force-sync the versioned paper replica after writes so the dashboard catches up

---

### ISSUE-031 — Manual `flatten` changed `paper.duckdb` but did not sync the versioned paper replica

**Status**: ✅ FIXED — CLI replica sync added on 2026-04-23
**Severity**: Medium (dashboard could remain stale immediately after manual intervention)
**Found**: 2026-04-23 after flattening both live sessions and seeing replica/dashboard lag

**Problem**: `nseml-paper flatten` updated `paper.duckdb` directly, but unlike the live/replay writers
it did not force a `VersionedReplicaSync` refresh. That left `paper_replica_v*.duckdb` and the
dashboard pointer stale until some later writer action happened.

**Fix**: The CLI flatten commands now force-sync the paper replica after database writes and alert-log
entries, so manual flatten state is visible in the dashboard immediately.

---

### ISSUE-032 — Live/session daily summary path depended on a non-existent `PaperDB.list_positions()` API

**Status**: ✅ FIXED — shared summary helper introduced on 2026-04-23
**Severity**: High (daily summary dispatch could fail at session finalization)
**Found**: 2026-04-23 during manual-flatten and EOD-summary audit

**Problem**: `paper_live.py`'s `_dispatch_daily_pnl_summary()` called `paper_db.list_positions(session_id)`,
but `PaperDB` only exposes `list_positions_by_session()`. That means the summary path could raise
at session finalization and silently suppress the operator's expected EOD summary.

**Fix**: Daily summary computation now lives in shared notifier helper code and uses the correct
session-position API for both live and CLI/manual-flatten flows.

---

### ISSUE-033 — Alert retries were too short for transient Telegram/network failures

**Status**: ✅ FIXED — `alert_dispatcher.py` (2026-04-23)
**Severity**: Medium (transient DNS / 429 / timeout issues could drop operator alerts)
**Found**: 2026-04-23 while comparing CPR alert hardening against NSE paper live

**Problem**: The alert dispatcher retried every failure only 3 times with a 1s/2s/4s backoff.
That was enough for trivial hiccups, but not for short DNS or Telegram outages.

**Fix**: The retry policy now:
- recognizes transient delivery failures such as `httpx.RequestError`, Telegram 429/5xx, and
  common network timeout / DNS messages
- retries them on a longer schedule: `1s, 2s, 4s, 30s, 120s, 300s`
- leaves permanent failures as terminal so misconfiguration does not spin for minutes

---

### ISSUE-034 — `FEED_STALE` / `FEED_RECOVERED` alerts were still plain text instead of CPR-style rich HTML

**Status**: ✅ FIXED — `paper_live.py` (2026-04-23)
**Severity**: Low-Medium (alerts were delivered, but operator scan quality was poor)
**Found**: 2026-04-23 while checking CPR-style feed alert parity

**Problem**: Trade open/close alerts already used the rich HTML formatter layer, but feed
stale/recovered transitions still built a log-like body string:

`transport=websocket streak=3 last_tick=...`

That was readable, but it was not on the same operator-friendly level as the CPR alerts.

**Fix**:
- reworked the live feed transition body into HTML with clear headings and fields
- added session context, last-tick age, IST timestamps, and open-position blocks
- formatted open positions as CPR-style rows with entry, SL, target, qty, and risk
- changed the Telegram subject to a richer `⚠️ Feed Stale — ...` / `✅ Feed Recovered — ...` form

---

### ISSUE-035 — DQ universe gate was overblocking `TIMESTAMP_INVALID` and shrinking the backtest universe

**Status**: ✅ FIXED — `market_db.py`, `paper_v2.py`, `paper_runtime.py`, `duckdb_backtest_runner.py`, `data_hygiene.py` (2026-04-23)
**Severity**: High (live/backtest parity regression; overfiltered universe changed run comparability)
**Found**: 2026-04-23 after comparing DQ-filtered vs reverted canonical runs

**Problem**: The universe gate used active `CRITICAL/HIGH` DQ severity as a hard block. That
treated `TIMESTAMP_INVALID` as trade-blocking even though it is mostly an older timestamp hygiene
signal, not a live tradability blocker. Because `TIMESTAMP_INVALID` covered most of the liquid
universe, the gate excluded far too many symbols and made the backtest/live universe diverge.

**Fix**:
- changed the live/backtest universe gate to block only true live-tradability corruption codes:
  `OHLC_VIOLATION`, `NULL_PRICE`, `ZERO_PRICE`, and `DUPLICATE_CANDLE`
- corrected the paper bootstrap symbol query to interpolate the live-blocking code list properly
- left `TIMESTAMP_INVALID`, `DATE_GAP`, `EXTREME_CANDLE`, `EXTREME_MOVE_DAILY`, `SHORT_HISTORY`,
  and `ZERO_VOLUME_DAY` as advisory DQ signals for reporting and cleanup
- updated the trade-date readiness gate to fail only on live-blocking DQ codes

**Result**: live and backtest now share the same tradability filter instead of a severity-only
filter, so DQ hygiene no longer removes most of the universe by accident.

---

### ISSUE-040 — `nseml-paper stop` via CLI leaves open positions stranded — no auto-flatten

**Status**: OPEN — deferred, operator workflow gap
**Severity**: Medium (positions left in OPEN state with no closed_at; PnL never realized in DB)
**Found**: 2026-04-24 — archived both sessions after CLI stop without flatten; 10 positions stranded

**Problem**: `nseml-paper stop --session <id>` marks the session COMPLETED in DuckDB immediately.
It does not check for open positions or auto-flatten them. If the operator skips the `flatten`
step before `stop`, positions remain in state=OPEN with closed_at=NULL indefinitely.

Stranded today: DATAPATTNS, HCLTECH, KHAICHEM, ADANIENSOL, FINOPB, STANLEY, JKTYRE (shorts) +
BLUESTONE, PAISALO, LLOYDSENGG (longs) — all from 2026-04-24 sessions a8412eed / 7169807d.

**Correct operator flow**: `flatten → stop → archive`
**Fix options** (deferred):
1. Add a pre-flight check in `stop` that refuses if open positions exist (unless `--force` flag)
2. Or auto-flatten at last-known mark prices before marking COMPLETED
3. Add a `list-stranded` CLI command to surface orphaned positions across sessions

---

### ISSUE-041 — EOD/daily summary alert not delivered when session stopped via CLI

**Status**: OPEN — deferred
**Severity**: Medium (operator gets no end-of-day P&L summary on Telegram)
**Found**: 2026-04-24 — alert_log shows 0 DAILY_PNL_SUMMARY or SESSION_COMPLETED entries

**Problem**: `DAILY_PNL_SUMMARY` is dispatched by the live process itself at line 933 of
`paper_live.py`, triggered when the process's internal loop detects session status=COMPLETED.
When the operator uses `nseml-paper stop` CLI to stop the session and immediately archives it,
the live process either:
1. Has not yet polled the DB for the COMPLETED status, or
2. Has already exited (if the process was killed externally)

In either case the cleanup path (SESSION_COMPLETED alert + DAILY_PNL_SUMMARY) is bypassed.
74 alerts were logged today (41 TRADE_OPENED + 31 SL_HIT + 2 SESSION_STARTED) — zero EOD summaries.

**Fix needed**: Add a standalone CLI command `nseml-paper send-summary --session <id>` that
computes and dispatches DAILY_PNL_SUMMARY + SESSION_COMPLETED alert directly from DuckDB, without
requiring the live process. This makes the EOD summary operator-triggerable independently of how
the session was terminated.

---

### ISSUE-036 — Session-started alert sends plain text body instead of HTML

**Status**: ✅ FIXED — `paper_live.py` (2026-04-24)
**Severity**: Low (alerts are delivered, but unformatted in Telegram)
**Found**: 2026-04-24 live session — first session-start alerts observed as plain text

**Problem**: `paper_live.py` dispatches `AlertType.SESSION_STARTED` with a plain-text body:

```python
body=f"Strategy: {strategy}\nSymbols: {len(symbols)}\nDate: {trade_date}"
```

The Telegram notifier always sends with `parse_mode: HTML`, so all other alerts (trade open/close,
feed stale/recovered) use proper `<code>`, `<b>`, `<i>` HTML tags. The session-started body is
plain text — no crash, but it renders without any formatting in Telegram.

**Fix**: Replaced the inline plain-text body with an HTML-formatted card using `<b>`, `<code>`,
and emoji labels — matching the style of `format_trade_opened_alert`. Also escaped the strategy
name in the subject line (`escape(strategy)`). Updated `paper_live.py:519-526`.

**Workaround (pre-fix)**: None — alerts arrived but looked like raw log lines.

---

### ISSUE-039 — Paper engine promotes breakeven stop on every 5-min bar; backtest only on daily close (parity gap)

**Status**: ✅ FIXED — `paper_runtime.py` + `intraday_execution.py` (2026-04-24)
**Severity**: High (paper systematically underperforms backtest on choppy/volatile days)
**Found**: 2026-04-24 live session — 15+ STOP_BREAKEVEN exits with pnl=0 on a -0.75% Nifty day where shorts should have profited

**Root cause**:

`paper_runtime.py:260-264` promotes the breakeven stop on **every 5-minute bar close**:
```python
if direction == "SHORT" and close < entry_price and stop_level > entry_price:
    stop_level = entry_price   # fires on any 5-min bar that closes below entry
```

`vectorbt_engine.py:583-585` promotes the breakeven stop only on the **daily close**:
```python
if not at_breakeven and float(close) < float(entry_price):
    stop_level = min(stop_level, float(entry_price))
    at_breakeven = True        # fires only at end-of-day
```

The backtest iterates one full trading day per loop. Paper iterates one 5-minute bar per loop.
A trade that dips 1 paisa in favour on a single 5-min bar gets its stop promoted to breakeven
immediately. Any intraday bounce then exits it at zero. The backtest would have held through the
same noise and only evaluated at day-close.

**Impact observed on 2026-04-24**:
- 15+ STOP_BREAKEVEN exits (pnl=0) across both sessions
- Zero profitable exits across the full morning session
- Affected shorts: CIPLA, LTM, STLTECH, LEMONTREE, ATHERENERG, SONATSOFTW, WAAREEENER, TCS, IKS, TDPOWERSYS, etc.
- On a -0.75% Nifty day, the BREAKDOWN shorts should have generated realized profit

**Fix (implemented, uncommitted)**:
1. Removed the intraday B/E promotion block entirely from `paper_runtime.py:_advance_open_position()`.
   Backtest doesn't promote B/E intraday — paper shouldn't either. B/E promotion happens at EOD via the
   H-carry step, not on every 5-min bar.
2. Created `evaluate_held_position_bar()` in `intraday_execution.py` — canonical shared bar evaluator
   called by both paper-live and paper-replay for already-open (held) positions. Handles post-day-3
   tightening, gap-through (first bar, overnight carries only), stop hit, and trail activation.
   NO intraday B/E promotion. Paper_runtime._advance_open_position now delegates to this function.
3. `evaluate_held_position_bar()` is the foundation for ISSUE-042 (port backtest hold-days to 5-min bars).

---

### ISSUE-042 — Backtest hold-days use daily bars; live sees 5-min bars — stop timing diverges

**Status**: OPEN — deferred (architecture work)
**Severity**: Medium (backtest overstates hold-day P&L; misses intraday stop-outs on hold days 2-5)
**Found**: 2026-04-24 architecture review

**Problem**: The backtest processes hold days (Day 2 to time_stop_days) using daily OHLCV in
`vectorbt_engine.py`. It checks stop hit using the day's HIGH/LOW, which correctly detects if the
stop was touched — but it cannot know at what intraday time the stop fired, nor whether price
recovered by EOD. The paper engine (and live trading) processes every 5-min bar and exits
immediately when the stop is touched intraday.

On a volatile day a position might touch the stop at 09:35, bounce back, and close above entry —
backtest: carry to next day; paper: stopped out at 09:35.

**Fix**: Make the backtest hold-day loop use 5-min candles via `evaluate_held_position_bar()`
(already in `intraday_execution.py`). Requires loading 5-min data for hold-day symbols, which
is a performance-sensitive change on an 11-year × 2000-symbol dataset.

---

### ISSUE-043 — Partial profit exit (80/20 scale-out) on abnormal move not implemented

**Status**: OPEN — feature request, deferred
**Severity**: Low-Medium (missed opportunity to lock profits on big moves)
**Found**: 2026-04-24 strategy design discussion

**Requested rule**: Within the first hour of entry (09:20–10:20), if the position moves
`abnormal_profit_pct` in favour (e.g. +8% for LONG, -8% for SHORT on a 5-min bar):
- Exit 80% of qty at that price (lock the profit)
- Keep remaining 20% running with stop promoted to entry (breakeven stop)

**Design requirements**:
1. `evaluate_held_position_bar()` in `intraday_execution.py` needs an `abnormal_profit_pct`
   parameter and partial-exit return type (qty_to_exit, qty_to_keep, tight_stop)
2. PaperDB position model needs to support partial close (reduce qty, realize partial PnL)
3. Backtest `_simulate_same_day_stop_execution()` needs the same rule for backtesting parity
4. Alert: PARTIAL_EXIT alert type for Telegram

**Implementation status**: Prototype built and backtested on 2026-04-24. See ISSUE-050 for
full details and the regression finding that caused it to be disabled in presets.

---

### ISSUE-038 — TICKER_HEALTH coverage denominator shrinks during quiet market periods, making stale=0 misleading

**Status**: OPEN — deferred
**Severity**: Low (no trade impact; misleads operator monitoring)
**Found**: 2026-04-24 live session — 10:20–10:30 IST coverage collapsed (1210→376→27→7 symbols)

**Problem**: The `coverage=X% (n/n)` metric in `TICKER_HEALTH` uses a rolling window of
recently-active symbols as both numerator and denominator. When the market quiets (e.g. 10:20–10:30
IST), most symbols stop ticking and drop out of the window. The denominator collapses to single
digits and `stale=0` — falsely signalling perfect health while 1200+ subscribed symbols have
received no ticks for minutes.

Observed sequence: `(1220/1220)` at 10:10 → `(376/376)` at 10:20 → `(27/27)` at 10:25 →
`(7/7) stale=0` at 10:30. Total ticks kept growing (+88K/bar) so the feed was live throughout.

**Fix needed**: Compute `stale` as count of symbols in the *fixed subscription list* (all 1224)
that exceed the stale-tick threshold, not just those in the rolling active window. This gives a
true "X of 1224 subs have gone quiet" signal rather than "all currently-active symbols look fine."

---

### ISSUE-037 — Position book saturation silently drops valid intraday signals

**Status**: OPEN — deferred, review risk params
**Severity**: Low-Medium (valid signals skipped; no crash, no alert)
**Found**: 2026-04-24 live session — recurring `execute_entry: no cash for <SYMBOL> qty=0` warnings

**Problem**: Once the position book reaches `max_positions` or deploys all capital, new signals that
pass all filters are silently skipped with a WARNING log. Affected symbols observed: EDELWEISS,
RVHL, PPLPHARMA, INDOSTAR, DIGJAMLMTD, COALINDIA (and others). No Telegram alert is sent, so the
operator has no visibility that signals are being dropped.

**Impact**: On high-signal days (bearish open with many breakdown triggers), the book fills in the
first 1–2 bars and subsequent higher-quality signals (later in the session when volatility settles)
cannot enter. This may reduce strategy performance vs backtest, where capital recycles faster.

**Fix options** (deferred):
1. Add a Telegram alert when signals are dropped due to capital exhaustion (at most once per bar)
2. Review `max_positions` and `portfolio_value` in `BREAKOUT_2PCT` / `BREAKDOWN_2PCT` presets
3. Track `signals_dropped_no_cash` counter in session stats for post-session analysis

---

### ISSUE-049 — DQ filter in `_get_liquid_symbols` blocks 780 liquid symbols, reducing BREAKOUT_4PCT from 2,214 → 1,034 trades

**Status**: ✅ FIXED — DQ universe exclusion removed; Apr-24 baselines established
**Severity**: High (backtest universe regression; canonical baselines no longer reproducible)
**Found**: 2026-04-24 (regression investigation during this session)

**Root cause**: The DQ filter added to `_get_liquid_symbols` in `duckdb_backtest_runner.py` (commit `d68038a3`, Apr 23) uses `LIVE_BLOCKING_DQ_CODES` (`OHLC_VIOLATION`, `NULL_PRICE`, `ZERO_PRICE`, `DUPLICATE_CANDLE`) to exclude symbols from the 11-year universe. The `ZERO_PRICE` code flags 655 of the top-2000 liquid NSE symbols (pre-market or auction candles with zero price in 5-min data), and `OHLC_VIOLATION` flags 212. Together, 780 of the 2,000 most liquid stocks are excluded, replaced by far less liquid symbols with fewer breakout setups.

**Confirmed by query** (2024 liquidity window):
```
ZERO_PRICE:    655 symbols blocked
OHLC_VIOLATION: 212 symbols blocked (some overlap)
Total blocked:  780 unique symbols in top-2000
```

**Impact**: The Apr-24 runs now reproduce the canonical behavior plus only the extra-day delta:
- BREAKOUT_4PCT: 2,217 trades vs 2,214 canonical (`+3`)
- BREAKOUT_2PCT: 7,097 trades vs 7,091 canonical (`+6`)
- BREAKDOWN_4PCT: 258 trades vs 258 canonical (`0`)
- BREAKDOWN_2PCT: 792 trades vs 790 canonical (`+2`)

**Fix**: Removed the DQ universe filter from `_get_liquid_symbols` and from live candidate seeding. Trading paths now rely on the strategy's own universe filters (`min_price`, `min_volume`, `min_value_traded_inr`) plus trade-level invalid-data guards.

**Verification**: Apr-24 rerun complete. The four new baseline experiments are now the canonical set.

**The partial-exit agent changes (uncommitted) are NOT the cause** — tested both committed and uncommitted; trade counts are identical (1,034).

---

### ISSUE-050 — Partial exit (80/20) feature implemented but disabled — backtested as regression at 0.20

**Status**: OPEN — code in place, disabled in presets
**Severity**: Low (feature exists, param defaults to None/disabled)
**Found**: 2026-04-24 (prototyped and backtested in this session)

**What was built**:
- `same_day_partial_exit_pct` and `same_day_partial_exit_carry_stop_pct` added to `BacktestParams` and `PaperStrategyConfig`
- `_simulate_same_day_stop_execution()` in `intraday_execution.py` checks for large intraday move on entry day and exits 80% at target, carries 20% with tight stop
- `evaluate_held_position_bar()` also supports partial exit path (`is_entry_day=True`)
- `PaperDB.partial_close_position()` and `SessionPositionTracker.partial_close()` implement DB-level partial close
- `paper_session_driver.py` handles `PARTIAL_EXIT` action

**Why disabled**: Backtested at `same_day_partial_exit_pct=0.20` (the gap-through threshold used as the partial-exit trigger). All 4 presets showed ~40-50% annualized return reduction — the partial exit amputates fat-tail winning trades before they can run. The same_day_r_ladder already handles progressive stop-tightening on intraday winners without capping upside.

**Decision**: `same_day_partial_exit_pct` intentionally omitted from `_ENGINE_DEFAULTS` in `backtest_presets.py`. Feature stays in code for future research at different thresholds (e.g., 0.30 or time-gated).

---

### ISSUE-051 — Phase 1 shared entry helper changed canonical baseline trade rows

**Status**: OPEN — investigate shared_eval / sizing drift
**Severity**: High (post-refactor baseline drift; trade-level parity not yet proven)
**Found**: 2026-04-25 during before-vs-after Phase 1 baseline comparison

**Observed deltas vs pre-Phase-1 runs**:
- BREAKOUT_4PCT: 3 removed, 1 added, 22 changed
- BREAKOUT_2PCT: 7 removed, 1 added, 44 changed
- BREAKDOWN_4PCT: 4 removed, 0 added, 3 changed
- BREAKDOWN_2PCT: 0 removed, 0 added, 0 changed

**Notable pattern**:
- Many changed rows are not just the final 2026 bar; they span 2015-2026.
- A large subset of BREAKOUT rows changed `initial_stop`, `pnl_r`, `qty`, `position_value`, and net/gross P&L.
- BREAKDOWN_2PCT stayed identical, so the drift is not universal.

**Current hypothesis**:
The new `shared_eval.evaluate_entry_trigger()` wiring is behaviorally close but not yet fully identical to the old per-engine entry logic for all baseline paths. The discrepancy may be in the entry trigger / stop derivation path, the sizing interaction, or a downstream field update in the refactor.

**Action**:
Compare pre/post `bt_trade` rows trade-by-trade and isolate the first field divergence for each affected symbol/date pair before promoting Phase 1 to canonical.

**Cleanup update (2026-04-26)**: All pre-Phase-1 and post-Phase-1 comparison runs (8 total) pruned from `data/backtest.duckdb`. New post-ISSUE-055 canonical IDs are now the only experiments in the DB:
- `6565aa5698186b01` (BREAKOUT_4%)
- `874515a0c02ba7ee` (BREAKOUT_2%)
- `a2f4063613d259b3` (BREAKDOWN_4%)
- `9a5ed7575f68613a` (BREAKDOWN_2%)

Zero regression confirmed against post-Phase-1 set. Window: 2015-01-01 → 2026-04-24, universe 2000.

---

### ISSUE-053 — Portfolio sizing is fixed-notional, not true risk-per-trade

**Status**: OPEN — risk-management review needed
**Severity**: Medium (design mismatch; may understate or misstate intended per-trade risk)
**Found**: 2026-04-25 during Phase 1 regression review

**Observation**: The current sizing model uses a `Rs 10L` portfolio base and roughly `10%` notional per slot (`max_positions=10`, `max_position_pct=0.10`). The `risk_per_trade_pct=0.01` config exists, but it does not drive position sizing in the canonical path. With an `8%` long stop cap, the worst-case loss is about `0.8%` of portfolio per full slot, not `1.0%`.

**Why this matters**: This is a fixed-slot allocation model, not a strict percentage-risk model. That may be fine by design, but it should be stated explicitly and reviewed because the current code/comments can be read as if risk is capped at 1% per trade.

**Action**: Revisit the risk framework later and decide whether:
- the strategy should remain fixed-slot notional sizing, or
- the code should size to a true risk budget derived from stop distance.

---

### ISSUE-054 — Short H-carry / breakdown exit path is fragile and needs dedicated regression coverage

**Status**: OPEN — regression tests added; carry/exit consolidation still pending (ISSUE-055)
**Severity**: Medium (breakdown exits changed on canonical rows after Phase 1)
**Found**: 2026-04-25 during Phase 1 postmortem

**Observation**: In the breakdown path, short trades with `hold_quality_passed=True` go through the H-carry rule and preserve a carried stop into the next session rather than exiting at the same-day close. On the Phase 1 comparison, `CENTRALBK` (`2018-10-09`) and `ADANIGREEN` (`2023-02-27`) both changed exit price even though entry date and initial stop were stable.

**Root cause confirmed (2026-04-26)**: Pre-Phase-1 backtest applied the LONG breakeven clamp (`max`) to SHORT trades. For a SHORT, breakeven means the stop must be ≤ entry price. Using `max` instead of `min` pushed the carry stop above entry for profitable shorts — in ADANIGREEN's case above the initial loss stop itself (485.30 > 472.25 > entry 462.20), causing the position to exit at a large loss. Post-Phase-1 `evaluate_hold_quality_carry_rule(is_short=True)` correctly uses `min(carry_stop, entry_price)`.

**Caveat**: The pre-Phase-1 carry_stop values (e.g. 485.30 for ADANIGREEN) are **inferred from exit prices**, not stored in the pre-Phase-1 DB rows. `carry_stop_next_session` was not persisted before the ISSUE-057 traceability fix. The post-Phase-1 rows correctly store the clamped carry stop. The inference is consistent but cannot be verified column-by-column from the old experiments.

**Why this matters**: The short carry path is path-sensitive and can change next-day exits materially without any visible entry-price change. That makes it harder to reason about baseline drift and easier to regress accidentally during refactors.

**Progress**:
- Two regression tests added to `tests/unit/services/paper/test_parity.py` (Layer 1c):
  - `test_short_carry_regression_centralbk_2018`: verifies carry_stop clamps to entry_price (30.20) for profitable SHORT.
  - `test_short_carry_regression_adanigreen_2023`: verifies carry_stop input above entry_price (485.30 > 462.20) is clamped to entry_price, preventing the wrong-direction clamp from blowing through the loss stop.

**Remaining**: Same-day stop execution and next-session stop application are still split across engine-specific paths (backtest vs. paper). The re-check of H-carry aggression and the shared carry/exit helper extraction are tracked in ISSUE-055.

---

### ISSUE-055 — Backtest and paper/live still do not share one carry/exit engine

**Status**: OPEN — partially resolved (H-filter extraction complete; same-day stop consolidation deferred)
**Severity**: Medium (H-filter now tested for parity; residual risk is batch-vs-streaming same-day stop)
**Found**: 2026-04-25 during H-carry / breakdown parity review
**Updated**: 2026-05-09 after code archaeology and partial fix

**Observation**: Entry admission was partially unified in Phase 1, but the carry/exit path is still split across multiple runners:
- backtest carry logic lives in `duckdb_backtest_runner.py` plus `vectorbt_engine.py`
- paper/live EOD carry logic lives in `paper_eod_carry.py`
- same-day stop execution still depends on the engine-specific runner path

**What was completed in this session**:

1. **H-filter computation extracted** — `compute_h_filter_passed()` added to `shared_eval.py`.
   Paper EOD carry now calls this function instead of the inline 8-line block. No behavior change.
   - LONG: `close_pos >= threshold` (0.70 default)
   - SHORT: `close_pos <= (1 - threshold)` (0.30 for default threshold)
   - `None` → False (parity with backtest `filters.py check_h()`)
   - `h_carry_enabled=False` → always True

2. **Backtest SHORT H-filter confirmed direction-aware** — investigation found the breakdown candidate
   SQL in `strategy_families.py` (line 374) already builds `filter_h = (signal_close_pos_in_range <= h_threshold_short)`
   where `h_threshold_short = round(1 - h_filter_close_pos_threshold, 6)`. This is direction-correct and
   matches the Python helper. No code change needed on the backtest side.

3. **Dead code removed** — `_apply_hold_quality_carry_rule()` static method in `duckdb_backtest_runner.py`
   (lines 2081-2114) was a compatibility wrapper with no callers. Deleted.

4. **Tests added** — `TestComputeHFilterPassed` in `test_parity.py` (14 tests) verifies:
   - LONG/SHORT pass/fail/boundary
   - None → False (both directions)
   - `h_carry_enabled=False` → True (override path)
   - Non-default threshold
   - Backtest SQL parity (SHORT close_pos=0.15 → True, 0.80 → False)
   - Direction string case-insensitive

**Remaining gap — same-day stop execution**:
Same-day stop execution and next-session stop application remain split: backtest uses a batch
5-min candle loop (`_simulate_same_day_stop_execution`), while paper uses a streaming WebSocket path.
These are fundamentally different data-flow models. Full unification would require a shared pure
function for intraday stop simulation, which is out of scope until the ENGINE_OPTIMIZATION plan.
Treat any future divergence here as a parity bug investigation trigger.

**Why this matters**: The H-filter formula is now tested for equivalence via explicit parity tests.
Same-day stop and next-session stop application remain engine-specific — a change in one path can
still produce different exit prices without the other path updating.

---

### ISSUE-056 — Live websocket timing vs historical replay timing can create bounded parity gaps

**Status**: OPEN — investigate / quantify acceptable drift
**Severity**: Medium (expected data-flow difference, but needs explicit bounds)
**Found**: 2026-04-25 during parity review

**Observation**: Live paper trading receives websocket ticks and bar completion events in real time, while backtest consumes finalized historical bars. That means the two environments do not have identical data arrival timing, even when they use the same rule.

This matters for:
- pre-open readiness
- same-day partial exits
- stop updates that depend on the first bar after entry
- any rule that can react before the bar is fully "known" in live

**Why this matters**: Some live-vs-backtest differences are unavoidable data-flow differences, not engine bugs. But they should be measured and labeled explicitly, not treated as automatic equivalence.

**Action**:
- Add replay/parity diagnostics that compare bar-arrival timing, carry-stop application time, and exit-trigger order.
- Define which drift is acceptable because of live market timing and which drift is a code regression.
- Revisit this after the shared carry/exit engine is in place.

---

### ISSUE-057 — Backtest carry-stop traceability is incomplete; stored exits cannot be reconstructed from persisted fields alone

**Status**: ✅ FIXED — `carry_stop_next_session` and `carry_action` persisted in `bt_execution_diagnostic`
**Severity**: Medium (blocks precise carry-stop postmortems and parity audits)
**Found**: 2026-04-25 during CENTRALBK / ADANIGREEN carry tracing

**Observation**: The persisted backtest tables store trade results and a limited diagnostic snapshot, but they do not persist the derived `carry_stop_next_session` that actually drives the next-session exit. On the current code path, calling the shared intraday helper for `CENTRALBK` (`2018-10-09`) and `ADANIGREEN` (`2023-02-27`) returns:
- `same_day_exit_reason = None`
- `carry_stop_next_session = initial_stop`

The backtest runner then applies the H-carry clamp, but the final stored exit prices in the historical experiments are not directly reconstructible from the persisted trade/diagnostic rows alone.

**Update**: The backtest diagnostics now persist `carry_stop_next_session` and `carry_action`, which closes the main traceability gap for postmortems.

**Why this matters**: When a baseline drifts, we need to answer "which stop value was used?" from the DB, not by rerunning the engine manually. Without persisting the derived carry stop, a parity audit cannot distinguish:
- same-day stop logic changes
- carry-clamp changes
- next-session stop application changes
- data-snapshot drift

**Action**:
- Add a regression test that asserts the stored carry-stop trace for `CENTRALBK` and `ADANIGREEN` can be reconstructed end-to-end from DB rows.
- Keep ISSUE-055 open until the shared carry/exit helper is extracted and the remaining engine-specific stop paths are consolidated.

---

## Current Canonical Baselines (2026-04-26)

Post-Phase-1 + ISSUE-055 (shared eval refactor, H-filter rounding fix).
Full 11-year window: `2015-01-01 → 2026-04-24`, universe 2000.

| Leg | Exp ID | Avg Annual | Max DD | Calmar | PF | Trades |
|-----|--------|-----------|--------|--------|----|--------|
| Breakout 4% | `6565aa5698186b01` | +54.5% | 3.16% | 17.3 | 20.78 | 2,215 |
| Breakout 2% | `874515a0c02ba7ee` | +121.8% | 2.73% | 44.6 | 19.21 | 7,091 |
| Breakdown 4% | `a2f4063613d259b3` | +3.1% | 0.74% | 4.2 | 5.82 | 254 |
| Breakdown 2% | `9a5ed7575f68613a` | +8.3% | 1.90% | 4.4 | 5.48 | 792 |

Zero regression confirmed vs post-Phase-1 comparison set. All prior IDs pruned from `data/backtest.duckdb`.
Run timestamp: 2026-04-26.
