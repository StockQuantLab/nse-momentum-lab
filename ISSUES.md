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

**Status**: OPEN (known limitation)
**Severity**: Low
**Found**: 2026-03-12

**Problem**: After a backtest run completes, the engine attempts to snapshot `backtest.duckdb` →
`backtest_dashboard.duckdb`. If the NiceGUI dashboard is open and holding a read connection to
`backtest_dashboard.duckdb`, the snapshot may be skipped (DuckDB single-writer constraint). A
warning is logged but the dashboard shows stale results.

**Workaround**: Close the dashboard before running a backtest, or refresh the dashboard page
after the run (it will re-open the file). Alternatively, manually copy `backtest.duckdb` to
`backtest_dashboard.duckdb` after closing the dashboard.

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

*Last updated: 2026-04-20*
