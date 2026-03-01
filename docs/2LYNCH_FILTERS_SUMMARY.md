# 2LYNCH Filters - Implementation Summary

**Date**: 2026-02-28
**Status**: IMPLEMENTED AND VALIDATED (840.67% total return, 84.07% annualized, 9,261 trades)

---

## Filter Definitions

Six filters applied to every 4% gap-up signal. Require **4 of 6** to pass.

| # | Letter | Name | Condition | Rationale |
|---|--------|------|-----------|-----------|
| 1 | **H** | Close near High | `close_pos_in_range >= 0.70` | Strong buying pressure, close near day's high |
| 2 | **N** | Narrow/Negative T-1 | `(prev_high - prev_low) < atr_20 * 0.5 OR prev_close < prev_open` | Compression before breakout ("coiled spring") |
| 3 | **2** | Not Up 2 Days | `ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0` | Avoid already-extended moves |
| 4 | **Y** | Young Breakout | `prior_breakouts_30d <= 2` | First breakouts succeed more than later ones |
| 5 | **C** | Consolidation | `vol_dryup_ratio < 1.3` | Below-average volume = consolidation before breakout |
| 6 | **L** | Trend Quality | 2 of 3: above MA20, positive 5d ret, R2_65 >= 0.70 | Orderly uptrend, not random walk |

---

## Implementation Locations

| Component | File |
|-----------|------|
| Feature computation | `src/.../db/market_db.py` (`build_feat_daily_table`) |
| Filter SQL (backtest) | `src/.../services/backtest/duckdb_backtest_runner.py` |
| Filter Python (scan) | `src/.../services/scan/duckdb_signal_generator.py` |
| Config defaults | `src/.../services/scan/rules.py` (`ScanConfig`) |

---

## Pre-computed Features (feat_daily table)

| Column | Description |
|--------|-------------|
| `r2_65` | R-squared of 65-day linear regression (close vs time) |
| `atr_compress_ratio` | Current ATR / 50-day avg ATR |
| `range_percentile` | Price position in 252-day range (0 = low, 1 = high) |
| `vol_dryup_ratio` | Today's volume / 20-day avg volume |
| `prior_breakouts_30d` | Count of 4%+ daily returns in last 30 trading days |
| `prior_breakouts_90d` | Count of 4%+ daily returns in last 90 trading days (compat) |

---

## Changes from Original Implementation (2025-02-24)

1. **N filter**: Now checks T-1 (prev day), not T (breakout day). Uses price units `(prev_high - prev_low) < atr_20 * 0.5`, not percentile.
2. **Y filter**: Window shortened from 90 days to **30 days**, max breakouts kept at 2.
3. **2 filter**: Added — was missing entirely. Checks `ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0`.
4. **Initial stop**: Changed from ATR-based (`entry - 2*ATR`) to **T-1's low** (Stockbee approach).
5. **R2 computation**: Now actually computed via `REGR_R2(close, rn)` over 65-day window (was hardcoded 0.0).
6. **Trail/time stops**: Trail activation 8% (was 5%), time stop 5 days (was 3 days).
7. **Weak follow-through**: Disabled (`threshold=0.0`) — Stockbee holds 3-5 days.
8. **Breakeven stop**: Added — stop moves to entry once close > entry price.

---

*Validated against the 2015-2024 decade: 840.67% total return, 84.07% annualized, 9,261 trades*
