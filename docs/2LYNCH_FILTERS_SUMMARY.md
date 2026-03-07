# 2LYNCH Filters — Implementation Summary

**Date**: 2026-03-07
**Status**: PRODUCTION (exp 429c79ac45b65086: Calmar 43.67, Ann Ret 193.9%, Max DD 4.4%)

---

## Filter Definitions — LONG Side (Indian2LYNCH / 2LYNCHBreakout)

Six filters applied to every breakout signal. Require **5 of 6** to pass.

| # | Letter | Name | Condition | Data Source | Rationale |
|---|--------|------|-----------|-------------|-----------|
| 1 | **H** | Close near High | `close_pos_in_range >= 0.70` | `feat_daily` | Strong buying pressure, close near day's high |
| 2 | **N** | Narrow/Negative T-1 | `(prev_high - prev_low) < atr_20 * 0.5 OR prev_close < prev_open` | inline LAG | Compression before breakout ("coiled spring") |
| 3 | **2** | Not Up 2 Days | `ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0` | **inline LAG (not feat_daily)** | Avoid already-extended moves |
| 4 | **Y** | Young Breakout | `prior_breakouts_30d <= 2` | `feat_daily` | First breakouts succeed more than later ones |
| 5 | **C** | Consolidation | `vol_dryup_ratio < 1.3` | `feat_daily` | Below-average volume = consolidation before breakout |
| 6 | **L** | Trend Quality | 2 of 3: close > MA20, ret_5d > 0, r2_65 >= 0.70 | `feat_daily` | Orderly uptrend, not a random walk |

### Critical: filter_2 uses inline LAG, not feat_daily

`ret_1d_lag1` and `ret_1d_lag2` are computed **inside the candidate SQL CTE** using window functions:

```sql
(LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)
 - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date))
/ NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag1,

(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date)
 - LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date))
/ NULLIF(LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag2,
```

These require at least 4 rows of price history. Always add `AND ret_1d_lag1 IS NOT NULL` to the candidate filter clause.

**Do NOT substitute `ret_5d <= 0` for filter_2.** That is a 5-day momentum check — a different concept. This was a bug in the original `ThresholdBreakout` implementation (fixed 2026-03-07, version 1.1.0).

---

## Filter Definitions — SHORT Side (2LYNCHBreakdown)

Each filter is mirrored for the SHORT direction:

| # | Letter | Name | SHORT Condition | Rationale |
|---|--------|------|-----------------|-----------|
| 1 | **H** | Close near Low | `close_pos_in_range <= 0.30` | Selling pressure, close near day's low |
| 2 | **N** | Narrow/Positive T-1 | `(prev_high - prev_low) < atr_20 * 0.5 OR prev_close > prev_open` | Compression or green day before the breakdown |
| 3 | **2** | Not Down 2 Days | `ret_1d_lag1 >= 0 OR ret_1d_lag2 >= 0` | At least one up day in the last 2 — avoid shorting cascading stocks |
| 4 | **Y** | Young Breakdown | `prior_breakouts_30d <= 2` | Same as LONG |
| 5 | **C** | Consolidation | `vol_dryup_ratio < 1.3` | Same as LONG |
| 6 | **L** | Trend Quality | 2 of 3: close < MA20, ret_5d < 0, r2_65 >= 0.70 | Orderly downtrend |

---

## Note on TI65

The original Stockbee filter uses `TI65 = MA7 / MA65 >= 1.05` (7-day MA 5% above 65-day MA). This implementation uses the equivalent 2-of-3 check (filter_L). TI65 is available as a pre-computed column (`ma_7 / ma_65_sma`) for universe screening but is NOT used as a same-day entry filter — breakouts often fire when the trend is just starting, before TI65 is established.

---

## Implementation Locations

| Component | File |
|-----------|------|
| Feature pre-computation | `src/nse_momentum_lab/features/daily_core.py` |
| Candidate SQL — Indian2LYNCH | `src/nse_momentum_lab/services/backtest/strategy_registry.py` (`_build_2lynch_candidate_query`) |
| Candidate SQL — 2LYNCHBreakout | `src/nse_momentum_lab/services/backtest/strategy_families.py` (`_build_threshold_breakout_candidate_query`) |
| Candidate SQL — 2LYNCHBreakdown | `src/nse_momentum_lab/services/backtest/strategy_families.py` (`_build_threshold_breakdown_candidate_query`) |
| Filter application (min_filters) | `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py` (line ~862) |
| Config defaults | `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py` (`BacktestParams.min_filters = 5`) |
| Live scan filter logic | `src/nse_momentum_lab/services/scan/rules.py` (`ScanRuleEngine`) |

---

## Pre-computed Features (feat_daily)

| Column | Description |
|--------|-------------|
| `close_pos_in_range` | (close - low) / (high - low), same-day |
| `ma_20` | 20-day simple moving average of close |
| `ret_5d` | 5-day return: (close / close[T-5]) - 1 |
| `r2_65` | R² of 65-day linear regression of close vs time index |
| `atr_20` | 20-day Average True Range |
| `vol_dryup_ratio` | Today's volume / 20-day avg volume |
| `prior_breakouts_30d` | Count of 4%+ daily returns in last 30 trading days |
| `prior_breakouts_90d` | Count of 4%+ daily returns in last 90 trading days (compat alias) |
| `atr_compress_ratio` | Current ATR / 50-day avg ATR |
| `range_percentile` | Price position in 252-day high/low range |
| `ma_7` | 7-day SMA (for TI65 screening) |
| `ma_65_sma` | 65-day SMA |

---

## Key Changes Log

| Date | Change |
|------|--------|
| 2025-02-24 | N filter moved to T-1 (prev day); Y window shortened from 90→30 days; filter_2 added (was missing); initial stop changed to breakout day low; R² now computed properly; trail activation 8%; breakeven stop added |
| 2026-03-06 | Post-day-3 stop tightening added (LONG: stop=max(stop,low); SHORT: stop=min(stop,high)); STOP_POST_DAY3 exit reason added |
| 2026-03-07 | **filter_2 bug fixed in 2LYNCHBreakout/2LYNCHBreakdown**: was using `ret_5d` (wrong), now uses `ret_1d_lag1/lag2` (correct, matching Indian2LYNCH); strategies renamed from ThresholdBreakout→2LYNCHBreakout, ThresholdBreakdown→2LYNCHBreakdown; version bumped to 1.1.0; profit_factor sign fixed for SHORT trades |

---

## FEE Window (Find and Enter Early)

| FEE Window | Trades | Win% | Ann Ret | Max DD | Calmar |
|------------|--------|------|---------|--------|--------|
| 30 min | 4,604 | 54.6% | 149.6% | 3.7% | 40.39 |
| 45 min | 5,955 | 51.9% | 172.0% | 4.5% | 38.45 |
| **60 min** | **7,073** | **51.3%** | **193.9%** | **4.4%** | **43.67** |

**Production default: 60 minutes** (09:15–10:15 IST). Entry must occur within the FEE window on the breakout day.

---

*Production baseline (exp 429c79ac45b65086): 2015–2025, ~1,776 stocks, 60-min FEE, 5/6 filters required*
