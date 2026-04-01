# NSE Momentum Lab — 2LYNCH Strategy Guide

**Status**: PRODUCTION READY
**Last Updated**: 2026-03-07
**Baseline**: exp `429c79ac45b65086` — Calmar 43.67, Ann Ret 193.9%, Max DD 4.4%

---

## What Is 2LYNCH?

2LYNCH is an **intraday momentum burst** strategy from Stockbee (US), adapted for NSE equities. The core idea: stocks that gap up 4%+ on a breakout day with specific quality conditions tend to continue higher within the first hour of trading.

**The 2LYNCH filter stack is the golden standard for ALL breakout variants in this system.** Every strategy in this repo — regardless of threshold — applies the same 6 filters. The threshold (4%, 2%, etc.) is a parameter; the filters are non-negotiable.

---

## Indian Market Adaptations

Adapted from Stockbee's US-market approach for NSE equities. Full rationale: [LEGACY_2LYNCH_ADAPTATION.md](archive/LEGACY_2LYNCH_ADAPTATION.md).

| Parameter | US (Stockbee) | India (NSE) | Why |
|-----------|---------------|-------------|-----|
| Min Price | $3 | **Rs. 10** | Filters penny stocks while keeping small-cap universe |
| Liquidity | 100,000 shares | **Rs. 30L value-traded** | Value-traded is more market-appropriate than raw share count |
| Gap Threshold | 4% | 4% (configurable) | Momentum is universal; threshold is a research parameter |
| Filters Required | All 6 | **5 of 6** | Higher selectivity, better risk-adjusted returns in India |

---

## Production Performance (2015–2025)

Full NSE universe (~1,820 stocks), 60-min FEE window.

### All Strategy Variants — Validated Results (2026-03-07)

| Strategy | Threshold | Dir | Trades | Win% | Ann Ret | Max DD | Calmar | Total Ret |
|----------|-----------|-----|--------|------|---------|--------|--------|-----------|
| **thresholdbreakout** | 4% | LONG | **7,073** | **51.3%** | **193.9%** | **4.44%** | **43.67** | **2132%** |
| 2LYNCHBreakout | 4% | LONG | 7,073 | 51.3% | 193.9% | 4.44% | 43.67 | 2132% |
| 2LYNCHBreakout | 2% | LONG | 18,657 | 45.8% | 338.7% | 5.75% | **58.94** | 3725% |
| 2LYNCHBreakdown | 4% | SHORT | 4,717 | 38.9% | 61.4% | 36.69% | 1.67 | 675% |
| 2LYNCHBreakdown | 2% | SHORT | 23,252 | 39.5% | 286.2% | 23.15% | 12.36 | 3148% |

**Key validation**: thresholdbreakout and 2LYNCHBreakout at 4% are **identical** — same trades, same year-by-year returns, same every metric. This confirms the 2LYNCH filter stack is correctly shared across all breakout variants.

**Key insight on 2% breakout**: Lower threshold generates more signals (18,657 vs 7,073) but the 2LYNCH filter stack maintains quality — Calmar 58.94 beats 4% (43.67). The filter stack, not the threshold, drives the edge.

**Short side note**: Breakdown strategies have structurally lower win rates (38–39% vs 51%) in a 10-year bull market (2015–2025). 2LYNCHBreakdown 2% (Calmar 12.36) is more viable than 4% (Calmar 1.67). Short strategies perform better in bear/choppy regimes.

### Production Baseline (thresholdbreakout)

| Metric | Value |
|--------|-------|
| Total Return | **2,132%** |
| Annualized Return | **193.9%** |
| Win Rate | 51.3% |
| Max Drawdown | **4.4%** |
| Calmar Ratio | **43.67** |
| Total Trades | 7,073 |
| FEE Window | 60 min (09:15–10:15 IST) |
| Experiment ID | `429c79ac45b65086` |

---

## The 6 Filters — The Core Edge

Six quality filters applied to every breakout signal. **Require 5 of 6 to pass**.

Filters are computed **inline in the candidate SQL query** using `feat_daily` for pre-computed features and inline `LAG()` functions for same-day context.

### Filter H — Close Near High

> "Strong buying pressure: stock closed near its day's high."

```sql
close_pos_in_range >= 0.70
-- where close_pos_in_range = (close - low) / (high - low)
```

For SHORT (breakdown): `close_pos_in_range <= 0.30` (closed near the day's low).

---

### Filter N — Narrow / Negative T-1

> "The day before the breakout was compressed or red — a 'coiled spring' setup."

```sql
(prev_high - prev_low) < (atr_20 * 0.5)   -- narrow range
OR prev_close < prev_open                   -- OR red day
```

This is evaluated on T-1 (the day before the breakout), not on the breakout day itself.
For SHORT: `OR prev_close > prev_open` (T-1 was a green/up day before the breakdown).

---

### Filter 2 — Not Up 2 Days in a Row

> "Avoids chasing already-extended moves. At least one of the two days before the breakout must be flat or down."

```sql
ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0
-- ret_1d_lag1 = return of T-1, ret_1d_lag2 = return of T-2
-- computed inline: (close[T-1] - close[T-2]) / close[T-2]
```

**This is the most selective filter in trending markets.** In a strong bull run, many stocks have been up 3–5 days in a row; filter_2 eliminates them cleanly.

**Implementation note**: `ret_1d_lag1` and `ret_1d_lag2` are computed **inline in the SQL CTE** using `LAG(close, 2)` and `LAG(close, 3)`. They are **not** from `feat_daily`. Adding `AND ret_1d_lag1 IS NOT NULL` to the candidate query ensures at least 4 rows of price history before the signal day.

For SHORT: `ret_1d_lag1 >= 0 OR ret_1d_lag2 >= 0` — at least one of the last 2 days was UP. This avoids shorting a stock already in a free-fall cascade.

---

### Filter Y — Young Breakout

> "First breakouts from consolidation succeed more than 4th or 5th repeated breakouts."

```sql
COALESCE(prior_breakouts_30d, 0) <= 2
-- prior_breakouts_30d = count of 4%+ daily returns in the last 30 trading days
-- pre-computed in feat_daily
```

---

### Filter C — Consolidation / Volume Dry-Up

> "Below-average volume before the breakout confirms accumulation, not distribution."

```sql
vol_dryup_ratio < 1.3
-- vol_dryup_ratio = today's volume / 20-day avg volume
-- pre-computed in feat_daily
```

---

### Filter L — Trend Quality

> "The stock is in an orderly uptrend, not a random walk."

```sql
(CAST(close > ma_20 AS INTEGER)
 + CAST(ret_5d > 0 AS INTEGER)
 + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER)) >= 2
```

Requires 2 of 3 sub-conditions:
1. `close > ma_20` — above 20-day moving average
2. `ret_5d > 0` — positive 5-day momentum
3. `r2_65 >= 0.70` — R² of 65-day linear regression ≥ 0.70 (orderly trend)

**Note on TI65**: The original Stockbee filter uses `TI65 = MA7/MA65 >= 1.05`. This implementation uses the 2-of-3 check as the breakout trend filter (`filter_L`) instead of a hard TI65 gate. TI65 is still available as `ma_7/ma_65_sma` in features for research and screening, but it is **not** used as a backtest admission gate for either breakout or breakdown.

For SHORT: `close < ma_20`, `ret_5d < 0`, R² still ≥ 0.70 (orderly downtrend).

---

## Filter Summary Table

| Letter | Name | LONG condition | SHORT condition | Source |
|--------|------|---------------|-----------------|--------|
| **H** | Close Near High | `close_pos_in_range >= 0.70` | `close_pos_in_range <= 0.30` | `feat_daily` |
| **N** | Narrow/Negative T-1 | narrow OR red day before | narrow OR green day before | inline LAG |
| **2** | Not Up 2 Days | `ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0` | `ret_1d_lag1 >= 0 OR ret_1d_lag2 >= 0` | inline LAG |
| **Y** | Young Breakout | `prior_breakouts_30d <= 2` | `prior_breakouts_30d <= 2` | `feat_daily` |
| **C** | Consolidation | `vol_dryup_ratio < 1.3` | `vol_dryup_ratio < 1.3` | `feat_daily` |
| **L** | Trend Quality | 2 of 3: above MA20, pos 5d, R²≥0.70 | 2 of 3: below MA20, neg 5d, R²≥0.70 | `feat_daily` |

---

## Decision-Time Rules

Backtest, paper simulation, and paper/live must use the same information at the same decision point.
If a feature is not available yet in paper/live, it does not belong in backtest admission either.

Use these buckets consistently:

1. **Pre-open admissible**
   - Known before the session opens.
   - Safe for backtest admission, paper simulation, and live pre-open watchlists.
   - Examples: prior-day watchlist features, T-1 `H_prev`, T-1 `N`, `Y`, `C`, `L`.

2. **Intraday admissible**
   - Known only after live bars arrive.
   - Safe only for the intraday trigger and same-session execution logic.
   - Examples: breakout trigger from live 5-minute bars, intraday stop checks.

3. **End-of-day hold management**
   - Known only after the session closes.
   - Safe only for carry-vs-exit decisions after close.
   - Example: same-day `H` if it is defined from the session close-in-range.

Hard rule:
- No same-day or hindsight-only values in pre-open admission or ranking.
- If a value is only available after the decision point, it must not influence earlier entry filters.

For 2LYNCH, treat `filter_h_prev` as the pre-open admission signal and `filter_h` as the same-day hold-management signal.

---

## Strategy Naming

| Strategy name | CLI flag | Description |
|---------------|----------|-------------|
| `thresholdbreakout` | `--strategy thresholdbreakout` | 4% threshold, LONG, canonical breakout baseline |
| `2LYNCHBreakout` | `--strategy 2lynchbreakout` | Configurable threshold, LONG, **same 2LYNCH filter stack** |
| `2LYNCHBreakdown` | `--strategy 2lynchbreakdown` | Configurable threshold, SHORT, **mirrored 2LYNCH filter stack** |
| `EpisodicPivot` | `--strategy episodicpivot` | Gap-based episodic setups, LONG |

**Key rule**: `2LYNCHBreakout` at `--breakout-threshold 0.04` must produce **identical results** to `thresholdbreakout`. If they diverge, the filter stack has drifted — investigate immediately.

Legacy aliases `2lynchbreakout` and `2lynchbreakdown` still resolve to `thresholdbreakout` and `thresholdbreakdown` for backward compatibility.

---

## FEE Window (Find and Enter Early)

The strategy enters on the **breakout day itself**, within the first 60 minutes of trading.

| FEE Window | Trades | Win% | Ann Ret | Max DD | Calmar |
|------------|--------|------|---------|--------|--------|
| 30 min | 4,604 | 54.6% | 149.6% | 3.7% | 40.39 |
| 45 min | 5,955 | 51.9% | 172.0% | 4.5% | 38.45 |
| **60 min** | **7,073** | **51.3%** | **193.9%** | **4.4%** | **43.67** |

**Production default: 60 minutes** — best Calmar even though win rate is slightly lower.

Entry mechanics (from 5-min bars):
- LONG: first 5-min bar where `high >= prev_close × (1 + threshold)` within 09:15–10:15 IST
- SHORT: first 5-min bar where `low <= prev_close × (1 − threshold)` within 09:15–10:15 IST

---

## Frozen Comparison Baselines

Use one fixed parameter set and only vary `breakout_threshold` between `0.02` and `0.04`.
These are the causal, live-tradable comparison settings to keep stable across reruns.

### Shared Settings

| Param | Value |
|---|---|
| `universe_size` | `100000` |
| `min_price` | `10` |
| `min_filters` | `5` |
| `start_year` | `2025` |
| `end_year` | `2026` |
| `start_date` | `2025-01-01` |
| `end_date` | `2026-03-30` |
| `entry_timeframe` | `5min` |
| `breakout_use_current_day_c_quality` | `false` |
| `abnormal_gap_mode` | `trail_after_gap` |
| `abnormal_profit_pct` | `0.1` |
| `time_stop_days` | `5` |
| `trail_activation_pct` | `0.08` |
| `trail_stop_pct` | `0.02` |

### Breakout Runs

| Param | Value |
|---|---|
| `strategy` | `2LYNCHBreakout` |
| `breakout_threshold` | `0.02` or `0.04` |
| `breakout_daily_candidate_budget` | `0` |
| `breakout_legacy_h_carry_rule` | `false` |

### Breakdown Runs

| Param | Value |
|---|---|
| `strategy` | `2LYNCHBreakdown` |
| `breakout_threshold` | `0.02` or `0.04` |
| `breakout_daily_candidate_budget` | `30` |
| `breakdown_daily_candidate_budget` | `5` |
| `breakdown_rs_min` | `0.0` |
| `breakdown_strict_filter_l` | `false` |
| `breakdown_filter_n_narrow_only` | `false` |
| `breakdown_skip_gap_down` | `false` |
| `breakdown_breadth_threshold` | `None` |
| `breakdown_require_atr_expansion` | `false` |
| `breakdown_ti65_mode` | `off` |
| `short_trail_activation_pct` | `0.04` |
| `short_time_stop_days` | `3` |
| `short_max_stop_dist_pct` | `0.05` |
| `short_abnormal_profit_pct` | `0.05` |

---

## Stop / Exit Logic

Layered exit system (Stockbee-derived):

| Layer | Trigger | Action |
|-------|---------|--------|
| **Initial Stop** | Entry | Stop = low of the breakout day (from 5-min intraday) |
| **Breakeven Stop** | Close > entry | Stop moves up to entry price |
| **Post-Day-3 Tightening** | Day 3+ | LONG: stop = max(stop, day's low); SHORT: stop = min(stop, day's high) |
| **Trail Stop** | Up 8%+ | Trail 2% below highest high (LONG) / above lowest low (SHORT) |
| **Time Stop** | Day 5 | Exit at day 5 close if still in position |
| **Gap-Through Stop** | Open gaps past stop | Exit at open price |
| **Abnormal Gap** | Gap > 20% | Take profit at open |

**No weak follow-through exit** — `follow_through_threshold=0.0`. Stockbee holds positions 3–5 days minimum.

**Short-side stop note**: For SHORT positions, the initial stop is the day's running HIGH (not low). Stop must be ABOVE entry (not below).

---

## PnL Calculation (Direction-Aware)

PnL percentage is direction-aware in the backtest runner:
- **LONG**: `pct = (exit_price - entry_price) / entry_price × 100`
- **SHORT**: `pct = (entry_price - exit_price) / entry_price × 100`

VectorBT handles absolute returns correctly for both directions. The `pnl_pct` stored per trade follows the same formula.

---

## System Architecture

```
Data Layer (DuckDB + Parquet)
    data/parquet/daily/{SYMBOL}/all.parquet
    data/parquet/5min/{SYMBOL}/all.parquet
         |
         v
Feature Store (DuckDB: feat_daily)
    ret_1d, ret_5d, atr_20, r2_65, vol_dryup_ratio,
    prior_breakouts_30d, close_pos_in_range, ma_20, ma_7, ma_65_sma
         |
         v
Strategy Registry (strategy_registry.py + strategy_families.py)
    Candidate SQL: 2LYNCH filters applied inline + feat_daily join
         |
         v
Backtest Runner (duckdb_backtest_runner.py)
    Intraday entry resolution from 5-min data (FEE window)
    Signal filtering: 5/6 filters required (params.min_filters)
         |
         v
VectorBT Engine (vectorbt_engine.py)
    Multi-year, multi-symbol simulation
    Stop stack: initial → breakeven → post-day3 → trail → time
         |
         v
Result Storage (DuckDB: bt_experiment, bt_trade, bt_yearly_metric)
    + PostgreSQL lineage (exp_run, artifacts via MinIO)
         |
         v
NiceGUI Dashboard (apps/nicegui/)
    http://localhost:8501
```

---

## How to Run

### Production Baseline (thresholdbreakout, 4%, 60-min FEE)

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --universe-size 2000 \
  --start-year 2015 \
  --end-year 2025
```

Expected: ~7,073 trades, 51.3% win rate, 193.9% annualized, 4.4% max DD, Calmar ~43.

### 2LYNCHBreakout — Configurable Threshold

```bash
# 4% threshold (must match thresholdbreakout baseline)
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy 2lynchbreakout --breakout-threshold 0.04 \
  --universe-size 2000 --start-year 2015 --end-year 2025

# 2% threshold
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy 2lynchbreakout --breakout-threshold 0.02 \
  --universe-size 2000 --start-year 2015 --end-year 2025
```

### 2LYNCHBreakdown — SHORT

```bash
# 4% breakdown (short)
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy 2lynchbreakdown --breakout-threshold 0.04 \
  --universe-size 2000 --start-year 2015 --end-year 2025

# 2% breakdown (short)
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --strategy 2lynchbreakdown --breakout-threshold 0.02 \
  --universe-size 2000 --start-year 2015 --end-year 2025
```

### List All Strategies

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest --list-strategies
```

### View Results in Dashboard

```bash
doppler run -- uv run nseml-dashboard
# http://localhost:8501
# Note: DuckDB single-writer — stop any running backtest first before launching dashboard
```

### Quality Gates

```bash
# Unit tests (must always pass before any commit)
doppler run -- uv run pytest tests/unit/ -q    # Expected: unit suite passes

# Type checking
doppler run -- uv run mypy src/ --ignore-missing-imports   # Expected: 0 errors

# Linting
doppler run -- uv run ruff check src/ tests/
```

---

## Key Files

| File | Purpose |
|------|---------|
| `src/nse_momentum_lab/services/backtest/strategy_registry.py` | Strategy registry — resolve/list strategies, 2LYNCH candidate query |
| `src/nse_momentum_lab/services/backtest/strategy_families.py` | Candidate query builders for 2LYNCHBreakout, 2LYNCHBreakdown, EpisodicPivot |
| `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py` | Backtest orchestration, intraday entry, stop logic, persistence |
| `src/nse_momentum_lab/services/backtest/vectorbt_engine.py` | VectorBT simulation engine (LONG + SHORT) |
| `src/nse_momentum_lab/features/daily_core.py` | Feature computation (R², ATR, returns, vol_dryup, etc.) |
| `src/nse_momentum_lab/cli/backtest.py` | CLI entry point (`--strategy`, `--breakout-threshold`, etc.) |
| `apps/nicegui/` | NiceGUI dashboard |

---

## Common Pitfalls to Avoid

### 1. filter_2 must use ret_1d_lag1/lag2 — NOT ret_5d

`filter_2` is the "Not Up 2 Days" check. It uses the return of T-1 and T-2 (computed inline with `LAG()`), NOT the 5-day return. Using `ret_5d <= 0` as a substitute is a different filter that will under-select signals in bull markets and over-select in bear markets.

### 2. DuckDB is single-writer on Windows

Only one read-write DuckDB connection at a time. Kill the dashboard before running a backtest, or the backtest will hang waiting for the lock.

### 3. RUN_LOGIC_VERSION invalidates cache

When the candidate SQL or stop logic changes, bump `RUN_LOGIC_VERSION` in `duckdb_backtest_runner.py`. The experiment cache key includes this version — forgetting to bump means stale cached results will be returned for the new strategy logic.

### 4. Short PnL sign

VectorBT handles SHORT returns correctly (inverted). However, any hand-computed `pnl_pct` using `(exit - entry)/entry` is sign-flipped for SHORT wins. Always use the direction-aware formula in the backtest runner.

### 5. The scan worker is still 2LYNCH-specific

The live scanning path (`services/scan/`) currently only supports the 2LYNCH filter logic via `ScanRuleEngine`. The `ScanWorker` accepts a `strategy_name` parameter but does not yet route to different scan engines. Multi-strategy live scanning is a planned Phase 1 completion item.

---

*Last validated: 2026-03-07 (exp 429c79ac45b65086: Calmar 43.67, Ann Ret 193.9%, Max DD 4.4%)*
*Strategy families version: 1.1.0 (2LYNCHBreakout, 2LYNCHBreakdown with correct filter_2)*
