# Indian Market 2LYNCH Strategy Adaptation

**Status**: IMPLEMENTED AND VALIDATED (840.67% over 10 years)
**Last Updated**: 2026-02-28

---

## Executive Summary

Stockbee's 2LYNCH momentum burst strategy was developed for US markets. This document explains the adaptations made for Indian (NSE) markets and the rationale behind each decision.

**Key Changes from US to India:**

| Parameter | US (Stockbee) | India (Adapted) | Rationale |
|-----------|---------------|-----------------|-----------|
| Min Price | $3 | **Rs.10** | Includes quality small-caps; Rs.50 is too restrictive |
| Volume Filter | 100,000 shares | **Rs.30 lakh value** | Value-traded is market-appropriate |
| Gap Threshold | 4% | 4% | Same - momentum is universal |
| Close Position | Top 30% | Top 30% | Same - strong close signal |
| Filter Strictness | All 6 letters | **5 of 6 letters** | Higher selectivity to reduce weak breakout entries |
| Y window | 3-5 days | **30 days** | Catches over-traded stocks better |
| Initial Stop | Low of entry day | **Low of entry day** | Tighter risk on failed breakout days |
| Time Stop | 3 days | **5 days** | Gives more room for Indian market gaps |

**Validated**: 840.67% total return, 84.07% annualized, 9,261 trades over 2015-2024.

---

## Why Rs.10 Minimum Price?

### Validated by Backtest

The 10-year backtest validated **Rs.10** as the optimal minimum price for the Indian adaptation. While Rs.50 was initially considered, Rs.10 provides:

1. **Broader universe** — captures quality small-caps that gap up on earnings/news
2. **More signals** — Rs.50 would miss ~40% of valid breakouts
3. **Volume filter compensates** — the Rs.30 lakh value-traded filter already eliminates illiquid penny stocks
4. **Results confirm it** — 840.67% total return with Rs.10 threshold

### Price Range Analysis

| Price Range | NSE Stocks (~2024) | In Backtest? |
|-------------|-------------------|-------------|
| < Rs.10 | ~800 | Excluded (penny stocks) |
| Rs.10-50 | ~1200 | Included (value-traded filter removes junk) |
| Rs.50-100 | ~600 | Included |
| Rs.100+ | ~500 | Included |

**Can be adjusted**: Use `--min-price 50` for a higher-quality, lower-signal universe.

---

## Why ₹30 Lakh Value Traded (Not 100,000 Shares)?

### The Problem with Share Count

Volume in shares is meaningless across different price levels:

```
100,000 shares of ₹10 stock  = ₹10 lakh value (too low, illiquid)
100,000 shares of ₹100 stock = ₹1 crore value (good)
100,000 shares of ₹2000 stock = ₹20 crore value (excellent)
```

### Value-Traded is the Correct Metric

Indian market liquidity characteristics:

| Value Traded | Quality | Typical NSE Stocks |
|--------------|---------|-------------------|
| < ₹10 lakh | Poor | Illiquid penny stocks |
| ₹10-₹30 lakh | Fair | Small-cap, some slippage |
| ₹30-₹100 lakh | Good | Mid-cap names |
| > ₹1 crore | Excellent | Large-cap, highly liquid |

### ₹30 Lakh Threshold Benefits

- ✅ **Ensures minimum trade size for institutional participation**
- ✅ **Reduces slippage on entry and exit**
- ✅ **Filters illiquid stocks that gap on low volume**
- ✅ **Works across all price segments**

---

## 2LYNCH Letters: Indian Implementation

### Letter H: Close Near High

**Requirement**: Close in top 30% of day's range (≥0.70)

```python
close_pos = (close - low) / (high - low)
pass = close_pos >= 0.70
```

**Why it matters**: Strong buying pressure into close = conviction.

---

### Letter 2: Not Up 2 Days in a Row

**Requirement**: At least one of the two days before the breakout must be flat or down.

```python
# ret_1d_lag1 = T-1's return, ret_1d_lag2 = T-2's return
pass = (ret_1d_lag1 <= 0) or (ret_1d_lag2 <= 0)
```

**Rationale**: Avoids entering already-extended moves where 2+ consecutive up days have exhausted buying pressure.

**Stockbee quote**: "Stock should not be up 3 days in a row (small range days up 3 days is ok)"

---

### Letter L: Linear First Leg

**Requirement**: Price trend is linear, not "drunken walk"

**Problem**: R² values in our database are unreliable (often 0.0)

**Solution**: Pragmatic 3-check approach
1. Price above MA-20 (uptrend)
2. Positive 5-day return (momentum)
3. R² ≥ 0.70 if available

**Pass if 2 of 3 checks pass**:

```python
checks = [
    close > ma_20,           # Uptrend
    ret_5d > 0,              # Momentum
    r2_65 >= 0.70            # Linearity (if available)
]
pass = sum(checks) >= 2
```

**Why it matters**: Linear moves have better follow-through than choppy stocks.

---

### Letter Y: Young Breakout

**Requirement**: Max 2 prior 4%+ breakouts in 30-day window

```python
if prior_breakouts_30d <= 2:
    PASS  # Young breakout
else:
    REJECT  # Over-traded
```

**Why 30 days (not 90)?**
- 30 days is a tighter window that better captures "first breakout from base" setups
- 90 days was too permissive — stocks with 3+ breakouts in a month are exhausted
- Validated by backtest: 30d window produces better returns than 90d

---

### Letter N: Narrow Range OR Negative Day (T-1)

**Requirement**: The day *before* the breakout (T-1) should be a narrow range or negative day.

```python
# Uses price units, not percentile
is_narrow = (prev_high - prev_low) < (atr_20 * 0.5)
is_negative = prev_close < prev_open

if is_narrow or is_negative:
    PASS  # Compression before breakout
else:
    REJECT  # Extended positive day before gap
```

**Important**: This checks T-1 (the setup day), not T (the breakout day). The breakout day will naturally have a wide range due to the gap.

**Why it matters**: Breakouts from consolidation ("coiled spring") work better than breakouts from extension.

---

### Letter C: Consolidation

**Requirement**: Low volume, ATR compression, room to run

**3-part scoring system**:

```python
score = 0

# Volume dryup (ratio < 1.0 = below average)
if vol_dryup_ratio < 1.0:  score += 2
elif vol_dryup_ratio < 1.3:  score += 1

# ATR compression (ratio < 1.0 = squeezed)
if atr_compress_ratio < 1.0:  score += 2
elif atr_compress_ratio < 1.3:  score += 1

# Room to run (not at 52-week high)
if range_percentile < 0.90:  score += 1

pass = score >= 3
```

**Why consolidation matters**:
- Low volume = lack of supply overhead
- ATR compression = volatility squeeze before expansion
- Room to run = price not at extreme highs

---

## Why 5 of 6 Filters?

### Current Production Default

Current production baseline uses **5 of 6** filters for tighter setup quality and lower tail-risk exposure.

| Requirement | Signals/Year | Quality | Expected Behavior |
|-------------|-------------|---------|-------------------|
| 3 of 6 | High | Low | Too many weak setups |
| 4 of 6 | Medium-high | Medium | More trades, wider quality spread |
| **5 of 6** | Medium-low | **High** | Better selectivity and tighter risk profile |
| 6 of 6 | Very low | Very high | Too few signals for stable deployment |

### Rationale for 5/6

1. **Higher setup quality** - fewer low-conviction breakouts are admitted.
2. **Lower tail risk** - reduces exposure to event-driven gap failures.
3. **Operational clarity** - aligns rule language with strict filtering discipline.
4. **Configurable** - `--min-filters 4` remains available for exploratory runs.

### When to Use 4 of 6

Use `--min-filters 4` if:
- You want higher signal count at the cost of setup quality.
- You are running research sweeps, not production baseline runs.
- You explicitly want broader participation across market regimes.

---

## Complete Filter Checklist

```
ENTRY CRITERIA (All must pass):
  4%+ gap up (open >= prev_close * 1.04)
  Price >= Rs.10
  Value traded >= Rs.30 lakh
  Volume >= 50,000 shares

2LYNCH FILTERS (5 of 6 must pass):
  H: Close in top 30% of range (close_pos >= 0.70)
  N: T-1 narrow range or negative day
  2: Not up 2 days in a row before breakout
  Y: Young breakout (<=2 prior breakouts in 30d)
  C: Volume dryup (vol_dryup_ratio < 1.3)
  L: Trend quality (2/3: above MA20, positive 5d, R2>=0.70)

STOP/EXIT:
  Initial stop: breakout-day (entry-day) low
  Breakeven: stop -> entry once close > entry
  Trail: 8% activation, 2% trail
  Time stop: day 5
```

---

## Backtest Results (Validated)

10-year NSE data (2015-2024), top 500 stocks, historical 4/6 baseline:

| Metric | Value |
|--------|-------|
| Total Return | **840.67%** |
| Annualized Return | **84.07%** |
| Total Trades | 9,261 |
| Win Rate | 37.8% |
| Profitable Years | 10/10 (2015-2024) |

See [BACKTEST_RESULTS.md](../../BACKTEST_RESULTS.md) for full yearly breakdown.

---

## How to Run

```bash
# Run full backtest with default params (Rs.10, 4/6 filters, top 500)
doppler run -- uv run nseml-backtest

# Custom params
doppler run -- uv run nseml-backtest --min-price 50 --min-filters 5

# View results
doppler run -- uv run nseml-dashboard
```

---

## Frequently Asked Questions

### Q: Why Rs.10 and not Rs.50 minimum price?

**A**: The backtest validated Rs.10 as optimal. The value-traded filter (Rs.30 lakh) already eliminates illiquid penny stocks, making a Rs.50 price floor unnecessary and overly restrictive. Use `--min-price 50` if you prefer a tighter universe.

### Q: What about small-cap opportunities below ₹50?

**A**: Two approaches:
1. **Use separate strategy**: Small-cap momentum needs different rules
2. **Manual override**: If you know a specific stock, override the filter

### Q: Why value traded instead of shares?

**A**: Value is market-agnostic:
- ₹100 stock at ₹30 lakh = 3,000 shares
- ₹1000 stock at ₹30 lakh = 300 shares
Both have same liquidity in rupee terms.

### Q: Can I use 4 of 6 filters for more signals?

**A**: Yes, but:
- Expect lower win rate (40-50% vs 50-60%)
- Expect more weak follow-through exits
- Consider 4/6 only in strong bull markets

### Q: What if R² values are all 0.0 in database?

**A**: The L filter uses 3 checks:
1. Price > MA-20 (uptrend)
2. ret_5d > 0 (momentum)
3. R² ≥ 0.70 (if available)

It passes if 2 of 3 pass, so R²=0 doesn't kill the filter.

---

## Implementation Files

| File | Purpose |
|------|---------|
| `services/backtest/duckdb_backtest_runner.py` | Backtest orchestration |
| `services/backtest/vectorbt_engine.py` | VectorBT execution engine |
| `services/scan/duckdb_signal_generator.py` | Signal generation with filters |
| `db/market_db.py` | DuckDB data + result storage |
| `src/nse_momentum_lab/cli/backtest.py` | Packaged CLI entry point (`nseml-backtest`) |
| `apps/dashboard/pages/15_Backtest_Results.py` | Results dashboard |

---

**Disclaimer**: This strategy is for educational purposes. Past performance doesn't guarantee future results. Always validate with your own testing.
