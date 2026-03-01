# NSE Momentum Lab - 2LYNCH Strategy Implementation

**Status**: PRODUCTION READY (Indian Market Adapted)
**Last Updated**: 2026-02-28

---

## Indian Market Adaptations

This strategy is adapted from Stockbee's US-market "4% breakout / momentum burst" approach for Indian (NSE) equities. See [INDIAN_2LYNCH_ADAPTATION.md](docs/INDIAN_2LYNCH_ADAPTATION.md) for full rationale.

| Parameter | US (Stockbee) | India (NSE) | Why? |
|-----------|---------------|-------------|------|
| Min Price | $3 | **Rs.10** | Filters penny stocks while keeping small-cap universe |
| Volume | 100,000 shares | **Rs.30 lakh value** | Value-traded is market-appropriate |
| Gap Threshold | 4% | 4% | Same - momentum is universal |
| Filters Required | All 6 | **4 of 6** | Balances quality with signal volume |

---

## Performance (2015-2024, Top 500 Stocks)

| Metric | Value |
|--------|-------|
| Total Return | **840.67%** |
| Annualized Return | **84.07%** |
| Win Rate | 37.8% |
| Max Drawdown | 23.0% |
| Total Trades | 9,261 |
| Profitable Years | 10/10 (2015-2024) |

---

## Architecture

```
DuckDB + Parquet (data layer)
    |
    v
Signal Generation SQL (2LYNCH filters applied in DuckDB)
    |
    v
VectorBTEngine (trade simulation with stops/exits)
    |
    v
DuckDB result storage (bt_experiment / bt_trade / bt_yearly_metric)
    |
    v
Streamlit Dashboard (pages/15_Backtest_Results.py)
```

---

## 2LYNCH Filters

Six quality filters applied to each 4% gap-up signal. Require 4/6 to pass.

### H - Close Near High
Close position in the day's range >= 0.70 (close near the high = strong buying pressure).

### N - Narrow Range / Negative Day (T-1)
The day before the breakout should show compression:
- `(prev_high - prev_low) < 0.5 * ATR_20` (narrow range), OR
- `prev_close < prev_open` (negative/red day)

This confirms a "coiled spring" setup before the gap.

### 2 - Not Up 2 Days in Row
At least one of the two days before the breakout must be flat or down:
- `ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0`

Avoids entering already-extended moves.

### Y - Young Breakout
Max 2 prior 4%+ breakouts in the last 30 days:
- `prior_breakouts_30d <= 2`

First breakouts from consolidation succeed more; later ones fail.

### C - Consolidation / Volume Dryup
Volume below average before breakout (consolidation):
- `vol_dryup_ratio < 1.3`

### L - Trend Quality
Requires 2 of 3 sub-checks:
1. Close above 20-day MA
2. Positive 5-day momentum
3. R-squared of 65-day trend >= 0.70

---

## Stop / Exit Logic

The exit system follows Stockbee's layered approach:

1. **Initial Stop**: T-1's low (the setup day's low — natural support)
2. **Breakeven Stop**: Once close > entry price, stop moves up to entry
3. **Trailing Stop**: Once up 8%+, trail at 2% below the highest high
4. **Time Stop**: Exit at close of day 5 if no other exit triggered

**No weak follow-through exit** — disabled (`follow_through_threshold=0.0`) because Stockbee holds 3-5 days minimum.

---

## How to Run

```bash
# Run backtest with default params
doppler run -- uv run nseml-backtest

# View results
doppler run -- uv run nseml-dashboard
```

---

## Key Files

| File | Purpose |
|------|---------|
| `src/.../services/backtest/duckdb_backtest_runner.py` | Backtest orchestration service |
| `src/.../services/backtest/vectorbt_engine.py` | VectorBT execution engine |
| `src/.../db/market_db.py` | DuckDB data + result storage |
| `src/.../services/scan/duckdb_signal_generator.py` | Signal generation with 2LYNCH filters |
| `src/nse_momentum_lab/cli/backtest.py` | Packaged CLI entry point (`nseml-backtest`) |
| `apps/dashboard/pages/15_Backtest_Results.py` | Dashboard visualization |

---

*Last validated: 2026-02-25*
