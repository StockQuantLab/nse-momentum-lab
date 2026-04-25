# Backtest Results

**Production Baseline** - Experiment ID: `0620c21eca81a567`
<br>Strategy: 2LYNCHBreakout 4% | Period: 2015-2026-04-23 | Status: COMPLETED

---

## All Strategy Results (2015–2026-04-23, universe 2000, 60-min FEE)

| Strategy | Threshold | Dir | Exp ID | Trades | Win% | Ann Ret | Max DD | Calmar | Total Ret |
|----------|-----------|-----|--------|--------|------|---------|--------|--------|-----------|
| **2LYNCHBreakout** | 4% | LONG | `0620c21eca81a567` | **2,217** | **40.6%** | **54.5%** | **3.16%** | **17.3** | **654.3%** |
| 2LYNCHBreakout | 2% | LONG | `4b6b548bbe0121f0` | 7,097 | 38.6% | 122.0% | 2.73% | **44.7** | 1,464.0% |
| 2LYNCHBreakdown | 4% | SHORT | `5f5d2c00471f07f6` | 258 | 36.0% | 3.1% | 0.74% | 4.2 | 37.8% |
| 2LYNCHBreakdown | 2% | SHORT | `0bf8ec0d38586e32` | 792 | 25.9% | 8.3% | 1.90% | 4.4 | 99.3% |

**Notes:**
- thresholdbreakout@4% = 2LYNCHBreakout@4% exactly — identical filter stack, confirmed bit-for-bit (2026-03-07)
- 2LYNCHBreakout@2% has better Calmar (58.94) than 4% (43.67) — filter quality, not threshold, drives edge
- 2LYNCHBreakdown (SHORT) structurally harder in 2015–2025 bull market; 2% more viable than 4%
- EpisodicPivot: only 6 trades — 5% gap threshold + 60-min FEE window rarely aligns on NSE. Infrastructure confirmed working; needs `min_gap=0.02–0.03` for viable signal count

---

## 2LYNCHBreakout 4% — Production Baseline

| Metric                | Value         |
|-----------------------|---------------|
| **Total Return**      | **654.3%**    |
| **Annualized Return** | **54.5%**     |
| **Win Rate**          | **40.6%**     |
| **Max Drawdown**      | **3.16%**     |
| **Calmar Ratio**      | **17.3**      |
| **Total Trades**      | **2,217**     |

**Key Configuration:**
- **Universe**: Top 2,000 by liquidity → filtered to ~1,800 stocks
  - Min price: ₹10
  - Min volume: 100,000 shares/day
  - Min value traded: ₹3M/day
- **FEE Window**: 60 minutes (entry cutoff: 09:15-10:15 IST)
- **Signal**: 4% gap up from previous close
- **Stop Loss**: Low of the breakout day (FEE method - tight stop)
- **Filters**: 5 of 6 filters must pass (H,N,2,Y,C,L)
- **Exit Rules**:
  - Time stop: Day 5 (sell at close)
  - Abnormal move (10% within day 1-2): Full exit at close
  - Abnormal gap (20%): Full exit at open
  - Trailing stop: Activates at 8% gain, 2% trail

### Portfolio Settings

| Setting | Value |
|---------|-------|
| **Initial Capital** | **₹10 Lakhs (₹10,00,000)** |
| **Max Positions** | **10** |
| **Max Position Size** | 10% of portfolio |
| **Risk Per Trade** | 1% |
| **Fees** | 0.1% per trade |

**Position Sizing Logic:**
- Risk amount = ₹10,000 (1% of ₹10L)
- Position size = ₹10,000 / stop_distance
- Example: ₹100 stock with ₹4 stop → 2,500 shares (₹2.5L), capped at ₹1L max
- Actual risk often <1% due to position size cap

**Estimated Concurrent Positions:**
- ~643 trades/year × 4-day avg hold ÷ 252 trading days ≈ **10 concurrent positions**
- Portfolio often fully deployed at max position limit

---

## Yearly Breakdown

| Year | Total Return | Trades | Wins | Losses | Win Rate | Avg R-Multiple | Max DD | Sharpe | Calmar |
|------|--------------|--------|------|--------|----------|----------------|--------|--------|--------|
| 2015 | +46.4%       | 230    | 105  | 125    | 45.7%    | 2.53           | 4.00%  | 2.88   | 11.6   |
| 2016 | +89.0%       | 340    | 173  | 167    | 50.9%    | 3.18           | 2.25%  | 3.83   | 39.6   |
| 2017 | +146.4%      | 451    | 244  | 207    | 54.1%    | 2.57           | 1.79%  | 5.21   | 81.8   |
| 2018 | +101.9%      | 403    | 197  | 206    | 48.9%    | 2.64           | 4.24%  | 3.69   | 24.0   |
| 2019 | +99.4%       | 320    | 163  | 157    | 50.9%    | 3.69           | 2.56%  | 4.39   | 38.8   |
| 2020 | +283.7%      | 667    | 384  | 283    | 57.6%    | 4.86           | 3.89%  | 7.29   | 72.9   |
| 2021 | +308.4%      | 1,038  | 517  | 521    | 49.8%    | 2.91           | 3.40%  | 4.44   | 90.7   |
| 2022 | +268.2%      | 886    | 462  | 424    | 52.1%    | 2.62           | 4.26%  | 4.34   | 63.0   |
| 2023 | +403.7%      | 1,254  | 655  | 599    | 52.2%    | 2.86           | 4.44%  | 5.21   | 90.9   |
| 2024 | +349.9%      | 1,307  | 641  | 666    | 49.0%    | 2.84           | 3.91%  | 4.01   | 89.5   |
| 2025 | +35.4%       | 177    | 84   | 93     | 47.5%    | 2.42           | 4.11%  | 3.17   | 8.6    |

*Note: 2025 data is partial (Jan-Mar 2025)*

---

## Current 2LYNCH Canonical Runset

The current 2LYNCH breakout/breakdown canonical baselines are tracked separately in
[`docs/research/CANONICAL_REPORTING_RUNSET_2026-04-22.md`](research/CANONICAL_REPORTING_RUNSET_2026-04-22.md).

---

## Performance Highlights

### Best Years
1. **2023**: +403.7% (1,254 trades, 52.2% win rate)
2. **2021**: +308.4% (1,038 trades, 49.8% win rate)
3. **2024**: +349.9% (1,307 trades, 49.0% win rate)

### Consistency
- **All profitable years**: 11/11 years showed positive returns
- **Win rate consistency**: 45.6% to 57.6% (stable across market cycles)
- **Max drawdown control**: Under 5% in all years except 2020

### Market Crash Performance (2020)
- **2020 Return**: +283.7% despite COVID crash
- **Max DD**: 3.89% (excellent risk control during volatility)
- **Win Rate**: 57.6% (highest of all years)

---

## Strategy Details

### Indian 2LYNCH (Stockbee Adaptation)

**Entry (FEE Method - Find and Enter Early)**
- **Signal**: 4% gap up from previous close (open ≥ prev_close × 1.04)
- **Volume**: Minimum 100,000 shares
- **Price**: Minimum ₹10
- **Entry**: Within first 60 minutes of NSE open (09:15-10:15 IST)
- **Stop loss**: Low of the breakout day (tight stop from 5-min data)

**6 Filters (5 must pass)**
| Filter | Description                               |
|--------|-------------------------------------------|
| H      | Close in top 30% of day's range           |
| N      | Previous day narrow/negative range        |
| 2      | Not up 2 days in a row                    |
| Y      | ≤2 breakouts in last 30 days (young)      |
| C      | Volume compression (dryup)                |
| L      | Lynch trend (close > MA20 OR momentum up) |

**Exit Rules**
- **Time stop**: Exit by day 5 (sell at close)
- **Abnormal move** (10% gain within day 1-2): Full exit at close
- **Abnormal gap** (20% gap up): Full exit at open
- **Trailing stop**: Activates at 8% gain, trails at 2% below high
- **Stop loss**: Initial stop at breakout day low, tightens after day 3

---

## How to Run

```bash
# Run backtest with production settings
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --universe-size 2000 \
  --start-year 2015 \
  --end-year 2025 \
  --entry-cutoff-minutes 60

# View results in dashboard
doppler run -- uv run nseml-dashboard

# Compare experiments
doppler run -- uv run python scripts/compare_backtest_runs.py \
  --baseline <exp_id_1> \
  --exps "<name>:<exp_id_2>"
```

---

## Historical Context

This result represents the **production configuration** after multiple iterations:

| Iteration      | FEE Window | Trades    | Ann Ret    | Max DD    | Calmar   |
|----------------|------------|-----------|------------|-----------|----------|
| Baseline       | None       | 9,261     | 84.1%      | 23.0%     | 3.7      |
| v1             | 30min      | 4,600     | 149.6%     | 3.7%      | 40.4     |
| v2             | 45min      | 5,955     | 172.0%     | 4.5%      | 38.2     |
| **Production** | **60min**  | **7,073** | **193.9%** | **4.4%**  | **44.1** |

**Key Learnings:**
- FEE window is critical - 60min provides best risk-adjusted returns
- Bad 5-min data guard (1.5x threshold) eliminates corrupted data
- VectorBT same-day entry/exit conflict fix prevents runaway positions

---

*Results generated using VectorBT backtesting engine with DuckDB data store. Data sourced from Zerodha (2015-2025).*
