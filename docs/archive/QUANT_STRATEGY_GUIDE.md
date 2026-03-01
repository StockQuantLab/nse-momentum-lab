# Quantitative Strategy Implementation Guide

This document provides detailed calculations, examples, and reference implementations for the NSE Momentum Lab quantitative strategy.

---

## 1. Gap-up Breakout Strategy

### 1.1 Entry Timing

**Correct Timing (No Look-ahead Bias):**

```
T-1 Close: Rs 100
T Open: Rs 104 (4% gap up detected at 9:15 AM market open)

Signal Detection:
  - At 9:15 AM, we see the stock gapped 4%+
  - Check 2LYNCH criteria using T-1 and prior data
  - If ALL criteria pass, enter immediately at T's open price

Entry: Rs 104 (same day as signal)
```

**Why This Works:**
- We can only know the gap percentage at market open (9:15 AM)
- We enter at the same price where we detect the signal
- NO look-ahead bias because we don't use T's close to make entry decisions

### 1.2 2LYNCH Quality Filter

All 2LYNCH criteria are evaluated on **T-1 and prior data**:

| Letter | Name | Check | Data Used |
|--------|------|-------|-----------|
| **2** | Not up 2 days | T-2 and T-3 returns not both positive | T-2, T-3 |
| **L** | Linearity | R² of prior move >= 0.70, slope positive | T-1 to T-20 |
| **Y** | Young trend | Prior breakouts <= 2 in 90-day window | T-1 and prior |
| **N** | Narrow/Negative | Prior day TR in bottom 20%ile OR negative return | T-1, T-2 |
| **C** | Consolidation | ATR compression, range compression, volume dry-up | T-1 to T-15 |
| **H** | Close near high | Close position in range >= 0.70 | T-1 |

---

## 2. Position Sizing

### 2.1 ATR-Based Risk Management

**Formula:**
```
Risk Amount = Portfolio Value × Risk Per Trade %
Risk Per Share = Entry Price - Stop Price
Shares = Risk Amount / Risk Per Share
```

**Example Calculation:**

```
Given:
  Portfolio Value: Rs 10,00,000
  Risk Per Trade: 1% (default)
  Entry Price: Rs 104.00
  Stop Price: Rs 98.00 (2 × ATR below entry)

Calculate:
  Risk Amount = 10,00,000 × 0.01 = Rs 10,000
  Risk Per Share = 104 - 98 = Rs 6.00
  Shares = 10,000 / 6 = 1,666 shares

Position Value = 1,666 × Rs 104 = Rs 1,73,264 (17.3% of portfolio)
Actual Risk = 1,666 × Rs 6 = Rs 9,996 (1.0% of portfolio)
```

### 2.2 Position Size Limits

| Limit | Value | Purpose |
|-------|-------|---------|
| Min position value | Rs 10,000 | Avoid tiny positions |
| Max position value | Rs 5,00,000 | Limit single-stock exposure |
| Max position % | 10% | Diversification |

**Example with Max Position Cap:**

```
Given:
  Portfolio: Rs 50,00,000
  Risk: 1% = Rs 50,000
  Entry: Rs 104, Stop: Rs 98

Calculate:
  Shares = 50,000 / 6 = 8,333 shares
  Position Value = 8,333 × 104 = Rs 8,66,632 (17.3% of portfolio)

Apply Max Position % (10%):
  Max Position Value = 50,00,000 × 0.10 = Rs 5,00,000
  Adjusted Shares = 5,00,000 / 104 = 4,807 shares
  Final Position Value = Rs 5,00,000 (10% of portfolio)
  Actual Risk = 4,807 × 6 = Rs 28,842 (0.58% of portfolio)
```

---

## 3. Portfolio Risk Limits

### 3.1 Maximum Positions

```
Max Positions: 10

If currently holding 8 positions:
  Can open 2 more positions

If currently holding 10 positions:
  Cannot open new positions until one exits
```

### 3.2 Maximum Drawdown Halt

```
Max Drawdown: 15%

Scenario:
  Peak Portfolio Value: Rs 12,00,000
  Current Portfolio Value: Rs 10,00,000
  Current Drawdown = (12,00,000 - 10,00,000) / 12,00,000 = 16.67%

Action: TRADING HALTED
  Reason: Max drawdown exceeded (16.67% > 15%)
  Resume: Manual reset required
```

### 3.3 Daily Position Limit

```
Max New Positions Per Day: 3

Scenario:
  Morning: Opened RELIANCE, TCS, INFY (3 positions)
  Afternoon: Want to open HDFC

Action: REJECTED
  Reason: Daily new position limit reached (3)
```

### 3.4 Cooling Period

```
Min Days Between Same Symbol: 5

Scenario:
  Last entry RELIANCE: 2024-01-15
  Today: 2024-01-17 (2 days later)
  Want to enter RELIANCE again

Action: REJECTED
  Reason: Cooling period (2 days < 5 days)

Next allowed entry: 2024-01-20 (5 days after last entry)
```

---

## 4. Complete Trade Example

### 4.1 Signal Detection

```
Date: 2024-01-15 (T)
Symbol: RELIANCE

T-1 (2024-01-14):
  Close: Rs 2,400.00
  ATR(20): Rs 72.00
  Close Position in Range: 0.72 (passes H >= 0.70)
  Prior 2 days: -1.2%, +0.3% (passes 2: not both up)
  Prior day return: -0.5% (passes N: negative)
  R² of prior move: 0.85 (passes L >= 0.70)
  Prior breakouts in 90d: 1 (passes Y <= 2)
  Consolidation quality: PASS

T (2024-01-15):
  Open: Rs 2,505.60 (4.4% gap up)

Signal:
  Gap % = (2505.60 - 2400) / 2400 = 4.4%
  Passes 4P threshold (>= 4%)
  All 2LYNCH checks pass ✓
```

### 4.2 Position Sizing

```
Portfolio: Rs 50,00,000
Risk Per Trade: 1%

Entry Price: Rs 2,505.60
Initial Stop: Entry - 2 × ATR = 2505.60 - 144 = Rs 2,361.60
Risk Per Share: Rs 144.00

Shares = (50,00,000 × 0.01) / 144 = 347 shares
Position Value = 347 × 2505.60 = Rs 8,69,443 (17.4% of portfolio)

Apply Max Position % (10%):
  Max Position = 50,00,000 × 0.10 = Rs 5,00,000
  Adjusted Shares = 5,00,000 / 2505.60 = 199 shares

Final:
  Shares: 199
  Position Value: Rs 4,98,614 (10.0% of portfolio)
  Actual Risk: 199 × 144 = Rs 28,656 (0.57% of portfolio)
```

### 4.3 Risk Management

```
Portfolio State Before Entry:
  Open Positions: 5
  Current Drawdown: 8%
  Daily New Positions: 2

Checks:
  ✓ Max Positions: 5 < 10
  ✓ Drawdown: 8% < 15%
  ✓ Daily Limit: 2 < 3
  ✓ Cooling Period: RELIANCE not in last_entry_dates

Entry Approved: YES
Record Entry:
  last_entry_dates[RELIANCE] = 2024-01-15
  daily_new_positions = 3
  open_positions = 6
```

---

## 5. Exit Rules

### 5.1 Initial Stop

```
Entry: Rs 2,505.60
Initial Stop: Rs 2,361.60 (2 × ATR = Rs 144)

If price hits Rs 2,361.60: EXIT
  Exit Reason: STOP_INITIAL
  Loss: 144 / 2505.60 = 5.7%
```

### 5.2 Trailing Stop

```
Trail Activation: +5% from entry
Trail Stop: 2% from high

Entry: Rs 2,505.60
Activation Level: 2505.60 × 1.05 = Rs 2,630.88

Day 1: High = 2,650, Close = 2,640
  Max High = 2,650
  Trail activated (high > 2630.88)
  Trail Stop = 2650 × 0.98 = Rs 2,597

Day 2: High = 2,700, Low = 2,580
  Max High = 2,700
  Trail Stop = 2700 × 0.98 = Rs 2,646

If price hits Rs 2,646: EXIT
  Exit Reason: STOP_TRAIL
  Profit: (2646 - 2505.60) / 2505.60 = 5.6%
```

### 5.3 Time Stop

```
Max Days: 3

Entry: Day 0 (2024-01-15)
Day 1: No exit triggered
Day 2: No exit triggered
Day 3: No exit triggered

Exit at Day 3 Close: EXIT
  Exit Reason: TIME_STOP_DAY3
```

### 5.4 Gap-Through Stop

```
Entry: Rs 104.00
Initial Stop: Rs 98.00

Next Day Open: Rs 95.00 (gap down through stop)

Exit: At open price Rs 95.00
  Exit Reason: GAP_THROUGH_STOP
  Loss: (104 - 95) / 104 = 8.7%
  Note: Loss > planned 5.7% due to gap
```

---

## 6. Expected Returns Analysis

### 6.1 Per-Trade R-Multiple

```
R = Risk Per Trade (as % of portfolio)
R-Multiple = Actual P&L / R

Example:
  Risk: 1% per trade
  Trade P&L: +2.5%
  R-Multiple = 2.5 / 1 = +2.5R

Expected R-Multiples by Exit Reason:
  STOP_INITIAL: -1.0R to -1.5R
  STOP_TRAIL: +1.0R to +5.0R
  TIME_STOP_DAY3: -0.5R to +2.0R
  GAP_THROUGH_STOP: -1.5R to -3.0R
```

### 6.2 Portfolio Expectancy

```
Win Rate: 40%
Average Win: +2.5R
Average Loss: -1.2R

Expectancy = (Win Rate × Avg Win) - (Loss Rate × Avg Loss)
            = (0.40 × 2.5) - (0.60 × 1.2)
            = 1.0 - 0.72
            = 0.28R per trade

With 100 trades per year at 1% risk:
  Expected Return = 100 × 0.28 × 1% = 28% annually
```

---

## 7. Code Reference

### 7.1 Position Sizing

```python
from nse_momentum_lab.services.risk import (
    PositionSizer,
    PositionSizingConfig,
)

config = PositionSizingConfig(
    risk_per_trade_pct=0.01,  # 1%
    max_position_pct=0.10,    # 10%
    default_portfolio_value=1_000_000,
)

sizer = PositionSizer(config)

position = sizer.calculate_position_size(
    symbol_id=1,
    symbol="RELIANCE",
    entry_price=2505.60,
    stop_price=2361.60,
    portfolio_value=5_000_000,
)

print(f"Shares: {position.shares}")
print(f"Position Value: Rs {position.position_value:,.0f}")
print(f"Risk Amount: Rs {position.risk_amount:,.0f}")
print(f"Risk %: {position.risk_pct:.2%}")
```

### 7.2 Portfolio Risk Management

```python
from nse_momentum_lab.services.risk import (
    PortfolioRiskManager,
    PortfolioRiskConfig,
)

config = PortfolioRiskConfig(
    max_positions=10,
    max_drawdown_pct=0.15,
    max_new_positions_per_day=3,
    min_days_between_same_symbol=5,
)

risk_mgr = PortfolioRiskManager(config)
risk_mgr.initialize(
    portfolio_value=5_000_000,
    asof_date=date(2024, 1, 15),
)

# Check before entry
can_open, reason = risk_mgr.can_open_position(
    symbol_id=1,
    asof_date=date(2024, 1, 15),
)

if can_open:
    risk_mgr.record_entry(
        symbol_id=1,
        asof_date=date(2024, 1, 15),
        position_value=500_000,
    )
else:
    print(f"Cannot open: {reason}")
```

### 7.3 Scan with 2LYNCH

```python
from nse_momentum_lab.services.scan.rules import (
    ScanRuleEngine,
    ScanConfig,
)

config = ScanConfig(
    breakout_threshold=0.04,
    close_pos_threshold=0.70,
    min_r2_l=0.70,
    max_prior_breakouts=2,
)

engine = ScanRuleEngine(config)

candidates = engine.run_scan(
    symbol_id=1,
    symbol="RELIANCE",
    features_list=features,
    asof_date=date(2024, 1, 15),
)

for candidate in candidates:
    if candidate.passed:
        print(f"Entry: Rs {candidate.entry_price}")
        print(f"Stop: Rs {candidate.initial_stop}")
        print(f"Gap: {candidate.gap_pct:.2%}")
```

---

## 8. Configuration Reference

### 8.1 Position Sizing Config

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `risk_per_trade_pct` | float | 0.01 | Risk per trade as fraction of portfolio |
| `max_position_pct` | float | 0.10 | Max single position as fraction |
| `min_position_value_inr` | float | 10000 | Min Rs per position |
| `max_position_value_inr` | float | 500000 | Max Rs per position |
| `default_portfolio_value` | float | 1000000 | Default Rs 10L |

### 8.2 Portfolio Risk Config

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_positions` | int | 10 | Max concurrent positions |
| `max_drawdown_pct` | float | 0.15 | Drawdown halt threshold |
| `max_new_positions_per_day` | int | 3 | Daily entry limit |
| `min_days_between_same_symbol` | int | 5 | Cooling period |

### 8.3 Backtest Config

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `initial_stop_atr_mult` | float | 2.0 | ATR multiplier for stop |
| `trail_activation_pct` | float | 0.05 | 5% activation for trailing |
| `trail_stop_pct` | float | 0.02 | 2% trailing stop |
| `time_stop_days` | int | 3 | Max days in trade |
| `breakout_threshold` | float | 0.04 | 4% gap threshold |

---

## 9. Survivorship Bias Handling

### 9.1 Overview

Survivorship bias occurs when backtests only include stocks that survived to the present, ignoring delisted/bankrupt stocks. This artificially inflates returns.

### 9.2 Implementation

**Scan-Level Filter:**
```python
# Skip stocks that are delisted or will be delisted during holding period
if symbol.delisting_date and symbol.delisting_date <= asof_date + timedelta(days=3):
    skip_symbol()  # Don't scan this stock
```

**Backtest-Level Exit:**
```python
# Force exit if stock gets delisted during open position
if delisting_date and current_date >= delisting_date:
    exit_position(reason=ExitReason.DELISTING)
```

### 9.3 Database Schema

The `ref_symbol` table tracks:
- `delisting_date`: Date stock was/will be delisted
- `status`: ACTIVE, DELISTED, SUSPENDED

---

## 10. Performance Metrics

### 10.1 Risk-Adjusted Returns

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **Sharpe Ratio** | (Return - Rf) / σ | Risk-adjusted return (total volatility) |
| **Sortino Ratio** | (Return - Rf) / σ_downside | Risk-adjusted return (downside only) |
| **Calmar Ratio** | Annual Return / Max DD | Return per unit of drawdown risk |

### 10.2 R-Multiple Distribution

R-Multiple measures trade performance relative to risk taken.

```python
R-Multiple = Actual P&L / Initial Risk

Example:
  Entry: Rs 100, Stop: Rs 95
  Risk = Rs 5 per share

  Exit at Rs 110:
    P&L = Rs 10 per share
    R-Multiple = 10 / 5 = +2.0R
```

**Distribution Metrics:**
| Metric | Description |
|--------|-------------|
| `avg_r` | Mean R-multiple |
| `median_r` | Median R-multiple (less affected by outliers) |
| `r_p10`, `r_p25`, `r_p50`, `r_p75`, `r_p90` | R-multiple percentiles |
| `win_rate_r` | % of positive R trades |
| `avg_winner_r` | Mean R for winning trades |
| `avg_loser_r` | Mean R for losing trades |

---

## 11. Data Quality Validation

### 11.1 Quality Checks

| Check | Severity | Description |
|-------|----------|-------------|
| Missing OHLC | ERROR | Any of open/high/low/close is null |
| OHLC Invalid | ERROR | High < Low, High < max(Open, Close), etc. |
| Zero Price | ERROR | Price below minimum threshold |
| Negative Volume | ERROR | Volume < 0 |
| Date Gap | WARNING | Missing trading days in sequence |
| Extreme Move | WARNING | Price change > 50% in one day |
| Zero Volume | INFO | Volume = 0 (may indicate halt) |

### 11.2 Usage

```python
from nse_momentum_lab.services.ingest.data_quality import (
    DataQualityValidator,
    validate_ingestion_batch,
)

# Validate single symbol
validator = DataQualityValidator()
report = validator.validate_symbol_data("RELIANCE", rows)

# Validate batch
reports, summary = validate_ingestion_batch(data_by_symbol)

print(f"Pass rate: {summary['pass_rate']:.1%}")
print(f"Total issues: {summary['total_issues']}")
```

### 11.3 Quality Score

```
Quality Score = Valid Rows / Total Rows

Example:
  250 trading days loaded
  3 rows with OHLC errors
  Quality Score = 247 / 250 = 98.8%
```
