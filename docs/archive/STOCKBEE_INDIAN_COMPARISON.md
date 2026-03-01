# Stockbee 2LYNCH: US vs Indian Markets

**Purpose**: Clarify the differences between Stockbee's original US strategy and the NSE adaptation.

---

## Origin: Stockbee's Momentum Burst Strategy

Stockbee (Pradeep Bonde) developed the 2LYNCH strategy for US markets based on years of studying episodic pivots and momentum bursts. Key principles:

1. **Buy momentum bursts** - stocks gapping up 4%+ with strong volume
2. **Quality filters** - 2LYNCH letters ensure proper setup
3. **Quick exits** - 3-day max hold, trailing stops
4. **Selective trading** - only the best setups

**Reference**: Stockbee blog, "Episodic Pivots" and "Momentum Burst" categories

---

## Market Structure Differences

| Aspect | US (NYSE/NASDAQ) | India (NSE) | Impact on Strategy |
|--------|------------------|-------------|-------------------|
| **Tick Size** | $0.01 | ₹0.05 | India has wider relative spreads for low-price stocks |
| **Circuit Limits** | None | 5-20% | Indian stocks can hit limits, preventing stop execution |
| **Retail Participation** | ~30% | ~50% | More operator-driven moves in India |
| **Stock Universe** | 8,000+ | 2,000+ | India has fewer quality liquid names |
| **Price Range** | $1-$1000+ | ₹5-₹4000+ | India has many low-price penny stocks |

---

## Threshold Adaptations

### 1. Price Threshold

| Market | Original | Adaptation | Rationale |
|--------|----------|------------|-----------|
| US | $3 | - | $3 in US = small but legitimate company |
| India | - | ₹50 | ₹3-₹10 in India = penny stock, manipulation risk |

**Why ₹50?**
- Filters out ~70% of low-quality NSE names
- Ensures stock trades in "quality" tier
- Reduces circuit limit risk
- Better institutional coverage

### 2. Volume/Value Threshold

| Market | Original | Adaptation | Rationale |
|--------|----------|------------|-----------|
| US | 100,000 shares | - | Works for $20-100 stocks |
| India | - | ₹30 lakh | Value-traded is price-agnostic |

**Why ₹30 lakh?**
- Equivalent to decent liquidity in any price tier
- 100,000 shares of ₹10 = ₹10 lakh (too low)
- 100,000 shares of ₹100 = ₹1 crore (too high)
- Value-traded normalizes across price levels

---

## 2LYNCH Filters: Consistent Across Markets

The 6 letters remain the same - momentum is universal:

| Letter | US | India | Notes |
|--------|----|----|----|
| H | Close top 30% | Close top 30% | Same |
| 2 | Not up 2 days | Not up 2 days | Same |
| L | Linear first leg | Linear first leg | Same (with pragmatic R² check) |
| Y | Young trend | Young trend | Same (≤2 breakouts) |
| N | Narrow/Negative | Narrow/Negative | Same |
| C | Consolidation | Consolidation | Same |

---

## Expected Performance Differences

Based on market structure differences:

| Metric | US Expected | India Expected | Why? |
|--------|-------------|----------------|------|
| **Signals/Year** | 100-150 | 20-30 | India has fewer quality names |
| **Win Rate** | 45-55% | 50-60% | Stricter filtering improves quality |
| **Avg Return** | 2-3% | 3-5% | Indian volatility creates bigger moves |
| **Max Drawdown** | 10-15% | 8-12% | Fewer trades = less DD risk |

---

## Implementation Comparison

### US Implementation (Original)

```python
# Stockbee's original parameters
config = ScanConfig(
    breakout_threshold=0.04,      # 4% gap
    close_pos_threshold=0.70,     # Top 30%
    min_price=3.0,                # $3
    min_volume=100000,            # Shares
    min_filters_pass=6,           # All 6 letters
)
```

### Indian Implementation (Adapted)

```python
from nse_momentum_lab.services.scan.indian_2lynch_signal_generator import (
    Indian2LynchSignalGenerator,
)

# NSE-adapted parameters
config = ScanConfig(
    breakout_threshold=0.04,           # 4% gap (same)
    close_pos_threshold=0.70,          # Top 30% (same)
    min_price=50.0,                    # ₹50 (adapted)
    min_value_traded_inr=3_000_000,    # ₹30 lakh (adapted)
    min_filters_pass=5,                # 5 of 6 (strict)
)

signal_gen = Indian2LynchSignalGenerator(config=config)
```

---

## Practical Considerations for Indian Markets

### 1. Circuit Limits

Indian stocks have circuit limits:
- **₹5-₹10**: 5% daily limit
- **₹10-₹50**: 10% daily limit
- **₹50+**: 20% daily limit (or sector-specific)

**Impact**: With ₹50 minimum, we avoid the 5% limit stocks (most problematic for stop execution).

### 2. Lot Sizes

Indian F&O has specific lot sizes. For cash market:
- Trade any quantity (1 share minimum)
- Round lots preferred (multiples of 10-100)

**Impact**: Position sizing needs to account for lot sizes if trading F&O.

### 3. Volatility

Indian markets are more volatile:
- NIFTY intra-day moves: 1-3% typical
- Stock moves: 5-10% common on news

**Impact**:
- Wider stops needed (2x ATR vs 1.5x)
- More gaps through stops
- Higher potential returns

### 4. Market Hours

| Market | Hours (IST) |
|--------|-------------|
| NSE | 9:15 AM - 3:30 PM |
| NYSE | 9:30 AM - 4:00 PM (7:00 PM - 1:30 AM IST) |

**Impact**:
- Pre-market gap analysis must happen before 9:15 AM
- Same-day entry at open is realistic (no pre-market like US)

---

## Entry Execution Differences

### US: Pre-Market and First 15 Minutes

```
4:00 AM IST: Pre-market opens
7:00 AM IST: Market opens
7:00-7:15 AM: Entry period (FEE - Find and Enter Early)
```

### India: Market Open Only

```
9:15 AM IST: Market opens
9:15-9:30 AM: Entry period
```

**Implication**: Indian traders MUST use same-day entry (at open) because there's no pre-market. This is correctly implemented in our signal generator.

---

## Exit Adjustments for India

| Exit Rule | US | India | Reason |
|-----------|----|----|----|
| Initial Stop | 2x ATR | **Low of day** | More conservative for India's volatility |
| Trail Activation | +5% | +5% | Same |
| Trail Stop | 2% from high | **2% from high** | Same |
| Time Stop | 3 days | 3 days | Same |
| Gap Through Stop | At open | At open | Same |

**Why "Low of Day" stop?**
- India has more overnight gap risk
- ATR-based stops can be too wide for volatile stocks
- Low of day is more conservative and realistic

---

## Recommended Workflow for Indian Traders

### Pre-Market (Before 9:15 AM)

```python
# 1. Run scan to identify gap-ups
signals = signal_gen.generate_symbols(
    symbols=nifty500_list,
    start_date=today,
    end_date=today,
)

# 2. Review each signal manually
for s in signals:
    print(f"{s['symbol']}: {s['gap_pct']:.1%} gap")
    print(f"  Filters: {s['filters_passed']}/6")
    print(f"  Details: {s['filter_details']}")
```

### At Market Open (9:15 AM)

```python
# 3. Enter immediately at open for qualified signals
# Use limit orders at or slightly above open price
```

### During Trade (9:15 AM - 3:30 PM)

```python
# 4. Monitor stops
# 5. Trail stops if +5% gain
# 6. Exit at day 3 close if no stop hit
```

---

## FAQ: US vs India

### Q: Can I use $3 price threshold for India?

**A**: Not recommended. Stocks below ₹50:
- Have circuit limits that prevent stop execution
- Are often operator-driven (manipulation)
- Have poor liquidity
- Have wider bid-ask spreads

### Q: Why 5 of 6 filters instead of 6?

**A**: Practical balance:
- 6/6 = too few signals (~10/year)
- 4/6 = too many weak signals
- 5/6 = sweet spot (~20-30 high-quality signals/year)

### Q: What about F&O stocks?

**A**: F&O stocks are generally >₹100 and highly liquid. They automatically pass our filters. Just ensure lot size compatibility for position sizing.

### Q: Do I need to adjust stops for India?

**A**: Use low-of-day stops (more conservative) instead of ATR stops. Indian volatility can cause ATR to be too wide.

### Q: What about small caps below ₹50?

**A**: Use a separate strategy. Small caps need different:
- Risk management
- Position sizing
- Exit rules

---

## Conclusion

The 2LYNCH strategy transfers well to Indian markets with these key adaptations:

1. **Price**: ₹50 minimum (not $3)
2. **Volume**: ₹30 lakh value (not 100,000 shares)
3. **Stops**: Low of day (more conservative)
4. **Quality**: 5 of 6 filters (strict)

**Core philosophy remains the same**: Trade quality momentum bursts, not everything that gaps up.

---

**Files**:
- Implementation: `indian_2lynch_signal_generator.py`
- Test: `test_indian_2lynch.py`
- Details: `INDIAN_2LYNCH_ADAPTATION.md`
