# FEE Method & Stop Analysis — 2LYNCH Backtest

**Last Updated**: 2026-03-01
**Analyst**: Stockbee 2LYNCH adaptation for NSE India

---

## Background: What Was Wrong

The original 10-year backtest (2015–2024, 500 stocks) produced 9,261 trades at 38.8% win rate and **23% max drawdown**. The worst trades showed losses of -23.15% (AAVAS, 2020-03-13) that should have been impossible given a stop at the breakout day's low.

**Root cause**: The `_resolve_intraday_entry_from_5min()` function accumulated the running low of ALL 5-minute candles since market open, including candles from hours before the breakout trigger. On COVID crash-bounce days (March 2020), stocks crashed at open (09:15-09:45), accumulated a low of ₹1,400, then bounced to a 4%+ gap-up by 10:30-11:00. The code set the stop at ₹1,400 (23% below entry), which was never triggered during the 5-day hold.

---

## Stockbee's Actual Method (from blog + video transcript)

### Entry Timing — FEE (Find and Enter Early)
> *"My breakouts — I enter them in the first half an hour. In the first 5, 10 or 20 minutes."*
> *"If they are on anticipation list I'll enter most of them in the first 10 or 15 minutes."*

**Key insight**: *"Because if you enter as early as possible, because your stop is low of the day, you will get a stop which will be much closer. If you enter very late in the day, you are not going to get a stop which is very close to the entry."*

The FEE method is not an arbitrary preference — it's the mechanism that makes the stop tight. Early entry = day low at entry time ≈ 3-6% below entry. Late entry = accumulated day low can be 15-25% below.

### Stop Placement
- **Stop = low of the breakout day (T)** — explicitly stated: *"your stop is low of the day"*
- **NOT T-1 low** — Stockbee is clear this is the current day's low
- The 5-min code is correct in principle (`known_low_at_bar_open`); FEE cutoff makes it work

### Exit Rules (from Exit Guidelines blog)
- Time stop: exit 50% by day 3 or 5 at close
- If up 8%+ immediately: sell 50%, tighten trailing stop
- If up 20%+ gap: exit entire position at open
- Trailing stop activates after 8%+ gain

### TI65 (Trend Intensity 65)
- **Formula**: `MA7 / MA65 >= 1.05`
  (7-day moving average is 5% above 65-day moving average)
- Source: *"average C7 by average C 65 is 1.05... its 7 days moving average was 5% above its 65 day moving average"*
- Meaning: TI65 > 1.05 confirms stock is in an intensely trending phase with above-average velocity
- NOT `close / MA65` (that's a common misread)

---

## Three Bugs Fixed (2026-02-28)

### Fix 1 — Entry Time Cutoff (FEE Method)
**File**: `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py`

Added `_minutes_from_nse_open()` helper and time check in `_resolve_intraday_entry_from_5min()`. NSE opens at 09:15 IST; candles after `entry_cutoff_minutes` from open are rejected.

New `BacktestParams` field: `entry_cutoff_minutes: int = 30`
Default changed to **60 minutes** after backtesting showed 60min gives best Calmar ratio for NSE.

```python
# Rejects breakout triggers more than N minutes after 09:15 NSE open
if mins is not None and mins > entry_cutoff_minutes:
    break  # All subsequent candles also past cutoff (sorted)
```

### Fix 2 — Max Stop Distance Guard
**File**: `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py`

Belt-and-suspenders guard: even within the time window, if stop is >8% below entry, the setup is invalid.

New `BacktestParams` field: `max_stop_dist_pct: float = 0.08`

```python
if entry_price > 0 and initial_stop < entry_price * (1 - params.max_stop_dist_pct):
    skipped_intraday_entry += 1
    continue
```

### Fix 3 — Gap-Down Through Stop Exits at Open
**File**: `src/nse_momentum_lab/services/backtest/vectorbt_engine.py`

Previously, when a stock gapped DOWN below the stop on a subsequent day, the simulation exited at the stop price (optimistic). Now it exits at the open price (realistic).

```python
# Gap-down through stop: stock opens below stop level → exit at open
if not pd.isna(current_open) and float(current_open) < stop_level:
    exit_date = current_dt
    exit_price = float(current_open)
    exit_reason = ExitReason.GAP_THROUGH_STOP
    break
```

---

## FEE Window Comparison — Top 500 Stocks (2015–2025)

| Metric | OLD (no FEE) | 30min | 45min | 60min |
|---|---|---|---|---|
| Universe | 500 | 500 | 500 | 500 |
| Total Trades | 9,261 | 1,915 | 2,493 | 2,982 |
| Win Rate | 38.8% | **54.6%** | 51.7% | 50.6% |
| Annualised Return | 84.1% | 53.7% | 60.8% | **68.8%** |
| Max Drawdown | 23.0% | 3.4% | 3.5% | **3.8%** |
| Profit Factor | 2.10 | **5.49** | 4.65 | 4.28 |
| **Calmar Ratio** | 3.65 | 15.73 | 17.37 | **18.09** |
| Worst Trade | -23.2% (AAVAS) | -11.3% | -11.3% | -11.3% |

### Year-by-Year (500 stocks)

| Year | OLD trades | OLD ret% | 30min | 30min ret% | 45min | 45min ret% | 60min | 60min ret% |
|---|---|---|---|---|---|---|---|---|
| 2015 | 534 | +21.7% | 61 | +14.7% | 88 | +17.2% | 112 | +19.1% |
| 2016 | 746 | +31.1% | 100 | +19.4% | 131 | +24.4% | 156 | +31.0% |
| 2017 | 873 | +91.4% | 94 | +28.7% | 137 | +32.5% | 172 | +36.4% |
| 2018 | 763 | +10.5% | 121 | +32.6% | 159 | +33.7% | 192 | +36.6% |
| 2019 | 735 | +24.1% | 110 | +34.4% | 141 | +39.7% | 181 | +44.7% |
| 2020 | 1,123 | +149.4% | 222 | +90.5% | 288 | +108.7% | 346 | +116.8% |
| 2021 | 1,222 | +150.0% | 275 | +90.2% | 373 | +101.0% | 446 | +123.6% |
| 2022 | 965 | +91.7% | 222 | +61.9% | 287 | +72.8% | 347 | +82.6% |
| 2023 | 1,137 | +167.9% | 270 | +75.7% | 350 | +93.4% | 413 | +111.8% |
| 2024 | 1,163 | +102.7% | 347 | +110.2% | 427 | +116.7% | 497 | +126.7% |
| 2025 | 0 | — | 93 | +32.0% | 112 | +28.9% | 120 | +27.9% |

### Key Observations
1. **Max drawdown is robust**: all three FEE windows maintain 3.4–3.8% max DD, a 6x improvement vs OLD.
2. **Return scales with window**: +7–8pp annualised per 15-minute extension.
3. **Win rate degrades slowly**: 54.6% → 51.7% → 50.6% — all far above OLD's 38.8%.
4. **Identical worst trade**: AVALON -11.34% 2023-08-07 is a `GAP_THROUGH_STOP` — genuine overnight risk.
5. **60min is best Calmar**: 18.09 vs 17.37 (45min) vs 15.73 (30min) vs 3.65 (OLD).

**NSE-specific note**: Stockbee trades US markets with a 30min window. NSE has a pre-open auction (09:00–09:15) meaning price discovery happens before the regular session. Many NSE gap-ups need 30–60 minutes to confirm, making 60min more appropriate.

---

## Exit Reason Analysis (500 stocks, FEE comparison)

| Exit Reason | OLD avg% | 30min avg% | 45min avg% | 60min avg% |
|---|---|---|---|---|
| ABNORMAL_GAP_EXIT | +25.6% | +24.2% | +24.2% | +24.0% |
| ABNORMAL_PROFIT | +10.0% | +10.4% | +10.3% | +10.4% |
| GAP_THROUGH_STOP | n/a (0) | -0.2% | -0.3% | -0.5% |
| STOP_TRAIL | +7.8% | +7.7% | +7.7% | +7.7% |
| TIME_STOP | +4.1% | +6.3% | +6.0% | +5.7% |
| STOP_POST_DAY3 | +0.01% | +1.4% | +1.1% | +1.0% |
| STOP_BREAKEVEN | -0.07% | -0.11% | -0.11% | -0.09% |
| STOP_INITIAL | -4.2% | -3.5% | -3.6% | -3.6% |

---

## Full Universe Results — Evolution (2015–2025, ~1,776 symbols)

### Phase 1: No fixes (2026-02-28)

| FEE | Trades | Win% | Ann Ret | Max DD | Calmar | Exp ID |
|---|---|---|---|---|---|---|
| 30min | 4,600 | 54.3% | 136.3% | 6.9% | 19.74 | `93da941f372a5d35` |
| 45min | 5,936 | 51.7% | 157.5% | 7.0% | 22.38 | `1d1eaea2c327bc22` |
| 60min | 7,034 | 51.1% | 179.0% | 6.6% | 27.06 | `efe9097265a9d555` |

### Phase 2: Bad 5-min data guard only (2026-03-01)

| FEE | Trades | Win% | Ann Ret | Max DD | Calmar | Exp ID |
|---|---|---|---|---|---|---|
| 30min | 4,600 | 54.6% | 151.1% | 5.8% | 26.21 | `b7b4d6cb4e020bbb` |
| 45min | 5,948 | 51.9% | 172.7% | 5.9% | 29.01 | `16c26c8a90af53fd` |
| 60min | 7,055 | 51.2% | 194.4% | 6.6% | 29.40 | `63562d016320fb89` |

**Guard fix alone: 60min went from Calmar 27.06 → 29.40, Ann Ret 179% → 194.4%.**

### Phase 4: Guard 1.5x + VBT conflict fix — FINAL / CURRENT BEST

`RUN_LOGIC_VERSION = "duckdb_backtest_runner_v2026_03_01_guard_1pt5x_vbt_conflict"`

| FEE | Trades | Win% | Ann Ret | Max DD | **Calmar** | Exp ID |
|---|---|---|---|---|---|---|
| 30min | 4,604 | 54.6% | 149.6% | 3.7% | 40.39 | `846ffa74be8d9f35` |
| 45min | 5,955 | 51.9% | 172.0% | 4.5% | 38.45 | `90a56d84db1eea75` |
| **60min** | **7,073** | **51.3%** | **193.9%** | **4.4%** | **43.67** | **`429c79ac45b65086`** |

**VBT fix impact**: Max DD 6.6% → 4.4% (−33%), Calmar 29.40 → 43.67 (+48%). Ann Ret unchanged ~194%.

The VBT conflict fix (`entries = entries & ~exits` before `Portfolio.from_signals()`) resolved trades like:
- **KESARENT**: entry 2021-07-02, held 192 days → was failing to close ABNORMAL_PROFIT exit on day 3 because a new breakout entry on the same day blocked the exit signal
- **ATFL**: entry 2024-02-22, held 141 days — same root cause

### Phase 3: TI65 as same-day entry filter (ABANDONED)

| FEE | Trades | Win% | Ann Ret | Max DD | Calmar | Exp ID |
|---|---|---|---|---|---|---|
| 30min | 3,515 | 51.9% | 99.9% | 5.5% | 18.04 | |
| 45min | 4,515 | 49.0% | 112.2% | 6.3% | 17.88 | |
| 60min | 5,349 | 48.2% | 125.5% | 7.4% | 17.01 | `4196bf737904c182` |

TI65 >= 1.05 on the breakout day is **counter-productive**: breakouts happen when trends are STARTING (TI65 < 1.05 by definition). This cut 24% of trades and Calmar fell from 27→17. **TI65 is appropriate as a universe pre-screener, not a same-day filter.**

### Bad 5-min Data Bug (Discovered 2026-03-01)

4 symbols had **100% zero-volume, wildly wrong 5-min data** (wrong instrument ingested):

| Symbol | Daily avg | 5-min avg | Ratio | Zero-vol bars |
|---|---|---|---|---|
| GABRIEL | ₹250 | ₹11,739 | 47x | 73,515 (100%) |
| GMMPFAUDLR | ₹1,487 | ₹35,661 | 24x | 6,900 (100%) |
| GHCL | ₹508 | ₹10,823 | 21x | 47,928 (100%) |
| UFLEX | ₹514 | ₹2,647 | 5x | 78,566 (100%) |

These were producing phantom trades like "GABRIEL entry ₹12,742 → exit ₹142" (apparently -98.8% loss but on wrong data). They inflated `max_drawdown_pct` in the backtest.

**Fix**: In `_resolve_intraday_entry_from_5min()`, reject if `entry_price > breakout_price * 3`.

### Full Universe vs Top-500

| Metric | 500, 60min (guard) | 2000, 60min (guard) | Improvement |
|---|---|---|---|
| Trades | ~2,982 | 7,055 | +137% |
| Ann Return | ~68.8% | **194.4%** | +126pp |
| Max Drawdown | ~3.8% | 6.6% | +2.8pp |
| Calmar | ~18.09 | **29.40** | +62% |

---

## Configuration Reference

### Recommended BacktestParams (as of 2026-03-01, exp `429c79ac45b65086`)

```python
BacktestParams(
    universe_size=2000,          # Full universe (~1,776 symbols after filters)
    min_price=10,
    min_value_traded_inr=3_000_000,
    min_volume=50_000,
    min_filters=5,               # 5 out of 6 2LYNCH filters must pass
    breakout_threshold=0.04,     # 4% gap-up
    start_year=2015,
    end_year=2025,
    entry_timeframe="5min",
    entry_cutoff_minutes=60,     # FEE: 60min NSE window (09:15–10:15 IST)
    max_stop_dist_pct=0.08,      # Max 8% stop distance guard
    trail_activation_pct=0.08,   # Trail stop activates at 8%+ gain
    trail_stop_pct=0.02,
    min_hold_days=3,
    time_stop_days=5,
    abnormal_profit_pct=0.10,
    abnormal_gap_exit_pct=0.20,
    follow_through_threshold=0.0,
)
```

### 2LYNCH Filters (6 total, 5/6 must pass)
| Filter | Code | Description |
|---|---|---|
| H | `close_pos_in_range >= 0.70` | Close in top 30% of day's range |
| N | `(prev_high - prev_low) < atr_20*0.5 OR prev_close < prev_open` | T-1 narrow/negative day (compression) |
| 2 | `ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0` | Not up 2 days in a row before breakout |
| Y | `prior_breakouts_30d <= 2` | Young breakout (≤2 gaps in last 30 days) |
| C | `vol_dryup_ratio < 1.3` | Volume dryup (compression before breakout) |
| L | `(close>MA20) + (ret_5d>0) + (r2_65>=0.70) >= 2` | Lynch trend confirmation |

### TI65 Status (as of 2026-03-01)

`ma_7` and `ma_65_sma` are now available in `feat_daily` (FEAT_DAILY_QUERY_VERSION bumped). TI65 = `ma_7 / ma_65_sma`.

**TI65 as same-day entry filter is WRONG** — tested and abandoned. Calmar dropped from 27.06 → 17.01.
**TI65 as universe pre-screener** — still to be explored (rank by avg TI65 over 20 days; enter only top-N by TI65 momentum).

---

## Experiment Registry

| Exp ID | Universe | FEE | Trades | Win% | Ann Ret | Max DD | Calmar |
|---|---|---|---|---|---|---|---|
*Phase 1 — no fixes*
| `21d35d9b903b7921` | 500 | none | 9,261 | 38.8% | 84.1% | 23.0% | 3.65 |
| `c515777b45ac0bf4` | 500 | 30min | 1,915 | 54.6% | 53.7% | 3.4% | 15.73 |
| `642f18cdf8c8e7f8` | 500 | 45min | 2,493 | 51.7% | 60.8% | 3.5% | 17.37 |
| `0ba48f889c978516` | 500 | 60min | 2,982 | 50.6% | 68.8% | 3.8% | 18.09 |
| `93da941f372a5d35` | ~1,776 | 30min | 4,600 | 54.3% | 136.3% | 6.9% | 19.74 |
| `1d1eaea2c327bc22` | ~1,776 | 45min | 5,936 | 51.7% | 157.5% | 7.0% | 22.38 |
| `efe9097265a9d555` | ~1,776 | 60min | 7,034 | 51.1% | 179.0% | 6.6% | 27.06 |
*Phase 2 — bad-5min-guard only (close>MA20)*
| `b7b4d6cb4e020bbb` | ~1,776 | 30min | 4,600 | 54.6% | 151.1% | 5.8% | 26.21 |
| `16c26c8a90af53fd` | ~1,776 | 45min | 5,948 | 51.9% | 172.7% | 5.9% | 29.01 |
| `63562d016320fb89` | ~1,776 | 60min | 7,055 | 51.2% | 194.4% | 6.6% | 29.40 |
*Phase 3 — TI65+guard (ABANDONED: TI65 hurts as same-day filter)*
| `4196bf737904c182` | ~1,776 | 60min | 5,349 | 48.2% | 125.5% | 7.4% | 17.01 |
*Phase 4 — Guard 1.5x + VBT conflict fix — PRODUCTION DEFAULT*
| `846ffa74be8d9f35` | ~1,776 | 30min | 4,604 | 54.6% | 149.6% | 3.7% | 40.39 |
| `90a56d84db1eea75` | ~1,776 | 45min | 5,955 | 51.9% | 172.0% | 4.5% | 38.45 |
| **`429c79ac45b65086`** | **~1,776** | **60min** | **7,073** | **51.3%** | **193.9%** | **4.4%** | **43.67** |
