# 2LYNCH Strategy — Reference

## What Is It

2LYNCH is a threshold breakout/breakdown momentum strategy for NSE equities. It triggers on a 5-minute intraday bar breaching a prior-day high (long) or low (short) by a fixed percentage threshold, subject to five pre-open admission filters.

**Two variants:**
- **Breakout (long)**: price crosses above `prev_high × (1 + threshold)` in the first 30 minutes
- **Breakdown (short)**: price crosses below `prev_low × (1 - threshold)` in the first 30 minutes

**Two threshold sizes:** 2% and 4% — four operating legs total.

---

## Filters

All five filters are evaluated on **prior-day** data (pre-open, before the trigger bar). `H` is the only exception — it is evaluated at the *end of the entry day* and governs carry/exit, not entry.

### N — Trend Confirmation

| Variant | Condition |
|---------|-----------|
| Breakout (long) | `prev_close >= prev_open` (prior day was bullish) |
| Breakdown (short) | `prev_close <= prev_open` (prior day was bearish) |

### Y — Prior Breakout Count

Counts the number of prior threshold-breakout (upside) days in the lookback window.

| Variant | Required count |
|---------|---------------|
| Breakout (long) | Exactly **2** |
| Breakdown 4% | Exactly **2** |
| Breakdown 2% | **1 or 2** |

### C — Consolidation Quality (Prior Day)

Proxy for pre-breakout consolidation tightness using three sub-conditions:

| Sub-condition | Metric |
|--------------|--------|
| Volume dry-up | `prev_vol_dryup_ratio <= 1.0` |
| ATR compression | `prev_atr_compress_ratio <= 1.10` |
| Range compression | `prev_range_percentile <= 0.60` |

- **Breakout**: require at least **2 of 3** sub-conditions (c_score ≥ 2)
- **Breakdown**: prefer looser pre-breakdown structure (`c_score <= 1`)

### L — Trend Filter

| Variant | Condition |
|---------|-----------|
| Breakout (long) | `close >= ma_20` AND `ret_5d >= 0` |
| Breakdown (short) | `close < ma_20` AND `ret_5d < 0` |

### H — Hold Quality (End-of-Day)

Measures where the breakout/breakdown day's close lands within the day's range.

| Variant | Condition |
|---------|-----------|
| Breakout (long) | `close_pos_in_range >= 0.70` |
| Breakdown (short) | `close_pos_in_range <= 0.30` |

**H is not known at entry time.** It is evaluated at end-of-day and governs overnight carry:

- `H = true`: carry overnight; tighten stop to at least breakeven
- `H = false` (long): if still in-favour at close → carry with tightened stop; if close is back through entry → exit with `WEAK_CLOSE_EXIT`
- `H = false` (short): if still in-favour at close (close < entry for shorts) → carry overnight with breakeven stop; if losing (close ≥ entry) → `WEAK_CLOSE_EXIT`

---

## Entry

| Variant | Value |
|---------|-------|
| Entry window | 09:20–09:45 IST (bars 2–7 only; first 5-minute bar excluded) |
| Trigger bar | 5-minute bar crosses threshold |
| Entry price | Bar open if gap-through; otherwise threshold price |
| Admission filters | N, Y, C, L (all must pass pre-open) |

---

## Exit Rules

### Trailing Stop

Initial stop is set at `entry_price - ATR × multiplier` (long) or `entry_price + ATR × multiplier` (short). Stop trails as the trade moves in favour.

### R-Ladder Ratchet

| Profit Level | Stop Moved To |
|-------------|--------------|
| +2R | Breakeven |
| +3R | Lock +1R |
| +4R | Lock +2R |

Stop updates take effect from the **next** 5-minute bar (no same-bar execution).

**Fill rule**: if a bar opens through the active stop → fill at bar open; if range crosses stop intra-bar → fill at stop price.

### Time Stop

Positions are closed at end-of-day on **day 5** if not stopped out earlier.

### Weak-Close Exit

If the breakout day's close is back through entry and `H = false` → exit at close (`WEAK_CLOSE_EXIT`).

---

## Position Sizing

Risk-based: **1R per trade** (R = distance from entry to initial stop). Universe and max concurrent positions are controlled by session parameters.

---

## Canonical Operating Points

All runs use window `2015-01-01 → 2026-04-17`, universe size 2000. Wave-1 fixes applied: H-carry enabled, entry gate at 09:20 (5 min after open), filter direction parity (N/H correct for shorts), pnl_r guard.

| Leg | Threshold | Exp ID | Avg Annual | Max DD | Calmar | Trades | Neg Years |
|-----|-----------|--------|-----------|--------|--------|--------|-----------|
| Breakout long | 4% | `0cd353d536dd6f91` | +54.1% | 3.16% | 17.1 | 2,211 | 0 |
| Breakout long | 2% | `f923e1a9517d9b2c` | +121.8% | 2.73% | 44.6 | 7,078 | 0 |
| Breakdown short | 4% | `f6e7646ac932697d` | +3.1% | 0.74% | 4.2 | 258 | 2 |
| Breakdown short | 2% | `b769984bf6d0c5c7` | +8.1% | 1.99% | 4.1 | 790 | 0 |

Run the full 4-leg preset (update dates as needed):
```bash
doppler run -- uv run python scripts/run_full_operating_point.py \
  --start-year 2015 --end-year 2026 \
  --start-date 2015-01-01 --end-date 2026-04-17 \
  --universe-size 2000 --parallel-workers 4 --force
```

---

## Short-Side Asymmetry

Shorts are **not** a mirrored clone of longs. Key differences:

- `H = false` forces same-day close exit (no overnight short carry)
- `N` filter requires prior-day *bearish* bar (opposite of long)
- `Y` count uses prior *upside* breakout days (measures exhaustion potential)
- `C` prefers looser structure (c_score ≤ 1) — tight consolidation is a long setup, not a short one
- `L` requires `close < ma_20` and `ret_5d < 0` (downtrend alignment)
- Do not apply 4% breakdown engine params to 2% breakdown — they are tuned independently

---

## Strategy Code Locations

| File | Purpose |
|------|---------|
| `services/backtest/filters.py` | 2LYNCH filter logic (N, Y, C, L, H) — Python + SQL |
| `services/backtest/backtest_presets.py` | `StrategyPreset` frozen dataclass; 4 canonical presets |
| `services/backtest/signal_models.py` | `BacktestSignal` dataclass with filter flags |
| `services/paper/engine/paper_runtime.py` | Live/replay engine using same filter logic |
| `features/registry.py` | Feature definitions used by all filter computations |
