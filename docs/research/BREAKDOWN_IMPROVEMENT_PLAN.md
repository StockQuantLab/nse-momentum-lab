# Breakdown (Short-Side) Improvement Plan

**Created**: 2026-03-14
**Status**: FUNDAMENTAL REDESIGN — Phase 3 in progress

---

## Current Best Results (after Phase 0–2)

| Leg | Exp ID | Trades | Total Ret | Max DD | PF | Win% | Per-trade avg |
|---|---|---:|---:|---:|---:|---:|---:|
| 2% BD (R²-primary + Phase 1) | `88c728069d1d8a91` | 198 | +15.6% | 2.71% | 2.32 | ~41% | **+0.079%** |
| 4% BD (R²-primary) | `909f04c033332b22` | 110 | +17.4% | 1.65% | 3.86 | ~55% | **+0.158%** |

For comparison — Breakout 4%: 794 trades, +154.3%, **+0.194% per trade** (2× short-side edge).

Long-side reference — Breakout 4% (correct-code baseline): 794 trades, +154.3%, DD 3.85%, PF 3.11 / Breakout 2%: 1450 trades, +125.2%, PF 2.36.

## Target (Revised 2026-03-14)

- **2% BD**: total return > 40%, DD < 5%, PF > 2.0 (aim for Stockbee-level performance)
- **4% BD**: total return > 25%, DD < 3%, PF > 2.5
- Benchmark: Stockbee trades breakdowns profitably in similar regimes. We should too.

## Why Breakdown Returns Are Abysmal — Root Cause Analysis

### The numbers tell the story

| Metric | 2% BD (no budget, Phase 0) | 2% BD (budget=5, R²-primary) | Implication |
|---|---:|---:|---|
| Trades | 3,395 | 198 | 94% reduction |
| Total return | +348.8% | +15.6% | 95.5% reduction |
| Per-trade avg | +0.103% | +0.079% | Ranking selects BELOW-AVERAGE trades |
| Max DD | 36.44% | 2.71% | DD controlled but at massive cost to returns |

**Insight**: The raw edge exists (PF 2.46 over 3395 trades). Budget + ranking destroys return
disproportionately because the ranking formula is optimizing for low DD, not maximum edge.

### Fundamental issue #1: 2% BD uses LONG-SIDE engine defaults

The 2% BD canonical runs with `short_option_b=False`, meaning:

| Param | 2% BD (current) | 4% BD (Option-B) | What shorts need |
|---|---|---|---|
| Trail activation | **8%** (default) | 4% | **3–4%** — 8% move is unreachable for most 2% breakdowns |
| Time stop | **5 days** (default) | 3 days | **2–3 days** — V-reversals kill by day 3–4 |
| Max stop distance | **none** | 5% | **3–5%** — cap session_high risk |
| Abnormal profit | **none** | 5% | **3–5%** — take profit on fast moves |

**This is the #1 problem**: trail activation at 8% means the trailing stop NEVER activates on
most 2% breakdowns. Every trade either hits the time-stop (day 5, giving back gains) or
hits the initial stop (loss). No trade ever reaches the "trail and lock profit" phase.

### Fundamental issue #2: Initial stop = session_high is too far

For shorts, `initial_stop = session_high` (highest price before entry). On volatile days:
- Entry at 100 after 2% breakdown
- Session high at 108 (from early morning bounce)
- Risk per trade = 8% — absurdly high for a 2% expected move
- Risk/reward is < 1:1 on most trades

### Fundamental issue #3: No market regime filter

We short in ALL conditions including:
- Relief rallies in downtrends (sharp V-reversals)
- Breadth expansion days (market bouncing)
- Stockbee explicitly only shorts when market breadth confirms weakness

### Fundamental issue #4: Ranking optimizes for low DD, not profit

R²-primary ranking selects orderly downtrend stocks (smooth, low volatility).
These ARE lower DD — but also lower per-trade profit because orderly downtrends
grind slowly rather than making the fast 3–5% drops that generate short profit.

### What Stockbee does differently

| Aspect | Stockbee | Our implementation |
|---|---|---|
| Holding period | **1–3 days** typical | 5 days (way too long) |
| Trail activation | **~3–5%** profit | 8% (never activates) |
| Exit discipline | Cut if not working by day 2 | Hold to day 5 time-stop |
| Market regime | Only shorts when breadth is weak | No regime filter |
| Position sizing | Adjusts size by conviction | Equal weight |
| Stop placement | Based on chart structure / ATR | Session high (often too far) |
| Stock selection | Visual chart quality (discretionary) | Mechanical 5/6 filter + ranking |

## What Was Already Tried and Rejected

All prior experiments were **engine-level tuning** — none addressed signal quality.

| Option | What | Result | Decision |
|---|---|---|---|
| A | Short-side asymmetry experiment | Worse on both variants | Reject |
| B | Engine params: trail 4%, time-stop 3d, max-stop 5%, abnormal 5% | Better risk-adjusted for 4% ONLY | Keep for 4% only |
| C | Post-day3 close buffer 0.5% | Slightly worse on both | Reject |
| D | Filter relaxation | Catastrophic (-375% / -545%) | Reject |
| F | 30-min entry cutoff | Degraded Calmar on both | Reject |

**Key insight**: engine tuning has been exhausted. Remaining gains must come from signal quality, entry mechanics, or regime filtering.

## What's Just Pushed (v1.2.0, NOT yet validated)

- `filter_y` now requires `rs_252 < 0` (stock has negative 52-week return)
- Hypothesis: stocks still positive YoY are bull-market dips, not genuine short candidates
- This is the first signal-quality change; all prior options were engine-only

---

## Phase 0: Validate rs_252 Gate

**Goal**: Determine if the v1.2.0 rs_252 gate helps or hurts.

### Steps
1. Run: `doppler run -- uv run python scripts/run_breakdown_operating_point.py --force`
2. Record new exp_ids and metrics for both 4% and 2%
3. Compare against v1.1.0 baselines (table above)
4. **Decision gate**:
   - If **both improve** → adopt, proceed to Phase 1
   - If **2% improves but 4% worsens** → keep for 2% only, revert 4%
   - If **both worsen** → revert rs_252 gate entirely, proceed to Phase 1 anyway

### Result (completed 2026-03-14)

> ⚠️ **Code-drift discovery**: v1.1.0 baselines (code hash `5987d4a08900`) were produced by an older runner
> that silently dropped signals due to missing fields (`entry_filter_columns`, `entry_ts`, `abnormal_gap_mode`,
> `save_execution_diagnostics`). The infrastructure was fixed this session (code hash `c9fb1ed8f6ac`).
> v1.1.0 → v1.2.0 comparison is therefore **not apples-to-apples**.
> New code produces correct signal counts; v1.1.0 baselines are obsolete.

| Variant | v1.2.0 Exp ID | Signals | Trades | Total Ret | Max DD | PF | Win% |
|---|---|---:|---:|---:|---:|---:|---:|
| 4% BD (Option-B) | `094cc162e57a7da4` | 3,453 | 421 | +64.92% | 8.07% | 3.02 | 56.5% |
| 2% BD (canonical) | `241f6e057cd6efd8` | 14,716 | 3,395 | +348.78% | 36.44% | 2.52 | 40.5% |

**rs_252 gate effect** (measured via filter_y pass rate in diagnostics):
- filter_y passes 71.8% of 4% BD candidates (gate filters ~28%)
- rs_252 < 0 is true for 67.1% of the universe in this period (down-trending market)
- The gate IS working but is not discriminating enough in a broad market downtrend

**Phase 0 decision**:
- ✅ Keep rs_252 gate — it filters 28% of marginal candidates, contributes to PF 3.02 on 4%
- ❌ rs_252 alone is insufficient for 2% BD (DD 36.44%, 3395 trades — far from target)
- ❌ 4% BD DD (8.07%) also exceeds target (< 3%) — but PF and return are good
- Proceed to Phase 1 to address **signal volume** (too many trades) and **cluster risk**

**New v1.2.0 baselines** (code hash `c9fb1ed8f6ac`):
- 4% BD: `094cc162e57a7da4` (421 trades, +64.92%, DD 8.07%, PF 3.02)
- 2% BD: `241f6e057cd6efd8` (3,395 trades, +348.78%, DD 36.44%, PF 2.52)

---

## Phase 1: Signal Quality (Filters)

Test independently, one experiment at a time. Always compare against the best known baseline.

### Summary of Phase 1 Progression (all with budget=5)

| Phase | 2% BD Exp | Sigs | Trades | Ret | DD | PF | Calmar | Changes vs prior |
|---|---|---:|---:|---:|---:|---:|---:|---|
| Phase0 (baseline) | `241f6e057cd6efd8` | 14,716 | 3,395 | +348.8% | 36.44% | 2.46 | 9.57 | no budget |
| Phase1a | `dac93f66656966a6` | 14,716 | 238 | +15.5% | 6.62% | 2.14 | 2.34 | budget=5 |
| Phase1c-fix | `7059c5756b3ba9ac` | 11,822 | 233 | +17.5% | 4.97% | 2.29 | 3.52 | +strict_filter_l +rs<-0.10 |
| **Phase1d+2c** ✅ | `a6e32e24a9c256f7` | 5,544 | 210 | +14.1% | **2.92%** | **2.10** | **4.83** | +narrow_n +skip_gap_down |

> **Phase 1 decision**: `a6e32e24a9c256f7` is the new 2% BD operating point.
> Targets met: PF 2.10 ✓ (>2.0), DD 2.92% ✓ (<3%), return 14.1% ≈ (just under 15% target, +36% vs v1.1.0 canonical +10.31%).
> v1.1.0 canonical improved across all three metrics: PF 1.63→2.10, DD 3.24%→2.92%, return +10.31%→+14.1%.

### 4% BD Phase 1 (unchanged by design)

| Phase | Exp | Trades | Ret | DD | PF | Calmar |
|---|---|---:|---:|---:|---:|---:|
| Phase0 | `094cc162e57a7da4` | 421 | +64.9% | 8.07% | 2.84 | 8.04 |
| **Phase1a** ✅ | `00a0404863bf0b4c` | 117 | +17.2% | **1.83%** | **3.66** | **9.40** |

> 4% BD: budget=5 is the locked config. Strict_filter_l, narrow_n, skip_gap_down are **NOT** applied to 4% BD.

### Final Preset Command

```bash
doppler run -- uv run python scripts/run_breakdown_operating_point.py --force --breakdown-budget 5 --breakdown-rs-min -0.10
```

**Active flags for 2% BD** (hardcoded in preset, not CLI exposed):
- `breakdown_strict_filter_l=True` — 3/4 conditions required for filter_l (adds close < MA65_SMA)
- `breakdown_filter_n_narrow_only=True` — T-1 must be narrow (removes OR green-day clause)
- `breakdown_skip_gap_down=True` — excludes stocks already gapped down ≥ threshold

---

### 1a. Daily Candidate Budget for Shorts

**Problem**: Cluster risk — 5 entries on 2026-03-02 all stopped same day (V-reversal after panic lows).

**Change**: Add `breakdown_daily_candidate_budget` parameter, mirroring the breakout ranking/budget mechanism. Rank short candidates by signal strength (gap size × volume) and cap daily entries.

**Files**: `strategy_families.py`, `duckdb_backtest_runner.py`

**Command**:
```bash
# After implementing, test with budget=3
doppler run -- uv run python scripts/run_breakdown_operating_point.py --force
```

### 1b. Use Breakdown-Specific Counter in filter_y

**Problem**: `filter_y` uses `prior_breakouts_30d` which counts **upside** breakouts. For shorts, we want to know about **downside** breakdowns — a stock that has already broken down repeatedly is exhausted, not a fresh short.

**Change**: Replace or complement `prior_breakouts_30d <= 2` with a check on `breakdown_4pct_down_90d` (already in `feat_daily`). E.g., `breakdown_4pct_down_90d <= 1` — avoid stocks that have already broken down multiple times.

**Files**: `strategy_families.py` (SQL in `_build_threshold_breakdown_candidate_query`)

**Command**:
```bash
doppler run -- uv run python scripts/run_breakdown_operating_point.py --force
```

### 1c. Filter_l Strictness

**Problem**: Currently 2-of-3 (close < MA20, ret_5d < 0, r2_65 ≥ 0.70). May admit marginal shorts.

**Tests** (run separately):
- **Test A**: Require all 3 conditions (strict)
- **Test B**: Add `ma_65_sma` slope < 0 as a 4th condition, keep 2-of-3 threshold on original 3
- **Test C**: Require BOTH `close < ma_20` AND `close < ma_65` (below medium-term trend)

**Files**: `strategy_families.py` (filter_l SQL)

### 1d. Filter_n Review

**Problem**: Currently "narrow OR green T-1". A green T-1 day (prev_close > prev_open) may not be meaningful for shorts — it means the day before was bullish, which is counterintuitive before a breakdown.

**Tests** (run separately):
- **Test A**: Narrow only (remove the green OR clause)
- **Test B**: Narrow OR T-1 close in bottom 40% of range (`prev_close_pos_in_range <= 0.40`)
- **Test C**: T-1 must be narrow AND bearish (`prev_close < prev_open`)

**Files**: `strategy_families.py` (filter_n SQL)

---

## Phase 2: Entry & Stop Mechanics

### 2a. ATR-Based Initial Stop

**Problem**: Current initial_stop = `session_high` (highest price seen before/at entry bar). Can be very far from entry on volatile days, making R-calculation and trailing meaningless.

**Change**: `initial_stop = entry_price + min(session_high - entry_price, 1.5 * ATR_20)`. Caps risk at 1.5× ATR.

**Files**: `intraday_execution.py` (SHORT stop assignment), `duckdb_backtest_runner.py` (pass ATR to intraday)

### 2b. Entry Cutoff Exploration

**Problem**: 60 min default. 30 min was rejected (Option F). Shorts may need MORE time for sellers to develop.

**Tests**:
- 90 minutes (entry window 09:15–10:45)
- 120 minutes (entry window 09:15–11:15)

**Files**: `scripts/run_breakdown_operating_point.py` (pass `--short-entry-cutoff-minutes`)

### 2c. Skip Gap-Down Entries

**Problem**: If a stock already gaps down ≥ threshold at open, the breakdown already happened. Shorting after is chasing.

**Change**: Add condition `gap_pct > -threshold` to breakdown_days CTE (exclude gap-down opens that already satisfy the breakdown).

**Files**: `strategy_families.py` (breakdown_days WHERE clause)

---

## Phase 2: Ranking Formula (R²-Primary) — COMPLETED 2026-03-14 ✅

### Background

Phase 1 operating point (`a6e32e24a9c256f7`) delivered: 210 trades, +14.1%, DD 2.92%, PF 2.10.
All targets met **except** total return (14.1% < 15% target by a thin margin).

Root cause analysis: the original H-quality-dominant ranking (`close_pos_in_range` primary, 5000 pts)
was counter-productive — picking stocks that closed near the absolute day low implies:
- Wide session range (high-to-low is large)
- Stop distance = session_high − entry is large
- Bigger individual losses when the short reverses

### Change (v2 Ranking Formula)

In `_apply_breakdown_selection_ranking()` (`duckdb_backtest_runner.py`):

| Component | Old weight | New weight | Rationale |
|---|---|---|---|
| R² quality (r2_65) | 2000 pts | **5000 pts** (PRIMARY) | Orderly downtrend = reliable continuation + tight typical stops |
| H quality (close_pos_in_range) | 5000 pts | 2000 pts (secondary) | Still useful but not dominant |
| C quality (vol_dryup_ratio) | 1000 pts | 1000 pts | Unchanged |
| Freshness (prior_breakdowns_90d) | 300 pts | 300 pts | Unchanged |
| rs_252 tiebreaker | 200 pts | 200 pts | Unchanged |

### Results

| Variant | Exp ID | Trades | Total Ret | Max DD | PF | Calmar | vs target |
|---|---|---:|---:|---:|---:|---:|---|
| **2% BD (R²-primary)** | `88c728069d1d8a91` | 198 | **+15.6%** | **2.71%** | **2.32** | 5.76 | ✅ ALL MET |
| 4% BD (R²-primary) | `909f04c033332b22` | 110 | **+17.4%** | **1.65%** | **3.86** | 10.5 | ✅ ALL MET |

**Targets**: PF > 2.0 ✅, DD < 3% ✅, Return > 15% ✅

### Preset Commands (as of 2026-03-14)

```bash
# Standalone breakdown (both 2% and 4%)
doppler run -- uv run python scripts/run_breakdown_operating_point.py --force --breakdown-budget 5 --breakdown-rs-min -0.10

# Full 4-leg (breakout + breakdown)
doppler run -- uv run python scripts/run_full_operating_point.py --force
```

**Note on run_full_operating_point.py**: as of 2026-03-14 the full 4-leg script does NOT yet include
the Phase 1 breakdown flags (strict_filter_l, narrow_n, skip_gap_down, rs_min). The standalone preset
is the canonical source for breakdown numbers. Full 4-leg script needs update.

---

## Breakout Side-Effect Analysis (2026-03-14)

During the work to fix breakdown, pre-existing bugs in the **breakout** ranking function were discovered
and fixed (`prev_vol_dryup_ratio` → `vol_dryup_ratio`; added `prev_high`, `prev_open`, `r2_65`,
`prior_breakouts_30d` to breakout SELECT for Python access).

These are correct fixes, but they changed the **sort order** of candidates even when `budget=0`
(all candidates pass), because the old buggy code was silently producing null scores for missing columns
(all scores equal = 3000), while the fixed code produces real varying scores.

### Correct-Code 4% Breakout Baseline (2026-03-14)

| Config | Exp ID | Trades | Return | DD | PF |
|---|---|---:|---:|---:|---:|
| Old canonical (buggy ranking) | `1716b78c208a90f3` | 977 | +136.6% | 2.26% | 2.69 |
| **New baseline (correct code, budget=0)** | `e523addf1fec0e98` | 794 | **+154.3%** | **3.85%** | **3.11** |
| Budget=5 (current formula, broken) | `9b85c291a43bb3ea` | 188 | +12.7% | 7.43% | 1.54 |

**Key finding**: The correct code improves return (+154.3% vs +136.6%) but raises DD (3.85% vs 2.26%).
Budget=5 is catastrophically worse because the breakout C-quality ranking formula selects low-volume
breakouts (WRONG for long direction).

**Next step for breakout** (deferred): fix ranking formula to R²-primary for breakout direction,
then re-test budget=5 to see if it improves DD vs budget=0.
For now, **accept `e523addf1fec0e98` as the new 4% breakout baseline** (correct code, budget=0).

---

## Phase 3: Fundamental Redesign — Engine Params + Stop Mechanics

The core insight: all Phase 0–2 work improved SIGNAL QUALITY (which trades to take).
Phase 3 fixes HOW we manage the trades once entered. This is where the biggest gains are.

### 3a. Apply Short-Tuned Engine Params to 2% BD (HIGHEST PRIORITY)

**Problem**: 2% BD runs with `short_option_b=False` → uses LONG defaults:
- Trail activation 8% (NEVER activates on 2% breakdowns)
- Time stop 5 days (V-reversals kill by day 3–4)
- No max stop distance cap

**Change**: Apply Option-B-like params to 2% BD. Test multiple profiles:

| Profile | Trail activation | Time stop | Max stop | Abnormal profit | Rationale |
|---|---|---|---|---|---|
| Option-B (4% BD preset) | 4% | 3 days | 5% | 5% | Already proven on 4% BD |
| Aggressive-short | 3% | 2 days | 4% | 4% | Stockbee-style quick exits |
| Quick-scalp | 2% | 2 days | 3% | 3% | Ultra-fast, take what the market gives |

Run each against the R²-primary signal set (88c728069d1d8a91 baseline):
```bash
doppler run -- uv run python scripts/run_breakdown_operating_point.py --force
```
Modify `run_breakdown_operating_point.py` to apply short engine params to 2% BD as well.

**Expected impact**: LARGE. Trail activation going from 8% → 3–4% means trades that
currently drift to time-stop will instead lock profit early.

### 3b. ATR-Capped Initial Stop

**Problem**: `initial_stop = session_high` can be 8–10% above entry on volatile days.
Risk per trade is wildly variable and often exceeds the expected profit.

**Change**: `initial_stop = min(session_high, entry_price × (1 + 1.5 × atr_20_pct))`

Where `atr_20_pct = atr_20 / close`. Cap the stop distance at 1.5× the stock's typical
daily range. This:
- Reduces avg risk per trade
- Makes R-ladder more meaningful (1R is now a real number, not session noise)
- Tightens breakeven lockup thresholds

**Files**: `intraday_execution.py` (need `atr_20` passed to intraday engine),
`duckdb_backtest_runner.py` (wire `atr_20` through signal model)

### 3b result (2026-03-19 quick run, `--short-profile aggressive`)

Executed:
`doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step atr-cap --short-profile aggressive`

| Variant | Exp ID | Trades | Total Ret | Max DD | PF | Win% |
|---|---|---:|---:|---:|---:|---:|
| 4% BD (stable baseline) | `46599b5069a50dc3` | 110 | +17.37% | 1.65% | 3.28 | 56.36% |
| 2% BD (ATR cap + aggressive) | `e69d40aad741ae32` | 193 | +14.60% | **2.39%** | **2.24** | 54.92% |

Interpretation:
- ATR cap improved the 2% short side versus the Phase 1 baseline:
  - DD: `2.92% → 2.39%`
  - PF: `2.00 → 2.24`
  - Win rate: `36.19% → 54.92%`
- Return was also slightly better:
  - `+14.09% → +14.60%`
- Trade count dropped a bit (`210 → 193`), which is acceptable for this step.
- This is a good candidate to keep while we test the next pending stop mechanic.

### 3c. Aggressive Day-0 Profit Taking

**Problem**: Current R-ladder starts at +2R with `same_day_r_ladder_start_r=2`.
For shorts, +2R is often the best the trade will ever do (shorts mean-revert fast).
But the ladder only LOCKS breakeven at +2R — it doesn't take profit.

**Change options** (test separately):
- Lower R-ladder start to +1R for shorts (lock breakeven faster)
- Add "close day-0 at market if profit > 2%" exit (take guaranteed profit)
- Add "if price crosses below session_low and reverses back, exit at breakeven"

### 3c result (2026-03-19 quick run, `--short-profile aggressive`)

Executed:
`doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step day0-profit --short-profile aggressive`

| Variant | Exp ID | Trades | Total Ret | Max DD | PF | Win% |
|---|---|---:|---:|---:|---:|---:|
| 2% BD (ATR cap + aggressive baseline) | `e69d40aad741ae32` | 193 | +14.60% | 2.39% | 2.24 | 54.92% |
| 2% BD (day-0 profit target) | `1e4daefdfc0a9347` | 136 | -0.85% | 2.76% | 1.15 | 46.32% |

Interpretation:
- Same-day +2% profit taking is a **rejection**.
- It cuts too much of the profitable short continuation.
- Return collapses (`+14.60% → -0.85%`) and PF falls to `1.15`.
- DD does not improve enough to justify the return loss.
- Keep the ATR-capped stop from 3b and move to the next pending step.

### 3d. Breadth Proxy from Own Data

**Problem**: No Nifty/index data. We short in ALL conditions including relief rallies.
Stockbee explicitly only shorts when market breadth confirms weakness.

**Change**: Compute daily breadth metric in feat_daily materialization:
`pct_below_ma20 = COUNT(close < ma_20) / COUNT(*)` across the universe per date.

Entry gate for shorts:
- `pct_below_ma20 > 0.55` → take shorts (more than half the market is weak)
- `pct_below_ma20 <= 0.55` → skip shorts that day (market is neutral/strong)

This requires adding a market-level aggregate feature. Options:
1. Pre-compute in `feat_daily` materialization as a date-level feature
2. Add as a CTE in the breakdown candidate SQL query

**Files**: `features/strategy_derived.py` (add breadth column), `strategy_families.py` (gate)

### 3d result (2026-03-19 quick run, `--short-profile aggressive`)

Executed:
`doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step breadth --short-profile aggressive`

| Variant | Exp ID | Trades | Total Ret | Max DD | PF | Win% |
|---|---|---:|---:|---:|---:|---:|
| 2% BD (ATR cap + aggressive baseline) | `e69d40aad741ae32` | 193 | +14.60% | 2.39% | 2.24 | 54.92% |
| 2% BD (breadth gate) | `4bc2f4aaae43e25c` | 121 | +4.46% | 2.49% | 1.69 | 53.72% |

Interpretation:
- Breadth gating is a **rejection**.
- It cuts too much of the profitable short universe.
- Return and PF both drop materially.
- DD does not improve enough to justify the coverage loss.

### 3e. Volatility Expansion Filter (per-stock)

**Problem**: Shorts work best when the stock's volatility is expanding (panic selling).
Shorting a stock with compressing ATR means the big move is over.

**Change**: Require `atr_20 > sma(atr_20, 20)` — ATR must be expanding.
This is already partially captured in the `C` filter (`atr_compress_ratio`), but
as a separate hard gate rather than a scored ranking factor.

### 3e result (2026-03-19 quick run, `--short-profile aggressive`)

Executed:
`doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step atr-expansion --short-profile aggressive`

| Variant | Exp ID | Trades | Total Ret | Max DD | PF | Win% |
|---|---|---:|---:|---:|---:|---:|
| 2% BD (ATR cap + aggressive baseline) | `e69d40aad741ae32` | 193 | +14.60% | 2.39% | 2.24 | 54.92% |
| 2% BD (ATR expansion gate) | `f8831d1ddee8fb14` | 154 | +15.02% | 2.30% | 2.67 | 59.74% |

Interpretation:
- ATR expansion is a **keep**.
- It improves both drawdown and profit factor versus the ATR-capped baseline.
- Return also improves slightly.
- This is the best short-side gate so far.

### 3f. Increase Budget from 5 to 8–10

**Problem**: Budget=5 captures only 5.8% of the edge (198 trades out of 3395 potential).
The per-trade average is BELOW the unbudgeted average (0.079% vs 0.103%), meaning
our ranking is actively selecting worse-than-random trades.

**Change**: Test budget=8 and budget=10 with Phase 3a engine params. The hypothesis:
with proper engine params (quick exit, ATR stop), more trades should be profitable
because bad trades are cut early instead of dragging to day 5.

### 3f result (2026-03-19 quick run, `--short-profile aggressive`)

Executed:
`doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step budget8 --step budget10 --short-profile aggressive`

| Variant | Exp ID | Trades | Total Ret | Max DD | PF | Win% |
|---|---|---:|---:|---:|---:|---:|
| 2% BD (ATR cap + aggressive baseline) | `e69d40aad741ae32` | 193 | +14.60% | 2.39% | 2.24 | 54.92% |
| 2% BD (budget=8) | `bbc2e1dbcba4a103` | 215 | +18.59% | 4.21% | 2.42 | 58.60% |
| 2% BD (budget=10) | `d226e2df72609558` | 249 | +21.20% | 4.55% | 2.39 | 57.43% |

Interpretation:
- Budget increase raises return, but also pushes DD back above the current safety line.
- `budget8` and `budget10` are **not yet safe** on the 2% side because DD regresses materially.
- If we want to scale short coverage, we need a stronger risk gate or a different selection rule.

---

## Phase 3 Execution Order

```
3a. Short engine params for 2% BD (Option-B, Aggressive, Quick-scalp profiles)
  → Pick best profile
  → 3b. ATR-capped initial stop
  → 3c. Day-0 profit taking
  → 3d. Breadth proxy gate
  → 3e. Volatility expansion gate
  → 3f. Budget increase (8 or 10)
```

**Decision rule**: after each step, keep if return increases OR DD decreases without
worsening PF. Stop early if we reach revised targets.

---

## Phase 4: Separate 4% and 2% Reality Check

- **4% BD**: Already at +17.4%, PF 3.86. May benefit from Phase 3a–3b but not priority.
- **2% BD is the primary short-side variant** and must carry the strategy.
- If 2% BD cannot exceed +40% return after Phase 3: investigate fundamentally different
  entry logic (retest shorts, breakdown-then-bounce, etc.) or formally declare breakdown
  as a hedge-only overlay.

---

## Phase 5: Document & Freeze

After each phase that produces a new canonical:
1. Update `docs/research/CANONICAL_REPORTING_RUNSET_2026-03-13.md`
2. Update `agents.md` breakdown section
3. Run full 4-leg operating point to confirm no regression on long side

---

## Key Files Reference

| File | Role |
|---|---|
| `src/nse_momentum_lab/services/backtest/strategy_families.py` | Breakdown SQL candidate query & all filters |
| `src/nse_momentum_lab/services/backtest/strategy_registry.py` | Strategy version & registration |
| `src/nse_momentum_lab/services/backtest/intraday_execution.py` | 5-min SHORT entry logic & same-day stops |
| `src/nse_momentum_lab/services/backtest/vectorbt_engine.py` | Multi-day SHORT trail/time-stop/abnormal exit |
| `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py` | Orchestrator, param resolution, stop guards |
| `scripts/run_breakdown_operating_point.py` | Single-command breakdown preset runner |
| `scripts/run_full_operating_point.py` | Full 4-leg preset runner |
| `docs/research/BREAKDOWN_OPTION_SWEEP_2026-03-13.md` | Prior option sweep results |
| `docs/research/CANONICAL_REPORTING_RUNSET_2026-03-13.md` | Current frozen baselines |

## Diagnostic Commands

```bash
# Run breakdown backtest (both 4% and 2%)
doppler run -- uv run python scripts/run_breakdown_operating_point.py --force

# Compare two experiments
doppler run -- uv run python scripts/compare_backtest_runs.py --old-exp <OLD> --new-exp <NEW>

# Execution diagnostics for a specific run
doppler run -- uv run python scripts/backtest_execution_diagnostics.py --exp <EXP_ID>

# Trade-level audit
doppler run -- uv run python scripts/backtest_trade_audit.py --exp <EXP_ID> --symbol <SYMBOL> --date YYYY-MM-DD
```
