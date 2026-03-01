# ADR-007: Strategy Specification — 4% + 2LYNCH (Stockbee-style)

Status: Accepted

## Context

The goal is to test whether a Stockbee-style “4% breakout / momentum burst” approach, plus the 2LYNCH quality layer, transfers to Indian equities.

The strategy must be encoded deterministically so backtests, walk-forward runs, and paper-trading simulations are reproducible.

## Decision

### Scope + timeframe

- Phase 1 is **EOD-only (daily bars)** for selection, backtesting, and paper ledger.
- Intraday data (1-min/5-min) is **optional** and explicitly deferred for Phase 1.
- Phase 1 is **long-only**. Bearish/short scans may be documented for reference but are not required for Phase 1 execution.

Rationale:
- The scan rules and 2LYNCH letters are defined on daily OHLCV and daily derived features (ATR, ranges, volume averages).
- “FEE (find and enter early)” is execution-path dependent; it requires intraday data to model exactly, so Phase 1 uses conservative daily execution variants.

### Breakout detection (daily)

For a 4% breakout threshold $T = 1.04 \cdot C_{t-1}$:

- **Touched intraday** (breakout occurred at some point): $H_t \ge T$
- **Confirmed by close** (stronger): $C_t \ge T$
- **Fizzled** (touched but failed): $H_t \ge T$ and $C_t < T$
- **Gap breakout**: $O_t \ge T$

Phase 1 uses daily bars to classify these outcomes; exact “when it happened” is not available without intraday.

### 2LYNCH (quality layer)

2LYNCH is treated as a selection-quality layer over the breakout scan.

Letters (as operationalized for Phase 1):

- **2**: Not up 2 days in a row before breakout (a small up day can be fine; the intent is to avoid extended/overheated multi-day pops).
- **L**: Linearity/orderliness of the prior move (avoid whipsaw “drunken walk”).
- **Y**: Young trend: 1st–3rd breakout from consolidation is lower risk; later breakouts fail more.
- **N**: Narrow range day or negative day pre-breakout.
- **C**: Consolidation/pullback is shallow, orderly, compact, narrow-range, low-volume; no more than one 4% day inside the consolidation.
- **H**: Close near high of the day.

Implementation note:
- These are pattern concepts; Phase 1 encodes **strict numeric proxies** and stores all pass/fail reasons in `reason_json`.

### Execution (FEE boundary)

Stockbee’s “FEE (find and enter early)” implies entering on the breakout day as early as possible (often on first touch).

Execution model:

- **Setup qualification:** daily bars (`v_daily`) for 2LYNCH checks.
- **Entry timing:** 5-minute bars (`v_5min`) on breakout day.
- **Entry price:** first touch of breakout trigger (`prev_close * 1.04`) intraday.
- **Initial stop:** low known at entry time (running day-low up to entry bar open).

Notes:
- This removes the daily-bar look-ahead issue for entry timing.
- Intra-bar sequencing inside a 5-minute candle is still approximated (tick-level is not modeled).

### Stops and exits (implemented rules)

The following stop/exit rules are implemented in `VectorBTEngine._build_exit_signals()`:

1. **Initial stop**: **low of the breakout day (entry day)**.
   - Rationale: keeps risk tight on failed breakouts and aligns with the documented Stockbee-style execution intent.
2. **Breakeven stop**: Once close > entry price, stop moves up to entry.
   - Prevents a winner from becoming a loser.
3. **Trailing stop**: Activate once up **+8%**, trail at **2%** below highest high.
   - Captures large moves while giving room for normal volatility.
4. **Post-day-3 stop tightening**: From day 3 onward, stop ratchets using daily low progression.
   - Daily-bar approximation of Stockbee's "trail from day-3 low" guidance.
5. **Abnormal profit exits**:
   - Day 1/2 +10% move triggers `ABNORMAL_PROFIT`.
   - +20% gap-up open triggers `ABNORMAL_GAP_EXIT`.
   - Stockbee suggests partial exits; current VectorBT single-leg model approximates with full exit.
6. **Time stop**: Exit at close of **day 5** if no other exit triggered.
   - Stockbee: "exit on 3rd to 5th day" with hard cap at day 5.
7. **Weak follow-through**: Disabled (`threshold=0.0`).
   - Stockbee holds 3-5 days; early exit on day 1 is counterproductive.
8. **Gap-through-stop**: If entry price < initial stop (gap-down through stop), exit immediately.
## Consequences

- Phase 1 results reflect EOD selection + conservative daily execution variants.
- FEE-style “first touch” entry is implemented at 5-minute resolution.
- Long-only scope simplifies risk and operational design.

## Risks

- Over-filtering reduces sample size.
- 5-minute execution still approximates intra-candle sequence; tick-level ordering can still differ in edge cases.

## Reference scan expressions (from project notes)

These are recorded as reference definitions for audits and future parity checks. Notation matches common scan shorthand:

- `c/o/h/l/v` = today close/open/high/low/volume
- `c1/c2/v1` = prior day(s)
- `avgc7/avgc65` = moving averages of close
- `minv3.1` = a liquidity proxy (exact mapping to our features must be defined explicitly when implementing)

### Low Threshold Breakout (LTB) — Bullish (daily)

`minv3.1>=300000 and c>=3 and avgc7/avgc65>=1.05`
`and c>o and c>c1 and c/c1>c1/c2 and c1/c2<1.011 and (c-l)/(h-l)>=.7`
`and CountTrue(c > 1.2 * c1 and (h-l) < 0.04 * c, 100) = 0`

### Low Threshold Breakout (LTB) — Bearish (daily, reference only)

`c1>c2 and c<c1 AND c<o and minv3.1>=900000 and c>3 and avgc7/avgc65<.95`
`and CountTrue(c > 1.2 * c1 and (h-l) < 0.04 * c, 100) = 0`

### 4% breakout scan — Bullish combination (daily)

`(c-o>=.90 and v>100000) or (c/c1>=1.04 and v>v1 and v>=100000) and c>=3 and (c-l)/(h-l)>=.70`

### 4% breakout scan — Bearish combination (daily, reference only)

`(o-c>=.90 and v>=300000) or (c/c1<=.96 and v>v1 and v>=300000) and c>=3`
