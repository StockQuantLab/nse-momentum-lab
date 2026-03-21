# Stockbee Market Monitor for NSE - Implementation Plan

**Date**: 2026-03-19
**Status**: Implemented
**Priority**: High

---

## Goal

Add an NSE-native Market Monitor to the NiceGUI dashboard, modeled on Stockbee market breadth methodology, and use it as a market-regime layer for deciding:

1. whether breakout or breakdown setups are favored
2. whether the system should be aggressive, standard, or defensive
3. whether candidate budgets, sizing, and selectivity should change by regime

Reference sources reviewed:

1. https://stockbee.blogspot.com/p/mm.html
2. https://stockbee.blogspot.com/2011/08/how-to-use-market-breadth-to-avoid.html?m=1&s=03

Source note:

1. the live `mm.html` page currently redirects to a published Google Sheet with year tabs
2. the default visible tab is the latest year, so quick fetches often land on 2026 first
3. the implementation plan uses the live sheet for current column structure and the 2011 article for interpretation rules

---

## Why This Belongs in the Repo

This repo already implements Stockbee-inspired trading ideas and already contains a simple short-side breadth proxy. A full Market Monitor would make the market-regime logic:

1. visible in the dashboard
2. testable in backtests
3. reusable across breakout, breakdown, paper-trading, and daily workflow pages

Current related code:

1. NiceGUI routing in [apps/nicegui/main.py](apps/nicegui/main.py)
2. sidebar navigation in [apps/nicegui/components/__init__.py](apps/nicegui/components/__init__.py)
3. dashboard data access in [apps/nicegui/state/__init__.py](apps/nicegui/state/__init__.py)
4. existing short-side breadth gate in [src/nse_momentum_lab/services/backtest/strategy_families.py](src/nse_momentum_lab/services/backtest/strategy_families.py)
5. prior breadth-gating research in [docs/research/BREAKDOWN_IMPROVEMENT_PLAN.md](docs/research/BREAKDOWN_IMPROVEMENT_PLAN.md)

---

## Key Design Decision

Do not clone the public US Stockbee spreadsheet mechanically.

Instead, build an NSE-native Market Monitor that preserves the method but recalibrates the metrics to the local universe.

Reason:

1. Stockbee public numbers are based on a US common-stock universe of roughly 6300 names.
2. NSE has a smaller and structurally different tradable universe.
3. Absolute thresholds such as 300, 500, 1000, and 200 should not be copied directly.
4. The method transfers better than the raw thresholds do.

Implementation rule:

1. display raw counts for transparency
2. use percentages, rolling percentiles, and historical bands for regime logic

India-market adjustment rule:

1. preserve the Stockbee breadth framework
2. recalculate all metrics on NSE data only
3. normalize thresholds to Indian universe size and local history
4. avoid copying US raw thresholds into operational logic

---

## Product Scope

### Phase-1 scope

1. create a Market Monitor page in the dashboard
2. compute daily breadth metrics from local NSE data
3. classify market regime from those metrics
4. show recommended posture for trading operations

### Current implementation status as of 2026-03-20

1. page scaffold exists and loads
2. `market_monitor_daily` exists and is populated
3. UI exposes the full Stockbee-style primary and secondary breadth table
4. India-specific `T2108` equivalent is implemented as `% above 40 DMA` / `t2108_equivalent_pct`
5. `ratio_5d` is visible in the KPI row and history chart
6. benchmark/index column remains optional and deferred

### Out of scope for first cut

1. hard-gating all strategies based on market monitor regime
2. importing live Stockbee spreadsheet values as operational inputs
3. mirroring every visual detail of the public Stockbee sheet

---

## Core Methodology To Preserve

The Stockbee Market Monitor uses breadth of large moves rather than generic advance-decline noise.

The public methodology focuses on:

1. 4% up and 4% down daily breadth
2. 5-day and 10-day breakout-to-breakdown ratios
3. 25% up and 25% down in a quarter
4. 25% up and 25% down in a month
5. 50% up and 50% down in a month
6. 13% up and 13% down in 34 days as a faster breadth signal
7. extremes, thrusts, and divergences

Primary interpretation from the Stockbee article:

1. 25% up in quarter greater than 25% down in quarter implies bullish primary regime
2. 25% up in quarter less than 25% down in quarter implies bearish primary regime
3. 10-day ratio above 2 implies bullish thrust and favorable long-side swing conditions
4. 10-day ratio below 0.5 implies bearish thrust and favorable short-side swing conditions
5. very low breadth often signals seller capitulation and a tradable rebound zone
6. divergence between index action and breadth matters as much as headline direction

---

## NSE Universe Definition

The quality of the monitor depends on a stable and tradable universe.

This plan is explicitly for Indian markets, not for replaying the US Stockbee sheet.

Recommended universe:

1. NSE EQ common stocks only
2. exclude ETFs, funds, rights, warrants, and non-equity instruments
3. apply minimum price and liquidity filters consistent with the strategy universe
4. prefer the same or nearly the same universe used for scan generation and backtests

Recommended baseline filter set:

1. price floor aligned with current strategy defaults
2. liquidity floor aligned with current daily value traded filters
3. instrument allowlist sourced from local NSE symbol reference data

Open choice to settle during implementation:

1. use full NSE EQ universe for regime visibility
2. use strategy-tradable universe for tighter operational relevance

Decision locked on 2026-03-20:

1. use the full eligible market universe, not Nifty 50 or Nifty 500 subsets
2. treat Market Monitor as a broad market-status layer first
3. judge usefulness from broad-market behavior before narrowing the universe

Implementation rule for universe selection:

1. include the entire eligible NSE EQ universe after instrument-type cleanup
2. exclude ETFs, rights, warrants, funds, and non-equity instruments
3. keep only broad sanity filters needed to avoid junk and broken prints
4. do not restrict the monitor to the strategy candidate universe in the first operating version

Reason:

1. the objective is to capture market status, not just tradable setup status
2. Stockbee MM is a market-breadth monitor, so broad participation is the correct first lens
3. a narrower universe can be tested later as a secondary variant if needed

Calibration note:

1. the US sheet currently shows a common-stock universe around 6300 names
2. NSE breadth counts will be much smaller in absolute terms
3. comparisons should therefore use local percent-of-universe and local historical percentile bands

---

## Data Model

Create a dedicated date-grain table in DuckDB instead of duplicating market-level values into every symbol row.

Recommended table:

1. market_monitor_daily

Recommended columns:

1. trading_date
2. universe_size
3. up_4pct_count
4. down_4pct_count
5. up_4pct_pct
6. down_4pct_pct
7. ratio_5d
8. ratio_10d
9. up_25q_count
10. down_25q_count
11. up_25q_pct
12. down_25q_pct
13. up_25m_count
14. down_25m_count
15. up_50m_count
16. down_50m_count
17. up_13_34_count
18. down_13_34_count
19. pct_above_ma20
20. pct_below_ma20
21. primary_regime
22. tactical_regime
23. aggression_score
24. posture_label
25. alert_flags_json

Additional planned columns for the next UI-complete version:

1. pct_above_ma40
2. pct_below_ma40
3. t2108_equivalent_pct
4. benchmark_name
5. benchmark_close

Reason for separate table:

1. cleaner dashboard queries
2. easier backtest joins by date
3. avoids repeating market-level data across all symbols
4. makes historical regime annotation straightforward

---

## Metric Definitions

All metrics should be computed from local daily OHLCV and feature data.

### Daily breadth

1. up_4pct_count = number of symbols up at least 4% versus prior close with volume and liquidity filters
2. down_4pct_count = number of symbols down at least 4% versus prior close with volume and liquidity filters

### Ratios

1. ratio_5d = sum of up_4pct_count over last 5 trading days divided by sum of down_4pct_count over last 5 trading days
2. ratio_10d = sum of up_4pct_count over last 10 trading days divided by sum of down_4pct_count over last 10 trading days

### Intermediate and primary breadth

1. up_25q_count = number of symbols up at least 25% from 65-day low or equivalent quarter-lookback basis
2. down_25q_count = number of symbols down at least 25% from 65-day high or equivalent quarter-lookback basis
3. up_25m_count = number of symbols up at least 25% over roughly 20 trading days
4. down_25m_count = number of symbols down at least 25% over roughly 20 trading days
5. up_50m_count = number of symbols up at least 50% over roughly 20 trading days
6. down_50m_count = number of symbols down at least 50% over roughly 20 trading days
7. up_13_34_count = number of symbols up at least 13% from 34-day low
8. down_13_34_count = number of symbols down at least 13% from 34-day high

### Internal participation overlays

1. pct_above_ma20 = count of symbols closing above 20-day moving average divided by eligible symbols
2. pct_below_ma20 = count of symbols closing below 20-day moving average divided by eligible symbols
3. pct_above_ma40 = count of symbols closing above 40-day moving average divided by eligible symbols
4. pct_below_ma40 = count of symbols closing below 40-day moving average divided by eligible symbols

These overlays are not part of the canonical Stockbee public spreadsheet but are useful for NSE-specific calibration and already align with current repo breadth work.

India-specific interpretation rule:

1. `T2108` equivalent for this repo should be `% of eligible NSE universe above 40 DMA`
2. `% above MA20` remains useful, but it is not a true T2108 analogue

---

## Threshold and Normalization Strategy

Do not rely on raw US absolute counts for operational signals.

This is the main India-market adjustment in the plan.

Use a hybrid model:

1. raw counts for display
2. percentages of universe for comparability across time
3. rolling historical percentile bands for extremes
4. fixed ratio thresholds for tactical signal interpretation where they remain portable

Recommended regime inputs:

1. primary regime from up_25q_count versus down_25q_count
2. tactical regime from ratio_10d and recent 4% breadth pressure
3. exhaustion from percentile bands on up_25m, down_25m, up_50m, down_50m
4. participation overlay from pct_above_ma20 and pct_below_ma20

Recommended first-pass tactical thresholds:

1. ratio_10d greater than or equal to 2.0 = bullish thrust
2. ratio_10d less than or equal to 0.5 = bearish thrust
3. otherwise = neutral or transition

Recommended first-pass normalization for daily 4% breadth:

1. define high and extreme buying or selling pressure using rolling percentiles over local history
2. avoid hard-coded US thresholds such as 300 and 1000

Recommended first-pass normalization for quarter and month breadth:

1. use local percent-of-universe values alongside raw counts
2. define extreme zones from Indian-market historical distributions instead of US fixed count levels such as 200

## India Calibration Options

This section converts the normalization guidance into concrete coding options.

### What needs calibration

The US public sheet uses a common-stock universe around 6300 names. NSE will run on a much smaller eligible universe, so raw counts do not transfer directly.

Examples:

1. a US threshold of 300 is about 4.8% of a 6300-name universe
2. a US threshold of 1000 is about 15.9% of a 6300-name universe
3. a US threshold of 200 is about 3.2% of a 6300-name universe

If an NSE strategy universe is 1800 names, rough count equivalents would be:

1. 300 US-equivalent becomes about 86 NSE names
2. 1000 US-equivalent becomes about 286 NSE names
3. 200 US-equivalent becomes about 57 NSE names

These are only sanity-check conversions, not recommended production thresholds.

### Option A - direct universe scaling

Method:

1. convert Stockbee raw thresholds into percent-of-universe
2. apply that percentage to the NSE eligible universe for each day

Formula:

1. nse_threshold_count = round(us_threshold_count / 6300 * nse_universe_size)

Pros:

1. simple and easy to explain
2. closest to the public sheet framing
3. quick to implement for page annotations

Cons:

1. assumes breadth distributions are structurally similar across markets
2. ignores Indian-market volatility and participation differences
3. too brittle for production regime logic by itself

Recommended use:

1. diagnostics only
2. optional tooltip reference on the dashboard

### Option B - percent-of-universe thresholds

Method:

1. compute each breadth count as a fraction of the eligible NSE universe
2. classify signals directly from those percentages

Examples:

1. up_4pct_pct = up_4pct_count / universe_size
2. down_4pct_pct = down_4pct_count / universe_size
3. up_25q_pct = up_25q_count / universe_size
4. down_25q_pct = down_25q_count / universe_size

Pros:

1. robust to gradual universe-size changes
2. naturally portable across markets
3. easy to join into daily regime logic

Cons:

1. still needs locally chosen cutoffs
2. may hide useful raw-count context unless both are displayed

Recommended use:

1. core feature in the stored table
2. default basis for first-pass regime calculations

### Option C - historical percentile bands

Method:

1. compute rolling or full-history percentiles for each NSE breadth series
2. define high or extreme conditions from local percentile zones

Examples:

1. top 10% of up_4pct_pct = strong buying pressure
2. top 2% of down_4pct_pct = selling climax zone
3. bottom 5% of up_25q_pct = deep bearish breadth exhaustion

Pros:

1. adapts to the actual Indian-market distribution
2. best fit for identifying extremes and exhaustion
3. avoids arbitrary fixed counts

Cons:

1. needs enough history to stabilize
2. slightly harder to explain than fixed counts
3. percentile windows must be chosen carefully

Recommended use:

1. primary method for extreme-zone detection
2. primary method for aggressive versus defensive posture shifts

### Option D - rolling z-score normalization

Method:

1. compute rolling mean and standard deviation for each breadth series
2. classify pressure and extremes from z-scores

Examples:

1. zscore(up_4pct_pct) above 2 = unusually strong breadth thrust
2. zscore(down_4pct_pct) above 2 = unusually strong downside pressure

Pros:

1. compact statistical normalization
2. useful for signal dashboards and alerting

Cons:

1. less intuitive than percentiles
2. unstable if the distribution is skewed or regime-dependent

Recommended use:

1. optional diagnostic overlay
2. not the primary first implementation choice

### Option E - hybrid operating model

Method:

1. store raw counts
2. store percent-of-universe values
3. compute percentile bands from NSE history
4. keep the Stockbee ratio thresholds where they remain conceptually portable

Pros:

1. transparent for users
2. robust for local calibration
3. easiest to evolve without changing stored raw facts

Cons:

1. slightly more implementation work

Recommended use:

1. this should be the default coding direction

### Recommended first-pass coding choice

Use Option E with the following split:

1. raw counts for dashboard display and auditability
2. percent-of-universe fields in `market_monitor_daily`
3. percentile bands for extreme-zone tagging
4. ratio_5d and ratio_10d thresholds kept initially at Stockbee-style values
5. quarter regime determined primarily by relative comparison of up_25q_count versus down_25q_count, with percent-of-universe fields logged alongside

### Concrete first-pass coding rules

Store these features daily:

1. all raw breadth counts
2. `*_pct` fields for every breadth count that can be normalized by universe size
3. optional percentile rank fields such as `up_4pct_pctile`, `down_4pct_pctile`, `up_25q_pctile`, `down_25q_pctile`

Use these rules first:

1. `ratio_10d >= 2.0` means bullish thrust
2. `ratio_10d <= 0.5` means bearish thrust
3. `up_25q_count > down_25q_count` means bullish primary bias
4. `up_25q_count < down_25q_count` means bearish primary bias
5. extremes are tagged from local percentile bands, not fixed copied counts

### Suggested coding sequence

1. build raw-count SQL first
2. add percent-of-universe fields in the same table build
3. defer percentile-band tagging until the raw table validates cleanly
4. keep hard gating out of the first implementation

---

## Regime Model

Market Monitor should emit a daily regime classification that can be consumed by the dashboard and later by strategy code.

Recommended fields:

1. primary_regime in bullish, bearish, transition
2. tactical_regime in long_favored, short_favored, mixed, rebound_watch, correction_watch
3. aggression_score as integer or bounded float
4. posture_label in aggressive, standard, defensive

### Proposed classification logic

#### Primary regime

1. bullish if up_25q_count is materially above down_25q_count
2. bearish if down_25q_count is materially above up_25q_count
3. transition if near parity or if recent crossover is unstable

#### Tactical regime

1. long_favored when primary regime is bullish and ratio_10d is strong
2. short_favored when primary regime is bearish and ratio_10d is weak
3. rebound_watch when primary regime is bearish but breadth is at a local downside extreme
4. correction_watch when primary regime is bullish but upside breadth is exhausted and diverging
5. mixed in all other cases

#### Posture mapping

1. aggressive when primary and tactical signals align strongly
2. standard when regime is constructive but not broad-based
3. defensive when signals conflict or breadth deteriorates sharply

---

## Strategy Integration Plan

The strategy integration should be staged.

### Stage A - advisory only

Use the regime only to inform dashboard recommendations.

Outputs:

1. breakout favored or not
2. breakdown favored or not
3. aggressive, standard, or defensive
4. risk notes for the day

### Stage B - soft controls

Allow regime to tune strategy behavior without fully disabling signal generation.

Examples:

1. reduce or expand daily breakout candidate budget
2. reduce or expand daily breakdown candidate budget
3. change position-size multiplier
4. require higher selection score in defensive mode
5. annotate paper-trading queue with posture guidance

### Stage C - hard controls

Only after backtesting proves value, allow regime-aware hard gating.

Examples:

1. suppress weak long setups in bearish regime
2. suppress weak shorts during capitulation rebound watch
3. disable aggressive carry behavior when breadth deteriorates

Important note:

The repo already recorded that a simple short-side breadth gate reduced profitable coverage enough to be rejected. That means Market Monitor should start as an annotation and soft-control layer, not as a blunt hard filter.

---

## Dashboard Page Design

Add a new page:

1. route: /market_monitor
2. nav label: Market Monitor
3. icon: monitoring or insights

### Page sections

#### 1. Current regime header

Show:

1. current primary regime
2. current tactical regime
3. posture label
4. aggression score
5. last data date

#### 2. KPI row

Show latest values for:

1. 4% up today
2. 4% down today
3. 5-day ratio
3. 10-day ratio
4. 25% up quarter
5. 25% down quarter
6. percent above MA20
7. percent below MA20
8. universe size
9. T2108 equivalent

#### 2b. Stockbee-style breadth table

Render a tabular section close to the public Stockbee monitor layout.

Primary breadth indicators:

1. Date
2. Number of stocks up 4% plus today
3. Number of stocks down 4% plus today
4. 5 day ratio
5. 10 day ratio
6. Number of stocks up 25% plus in a quarter
7. Number of stocks down 25% plus in a quarter

Secondary breadth indicators:

1. Number of stocks up 25% plus in a month
2. Number of stocks down 25% plus in a month
3. Number of stocks up 50% plus in a month
4. Number of stocks down 50% plus in a month
5. Number of stocks up 13% plus in 34 days
6. Number of stocks down 13% plus in 34 days
7. Market universe size
8. T2108 equivalent
9. Benchmark column if available

This should be visible in the dashboard, not only stored in the table.

#### 3. Breadth charts

Plot:

1. up_4pct_count and down_4pct_count
2. ratio_10d
3. up_25q_count versus down_25q_count
4. up_25m_count and down_25m_count
5. up_50m_count and down_50m_count
6. up_13_34_count and down_13_34_count

#### 4. Divergence panel

Compare breadth against local market benchmark or internal breadth trend.

Possible chart pairings:

1. breadth versus Nifty index proxy if available
2. breadth versus local universe median return
3. breadth lower highs while benchmark rises

Current decision:

1. benchmark column is optional for the next coding pass
2. do not block breadth-table completion on benchmark-data plumbing
3. if added, prefer a broad-market Indian benchmark rather than a narrow large-cap proxy

#### 5. Decision panel

Human-readable output such as:

1. favored side: breakout, balanced, or breakdown
2. risk posture: aggressive, standard, defensive
3. notes: breadth thrust, exhaustion, divergence, rebound watch

#### 6. Audit panel

Show:

1. universe rules
2. formula notes
3. threshold logic
4. data freshness

---

## Backend Implementation Plan

### Data layer

Add market-monitor aggregate queries and table management in [src/nse_momentum_lab/db/market_db.py](src/nse_momentum_lab/db/market_db.py).

Potential API methods:

1. build_market_monitor_table(force: bool = False)
2. get_market_monitor_latest()
3. get_market_monitor_range(start_date, end_date)
4. get_market_monitor_regime_summary(days: int)

### Dashboard state layer

Add cached state helpers in [apps/nicegui/state/__init__.py](apps/nicegui/state/__init__.py).

Potential functions:

1. get_market_monitor_latest()
2. get_market_monitor_history()
3. aget_market_monitor_latest()
4. aget_market_monitor_history()

### UI layer

Add page module:

1. apps/nicegui/pages/market_monitor.py

Wire into:

1. [apps/nicegui/main.py](apps/nicegui/main.py)
2. [apps/nicegui/components/__init__.py](apps/nicegui/components/__init__.py)
3. optionally [apps/nicegui/pages/home.py](apps/nicegui/pages/home.py) with a new nav card

---

## Validation Plan

### Data validation

1. verify universe size stability over time
2. spot-check daily counts for selected dates
3. validate ratio calculations against underlying 4% counts
4. validate quarter and month breadth against manual SQL samples

### Visual validation

1. confirm chart history is readable on both desktop and laptop widths
2. verify empty states and no-data handling
3. verify page loads without blocking the dashboard event loop

### Strategy validation

1. annotate existing trades with daily market-monitor regime
2. compare performance by regime bucket
3. test soft controls before hard controls
4. reject any gating rule that materially cuts profitable coverage without improving drawdown or robustness

---

## Rollout Phases

### Phase 1 - spec and scaffold

1. create this plan
2. add page route and placeholder UI
3. define DuckDB schema and query skeleton

### Phase 2 - metric computation

1. build market_monitor_daily table
2. implement range and latest queries
3. validate metrics on sample dates

### Phase 3 - dashboard page

1. implement KPI strip
2. implement historical charts
3. implement regime and decision panels

### Phase 4 - workflow integration

1. surface market posture on daily summary page
2. surface posture in paper ledger workflow
3. optionally tag backtest and scan outputs with regime metadata

### Phase 5 - backtest regime overlay

1. advisory annotation only
2. budget and sizing experiments
3. hard-gating experiments only if soft-control results justify them

---

## Open Questions

1. Should the regime universe match the backtest tradable universe exactly, or should it be a slightly broader NSE EQ market universe?
2. Do we want to include an NSE benchmark proxy on the page from day one?
3. Should the first posture model use only breadth metrics, or also incorporate existing features such as TI65 and percent-below-MA20 overlays?
4. Should Market Monitor remain a pure dashboard and research feature initially, or also feed daily operational queues in paper trading?

---

## Recommended Next Step

Implement Phase 1 immediately:

1. add the Market Monitor page scaffold
2. add the Market Monitor route and nav item
3. add the DuckDB query skeleton for market_monitor_daily

That gives a stable place to iterate on the data model without mixing research logic directly into unrelated pages.

## Delivery Summary

The first scaffold pass has been completed. The current implementation already includes:

1. the full Stockbee-style primary and secondary breadth table in the dashboard
2. `ratio_5d` in the KPI row and historical UI
3. year tabs plus collapsible year tables for detailed history review
4. history charts for 4% breadth, extended breadth, MA breadth, and breadth ratios
5. `t2108_equivalent_pct` as the Indian `T2108` equivalent, surfaced as `% above 40 DMA`
6. benchmark integration kept optional and secondary
7. no strategy gating changes in the same pass
