# Stockbee Market Monitor - Coding Brief for GPT mini

**Purpose**: Implement the next Market Monitor coding pass for the NiceGUI dashboard.

Use this brief together with [docs/architecture/STOCKBEE_MARKET_MONITOR_PLAN.md](docs/architecture/STOCKBEE_MARKET_MONITOR_PLAN.md).

Implementation status:

1. the requested breadth presentation is already implemented in the current tree
2. this brief is now a reference for follow-up refinements, not a bootstrap checklist
3. benchmark/index support remains optional

---

## Objective

Extend the existing Market Monitor implementation toward the planned Stockbee-style dashboard presentation.

This is a Stockbee-inspired Market Monitor adapted for Indian markets. Do not copy US raw thresholds directly into logic.

Current project decision:

1. use the full eligible NSE market universe for now
2. do not restrict the monitor to Nifty 50 or Nifty 500 subsets
3. treat the monitor as a broad market-status layer first

---

## Scope for This Coding Pass

The scaffold is already in place and the requested breadth presentation has been shipped. Any follow-up work should stay small and safe.

Current shipped behavior:

1. the full primary and secondary breadth table is rendered in `/market_monitor`
2. the existing `market_monitor_daily` fields are exposed in DuckDB and UI
3. `ratio_5d` is visible in the KPI row and page presentation
4. the page includes year tabs and collapsible year tables for detailed history review
5. the page includes history charts for 4% breadth, extended breadth, MA breadth, and ratios
6. India-specific `T2108` equivalent support is available as `% above 40 DMA`
7. benchmark/index support remains optional

If adding the full benchmark plumbing is too large for one pass, it is acceptable to:

1. leave benchmark blank or hidden
2. render the breadth table without a benchmark column initially
3. keep the page stable and readable

---

## Files to Modify

Primary files:

1. [apps/nicegui/pages/market_monitor.py](apps/nicegui/pages/market_monitor.py)
2. [apps/nicegui/state/__init__.py](apps/nicegui/state/__init__.py)
3. [src/nse_momentum_lab/db/market_db.py](src/nse_momentum_lab/db/market_db.py)

Reference spec:

1. [docs/architecture/STOCKBEE_MARKET_MONITOR_PLAN.md](docs/architecture/STOCKBEE_MARKET_MONITOR_PLAN.md)

---

## Required Behavior

### 1. Page completion

The route and nav already exist. Do not rework them unless needed for cleanup.

Update the page so it shows a Stockbee-style breadth view that includes:

1. KPI row with at least:
   1. 4% up today
   2. 4% down today
   3. 5-day ratio
   4. 10-day ratio
   5. 25% up quarter
   6. 25% down quarter
   7. universe size
   8. T2108 equivalent
2. a primary and secondary breadth table using the fields already in `market_monitor_daily`
3. existing regime header and posture output
4. existing empty-state safety

Recommended table fields:

1. Date
2. Number of stocks up 4% plus today
3. Number of stocks down 4% plus today
4. 5 day ratio
5. 10 day ratio
6. Number of stocks up 25% plus in a quarter
7. Number of stocks down 25% plus in a quarter
8. Number of stocks up 25% plus in a month
9. Number of stocks down 25% plus in a month
10. Number of stocks up 50% plus in a month
11. Number of stocks down 50% plus in a month
12. Number of stocks up 13% plus in 34 days
13. Number of stocks down 13% plus in 34 days
14. market universe size
15. India `T2108` equivalent

### 2. State layer

Add state helpers that wrap backend access cleanly.

Existing functions already exist. Extend only if needed for:

1. additional history range
2. benchmark lookup
3. explicit latest-row convenience mapping

Behavior:

1. return empty Polars DataFrame or `None` safely if the table does not exist
2. do not crash the dashboard if data is missing

### 3. DuckDB layer

Add backend methods in `MarketDataDB` for the Market Monitor.

Existing methods already exist. Extend schema/query only if needed.

Suggested additions for this pass:

1. `pct_above_ma40`
2. `pct_below_ma40`
3. `t2108_equivalent_pct`

Behavior:

1. if `market_monitor_daily` does not exist, return empty results safely
2. avoid breaking existing dashboard behavior
3. keep implementation read-friendly and minimal

---

## Data Model Guidance

If adding SQL or schema, use the planned table shape:

1. `trading_date`
2. `universe_size`
3. `up_4pct_count`
4. `down_4pct_count`
5. `up_4pct_pct`
6. `down_4pct_pct`
7. `ratio_5d`
8. `ratio_10d`
9. `up_25q_count`
10. `down_25q_count`
11. `up_25q_pct`
12. `down_25q_pct`
13. `up_25m_count`
14. `down_25m_count`
15. `up_50m_count`
16. `down_50m_count`
17. `up_13_34_count`
18. `down_13_34_count`
19. `pct_above_ma20`
20. `pct_below_ma20`
21. `pct_above_ma40`
22. `pct_below_ma40`
23. `t2108_equivalent_pct`
24. `primary_regime`
25. `tactical_regime`
26. `aggression_score`
27. `posture_label`
28. `alert_flags_json`

It is acceptable in this pass to implement only a subset, as long as the naming direction stays aligned with the plan.

---

## India-Market Constraints

These are mandatory:

1. do not hard-code US thresholds like `300`, `1000`, or `200` into regime logic
2. do not describe the monitor as a direct clone of the public US Stockbee sheet
3. do compute or plan for NSE-local metrics and percent-of-universe fields
4. use the full eligible NSE market universe for now
5. implement `T2108` equivalent as `% above 40 DMA`, not `% above 20 DMA`
6. if any default thresholds are needed, use only what is already approved in the plan:
   1. `ratio_10d >= 2.0` bullish thrust
   2. `ratio_10d <= 0.5` bearish thrust

---

## Explicit Non-Goals

Do not do these in this coding pass:

1. do not wire Market Monitor into backtest entry gating
2. do not add hard strategy filters based on MM regime
3. do not invent final percentile-band thresholds
4. do not refactor unrelated dashboard pages
5. do not add broad styling changes outside what is needed for the new page

---

## Acceptance Criteria

The task is successful if:

1. `/market_monitor` loads without errors
2. the page shows the Stockbee-style breadth table from local NSE data
3. `ratio_5d` is visible in the page
4. India `T2108` equivalent is present as `% above 40 DMA`
5. full-market universe is used, not Nifty subsets
6. no existing routes or pages break

---

## Suggested Implementation Order

1. create `apps/nicegui/pages/market_monitor.py`
2. extend `market_monitor_daily` with MA40-based T2108-equivalent fields if missing
3. update page KPI row and breadth table rendering
4. keep benchmark support optional

---

## Archived Follow-Up Prompt

If a future refinement pass is needed, keep it small and safe:

Requirements:

1. keep the existing `/market_monitor` route and page stable
2. render a Stockbee-style breadth table using the existing `market_monitor_daily` data
3. add `ratio_5d` to the KPI row and page presentation
4. add India `T2108` equivalent support as `% above 40 DMA`
5. use the full eligible NSE market universe for now
6. keep the implementation advisory and dashboard-only
7. do not hard-code US Stockbee thresholds like 300, 1000, or 200
8. do not add strategy gating in this pass
9. benchmark/index column is optional if it increases scope too much

Prefer small, safe changes that compile cleanly.
