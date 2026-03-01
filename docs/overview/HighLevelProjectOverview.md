4% Breakout / Momentum Burst Project for Indian Markets

This project builds a fully automated, local-first research + paper-trading platform to test whether the Stockbee-style “4% Breakout / Momentum Burst” and “2LYNCH” selection process transfers to Indian equities.

The system is designed to be:

- Survivorship-bias aware (delisted symbols included)
- Corporate-action aware and adjusted (splits/bonus/rights adjusted; dividends stored as events; TRI optional later)
- Deterministic for all calculations (LLMs never compute prices)
- Reproducible (dataset + code + parameters are versioned and hashed)
- Operable (monitoring/alerts, quarantines for bad data days)

Primary end goal: nightly EOD pipeline that produces candidates, backtests, walk-forward evaluation, and a paper-trading ledger with alerts and monitoring.

Reference strategy posts (used to anchor rule definitions):

- https://stockbee.blogspot.com/2014/01/how-to-identify-good-momentum-burst-and.html
- https://stockbee.blogspot.com/2015/11/how-to-use-4-breakout-scan-to-make-money.html

🎯 Objectives

Validate whether the 4% breakout + 2LYNCH rules work on Indian stocks.

Measure real performance using walk-forward testing (rolling windows, out-of-sample).

Automate scanning, filtering, evaluation, and artifact storage.

Run paper trading with alerts (no live execution).

Minimize repeated work using experiment registries (cache by dataset hash + params).

Operate locally with API-based LLMs (reasoning/summarization only).

Non-goals (Phase 1):

- Automated broker execution with real capital
- Intraday / tick-level strategies (EOD-only first)
- Options/futures
- Alternative data

Success definition (Phase 1):

- Data pipeline runs end-to-end locally via docker-compose
- Daily ingest + adjust completes within a predictable time budget
- Scan results are explainable and auditable (why a symbol qualified)
- Walk-forward results are reproducible from experiment registry entries
- Paper-trading ledger matches signal rules and is consistent day-to-day

Locked Phase 1 scope decisions (confirmed):

- Universe: NSE cash equities only (NSE-EQ)
- Broker feed for later live monitoring: Zerodha only (for now)
- Backtesting & analysis: EOD-only (daily bars)
- Strategy direction: long-only (no shorts in Phase 1)
- Liquidity/price thresholds: implement as configurable parameters, but do not hard-lock defaults until exploratory analysis is done

📐 Strategy Specification — 4% Breakout + 2LYNCH

Important translation note for India:

- The original posts reference US-specific thresholds (e.g., $0.90 close-open for higher priced US stocks). In India, absolute price moves scale with price level and tick size differs across stocks. We will keep the rule as a configurable “absolute range expansion” fallback for high-priced names, but default to a percentage-based scan for NSE.
- Liquidity thresholds should be adapted to NSE turnover; we will implement liquidity filters as optional parameters and evaluate sensitivity during analysis.

Base Breakout Scan

Daily scan at market close (EOD scan):

(c - o >= abs_move_threshold AND v > vol_threshold)
OR
(c / c1 >= 1.04 AND v > v1 AND v >= vol_threshold)
AND c >= 3
AND (c - l) / (h - l) >= 0.7

Where:

c = today close

o = open

h/l = high/low

c1/v1 = previous day

Default parameterization for NSE (Phase 1 starting point):

- `vol_threshold`: configurable (do not lock; default can start at 100,000 to match Stockbee scan, then adjust)
- `value_traded_threshold`: configurable (recommended for later realism, but not locked initially)
- `abs_move_threshold`: configurable; the Stockbee $0.90 close-open rule is included for reference and for possible “high priced focus” variants

Hard filters (configurable, OFF by default initially):

- Price floor (optional)
- Upper circuit / lower circuit days: treat carefully (can distort signals)
- Exclude symbols in ASM/GSM lists if you want “institutional quality” (optional; India-specific)

2LYNCH Filters (Selection Quality Layer)

Letter

Rule

2

Not up 2 days in a row prior to breakout (avoid extended 3rd/4th day pops; a small up day can be fine).

L

Prior move linear, orderly (avoid “drunken walk” / whipsaw).

Y

Young trend: 1st–3rd breakout from consolidation is preferred (later breakouts have higher failure risk).

N

Narrow range day or negative day pre-breakout.

C

Consolidation/pullback is shallow, orderly and compact with narrow range bars and low volume; no more than one 4% breakout day inside the consolidation.

H

Close near high of the day.

Implementation note:

- Some of these are “pattern” concepts. For backtesting automation we need deterministic, parameterized proxies. Phase 1 will implement numeric heuristics for each letter (documented in Technical Design) and allow later tuning.

Entry

Buy next open or breakout close.

FEE note (“find and enter early”):

- Stockbee’s intent is to enter breakouts as early as possible on the breakout day (often “first touch”).
- Phase 1 uses daily bars, so we can detect whether a breakout **touched intraday** (via daily high) and whether it **held into close** (via daily close), but we cannot model the exact intraday timing/path without minute data.
- Phase 1 therefore evaluates two deterministic daily execution variants:
     - next-open entry (primary)
     - same-day close entry (control)

Prefer first breakout from base.

Entry modes to test:

- EOD entry: buy at close on breakout day (assumes you can enter near close)
- Next open: buy next day open (more realistic for EOD-only workflow)

Both modes must be supported and tracked separately in the experiment registry.

Stops & Exits

Initial stop = breakout-day low.

Aggressive stop = half breakout-day gain (optional variant).

Move stop to breakeven once up 3–5% (intraday in original; Phase 1 will model conservatively with next-day logic unless intraday data is available).

Trail stop after +8%.

Time stop after 3 days if no progress.

Encode “weak follow-through” exits by day 3 as a separate variant.

🐜 Anticipation (Pre-Breakout) Scans

ANT-99 Style

- Volatility compression (lowest 10% range last 20 days)
- Volume dry-up
- Tight multi-day base
- Near resistance

FHP (High-Priced Focus)

- Price above upper quartile of universe
- Strong RS vs index
- Institutions prefer these names

🔍 OLC Process (Organize Like Crazy)

Automation mapping of OLC:

- “Scan” becomes a DB table + API endpoint + dashboard view
- “Watchlist” becomes persisted lists per strategy variant and regime
- “Review” becomes a daily summary report + alerts

Stockbee OLC process mapping (to avoid missing setups):

- Keep a nightly “combo scan” (breakouts + quality filters)
- Run ANT-99 “anticipation” scans (pre-breakout)
- Run FHP process for higher priced names
- Do anticipation work between ~3:30–4:00pm or after market close (Phase 1: after close)
- Set price alerts near resistance levels (Phase 2+ when live feed is available)

📊 Metrics & Acceptance Criteria

Minimum initial targets (net of conservative costs):

- CAGR >= 15%
- Sharpe > 1
- Max DD < 20%
- Win Rate > 40%
- Profit Factor > 1.5
- Avg R > 0.5

Additional metrics to track (must be in registry):

- Turnover, exposure, average holding period
- Slippage impact (gross vs net)
- Tail metrics: worst 1% day, max adverse excursion (MAE) per trade
- Conditional performance by regime (trend up vs down)

🗂 Experiment Registry (Avoid Rework)

Every run stores:

- Dataset hash
- Scan rules (versioned)
- Parameters
- Time window
- Equity curve
- Metrics
- Code version (git SHA)

Agents check registry before launching new tests.

🗄 Data Requirements

Historical data (10–15 yrs preferred):

- NSE equities daily OHLCV (primary)
- Corporate actions adjusted
- Survivorship-bias free
- Delisted stocks included

Real-time/paper trading (later):

- Broker websocket feeds (Zerodha)

🤖 AI Agent System

Agents:

- Scan Agent — generates candidates + explanations
- Quant Agent — backtests + walk-forward
- Eval Agent — rejects weak systems and suggests parameter boundaries (reasoning only)
- Risk Agent — limits exposure + kill-switch policies
- Monitor Agent — live degradation + data-quality alarms
- Archivist Agent — stores results + links artifacts

🔁 Workflow

Data → Validate → Adjust → Scan → Filter → Backtest → Walk-Forward
           ↓
      Registry Check
           ↓
        Deploy → Paper Trade → Monitor

🛠 Tech Stack

- Python (target 3.14; or latest stable available)
- Postgres (target 18; or latest stable available) + MinIO
- vectorbt
- FastAPI + NiceGUI
- APScheduler
- LLM routing via LiteLLM proxy (reasoning/summarization only)
