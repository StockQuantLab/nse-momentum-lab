# ADR-003: Backtesting System

**Status**: Accepted
**Date**: 2026-03-06
**Consolidates**: ADR-003 (Engine Selection), ADR-007 (2LYNCH Strategy), ADR-008 (Walk-Forward), ADR-009-exp (Registry), ADR-021 (VectorBT+DuckDB)

---

## Overview

This ADR defines the backtesting system architecture, including the VectorBT engine, 2LYNCH strategy specification, experiment registry, and walk-forward testing framework.

---

## 1. Backtesting Engine

### 1.1 Decision: VectorBT

**VectorBT** is the canonical backtesting engine for NSE Momentum Lab.

**Rationale:**
- Vectorized NumPy core for performance
- Walk-forward friendly
- Easy to parallelize
- Proven in production (840.67% return over 10 years)

### 1.2 Alternatives Considered

| Alternative | Status | Reason Rejected |
|--------------|--------|-----------------|
| Backtrader | Not chosen | Less performant than vectorized approaches |
| Zipline | Not chosen | More complex, DataFrame-centric stack |
| Custom loop engine | Not chosen | Reimplementing vectorization is wasteful |

### 1.3 Engine Components

| Component | File | Purpose |
|-----------|------|---------|
| **VectorBTEngine** | `vectorbt_engine.py` | Portfolio simulation, entry/exit logic |
| **DuckDBBacktestRunner** | `duckdb_backtest_runner.py` | Orchestration, year-by-year execution |
| **BacktestParams** | `duckdb_backtest_runner.py` | Parameter definition with hash-based deduplication |

### 1.4 Stop/Exit Logic

Implemented in `VectorBTEngine._build_exit_signals()`:

1. **Initial Stop**: Low of breakout day T (from 5-min data within FEE window)
2. **Breakeven Stop**: Stop moves to entry once close > entry price
3. **Trailing Stop**: Activate at +8%, trail 2% below highest high
4. **Time Stop**: Exit at close of day 5 if no other exit
5. **Abnormal Profit Exits**: +10% on day 1-2, +20% gap open
6. **Gap-Through-Stop**: Immediate exit if price gaps through initial stop

---

## 2. 2LYNCH Strategy Specification

### 2.1 Overview

Stockbee-style "4% breakout / momentum burst" adapted for Indian (NSE) equities.

**Production Results** (exp 429c79ac45b65086, 2015-2025):
- Total Return: 1,939%
- Annualized Return: 193.9%
- Max Drawdown: 4.4%
- Calmar Ratio: 43.67

### 2.2 Breakout Detection

For 4% breakout threshold T = 1.04 × C(t-1):

| Type | Condition |
|------|-----------|
| **Touched** | H(t) ≥ T |
| **Confirmed** | C(t) ≥ T (stronger signal) |
| **Gap** | O(t) ≥ T |
| **Fizzled** | H(t) ≥ T and C(t) < T |

### 2.3 2LYNCH Filters

Require **5 of 6** filters to pass:

| Letter | Name | Condition |
|--------|------|-----------|
| **H** | Close near High | close_pos_in_range ≥ 0.70 |
| **N** | Narrow/Negative T-1 | (prev_high - prev_low) < 0.5×ATR20 OR prev_close < prev_open |
| **2** | Not Up 2 Days | ret_1d_lag1 ≤ 0 OR ret_1d_lag2 ≤ 0 |
| **Y** | Young Breakout | prior_breakouts_30d ≤ 2 |
| **C** | Consolidation | vol_dryup_ratio < 1.3 |
| **L** | Trend Quality | 2 of 3: close > MA20, ret_5d > 0, R²_65 ≥ 0.70 |

**TI65 Formula**: `MA7 / MA65 ≥ 1.05` (available as feature, NOT used as same-day filter—breakouts occur when trend is starting).

### 2.4 FEE Window (Find and Enter Early)

Entry must occur within the FEE window after market open:

| Window | Trades | Calmar |
|--------|--------|--------|
| 30 min | 4,604 | 40.39 |
| 45 min | 5,955 | 38.45 |
| **60 min** | **7,073** | **43.67** ← Production default |

**Entry cutoff**: 09:15-10:15 IST (60 minutes from NSE open)

---

## 3. Experiment Registry

### 3.1 Purpose

Prevent duplicate runs and enable result comparison across parameter sweeps.

### 3.2 Registry Tables

| Table | Purpose |
|-------|---------|
| `exp_run` | Experiment metadata, hash, status |
| `exp_metric` | Metrics (Calmar, win rate, etc.) |
| `exp_artifact` | Links to MinIO artifacts |

### 3.3 Deduplication

- **Dataset hash**: Hash of input data version
- **Strategy hash**: Hash of strategy code/parameters
- **Experiment hash**: Combined hash for unique identification

Same parameters = same hash = skip re-run (fast iteration).

---

## 4. Walk-Forward Testing

### 4.1 Framework

**3-year train / 6-month test** windows rolled monthly.

### 4.2 Purpose

- Validate strategy robustness across different market conditions
- Prevent overfitting to specific time periods
- Test parameter stability

### 4.3 Execution

Scheduled overnight due to high compute cost.

---

## 5. Result Storage

### 5.1 Storage Architecture

| Component | Storage | Purpose |
|-----------|---------|---------|
| **VectorBT** | In-memory | Portfolio simulation |
| **DuckDB** | `data/market.duckdb` | Result storage |
| **PostgreSQL** | `nseml.exp_run`, `nseml.bt_trade` | Operational queries, UI pagination |

### 5.2 Result Tables

```sql
-- Experiment metadata
bt_experiment(exp_hash, strategy_name, dataset_hash, params_json)

-- Individual trades
bt_trade(exp_hash, symbol_id, entry_date, exit_date, pnl_pct, exit_reason)

-- Yearly aggregates
bt_yearly_metric(exp_hash, year, num_trades, total_pnl, max_dd)
```

---

## 6. Implementation

### 6.1 CLI Entry Point

```bash
# Run backtest with default params
doppler run -- uv run python -m nse_momentum_lab.cli.backtest

# With specific universe and date range
doppler run -- uv run python -m nse_momentum_lab.cli.backtest \
  --universe-size 2000 --start-year 2015 --end-year 2025

# List available strategies
doppler run -- uv run python -m nse_momentum_lab.cli.backtest --list-strategies
```

### 6.2 Key Files

| File | Purpose |
|------|---------|
| `cli/backtest.py` | CLI entry point |
| `services/backtest/vectorbt_engine.py` | VectorBT engine wrapper |
| `services/backtest/duckdb_backtest_runner.py` | Orchestration |
| `services/backtest/filters.py` | 2LYNCH filter implementations |
| `services/backtest/strategy_registry.py` | Strategy definitions |

---

## 7. Multi-Strategy Support

The system supports multiple strategies via the strategy registry:

| Strategy | Description |
|----------|-------------|
| `indian2lynch` | 4% breakout + 2LYNCH filters (production) |
| `thresholdbreakout` | Configurable threshold breakout |
| `thresholdbreakdown` | Short mirror (breakdown below support) |
| `episodicpivot` | Large gap detection |

---

## 8. Consequences

### Positive
- ✅ Fast, proven backtesting engine
- ✅ Hash-based deduplication prevents redundant runs
- ✅ Multi-strategy support for research
- ✅ Walk-forward validation prevents overfitting

### Trade-offs
- ⚠️ VectorBT requires data pivoted to wide matrices
- ⚠️ Memory pressure for large universes (mitigated by year-by-year execution)

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Over-filtering reduces sample size | Parameter tuning, universe size options |
| VectorBT unmaintained | Consider fallback to vectorbt-pro or custom engine |

---

## 9. Related Documents

- **ADR-001**: Data & Storage Architecture
- **ADR-004**: Paper Trading & Risk
- **2LYNCH_STRATEGY_GUIDE.md**: Complete strategy documentation
- **2LYNCH_FILTERS_SUMMARY.md**: Filter reference

---

*This ADR consolidates and supersedes: ADR-003, ADR-007, ADR-008, ADR-009-exp, ADR-021*
