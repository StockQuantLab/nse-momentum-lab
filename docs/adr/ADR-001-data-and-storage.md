# ADR-001: Data & Storage Architecture

**Status**: Accepted
**Date**: 2026-03-06
**Consolidates**: ADR-001 (Market Data), ADR-002 (Storage), ADR-009 (DuckDB Architecture), ADR-013 (Schema), ADR-014 (Artifacts)

---

## Overview

This ADR defines the hybrid storage architecture for NSE Momentum Lab, combining DuckDB+Parquet for analytical workloads with PostgreSQL for operational data and MinIO for artifact storage.

**Architecture Summary:**

| Component | Storage | Purpose |
|-----------|---------|---------|
| **Market Data** | DuckDB + Parquet | Fast analytical queries (100-1000x faster) |
| **Features** | DuckDB (`feat_daily`) | Pre-computed features (ATR, R², returns) |
| **Results** | PostgreSQL | exp_run, bt_trade, exp_metric |
| **Signals** | PostgreSQL | signal, paper_position, paper_fill |
| **Reference Data** | PostgreSQL | ref_symbol, ref_exchange_calendar |
| **Artifacts** | MinIO | Parquet exports, charts, large files |

---

## 1. Market Data Sources

### 1.1 Primary Sources

| Data Type | Source | Method |
|-----------|--------|--------|
| Historical OHLCV (2015-2025) | Zerodha (Jio Cloud) | Manual download, already in Parquet |
| Corporate Actions | NSE Website | Manual download for splits/bonus/dividends |
| Delisting Data | NSE Website | Manual download for symbol lifecycle |
| Real-time Data | Broker API (future) | WebSocket feeds for paper trading |

### 1.2 Data Format

Market data is stored in **columnar Parquet format** for optimal analytical performance:

```
data/parquet/
├── daily/
│   ├── SYMBOL1/all.parquet      # All daily data for SYMBOL1
│   ├── SYMBOL2/all.parquet
│   └── ...
└── 5min/
    ├── SYMBOL1/all.parquet      # All 5-min data for SYMBOL1
    └── ...
```

**Parquet Schema:**
```python
# Daily candles
{
    "date": date,
    "symbol": str,
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "volume": int,
}
```

### 1.3 External Dependencies

| System | Purpose |
|---|---|
| Zerodha (Jio Cloud) | Historical files (2015-2025) |
| NSE Website | Corporate actions, delisting |
| Broker API | Live bars (future) |

---

## 2. DuckDB + Parquet (Analytics Layer)

### 2.1 Why DuckDB for Market Data?

| Benefit | Explanation |
|---------|-------------|
| **10-100x faster** | Columnar storage reads only needed columns |
| **Zero-copy views** | Queries Parquet files directly without import |
| **Vectorized execution** | SIMD instructions for CPU cache efficiency |
| **In-process** | No network overhead, simpler deployment |

### 2.2 Performance Comparison

| Operation | PostgreSQL | DuckDB + Parquet | Speedup |
|-----------|------------|------------------|---------|
| Load 1 year OHLCV (1000 symbols) | ~30-60s | ~2-5s | 10-20x |
| Compute 20-day ATR | ~30-60s | ~1-2s | 15-30x |
| Scan query (simple filters) | ~10-20s | ~0.5-1s | 10-20x |
| Full backtest (1000 symbols, 5 years) | ~5-10min | ~10-30s | 10-20x |

### 2.3 Materialized Features

The `feat_daily` table is materialized in DuckDB with pre-computed features:

```sql
CREATE TABLE feat_daily AS
SELECT
    symbol_id,
    trading_date,
    -- Returns
    ret_1d, ret_5d, ret_20d,
    -- Volatility
    atr_20, range_pct,
    -- Trend
    close_pos_in_range, ma_7, ma_20, ma_65,
    -- R-squared
    r2_65,
    -- Volume
    vol_dryup_ratio, vol_20, dollar_vol_20,
    -- Breakout tracking
    prior_breakouts_30d, prior_breakouts_90d,
FROM v_ohlcv_adj
```

Built by: `src/nse_momentum_lab/features/daily_core.py`

### 2.4 Data Flow

```
Zerodha CSV Files (one-time download)
    │
    ▼
Parquet Files (data/parquet/daily/SYMBOL/all.parquet)
    │
    │ (DuckDB zero-copy reads)
    ▼
DuckDB Queries → Feature Computation → Backtest Engine
    │
    ▼
PostgreSQL (results storage)
```

---

## 3. PostgreSQL (Operational Layer)

### 3.1 What Goes in PostgreSQL?

| Data Type | Tables | Rationale |
|-----------|--------|-----------|
| **Reference Data** | ref_symbol, ref_exchange_calendar, ref_symbol_alias | Relationships, FKs, ACID |
| **Corporate Actions** | ca_event | Adjustment calculations, historical tracking |
| **Scans** | scan_definition, scan_run, scan_result | Metadata, pagination |
| **Experiments** | exp_run, exp_metric, exp_artifact | Registry, lineage, queries |
| **Backtest Results** | bt_trade, rpt_bt_daily, rpt_bt_failure_daily | Fast random access, UI display |
| **Paper Trading** | signal, paper_order, paper_fill, paper_position | Transactional integrity, state machine |
| **Jobs** | job_run | Pipeline tracking |

### 3.2 Why PostgreSQL for Results?

1. **UI Pagination**: `OFFSET 50 LIMIT 50` is instant with indexes
2. **Agent Queries**: `AVG(pnl_r) GROUP BY exp_run_id` uses aggregations efficiently
3. **Relationships**: Foreign keys between experiments, trades, symbols
4. **ACID**: Transactional integrity for paper trading state
5. **Small Data**: Results are thousands of rows (not millions like OHLCV)

### 3.3 Key Schema Design Principles

- **Primary Keys**: `(symbol_id, trading_date)` for time-series tables
- **Foreign Keys**: All tables link to experiment registry or symbol reference
- **Indexes**: On frequently queried columns (asof_date, symbol_id, exp_hash)
- **Naming**: snake_case, `timestamptz` for event time, `date` for trading_date

---

## 4. MinIO (Artifact Storage)

### 4.1 Content-Addressed Layout

```
artifacts/
├── experiments/
│   ├── {exp_hash}/
│   │   ├── equity.parquet
│   │   ├── trades.parquet
│   │   ├── charts/
│   │   └── metrics.json
│   └── ...
└── datasets/
    └── {dataset_hash}/
        └── snapshot.parquet
```

### 4.2 Artifact Rules

- **Immutable**: No overwrites permitted
- **Content-addressed**: SHA-256 checksums recorded in PostgreSQL
- **Reproducible**: Artifacts identified by hash of inputs (dataset + strategy + params)

### 4.3 MinIO Configuration

| Setting | Value |
|---------|-------|
| API Port | 9003 (default) |
| Console Port | 9004 (default) |
| Buckets | `market-data`, `artifacts` |

---

## 5. Schema & Partitioning

### 5.1 PostgreSQL Table Structure

Key tables (simplified):

```sql
-- Reference
ref_exchange_calendar(trading_date, is_trading_day, notes)
ref_symbol(symbol_id, symbol, isin, name, listing_date, delisting_date)

-- Market Data Events
ca_event(event_id, symbol_id, ex_date, action_type, ratio_num, ratio_den)

-- Scans
scan_definition(scan_def_id, name, config_json, code_sha)
scan_run(scan_run_id, asof_date, dataset_hash, status)
scan_result(scan_run_id, symbol_id, passed, reason_json)

-- Experiments
exp_run(exp_run_id, exp_hash, strategy_name, params_json, code_sha)
exp_metric(exp_run_id, metric_name, metric_value)
exp_artifact(exp_run_id, artifact_name, uri, sha256)

-- Backtest Results
bt_trade(trade_id, exp_run_id, symbol_id, entry_date, exit_date, pnl_pct)
rpt_bt_daily(exp_run_id, trading_date, num_trades, total_pnl)

-- Paper Trading
signal(signal_id, symbol_id, asof_date, state, initial_stop)
paper_order(order_id, signal_id, side, qty, status)
paper_fill(fill_id, order_id, fill_price, fees)
```

Full schema: `src/nse_momentum_lab/db/models.py`

### 5.2 Partitioning Strategy

- **Time-series tables**: Monthly partitions by `trading_date`
- **Archival**: Old partitions can be detached and archived
- **Query performance**: Partition pruning on date range queries

---

## 6. Implementation Status

| Component | Status | Location |
|-----------|--------|----------|
| DuckDB Integration | ✅ Complete | `src/nse_momentum_lab/db/market_db.py` |
| Feature Materialization | ✅ Complete | `src/nse_momentum_lab/features/daily_core.py` |
| PostgreSQL Schema | ✅ Complete | `src/nse_momentum_lab/db/models.py` |
| MinIO Artifacts | ✅ Complete | `src/nse_momentum_lab/services/data_lake/` |
| Backtest Engine | ✅ Complete | `src/nse_momentum_lab/services/backtest/` |

---

## 7. Consequences

### Positive
- ✅ **Fast backtests** (10-100x improvement over pure PostgreSQL)
- ✅ **Scalable architecture** (performance doesn't degrade with more data)
- ✅ **Clear separation** (PostgreSQL = results, DuckDB = analytics)
- ✅ **Proven pattern** (validated in production systems)

### Trade-offs
- ⚠️ **Two systems** (DuckDB + PostgreSQL to maintain)
- ⚠️ **Learning curve** (DuckDB for team members familiar with PostgreSQL only)

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| DuckDB file corruption | Parquet files are immutable source of truth |
| Query bugs | Integration tests compare results across systems |
| Team unfamiliarity | Reference code available from similar projects |

---

## 8. Related Documents

- **ADR-002**: Ingestion & Adjustment
- **ADR-003**: Backtesting System
- **TECHNICAL_DESIGN.md**: Full technical specification

---

*This ADR consolidates and supersedes: ADR-001, ADR-002, ADR-009, ADR-013, ADR-014*
