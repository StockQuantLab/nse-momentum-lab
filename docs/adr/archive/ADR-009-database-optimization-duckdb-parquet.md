# ADR-009: Database Architecture - DuckDB + Parquet + PostgreSQL Hybrid

**Status**: Accepted

**Date**: 2026-02-22

## Context

**Current Situation:**
- NSE Momentum Lab is a **greenfield project** (no production data yet)
- Zerodha market data already available in Parquet format
- Architecture needs to be defined for optimal backtesting performance

**User Requirements:**
1. Use Zerodha data (5-min + daily OHLCV)
2. Fast backtesting performance (current approach is too slow)
3. Store backtest results for UI display and AI agent queries
4. Leverage best practices from CPR Pivot Lab project

**Reference Implementation (CPR Pivot Lab):**
- Uses **hybrid architecture**: DuckDB for analytics, PostgreSQL for operational data
- Market data stored in **Parquet format** (columnar, compressed)
- DuckDB queries Parquet directly via zero-copy views
- Pre-materialized tables for expensive computations (CPR, ATR, volume profiles)
- Backtesting 10-100x faster than pure PostgreSQL approach

## Analysis

### Current NSE Momentum Lab State

**What We Have:**
✅ Zerodha data already in `data/parquet/` (5-min + daily)
✅ Greenfield project - no legacy PostgreSQL OHLCV data
✅ Reference implementation available (CPR Pivot Lab)

**What We Need:**
- DuckDB integration for fast analytics
- Feature computation in DuckDB (not PostgreSQL)

### CPR Pivot Lab Architecture (DuckDB + Parquet + PostgreSQL)

**Design:**
```
PostgreSQL (Operational):
  - agent_sessions, agent_messages
  - signals, alert_log
  - User state, config

DuckDB + Parquet (Analytics):
  - 5-min OHLCV → Parquet files (one per year per symbol)
  - Daily OHLCV → Parquet files
  - Pre-materialized: cpr_daily, atr_intraday, cpr_thresholds, volume_profile
  - Backtest results stored in DuckDB
```

**Why This is Fast:**
1. **Columnar Storage (Parquet)**: Read only columns you need
   - Query: `SELECT date, close FROM ...` → reads 2/6 columns
   - Compression: 5-10x smaller than CSV/PostgreSQL

2. **Zero-Copy Views**: DuckDB queries Parquet files directly
   ```sql
   CREATE VIEW v_5min AS
   SELECT * FROM read_parquet('data/parquet/5min/*/*.parquet')
   ```
   - No data import needed
   - Parallel scanning (multiple files)

3. **Materialized Tables**: Pre-compute expensive calculations
   ```sql
   -- Built once, queried millions of times
   CREATE TABLE cpr_daily AS
   SELECT symbol, trade_date, (high+low+close)/3 AS pivot, ...
   ```

4. **Vectorized Execution**: Polars + DuckDB use SIMD instructions
   - Process 1000s of rows in CPU cache
   - No Python interpreter overhead

**Performance Comparison:**

| Operation | PostgreSQL | DuckDB + Parquet | Speedup |
|-----------|------------|------------------|---------|
| Load 1 year OHLCV (1 symbol) | ~2-5s | ~50-100ms | 20-50x |
| Compute 20-day ATR (all symbols) | ~30-60s | ~1-2s | 15-30x |
| Scan query (simple filters) | ~10-20s | ~0.5-1s | 10-20x |
| Full backtest (1000 symbols) | ~5-10min | ~10-30s | 10-20x |

### Key Differences Between Projects

| Aspect | NSE Momentum Lab | CPR Pivot Lab |
|--------|------------------|---------------|
| **Data Source** | **Zerodha 5-min + daily** (same as CPR!) | Zerodha 5-min + daily |
| **Strategy Type** | Daily gap-up breakout | Intraday CPR levels |
| **Time Granularity** | Daily (EOD) + 5-min available | 5-min candles |
| **Feature Complexity** | 20+ features per day | CPR, ATR, volume profile |
| **Backtest Engine** | VectorBT (Pandas) | Custom Polars engine |
| **Universe Size** | ~1000+ symbols | ~500 symbols (filtering) |
| **History Length** | 10+ years | 10 years |
| **Project State** | **Greenfield** (no production data) | Production |

**Critical Insight**: Both projects use the **same Zerodha data source**! This means we can directly adopt CPR's proven architecture.

## Decision

**Adopt Hybrid Architecture: DuckDB + Parquet + PostgreSQL**

This is a **greenfield implementation** (no migration from existing PostgreSQL data).

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│              PostgreSQL (Hot Data - Results & Metadata)      │
├─────────────────────────────────────────────────────────────┤
│  Reference Data:                                            │
│    - ref_symbol, ref_exchange_calendar, ca_event            │
│                                                             │
│  Strategy Metadata:                                         │
│    - scan_definition, scan_run, scan_result                 │
│    - exp_run, exp_metric                                    │
│                                                             │
│  Backtest Results:                                          │
│    - bt_trade (individual trades)                           │
│    - rpt_bt_daily (daily aggregations)                      │
│    - rpt_bt_failure_daily (failure analysis)                │
│                                                             │
│  Paper Trading & Signals:                                   │
│    - signal, paper_order, paper_fill, paper_position        │
│                                                             │
│  Jobs:                                                       │
│    - job_run (pipeline tracking)                            │
│                                                             │
│  Usage: UI display, Agent queries, Fast random access       │
└─────────────────────────────────────────────────────────────┘
                            ↑
                            │

┌─────────────────────────────────────────────────────────────┐
│         DuckDB + Parquet (Cold Data - Market Data)          │
├─────────────────────────────────────────────────────────────┤
│  Parquet Files (from Zerodha, already converted):          │
│    data/parquet/5min/SYMBOL/YYYY.parquet                   │
│      - candle_time, open, high, low, close, volume         │
│      - date, year, symbol (partitioning columns)            │
│                                                             │
│    data/parquet/daily/SYMBOL/all.parquet                   │
│      - date, open, high, low, close, volume, symbol        │
│                                                             │
│  Materialized DuckDB Tables (data/market.duckdb):           │
│    - feat_daily (ret_1d, ret_5d, atr_20, range_pct,        │
│                 close_pos_in_range, ma_20, ma_65,           │
│                 rs_252, vol_20, dollar_vol_20)              │
│                                                             │
│    - cpr_daily, atr_intraday (if using CPR strategy)       │
│                                                             │
│  Usage: Backtest data loading, Feature building, Analysis  │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Zerodha CSV Files (data/raw/)
    │
    │ (one-time conversion, already done)
    ▼
Parquet Files (data/parquet/) ← Already have these!
    │
    │ (DuckDB reads directly - zero copy)
    ▼
DuckDB Queries
    │
    ├─▶ Materialized Tables (feat_daily)
    │       │
    │       ▼
    │   Backtest Engine (VectorBT)
    │       │
    │       ▼
    └──▶ PostgreSQL (bt_trade, exp_metric)
            │
            ▼
        NiceGUI UI + AI Agent
```

### Phase 2: Query Layer
```python
# New module: src/nse_momentum_lab/db/duckdb.py

class MarketDataDB:
    """DuckDB for analytics - mirrors CPR design"""

    def __init__(self):
        self.con = duckdb.connect("data/market.duckdb")
        self._setup_parquet_views()

    def query_ohlcv(self, symbols, start, end):
        """Vectorized query via Polars"""
        return self.con.execute("""
            SELECT symbol_id, trading_date, open_adj, high_adj, low_adj, close_adj, volume
            FROM v_ohlcv_adj
            WHERE symbol_id IN (?)
              AND trading_date BETWEEN ? AND ?
            ORDER BY symbol_id, trading_date
        """, [symbols, start, end]).pl()

    def get_features(self, symbol_id, date):
        """Pre-computed features from materialized table"""
        return self.con.execute("""
            SELECT ret_1d, ret_5d, atr_20, range_pct, close_pos_in_range,
                   ma_20, ma_65, rs_252, vol_20, dollar_vol_20
            FROM feat_daily
            WHERE symbol_id = ? AND trading_date = ?
        """, [symbol_id, date]).fetchone()
```

### Phase 3: Data Pipeline
```python
# New module: scripts/convert_to_parquet.py

def migrate_postgres_to_parquet():
    """
    One-time migration: PostgreSQL → Parquet

    Output structure:
    data/parquet/ohlcv_raw/SYMBOL_ID/YYYY.parquet
    data/parquet/ohlcv_adj/SYMBOL_ID/YYYY.parquet
    """

    for symbol_id in get_all_symbols():
        for year in range(2015, 2026):
            df = pl.read_database(
                f"SELECT * FROM nseml.md_ohlcv_raw WHERE symbol_id={symbol_id} AND EXTRACT(YEAR FROM trading_date)={year}",
                connection_uri
            )
            df.write_parquet(f"data/parquet/ohlcv_raw/{symbol_id}/{year}.parquet")

def build_feature_tables():
    """Materialize feat_daily from Parquet (one-time, incremental updates)"""
    db = MarketDataDB()
    db.con.execute("""
        CREATE TABLE feat_daily AS
        WITH base AS (
            SELECT symbol_id, trading_date,
                   close_adj / LAG(close_adj) OVER (PARTITION BY symbol_id ORDER BY trading_date) - 1 AS ret_1d,
                   -- ... compute all features
            FROM v_ohlcv_adj
        )
        SELECT * FROM base
    """)
```

## Rationale

### Why Hybrid Architecture?

1. **Use the Right Tool for the Job**
   - **PostgreSQL**: Transactional data, relationships, consistency, random access
   - **DuckDB + Parquet**: Analytics, time-series, aggregations, sequential scans

2. **Greenfield Advantage**
   - No migration needed (starting fresh!)
   - Zerodha Parquet files already available
   - Can adopt proven pattern from CPR project immediately

3. **Performance**
   - **10-100x faster backtests** (validated in CPR project)
   - Reduced memory pressure (columnar reads only needed columns)
   - Parallel query execution (DuckDB scans multiple Parquet files)

4. **Scalability**
   - Add more years/data → minimal performance impact
   - Parquet files are immutable (easy to cache/replicate)
   - DuckDB is in-process (no network overhead)

### What Goes Where?

| Data Type | Storage | Rationale |
|-----------|---------|-----------|
| **Zerodha OHLCV (raw)** | Parquet files | Columnar, compressed, analytics-optimized |
| **Features (feat_daily)** | DuckDB materialized | Pre-compute once, query millions of times |
| **Backtest results** | PostgreSQL | Fast random access, UI pagination, agent queries |
| **Reference data** | PostgreSQL | Relationships, foreign keys, ACID |
| **Signals / Paper trading** | PostgreSQL | Transactional integrity, state management |
| **Corporate actions** | PostgreSQL | Adjustment calculations, historical tracking |

### Why PostgreSQL for Backtest Results?

**Question**: Shouldn't backtest results go in DuckDB too?

**Answer**: NO. Here's why:

1. **UI needs fast pagination**: "Show me trades 51-100 for experiment 123"
   - PostgreSQL: `OFFSET 50 LIMIT 50` (instant, indexed)
   - DuckDB: Scan all trades, then offset (slow for large datasets)

2. **Agent needs aggregated queries**: "What's the win rate for strategy X?"
   - PostgreSQL: `AVG(pnl_r) GROUP BY exp_run_id` (indexed, fast)
   - DuckDB: Would need to scan all trade data

3. **Relationships**: Results link to symbols, experiments, runs
   - PostgreSQL: Foreign keys, joins, referential integrity
   - DuckDB: No foreign key constraints

4. **Results are small**: Thousands of rows (not millions like OHLCV)
   - PostgreSQL overhead is negligible for this size
   - Benefits (ACID, relationships, indexing) outweigh costs

## Implementation Plan

**Total Effort**: ~6 hours (1 day) - Greenfield implementation

### Phase 1: DuckDB Integration (3 hours)
- Add dependencies: `duckdb>=1.1.0`, `polars>=0.20.0`
- Create `src/nse_momentum_lab/db/market_db.py`
- Register Parquet views for `data/parquet/5min/` and `data/parquet/daily/`
- Build `feat_daily` materialized table

### Phase 2: Update Backtest Engine (2 hours)
- Modify `vectorbt_engine.py` to load market data from DuckDB
- Keep PostgreSQL for results storage (unchanged)

### Phase 3: Update Agent Tools (1 hour)
- Add DuckDB query tools for data analysis
- Keep PostgreSQL tools for results/metrics

### Summary

| Phase | Effort | Description |
|-------|--------|-------------|
| DuckDB | 3 hours | Add market data layer |
| Backtest | 2 hours | Update data loading |
| Agent | 1 hour | Update query tools |
| **Total** | **6 hours** | **1 day** |

## Consequences

### Positive
- ✅ **10-100x faster backtests** (critical for research velocity)
- ✅ **Lower memory usage** (columnar reads only needed columns)
- ✅ **Better scalability** (performance doesn't degrade with more data)
- ✅ **Simpler feature engineering** (SQL + DuckDB vs. complex Python)
- ✅ **Proven pattern** (CPR project validates this approach)

### Negative
- ❌ **Complexity**: Two database systems to maintain
- ❌ **Learning curve**: Team must learn DuckDB/Polars
- ❌ **Operational overhead**: Monitor both PostgreSQL and DuckDB file sizes

**Mitigations**:
- ✅ **Greenfield**: No migration complexity (starting fresh!)
- ✅ **Proven pattern**: CPR project validates this approach
- ✅ **Clear separation**: PostgreSQL = results, DuckDB = market data
- ✅ **Reference code**: Copy from CPR project (don't reinvent)

### Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| DuckDB file corruption | Low | High | Parquet files are immutable source of truth |
| Parquet data loss | Low | High | Files are immutable, easy to backup |
| Query bugs | Medium | Medium | Integration tests comparing results |
| Performance regression | Low | Low | Benchmark before/after (CPR already validated) |
| Team unfamiliarity | Medium | Low | Copy proven code from CPR project |

**Key Risk Reduction**:
- ✅ Parquet files are **immutable** (source of truth, can't corrupt)
- ✅ DuckDB file is **rebuildable** from Parquet (not source of truth)
- ✅ PostgreSQL for results (proven, unchanged)
- ✅ Reference implementation available (CPR project)

## Implementation Strategy

### Greenfield Approach (Recommended)

**This is NOT a migration** - we're starting fresh!

1. **Add DuckDB alongside PostgreSQL**
   - No changes to PostgreSQL schema (keep as-is)
   - Add DuckDB layer for market data access
   - Both systems coexist from day one

2. **Use Existing Parquet Files**
   - Zerodha data already converted (copied from CPR project)
   - Register Parquet views in DuckDB
   - Build materialized feature tables

3. **Update Backtest Engine**
   - Load market data from DuckDB (fast)
   - Store results in PostgreSQL (unchanged)
   - No breaking changes to existing code

4. **Validate & Deploy**
   - Run performance benchmarks
   - Verify data integrity
   - Start using DuckDB for all new backtests

### What We DON'T Do

❌ NO migration from PostgreSQL to Parquet (never had OHLCV in PostgreSQL)
❌ NO data export scripts (Parquet files already exist)
❌ NO gradual rollout (greenfield - can adopt immediately)
❌ NO rollback needed (nothing to roll back from)

## Performance Targets

Based on CPR project benchmarks (same Zerodha data, same architecture):

| Metric | Expected (DuckDB + Parquet) | Improvement |
|--------|---------------------------|-------------|
| Load 1 year data (1000 symbols) | ~2-5s | 10-20x faster |
| Compute 20-day ATR | ~0.5-1s | 15-20x faster |
| Scan + feature query | ~0.2-0.5s | 20-25x faster |
| Full backtest (1000 symbols, 5 years) | ~20-40s | 10-15x faster |

**Note**: No "before" metrics for this project (greenfield), but CPR project saw these improvements switching from PostgreSQL to DuckDB.

## Data Layout

**Current State** (already copied from CPR project):

```
data/
├── raw/                        # Zerodha CSV files (source)
│   ├── 5min/
│   │   ├── Part 1/
│   │   ├── Part 2/
│   │   └── ...
│   └── daily/
│
├── parquet/                    # ALREADY EXISTS - use directly
│   ├── 5min/
│   │   ├── RELIANCE/
│   │   │   ├── 2015.parquet
│   │   │   ├── 2016.parquet
│   │   │   └── ...
│   │   ├── TCS/
│   │   └── ... (1000+ symbols)
│   │
│   └── daily/
│       ├── RELIANCE/
│       │   └── all.parquet
│       ├── TCS/
│       └── ... (1000+ symbols)
│
└── market.duckdb               # TO BE CREATED - materialized tables
    ├── feat_daily             # Pre-computed features
    └── (other materialized views as needed)
```

**Parquet Schema** (from CPR project):

```python
# 5-minute candles
{
    "candle_time": datetime,    # 2015-04-01T09:15:00+05:30
    "date": date,               # 2015-04-01 (partitioning column)
    "year": int,                # 2015 (partitioning column)
    "symbol": str,              # "RELIANCE"
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "volume": int,
    "true_range": float,        # Pre-computed true range
}

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

## Next Steps

1. ✅ **Review this ADR** - Done (you're reading it!)
2. 🏗️ **Implement MarketDataDB** - Create db/market_db.py module
3. 🧪 **Build feat_daily table** - One-time setup
4. ⚡ **Update backtest engine** - Use DuckDB for data loading
5. 📊 **Benchmark performance** - Validate 10x improvement
6. 🚀 **Deploy to production** - Start using DuckDB

## Related Documents

- **ADR-002**: Storage Architecture (PostgreSQL + MinIO)
- **ADR-003**: Backtesting Engine Selection (VectorBT)
- **CPR Reference**: `C:\Users\kanna\github\cpr-pivot-lab\db\duckdb.py`

## Status

**Accepted** - Ready for implementation

**Key Points**:
- ✅ Greenfield implementation (no migration)
- ✅ Use Zerodha data (not NSE bhavcopy)
- ✅ Adopt proven DuckDB + Parquet architecture
