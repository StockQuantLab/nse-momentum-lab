# DuckDB + Parquet Implementation Guide

## Greenfield Implementation

This guide shows how to use DuckDB + Parquet for NSE Momentum Lab.

**Key Points:**
- Zerodha data is already in Parquet format at `data/parquet/`
- DuckDB reads Parquet files directly (zero-copy views)
- PostgreSQL stores results, DuckDB stores market data

## Step 1: Dependencies

Already in `pyproject.toml`:
- `duckdb>=1.1.0`
- `polars>=0.20.0`

## Step 2: Data Layout

```
data/
├── parquet/                    # Zerodha data (already exists)
│   ├── 5min/
│   │   └── SYMBOL/YYYY.parquet
│   └── daily/
│       └── SYMBOL/all.parquet
│
└── market.duckdb               # Materialized tables (to be created)
    └── feat_daily
```

## Step 3: Create DuckDB Layer

Create `src/nse_momentum_lab/db/market_db.py`:

```python
"""
DuckDB market data layer.

PostgreSQL: reference data, signals, experiments, results
DuckDB: OHLCV data, features, analytics
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import polars as pl

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
DUCKDB_FILE = DATA_DIR / "market.duckdb"


class MarketDataDB:
    """DuckDB-based market data store for fast analytics."""

    def __init__(self, db_path: Path = DUCKDB_FILE, read_only: bool = False):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path), read_only=read_only)
        self._setup()

    def _setup(self) -> None:
        """Register Parquet views for Zerodha data."""
        daily_glob = str(PARQUET_DIR / "daily" / "*" / "*.parquet").replace("\\", "/")
        min5_glob = str(PARQUET_DIR / "5min" / "*" / "*.parquet").replace("\\", "/")

        daily_files = list(PARQUET_DIR.glob("daily/**/*.parquet"))
        min5_files = list(PARQUET_DIR.glob("5min/**/*.parquet"))

        if daily_files:
            self.con.execute(f"""
                CREATE OR REPLACE VIEW v_daily AS
                SELECT * FROM read_parquet('{daily_glob}', hive_partitioning=false)
            """)

        if min5_files:
            self.con.execute(f"""
                CREATE OR REPLACE VIEW v_5min AS
                SELECT * FROM read_parquet('{min5_glob}', hive_partitioning=false)
            """)

    def query_daily(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> pl.DataFrame:
        """Fetch daily OHLCV data for multiple symbols."""
        placeholders = ", ".join("?" * len(symbols))
        return self.con.execute(
            f"""
            SELECT *
            FROM v_daily
            WHERE symbol IN ({placeholders})
              AND date >= ? AND date <= ?
            ORDER BY symbol, date
            """,
            symbols + [start_date, end_date],
        ).pl()

    def build_feat_daily_table(self, force: bool = False) -> int:
        """Pre-compute feat_daily from Parquet data."""
        if not force:
            try:
                n = self.con.execute("SELECT COUNT(*) FROM feat_daily").fetchone()[0]
                if n > 0:
                    print(f"feat_daily: {n:,} rows already built.")
                    return n
            except Exception:
                pass

        print("Building feat_daily materialized table...")
        self.con.execute("DROP TABLE IF EXISTS feat_daily")
        self.con.execute("""
            CREATE TABLE feat_daily AS
            WITH base AS (
                SELECT
                    symbol,
                    date,
                    close,
                    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS close_1d,
                    LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) AS close_5d,
                    high,
                    low,
                    volume
                FROM v_daily
            )
            SELECT
                symbol,
                date,
                (close / NULLIF(close_1d, 0)) - 1 AS ret_1d,
                (close / NULLIF(close_5d, 0)) - 1 AS ret_5d,
                (high - low) / NULLIF(close, 0) AS range_pct,
                (close - low) / NULLIF(high - low, 0) AS close_pos_in_range,
                volume
            FROM base
            WHERE close IS NOT NULL
        """)

        self.con.execute("CREATE INDEX idx_feat_symbol_date ON feat_daily(symbol, date)")
        n = self.con.execute("SELECT COUNT(*) FROM feat_daily").fetchone()[0]
        print(f"feat_daily built: {n:,} rows")
        return n

    def get_status(self) -> dict[str, Any]:
        """Get database status."""
        return {
            "parquet_daily": len(list(PARQUET_DIR.glob("daily/**/*.parquet"))),
            "parquet_5min": len(list(PARQUET_DIR.glob("5min/**/*.parquet"))),
            "tables": {
                "feat_daily": self._table_count("feat_daily"),
            },
        }

    def _table_count(self, table: str) -> int:
        try:
            return self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            return 0

    def close(self) -> None:
        self.con.close()


_db: MarketDataDB | None = None


def get_market_db() -> MarketDataDB:
    """Return the global MarketDataDB instance."""
    global _db
    if _db is None:
        _db = MarketDataDB()
    return _db


def close_market_db() -> None:
    global _db
    if _db is not None:
        _db.close()
        _db = None
```

## Step 4: Build Features

Use the current CLI instead:

```bash
doppler run -- uv run nseml-build-features
```

For an exceptional full rebuild:

```bash
doppler run -- uv run nseml-build-features --force --allow-full-rebuild
```

Add to `pyproject.toml`:

```toml
[project.scripts]
nseml-build-features = "nse_momentum_lab.cli.build_features:main"
```

Run:
```bash
doppler run -- uv run nseml-build-features
```

## Step 5: Update Backtest Engine

Modify `src/nse_momentum_lab/services/backtest/vectorbt_engine.py`:

```python
from nse_momentum_lab.db.market_db import get_market_db

def load_market_data_duckdb(
    self,
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> pl.DataFrame:
    """Load market data using DuckDB (10-100x faster)."""
    db = get_market_db()
    return db.query_daily(symbols, start_date, end_date)
```

## Implementation Checklist

- [x] Add duckdb and polars to pyproject.toml
- [ ] Create `src/nse_momentum_lab/db/market_db.py`
- [ ] Create `scripts/build_features.py`
- [ ] Build features: `uv run nseml-build-features`
- [ ] Update backtest engine to use DuckDB
- [ ] Run performance benchmarks

## Performance

Expected (based on CPR project with same architecture):

| Operation | PostgreSQL | DuckDB + Parquet | Speedup |
|-----------|------------|------------------|---------|
| Load 1 year data (1000 symbols) | ~2-5s | ~50-100ms | 20-50x |
| Full backtest (1000 symbols, 5 years) | ~5-10min | ~10-30s | 10-20x |
