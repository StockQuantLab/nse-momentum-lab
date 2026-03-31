"""Tradeable symbol utilities — determine live vs dead symbols.

The Kite instrument master CSV (``data/raw/kite/instruments/NSE.csv``) is the
single source of truth.  Symbols present in parquet files but **absent** from
the instrument master (segment=NSE) are considered "dead" (delisted /
suspended).

Usage::

    from nse_momentum_lab.services.kite.tradeable import (
        get_tradeable_symbols,
        get_dead_symbols,
        get_dead_symbol_stats,
    )

    tradeable = get_tradeable_symbols()
    dead = get_dead_symbols(Path("data/parquet/daily"), tradeable)
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Literal

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
INSTRUMENTS_DIR = PROJECT_ROOT / "data" / "raw" / "kite" / "instruments"

# Minimum instrument master size — prevents accidental wipe if CSV is
# corrupted / empty.
MIN_INSTRUMENT_COUNT = 1_000


def get_tradeable_symbols(instruments_dir: Path | None = None) -> set[str]:
    """Load tradeable symbols from the Kite instrument master CSV.

    Filters for ``segment == "NSE"`` and ``instrument_type == "EQ"``.

    Returns an **empty set** if the instrument master is missing (graceful
    degradation — callers should check ``if tradeable:`` before proceeding).
    """
    csv_path = (instruments_dir or INSTRUMENTS_DIR) / "NSE.csv"
    if not csv_path.exists():
        logger.warning("Instrument master not found: %s", csv_path)
        return set()

    symbols: set[str] = set()
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for raw_row in reader:
                row: dict[str, str] = {str(k).strip(): str(v).strip() for k, v in raw_row.items()}
                segment = row.get("segment", "").upper()
                instrument_type = row.get("instrument_type", "").upper()
                tradingsymbol = row.get("tradingsymbol", "").upper()
                if segment != "NSE" or instrument_type != "EQ" or not tradingsymbol:
                    continue
                symbols.add(tradingsymbol)
    except Exception:
        logger.exception("Failed to read instrument master: %s", csv_path)
        return set()

    if len(symbols) < MIN_INSTRUMENT_COUNT:
        logger.error(
            "Instrument master has only %d symbols (minimum %d) — refusing to proceed. "
            "The file may be corrupted or stale. Refresh with: kite.refresh_instruments()",
            len(symbols),
            MIN_INSTRUMENT_COUNT,
        )
        return set()

    logger.info("Loaded %d tradeable symbols from %s", len(symbols), csv_path.name)
    return symbols


ParquetLayout = Literal["daily", "5min"]


def get_parquet_symbols(parquet_dir: Path, *, layout: ParquetLayout = "daily") -> set[str]:
    """Return symbol names that have parquet data for a specific layout.

    ``layout="daily"`` expects ``{SYMBOL}/all.parquet`` or ``{SYMBOL}/kite.parquet``.
    ``layout="5min"`` expects ``{SYMBOL}/*.parquet`` (typically year-partitioned files).
    """
    if not parquet_dir.exists():
        return set()

    symbols: set[str] = set()
    for child in sorted(parquet_dir.iterdir()):
        if not child.is_dir():
            continue
        has_parquet = False
        if layout == "daily":
            has_parquet = (child / "all.parquet").exists() or (child / "kite.parquet").exists()
        else:
            has_parquet = any(f.is_file() for f in child.rglob("*.parquet"))
        if has_parquet:
            symbols.add(child.name.strip().upper())
    return symbols


def get_dead_symbols(
    parquet_dir: Path,
    tradeable: set[str] | None = None,
    *,
    layout: ParquetLayout = "daily",
) -> set[str]:
    """Symbols present in parquet but **not** in the tradeable set.

    Args:
        parquet_dir: Directory containing ``{SYMBOL}/`` subdirectories.
        tradeable: Pre-loaded tradeable set.  If *None*, loaded automatically.
        layout: Parquet layout to inspect.  Use ``"5min"`` for year-partitioned 5-minute data.

    Returns:
        Set of dead symbol names.  Empty if tradeable set is empty (safety).
    """
    if tradeable is None:
        tradeable = get_tradeable_symbols()
    if not tradeable:
        logger.warning("No tradeable symbols available — cannot determine dead symbols")
        return set()

    parquet_symbols = get_parquet_symbols(parquet_dir, layout=layout)
    dead = parquet_symbols - tradeable
    logger.info(
        "Dead symbols: %d / %d (%.1f%%)",
        len(dead),
        len(parquet_symbols),
        len(dead) / len(parquet_symbols) * 100 if parquet_symbols else 0,
    )
    return dead


def get_dead_symbol_stats(
    parquet_dir: Path,
    dead_symbols: set[str],
) -> list[dict[str, Any]]:
    """Per-symbol stats for dead symbols from parquet files.

    Returns list of dicts with keys: symbol, row_count, last_date, dir_size_bytes.
    """
    if not dead_symbols:
        return []

    import duckdb

    stats: list[dict[str, Any]] = []
    parquet_abs = parquet_dir.resolve()

    try:
        con = duckdb.connect()
        for symbol in sorted(dead_symbols):
            sym_pattern = str(parquet_abs / symbol / "*.parquet").replace("\\", "/")
            try:
                row = con.execute(
                    f"""
                    SELECT COUNT(*) as cnt, MAX(CAST(date AS DATE)) as last_dt
                    FROM read_parquet('{sym_pattern}', hive_partitioning=false)
                    """
                ).fetchone()
                row_count = row[0] if row else 0
                last_date = str(row[1]) if row and row[1] else "unknown"
            except Exception:
                row_count = 0
                last_date = "error"

            # Directory size
            sym_dir = parquet_abs / symbol
            dir_size = (
                sum(f.stat().st_size for f in sym_dir.rglob("*") if f.is_file())
                if sym_dir.exists()
                else 0
            )

            stats.append(
                {
                    "symbol": symbol,
                    "row_count": row_count,
                    "last_date": last_date,
                    "dir_size_bytes": dir_size,
                }
            )
        con.close()
    except Exception:
        logger.exception("Failed to compute dead symbol stats")

    return sorted(stats, key=lambda s: s["row_count"], reverse=True)


def get_duckdb_dead_row_counts(
    db_path: Path,
    dead_symbols: set[str],
    tables: list[str] | None = None,
) -> dict[str, int]:
    """Count rows for dead symbols in each DuckDB table.

    Args:
        db_path: Path to market.duckdb
        dead_symbols: Set of dead symbol names
        tables: Tables to check (default: feat_* tables only, NOT bt_* tables)

    Returns:
        Dict mapping table_name → dead_row_count
    """
    if not dead_symbols or not db_path.exists():
        return {}

    if tables is None:
        tables = [
            "feat_daily",
            "feat_daily_core",
            "feat_intraday_core",
            "feat_2lynch_derived",
            "feat_event_core",
        ]

    import duckdb

    counts: dict[str, int] = {}
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        placeholders = ",".join(["?"] * len(dead_symbols))
        symbol_list = sorted(dead_symbols)
        for table in tables:
            try:
                row = con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE symbol IN ({placeholders})",
                    symbol_list,
                ).fetchone()
                counts[table] = row[0] if row else 0
            except Exception:
                logger.debug("Table %s not found or has no symbol column", table)
                counts[table] = 0
        con.close()
    except Exception:
        logger.exception("Failed to query DuckDB for dead row counts")

    return counts
