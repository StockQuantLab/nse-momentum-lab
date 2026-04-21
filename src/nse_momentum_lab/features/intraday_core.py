"""
Intraday Core Features for NSE Momentum Lab.

feat_intraday_core contains strategy-agnostic intraday features for
strategies that use 5-minute entry timing:

- Opening range high/low (first 15/30/60 minutes)
- First trigger time (first time price crosses a threshold)
- First break of prior high/low
- Intraday volume percentile
- Intraday range expansion
- First-hour high/low
- Trigger-to-stop distance calculations
- Entry cutoff window markers

These features use v_5min and are designed for strategies that need
precise intraday entry timing (like FEE - Find and Enter Early).
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import TypedDict

from nse_momentum_lab.features.progress import (
    FeatureBuildProgressReporter,
    configure_duckdb_for_feature_build,
)
from nse_momentum_lab.features.registry import (
    FeatureDefinition,
    FeatureDependency,
    FeatureGranularity,
    IncrementalPolicy,
)

logger = logging.getLogger(__name__)


class _Manifest(TypedDict):
    five_min: dict[str, list[str]]
    daily: dict[str, list[str]]
    symbols_5min: list[str]
    symbols_daily: list[str]


# Version for feat_intraday_core - bump when SQL logic changes
FEAT_INTRADAY_CORE_VERSION = "feat_intraday_core_v1_2026_03_06"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_local_parquet_dir() -> Path:
    return Path(os.getenv("DATA_LAKE_LOCAL_DIR", str(_repo_root() / "data" / "parquet")))


def _default_batch_size() -> int:
    raw = os.getenv("INTRADAY_CORE_BATCH_SIZE", "32").strip()
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid INTRADAY_CORE_BATCH_SIZE=%r; falling back to 32", raw)
        return 32
    return max(1, value)


def _iter_symbol_batches(symbols: list[str], batch_size: int) -> list[list[str]]:
    size = max(1, int(batch_size))
    return [symbols[i : i + size] for i in range(0, len(symbols), size)]


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_parquet_manifest(
    parquet_dir: Path,
) -> _Manifest | None:
    """Walk parquet_dir once, returning {symbol: [sorted file paths]} for 5min and daily.

    Replaces per-symbol filesystem globs with a single pathlib traversal.
    Returns None if parquet_dir does not exist.
    """
    if not parquet_dir.exists():
        return None

    five_min: dict[str, list[str]] = {}
    daily: dict[str, list[str]] = {}

    for subdir, target in [("5min", five_min), ("daily", daily)]:
        root = parquet_dir / subdir
        if not root.exists():
            continue
        for symbol_dir in root.iterdir():
            if not symbol_dir.is_dir():
                continue
            files = sorted(str(f) for f in symbol_dir.glob("*.parquet"))
            if files:
                target[symbol_dir.name.strip().upper()] = files

    result: _Manifest = {
        "five_min": five_min,
        "daily": daily,
        "symbols_5min": sorted(five_min.keys()),
        "symbols_daily": sorted(daily.keys()),
    }
    logger.info(
        "Parquet manifest: %d 5min symbols, %d daily symbols",
        len(five_min),
        len(daily),
    )
    return result


def _list_parquet_symbols(
    parquet_dir: Path,
    subdir: str,
    manifest: _Manifest | None = None,
) -> list[str]:
    if manifest is not None:
        if subdir == "5min":
            return manifest["symbols_5min"]
        return manifest["symbols_daily"]

    root = parquet_dir / subdir
    if not root.exists():
        return []
    symbols: list[str] = []
    for child in root.iterdir():
        if child.is_dir() and any(child.glob("*.parquet")):
            symbols.append(child.name.strip().upper())
    return sorted(set(symbols))


def _split_symbols_with_required_parquet(
    parquet_dir: Path,
    symbols: list[str],
    manifest: _Manifest | None = None,
) -> tuple[list[str], list[str]]:
    """Split symbols into buildable vs missing-in-lake buckets."""

    if manifest is not None:
        five_min_map = manifest["five_min"]
        daily_map = manifest["daily"]
        buildable_manifest = [s for s in symbols if s in five_min_map and s in daily_map]
        missing_manifest = [s for s in symbols if s not in buildable_manifest]
        return buildable_manifest, missing_manifest

    buildable: list[str] = []
    missing: list[str] = []
    for symbol in symbols:
        five_min_dir = parquet_dir / "5min" / symbol
        daily_dir = parquet_dir / "daily" / symbol
        has_5min = five_min_dir.is_dir() and any(five_min_dir.glob("*.parquet"))
        has_daily = daily_dir.is_dir() and any(daily_dir.glob("*.parquet"))
        if has_5min and has_daily:
            buildable.append(symbol)
        else:
            missing.append(symbol)
    return buildable, missing


def _build_symbol_source_select(
    *,
    parquet_dir: Path,
    subdir: str,
    symbols: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    fallback_view: str | None = None,
    manifest: _Manifest | None = None,
) -> str:
    """Return a SELECT statement for a symbol-scoped parquet source."""

    if parquet_dir.exists():
        path_list: list[str] = []
        if manifest is not None:
            file_map = manifest["five_min"] if subdir == "5min" else manifest["daily"]
            for symbol in symbols:
                if symbol in file_map:
                    path_list.extend(file_map[symbol])
        else:
            for symbol in symbols:
                glob_path = (parquet_dir / subdir / symbol / "*.parquet").as_posix()
                path_list.append(glob_path)
        if not path_list:
            raise RuntimeError("No parquet paths resolved for intraday batch")
        where_clauses: list[str] = []
        if start_date is not None:
            where_clauses.append(f"date >= DATE '{start_date.isoformat()}'")
        if end_date is not None:
            where_clauses.append(f"date <= DATE '{end_date.isoformat()}'")
        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        escaped = ",".join(f"'{_escape_sql_literal(p)}'" for p in path_list)
        return (
            "SELECT * FROM read_parquet("
            f"[{escaped}], hive_partitioning=false, union_by_name=true)"
            f"{where_sql}"
        )

    if fallback_view is None:
        raise RuntimeError("No parquet directory available and no fallback view provided")

    sym_literals = ",".join(f"'{_escape_sql_literal(symbol)}'" for symbol in symbols)
    where_clauses = [f"symbol IN ({sym_literals})"]
    if start_date is not None:
        where_clauses.append(f"date >= DATE '{start_date.isoformat()}'")
    if end_date is not None:
        where_clauses.append(f"date <= DATE '{end_date.isoformat()}'")
    return f"SELECT * FROM {fallback_view} WHERE {' AND '.join(where_clauses)}"


# SQL for building feat_intraday_core
FEAT_INTRADAY_CORE_SQL = """
CREATE TABLE feat_intraday_core AS
WITH base AS (
    SELECT
        symbol,
        date AS trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        close * volume AS dollar_vol,
        -- Row number per day for intraday calculations
        ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY candle_time) AS rn_5min,
        -- Count of 5-min candles per day (for robustness checks)
        COUNT(*) OVER (PARTITION BY symbol, date) AS candles_per_day
    FROM v_5min
    WHERE volume IS NOT NULL
),
with_prior_day AS (
    SELECT
        b.*,
        -- Get prior day's high/low for breakout detection
        ld.high AS prior_day_high,
        ld.low AS prior_day_low,
        ld.close AS prior_day_close
    FROM base b
    LEFT JOIN (
        SELECT
            symbol,
            date,
            high,
            low,
            close,
            LEAD(date) OVER (PARTITION BY symbol ORDER BY date) AS next_date
        FROM v_daily
    ) ld ON b.symbol = ld.symbol AND b.trading_date = ld.next_date
),
opening_ranges AS (
    SELECT
        symbol,
        trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        dollar_vol,
        prior_day_high,
        prior_day_low,
        prior_day_close,
        rn_5min,
        candles_per_day,
        -- Opening range metrics
        MAX(high) FILTER (WHERE rn_5min <= 3) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS or_15min_high,  -- First 15 min (3 candles)
        MAX(high) FILTER (WHERE rn_5min <= 6) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS or_30min_high,  -- First 30 min (6 candles)
        MAX(high) FILTER (WHERE rn_5min <= 12) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS or_60min_high,  -- First 60 min (12 candles)
        MIN(low) FILTER (WHERE rn_5min <= 3) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS or_15min_low,
        MIN(low) FILTER (WHERE rn_5min <= 6) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS or_30min_low,
        MIN(low) FILTER (WHERE rn_5min <= 12) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS or_60min_low
    FROM with_prior_day
),
first_hour AS (
    SELECT
        symbol,
        trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        dollar_vol,
        prior_day_high,
        prior_day_low,
        prior_day_close,
        rn_5min,
        candles_per_day,
        or_15min_high,
        or_30min_high,
        or_60min_high,
        or_15min_low,
        or_30min_low,
        or_60min_low,
        -- First hour metrics
        MAX(high) FILTER (WHERE rn_5min <= 12) OVER (
            PARTITION BY symbol, trading_date
        ) AS first_hour_high,
        MIN(low) FILTER (WHERE rn_5min <= 12) OVER (
            PARTITION BY symbol, trading_date
        ) AS first_hour_low,
        AVG(volume) FILTER (WHERE rn_5min <= 12) OVER (
            PARTITION BY symbol, trading_date
        ) AS first_hour_avg_vol
    FROM opening_ranges
),
breakout_detection AS (
    SELECT
        symbol,
        trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        dollar_vol,
        prior_day_high,
        prior_day_low,
        prior_day_close,
        rn_5min,
        candles_per_day,
        or_15min_high,
        or_30min_high,
        or_60min_high,
        or_15min_low,
        or_30min_low,
        or_60min_low,
        first_hour_high,
        first_hour_low,
        first_hour_avg_vol,
        -- First breakout of prior high (time when it happened)
        CASE
            WHEN prior_day_high IS NOT NULL AND high > prior_day_high THEN
                MIN(candle_time) FILTER (WHERE high > prior_day_high) OVER (
                    PARTITION BY symbol, trading_date
                    ORDER BY candle_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ELSE NULL
        END AS first_breakout_time,
        -- First breakdown of prior low
        CASE
            WHEN prior_day_low IS NOT NULL AND low < prior_day_low THEN
                MIN(candle_time) FILTER (WHERE low < prior_day_low) OVER (
                    PARTITION BY symbol, trading_date
                    ORDER BY candle_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ELSE NULL
        END AS first_breakdown_time,
        -- Gap at open
        CASE
            WHEN prior_day_close IS NOT NULL THEN
                (open - prior_day_close) / NULLIF(prior_day_close, 0)
            ELSE NULL
        END AS gap_open_pct,
        -- Intraday high vs opening range
        CASE
            WHEN or_15min_high IS NOT NULL THEN
                (high - or_15min_high) / NULLIF(or_15min_high, 0)
            ELSE NULL
        END AS range_expansion_vs_15min,
        -- Intraday low vs opening range
        CASE
            WHEN or_15min_low IS NOT NULL THEN
                (low - or_15min_low) / NULLIF(or_15min_low, 0)
            ELSE NULL
        END AS range_contraction_vs_15min
    FROM first_hour
),
volume_metrics AS (
    SELECT
        *,
        -- Cumulative volume by time of day
        SUM(volume) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_volume,
        -- Daily total volume (for percentile calc)
        SUM(volume) OVER (PARTITION BY symbol, trading_date) AS daily_total_volume
    FROM breakout_detection
),
final AS (
    SELECT
        symbol,
        trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        dollar_vol,
        rn_5min,
        candles_per_day,
        -- Opening ranges
        or_15min_high,
        or_15min_low,
        or_30min_high,
        or_30min_low,
        or_60min_high,
        or_60min_low,
        -- First hour
        first_hour_high,
        first_hour_low,
        first_hour_avg_vol,
        -- Breakout/breakdown times
        first_breakout_time,
        first_breakdown_time,
        gap_open_pct,
        -- Range expansion
        range_expansion_vs_15min,
        range_contraction_vs_15min,
        -- Volume percentile (how much of day's volume has traded by this candle)
        CASE
            WHEN daily_total_volume > 0 THEN
                cumulative_volume / NULLIF(daily_total_volume, 0)
            ELSE NULL
        END AS volume_percentile,
        -- Entry window flags (for FEE strategy)
        CASE
            -- NSE opens at 09:15 IST
            -- First 30 min = 09:15 to 09:45 (candles 1-6)
            WHEN rn_5min <= 6 THEN 'first_30min'
            -- First 60 min = 09:15 to 10:15 (candles 1-12)
            WHEN rn_5min <= 12 THEN 'first_60min'
            -- First 90 min = 09:15 to 10:45 (candles 1-18)
            WHEN rn_5min <= 18 THEN 'first_90min'
            ELSE 'after_90min'
        END AS entry_window
    FROM volume_metrics
    WHERE close IS NOT NULL
)
SELECT * FROM final
"""


def _sql_date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _intraday_core_sql_for_sources(source_5min_view: str, source_daily_view: str) -> str:
    return (
        FEAT_INTRADAY_CORE_SQL.replace("CREATE TABLE feat_intraday_core AS\n", "")
        .replace("FROM v_5min", f"FROM {source_5min_view}")
        .replace("FROM v_daily", f"FROM {source_daily_view}")
    )


def _build_feat_intraday_core_incremental(con, *, since_date: date, dataset_hash: str) -> int:
    source_5min_view = "_feat_intraday_core_5min_src"
    source_daily_view = "_feat_intraday_core_daily_src"
    delta_table = "_feat_intraday_core_delta"
    table_exists = True

    try:
        con.execute("SELECT 1 FROM feat_intraday_core LIMIT 1").fetchone()
    except Exception:
        table_exists = False

    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {source_5min_view} AS "
        f"SELECT * FROM v_5min WHERE date >= {_sql_date_literal(since_date)}"
    )
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {source_daily_view} AS "
        f"SELECT * FROM v_daily WHERE date >= {_sql_date_literal(since_date - timedelta(days=1))}"
    )
    try:
        con.execute(f"DROP TABLE IF EXISTS {delta_table}")
        con.execute(
            f"CREATE TEMP TABLE {delta_table} AS "
            f"{_intraday_core_sql_for_sources(source_5min_view, source_daily_view)}"
        )
        if table_exists:
            con.execute("DELETE FROM feat_intraday_core WHERE trading_date >= ?", [since_date])
            con.execute(
                f"""
                INSERT INTO feat_intraday_core
                SELECT *
                FROM {delta_table}
                WHERE trading_date >= ?
                """,
                [since_date],
            )
        else:
            con.execute(
                f"""
                CREATE TABLE feat_intraday_core AS
                SELECT *
                FROM {delta_table}
                WHERE trading_date >= {_sql_date_literal(since_date)}
                """
            )
    finally:
        con.execute(f"DROP VIEW IF EXISTS {source_5min_view}")
        con.execute(f"DROP VIEW IF EXISTS {source_daily_view}")
        con.execute(f"DROP TABLE IF EXISTS {delta_table}")

    if not table_exists:
        con.execute(
            "CREATE INDEX idx_feat_intraday_core_symbol_date ON feat_intraday_core(symbol, trading_date)"
        )
        con.execute(
            "CREATE INDEX idx_feat_intraday_core_date_time ON feat_intraday_core(trading_date, candle_time)"
        )

    row = con.execute("SELECT COUNT(*) FROM feat_intraday_core").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0
    con.execute(
        """
        INSERT OR REPLACE INTO bt_materialization_state
        (table_name, dataset_hash, query_version, row_count, updated_at)
        VALUES (?, ?, ?, ?, current_timestamp)
    """,
        ["feat_intraday_core", dataset_hash, FEAT_INTRADAY_CORE_VERSION, n],
    )
    logger.info("feat_intraday_core incrementally refreshed from %s: %d rows", since_date, n)
    return n


def _build_feat_intraday_core_symbols(
    con,
    symbols: list[str],
    dataset_hash: str,
) -> int:
    """UPSERT feat_intraday_core for specific symbols only."""
    import time as _time

    logger.info("feat_intraday_core: symbol-level upsert for %d symbols", len(symbols))
    filter_table = "_fic_sym_filter"
    source_5min_view = "_fic_sym_5min_src"
    source_daily_view = "_fic_sym_daily_src"
    delta_table = "_fic_sym_delta"

    logger.info("  Preparing source views...")
    con.execute(f"CREATE OR REPLACE TEMP TABLE {filter_table} (symbol VARCHAR)")
    if symbols:
        phs = ",".join(["(?)"] * len(symbols))
        con.execute(f"INSERT INTO {filter_table} VALUES {phs}", symbols)

    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {source_5min_view} AS "
        f"SELECT * FROM v_5min WHERE symbol IN (SELECT symbol FROM {filter_table})"
    )
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {source_daily_view} AS "
        f"SELECT * FROM v_daily WHERE symbol IN (SELECT symbol FROM {filter_table})"
    )

    table_exists = True
    try:
        con.execute("SELECT 1 FROM feat_intraday_core LIMIT 1").fetchone()
    except Exception:
        table_exists = False

    try:
        logger.info("  Computing intraday features (window functions over 5min data)...")
        t0 = _time.monotonic()
        con.execute(f"DROP TABLE IF EXISTS {delta_table}")
        con.execute(
            f"CREATE TEMP TABLE {delta_table} AS "
            f"{_intraday_core_sql_for_sources(source_5min_view, source_daily_view)}"
        )
        logger.info("  SQL complete in %.1fs", _time.monotonic() - t0)

        if table_exists:
            logger.info("  Upserting into existing table (delete + insert)...")
            del_phs = ",".join(["?"] * len(symbols))
            con.execute(f"DELETE FROM feat_intraday_core WHERE symbol IN ({del_phs})", symbols)
            con.execute(f"INSERT INTO feat_intraday_core SELECT * FROM {delta_table}")
        else:
            con.execute(f"CREATE TABLE feat_intraday_core AS SELECT * FROM {delta_table}")
            con.execute(
                "CREATE INDEX idx_feat_intraday_core_symbol_date ON feat_intraday_core(symbol, trading_date)"
            )
            con.execute(
                "CREATE INDEX idx_feat_intraday_core_date_time ON feat_intraday_core(trading_date, candle_time)"
            )
    finally:
        con.execute(f"DROP VIEW IF EXISTS {source_5min_view}")
        con.execute(f"DROP VIEW IF EXISTS {source_daily_view}")
        con.execute(f"DROP TABLE IF EXISTS {delta_table}")
        con.execute(f"DROP TABLE IF EXISTS {filter_table}")

    row = con.execute("SELECT COUNT(*) FROM feat_intraday_core").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0
    con.execute(
        """
        INSERT OR REPLACE INTO bt_materialization_state
        (table_name, dataset_hash, query_version, row_count, updated_at)
        VALUES (?, ?, ?, ?, current_timestamp)
        """,
        ["feat_intraday_core", dataset_hash, FEAT_INTRADAY_CORE_VERSION, n],
    )
    logger.info(
        "feat_intraday_core symbol-level refresh: %d symbols → %d total rows", len(symbols), n
    )
    return n


def _build_feat_intraday_core_yearly(
    con,
    *,
    dataset_hash: str,
    year_start: int | None = None,
    year_end: int | None = None,
) -> int:
    """Legacy yearly helper retained only for emergency debugging.

    The production path now uses symbol batches and symbol-specific parquet reads.
    This helper stays gated so it cannot be invoked accidentally.
    """

    if os.getenv("ALLOW_LEGACY_INTRADAY_YEARLY_REBUILD", "").strip() != "1":
        raise RuntimeError(
            "Legacy feat_intraday_core yearly rebuild is disabled. "
            "Use the batched rebuild path instead. "
            "Set ALLOW_LEGACY_INTRADAY_YEARLY_REBUILD=1 only for emergency debugging."
        )

    import time as _time

    source_5min_view = "_fic_year_5min_src"
    source_daily_view = "_fic_year_daily_src"
    delta_table = "_fic_year_delta"

    bounds = con.execute("SELECT MIN(date), MAX(date) FROM v_5min").fetchone()
    if not bounds or bounds[0] is None or bounds[1] is None:
        logger.warning("v_5min has no date bounds, skipping feat_intraday_core")
        return 0

    min_date: date = bounds[0]
    max_date: date = bounds[1]
    selected_start = year_start if year_start is not None else min_date.year
    selected_end = year_end if year_end is not None else max_date.year
    if selected_start > selected_end:
        raise ValueError(f"year_start must be <= year_end (got {selected_start} > {selected_end})")
    years = [
        year
        for year in range(min_date.year, max_date.year + 1)
        if selected_start <= year <= selected_end
    ]
    if not years:
        logger.warning(
            "feat_intraday_core yearly rebuild: no years selected within %s to %s",
            selected_start,
            selected_end,
        )
        return 0

    logger.info(
        "feat_intraday_core yearly rebuild: %d year(s) from %s to %s (selected %s-%s)",
        len(years),
        min_date,
        max_date,
        selected_start,
        selected_end,
    )

    con.execute("DROP TABLE IF EXISTS feat_intraday_core")
    total_rows = 0

    for idx, year in enumerate(years, 1):
        loop_start = date(year, 1, 1)
        loop_end = date(year, 12, 31)
        if year == min_date.year and min_date > loop_start:
            loop_start = min_date
        if year == max_date.year and max_date < loop_end:
            loop_end = max_date

        daily_start = loop_start - timedelta(days=1)
        logger.info(
            "  Year %d/%d: building %s to %s",
            idx,
            len(years),
            loop_start,
            loop_end,
        )

        con.execute(
            f"CREATE OR REPLACE TEMP VIEW {source_5min_view} AS "
            f"SELECT * FROM v_5min "
            f"WHERE date >= DATE '{loop_start.isoformat()}' "
            f"AND date <= DATE '{loop_end.isoformat()}'"
        )
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW {source_daily_view} AS "
            f"SELECT * FROM v_daily "
            f"WHERE date >= DATE '{daily_start.isoformat()}' "
            f"AND date <= DATE '{loop_end.isoformat()}'"
        )

        con.execute(f"DROP TABLE IF EXISTS {delta_table}")
        t0 = _time.monotonic()
        con.execute(
            f"CREATE TEMP TABLE {delta_table} AS "
            f"{_intraday_core_sql_for_sources(source_5min_view, source_daily_view)}"
        )
        year_count_row = con.execute(f"SELECT COUNT(*) FROM {delta_table}").fetchone()
        year_count = (
            int(year_count_row[0]) if year_count_row and year_count_row[0] is not None else 0
        )

        if idx == 1:
            con.execute(f"CREATE TABLE feat_intraday_core AS SELECT * FROM {delta_table}")
        else:
            con.execute(f"INSERT INTO feat_intraday_core SELECT * FROM {delta_table}")
        total_rows += year_count
        logger.info(
            "  Year %d/%d complete in %.1fs: %d rows (running total %d)",
            idx,
            len(years),
            _time.monotonic() - t0,
            year_count,
            total_rows,
        )

    logger.info("  Phase 3/4: Creating indexes...")
    t0 = _time.monotonic()
    con.execute(
        "CREATE INDEX idx_feat_intraday_core_symbol_date ON feat_intraday_core(symbol, trading_date)"
    )
    con.execute(
        "CREATE INDEX idx_feat_intraday_core_date_time ON feat_intraday_core(trading_date, candle_time)"
    )
    logger.info("  Phase 3/4: Indexes created in %.1fs", _time.monotonic() - t0)

    row = con.execute("SELECT COUNT(*) FROM feat_intraday_core").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0
    logger.info("  Phase 4/4: Updating materialization state...")
    con.execute(
        """
        INSERT OR REPLACE INTO bt_materialization_state
        (table_name, dataset_hash, query_version, row_count, updated_at)
        VALUES (?, ?, ?, ?, current_timestamp)
    """,
        ["feat_intraday_core", dataset_hash, FEAT_INTRADAY_CORE_VERSION, n],
    )
    logger.info("feat_intraday_core built year-by-year: %d rows", n)
    return n


def _build_feat_intraday_core_batched(
    con,
    *,
    dataset_hash: str,
    symbols: list[str] | None = None,
    batch_size: int | None = None,
    progress: FeatureBuildProgressReporter | None = None,
    year_start: int | None = None,
    year_end: int | None = None,
    replace_existing: bool = False,
) -> int:
    """Build feat_intraday_core in symbol batches.

    This mirrors the production batch-builder pattern: small symbol batches,
    per-batch transactions, symbol-scoped parquet reads, and structured progress output.
    """

    import time as _time

    progress = progress or FeatureBuildProgressReporter()
    parquet_dir = _default_local_parquet_dir()
    batch_size = max(1, int(batch_size or _default_batch_size()))
    manifest = _build_parquet_manifest(parquet_dir) if parquet_dir.exists() else None

    if symbols is None:
        discovered: list[str] = []
        if parquet_dir.exists():
            five_min_symbols = _list_parquet_symbols(parquet_dir, "5min", manifest)
            daily_symbols = _list_parquet_symbols(parquet_dir, "daily", manifest)
            if five_min_symbols and daily_symbols:
                discovered = sorted(set(five_min_symbols).intersection(daily_symbols))
            else:
                discovered = five_min_symbols or daily_symbols
        if not discovered:
            try:
                rows = con.execute("SELECT DISTINCT symbol FROM v_5min ORDER BY symbol").fetchall()
                discovered = [r[0] for r in rows if r and r[0]]
            except Exception:
                discovered = []
        target_symbols = discovered
    else:
        target_symbols = sorted({s.strip().upper() for s in symbols if s and s.strip()})

    if not target_symbols:
        logger.warning("No symbols resolved for feat_intraday_core batched rebuild")
        return 0

    if parquet_dir.exists():
        buildable_symbols, missing_symbols = _split_symbols_with_required_parquet(
            parquet_dir, target_symbols, manifest
        )
        if missing_symbols:
            preview = ", ".join(missing_symbols[:5])
            suffix = "..." if len(missing_symbols) > 5 else ""
            logger.warning(
                "Skipping %d symbol(s) without both daily and 5-min parquet (%s%s)",
                len(missing_symbols),
                preview,
                suffix,
            )

        if not buildable_symbols:
            logger.warning("No buildable symbols found for feat_intraday_core")
            return 0
    else:
        buildable_symbols = target_symbols

    year_start_date = date(year_start, 1, 1) if year_start is not None else None
    year_end_date = date(year_end, 12, 31) if year_end is not None else None
    daily_start_date = (
        (year_start_date - timedelta(days=1)) if year_start_date is not None else None
    )

    batches = _iter_symbol_batches(buildable_symbols, batch_size)
    total_batches = len(batches)
    source_desc = f"{len(buildable_symbols):,} symbols from {parquet_dir}"
    if year_start_date or year_end_date:
        source_desc += (
            f" ({year_start_date.isoformat() if year_start_date else 'min'}"
            f"..{year_end_date.isoformat() if year_end_date else 'max'})"
        )

    progress.emit(
        stage="start",
        message=(
            f"Starting batched feat_intraday_core rebuild with {len(buildable_symbols):,} symbols "
            f"in {total_batches} batch(es)"
        ),
        status="running",
        progress_pct=0.0,
        step=0,
        step_total=total_batches,
        pending_features=total_batches,
        feature_name="feat_intraday_core",
    )
    progress.emit(
        stage="source_summary",
        message=f"Source summary: {source_desc}; batch_size={batch_size}",
        status="running",
        progress_pct=0.0,
        step=0,
        step_total=total_batches,
        pending_features=total_batches,
        feature_name="feat_intraday_core",
    )

    logger.info(
        "feat_intraday_core batched rebuild: %d symbol(s) in %d batch(es) (batch_size=%d)",
        len(buildable_symbols),
        total_batches,
        batch_size,
    )
    if year_start_date or year_end_date:
        logger.info(
            "feat_intraday_core batched rebuild date window: %s to %s",
            year_start_date.isoformat() if year_start_date else "min",
            year_end_date.isoformat() if year_end_date else "max",
        )

    table_exists = True
    try:
        con.execute("SELECT 1 FROM feat_intraday_core LIMIT 1").fetchone()
    except Exception:
        table_exists = False

    if not replace_existing:
        con.execute("DROP TABLE IF EXISTS feat_intraday_core")
        table_exists = False

    total_rows = 0
    for idx, batch in enumerate(batches, 1):
        batch_started = _time.monotonic()
        progress.emit(
            stage="batch_start",
            message=f"Building batch {idx}/{total_batches} with {len(batch)} symbol(s)",
            status="running",
            progress_pct=((idx - 1) / max(total_batches, 1)) * 100.0,
            step=idx,
            step_total=total_batches,
            pending_features=total_batches - idx + 1,
            feature_name="feat_intraday_core",
        )
        logger.info(
            "  [intraday] batch %d/%d (%d symbols): %s",
            idx,
            total_batches,
            len(batch),
            ", ".join(batch[:5]) + ("..." if len(batch) > 5 else ""),
        )
        tx_open = False
        source_5min_view = "_fic_batch_5min_src"
        source_daily_view = "_fic_batch_daily_src"
        delta_table = "_fic_batch_delta"
        try:
            con.execute("BEGIN TRANSACTION")
            tx_open = True

            if replace_existing and table_exists:
                placeholders = ",".join("?" for _ in batch)
                con.execute(
                    f"DELETE FROM feat_intraday_core WHERE symbol IN ({placeholders})",
                    batch,
                )

            con.execute(f"DROP TABLE IF EXISTS {delta_table}")

            source_5min_sql = _build_symbol_source_select(
                parquet_dir=parquet_dir,
                subdir="5min",
                symbols=batch,
                start_date=year_start_date,
                end_date=year_end_date,
                fallback_view="v_5min",
                manifest=manifest,
            )
            con.execute(f"CREATE OR REPLACE TEMP VIEW {source_5min_view} AS {source_5min_sql}")
            source_daily_sql = _build_symbol_source_select(
                parquet_dir=parquet_dir,
                subdir="daily",
                symbols=batch,
                start_date=daily_start_date,
                end_date=year_end_date,
                fallback_view="v_daily",
                manifest=manifest,
            )
            con.execute(f"CREATE OR REPLACE TEMP VIEW {source_daily_view} AS {source_daily_sql}")

            t0 = _time.monotonic()
            con.execute(
                f"CREATE TEMP TABLE {delta_table} AS "
                f"{_intraday_core_sql_for_sources(source_5min_view, source_daily_view)}"
            )
            batch_rows_row = con.execute(f"SELECT COUNT(*) FROM {delta_table}").fetchone()
            batch_rows = (
                int(batch_rows_row[0]) if batch_rows_row and batch_rows_row[0] is not None else 0
            )

            if table_exists:
                con.execute(f"INSERT INTO feat_intraday_core SELECT * FROM {delta_table}")
            else:
                con.execute(f"CREATE TABLE feat_intraday_core AS SELECT * FROM {delta_table}")
                table_exists = True

            total_rows += batch_rows
            batch_elapsed = _time.monotonic() - batch_started
            progress.emit(
                stage="batch_success",
                message=(
                    f"Batch {idx}/{total_batches} complete: {batch_rows:,} rows "
                    f"(running total {total_rows:,})"
                ),
                status="running",
                progress_pct=(idx / max(total_batches, 1)) * 100.0,
                step=idx,
                step_total=total_batches,
                pending_features=total_batches - idx,
                feature_name="feat_intraday_core",
                row_count=batch_rows,
                duration_seconds=batch_elapsed,
            )
            logger.info(
                "  [intraday] batch %d/%d complete in %.1fs: %d rows (running total %d)",
                idx,
                total_batches,
                batch_elapsed,
                batch_rows,
                total_rows,
            )
            logger.info("  [intraday] batch %d SQL finished in %.1fs", idx, _time.monotonic() - t0)
            con.execute("COMMIT")
            tx_open = False
        except Exception as exc:
            if tx_open:
                con.execute("ROLLBACK")
            progress.emit(
                stage="batch_failed",
                message=f"Batch {idx}/{total_batches} failed",
                status="failed",
                progress_pct=((idx - 1) / max(total_batches, 1)) * 100.0,
                step=idx,
                step_total=total_batches,
                pending_features=total_batches - idx + 1,
                feature_name="feat_intraday_core",
                error_message=str(exc),
            )
            progress.emit(
                stage="complete",
                message=(
                    f"feat_intraday_core batched rebuild failed after {idx - 1} completed batch(es)"
                ),
                status="failed",
                progress_pct=((idx - 1) / max(total_batches, 1)) * 100.0,
                step=idx - 1,
                step_total=total_batches,
                pending_features=total_batches - idx + 1,
                feature_name="feat_intraday_core",
                row_count=total_rows,
                error_message=str(exc),
            )
            logger.exception(
                "Failed while building feat_intraday_core batch %d/%d", idx, total_batches
            )
            raise
        finally:
            con.execute(f"DROP VIEW IF EXISTS {source_5min_view}")
            con.execute(f"DROP VIEW IF EXISTS {source_daily_view}")
            con.execute(f"DROP TABLE IF EXISTS {delta_table}")

    logger.info("  Phase 3/4: Creating indexes...")
    t0 = _time.monotonic()
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_feat_intraday_core_symbol_date ON feat_intraday_core(symbol, trading_date)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_feat_intraday_core_date_time ON feat_intraday_core(trading_date, candle_time)"
    )
    logger.info("  Phase 3/4: Indexes created in %.1fs", _time.monotonic() - t0)

    row = con.execute("SELECT COUNT(*) FROM feat_intraday_core").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0
    logger.info("  Phase 4/4: Updating materialization state...")
    con.execute(
        """
        INSERT OR REPLACE INTO bt_materialization_state
        (table_name, dataset_hash, query_version, row_count, updated_at)
        VALUES (?, ?, ?, ?, current_timestamp)
    """,
        ["feat_intraday_core", dataset_hash, FEAT_INTRADAY_CORE_VERSION, n],
    )
    logger.info("feat_intraday_core built in symbol batches: %d rows", n)
    progress.emit(
        stage="complete",
        message=f"feat_intraday_core built in symbol batches: {n:,} rows",
        status="success",
        progress_pct=100.0,
        step=total_batches,
        step_total=total_batches,
        pending_features=0,
        feature_name="feat_intraday_core",
        row_count=n,
    )
    return n


def build_feat_intraday_core(
    con,  # DuckDBPyConnection
    force: bool = False,
    dataset_hash: str | None = None,
    since_date: date | None = None,
    symbols: list[str] | None = None,
    year_start: int | None = None,
    year_end: int | None = None,
    progress: FeatureBuildProgressReporter | None = None,
) -> int:
    """
    Build the feat_intraday_core materialized table.

    Args:
        con: DuckDB connection
        force: Force rebuild even if up-to-date
        dataset_hash: Hash of input dataset for incremental detection
        since_date: Incremental rebuild from this date forward
        symbols: If given, UPSERT only these symbols (overrides since_date/force)
        year_start/year_end: Optional calendar-year bounds for full yearly rebuilds

    Returns:
        Number of rows in the built table
    """
    import time as _time

    configure_duckdb_for_feature_build(con)

    # Check if v_5min exists before trying to hash the source snapshot.
    try:
        con.execute("SELECT 1 FROM v_5min LIMIT 1").fetchone()
    except Exception:
        logger.warning("v_5min view not available, skipping feat_intraday_core")
        return 0

    if dataset_hash is None:
        import hashlib
        import json

        logger.info("Hashing v_5min source data...")
        snapshot_row = con.execute("""
            SELECT
                COUNT(*)::BIGINT AS rows,
                COUNT(DISTINCT symbol)::BIGINT AS symbols,
                MIN(date)::VARCHAR AS min_date,
                MAX(date)::VARCHAR AS max_date
            FROM v_5min
        """).fetchone()
        snapshot = {
            "rows": int(snapshot_row[0]) if snapshot_row and snapshot_row[0] else 0,
            "symbols": int(snapshot_row[1]) if snapshot_row and snapshot_row[1] else 0,
            "min_date": snapshot_row[2] if snapshot_row else None,
            "max_date": snapshot_row[3] if snapshot_row else None,
        }
        dataset_hash = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()[
            :16
        ]
        logger.info(
            "  v_5min: %s symbols, %s rows (%s to %s) hash=%s",
            f"{snapshot['symbols']:,}",
            f"{snapshot['rows']:,}",
            snapshot["min_date"],
            snapshot["max_date"],
            dataset_hash,
        )

    if symbols is not None:
        if not symbols:
            row = con.execute("SELECT COUNT(*) FROM feat_intraday_core").fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        return _build_feat_intraday_core_batched(
            con,
            symbols=symbols,
            dataset_hash=dataset_hash,
            progress=progress,
            year_start=year_start,
            year_end=year_end,
            replace_existing=True,
        )

    if since_date is not None and not force:
        return _build_feat_intraday_core_incremental(
            con, since_date=since_date, dataset_hash=dataset_hash
        )

    # Check if already built
    if not force:
        try:
            row = con.execute(
                "SELECT table_name, dataset_hash, query_version, row_count FROM bt_materialization_state "
                "WHERE table_name = 'feat_intraday_core'"
            ).fetchone()
            if row:
                _table_name, current_dataset_hash, query_version, row_count = row
                if (
                    query_version == FEAT_INTRADAY_CORE_VERSION
                    and current_dataset_hash == dataset_hash
                ):
                    logger.info("feat_intraday_core is up-to-date (%d rows).", row_count)
                    return int(row_count)
        except Exception:
            pass  # Table doesn't exist yet

    # Drop and rebuild with symbol batches and symbol-specific parquet reads.
    logger.info("Building feat_intraday_core materialized table...")
    logger.info("  Phase 2/4: Executing symbol-batched rebuild...")
    t0 = _time.monotonic()
    n = _build_feat_intraday_core_batched(
        con,
        dataset_hash=dataset_hash,
        progress=progress,
        year_start=year_start,
        year_end=year_end,
        replace_existing=False,
    )
    logger.info("  Phase 2/4: Symbol-batched rebuild complete in %.1fs", _time.monotonic() - t0)
    return n


def register_feat_intraday_core(registry) -> None:
    """Register feat_intraday_core with the feature registry."""

    registry.register(
        FeatureDefinition(
            name="feat_intraday_core",
            version=FEAT_INTRADAY_CORE_VERSION,
            description="Core intraday features: opening ranges, breakout/breakdown times, volume metrics, entry windows. For FEE strategies.",
            granularity=FeatureGranularity.FIVE_MIN,
            layer="core",
            input_datasets=["v_5min", "v_daily"],
            feature_dependencies=[
                FeatureDependency(name="v_5min", is_dataset=True, required_lookback_days=1),
                FeatureDependency(name="v_daily", is_dataset=True, required_lookback_days=1),
            ],
            required_lookback_days=1,
            build_sql=FEAT_INTRADAY_CORE_SQL,
            incremental_policy=IncrementalPolicy.ROLLING_WINDOW,
            partition_grain="year",
            output_columns=[
                "symbol",
                "trading_date",
                "candle_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "dollar_vol",
                "rn_5min",
                "candles_per_day",
                "or_15min_high",
                "or_15min_low",
                "or_30min_high",
                "or_30min_low",
                "or_60min_high",
                "or_60min_low",
                "first_hour_high",
                "first_hour_low",
                "first_hour_avg_vol",
                "first_breakout_time",
                "first_breakdown_time",
                "gap_open_pct",
                "range_expansion_vs_15min",
                "range_contraction_vs_15min",
                "volume_percentile",
                "entry_window",
            ],
        )
    )
