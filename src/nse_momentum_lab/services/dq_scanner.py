"""
Data Quality Scanner -- pure SQL scan functions for NSE Momentum Lab.

Each scan function receives a DuckDB connection and optional date window,
and returns a ``DQScanResult`` with the affected symbols and metadata.
Scanners do NOT write to the ``data_quality_issues`` table -- the caller
handles persistence.

Usage::

    from nse_momentum_lab.services.dq_scanner import run_full_scan, run_fast_scan

    results = run_full_scan(con, window_start=date(2025, 1, 1))
    for result in results:
        db.upsert_data_quality_issues(result.symbols, result.issue_code, ...)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import duckdb

logger = logging.getLogger(__name__)


@dataclass
class DQScanResult:
    """Result from a single DQ scan."""

    issue_code: str
    severity: str
    symbols: list[str]
    details: str = ""
    count: int = 0

    def __post_init__(self) -> None:
        if self.count == 0:
            self.count = len(self.symbols)


# ---------------------------------------------------------------------------
# Individual scan functions
# ---------------------------------------------------------------------------


def scan_ohlc_violation(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    source: str = "both",
) -> DQScanResult:
    """Detect high < low, or open/close outside [low, high]."""
    views = _resolve_views(con, source)
    if not views:
        return DQScanResult(issue_code="OHLC_VIOLATION", severity="CRITICAL", symbols=[])

    all_symbols: list[str] = []
    for view in views:
        rows = con.execute(
            f"""
            SELECT DISTINCT symbol
            FROM {view}
            WHERE high < low
               OR open < low OR open > high
               OR close < low OR close > high
               {_date_filter(view, window_start, window_end)}
            """
        ).fetchall()
        all_symbols.extend(r[0] for r in rows)

    symbols = sorted(set(all_symbols))
    return DQScanResult(
        issue_code="OHLC_VIOLATION",
        severity="CRITICAL",
        symbols=symbols,
        details=f"high<low or open/close outside [low,high] in {len(views)} view(s)",
    )


def scan_null_price(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    source: str = "both",
) -> DQScanResult:
    """Detect NULL values in OHLC columns."""
    views = _resolve_views(con, source)
    if not views:
        return DQScanResult(issue_code="NULL_PRICE", severity="CRITICAL", symbols=[])

    all_symbols: list[str] = []
    for view in views:
        rows = con.execute(
            f"""
            SELECT DISTINCT symbol
            FROM {view}
            WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
               {_date_filter(view, window_start, window_end)}
            """
        ).fetchall()
        all_symbols.extend(r[0] for r in rows)

    symbols = sorted(set(all_symbols))
    return DQScanResult(
        issue_code="NULL_PRICE",
        severity="CRITICAL",
        symbols=symbols,
        details="NULL in OHLC columns",
    )


def scan_zero_price(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    source: str = "both",
) -> DQScanResult:
    """Detect zero open/close/high prices (low=0 is common for some feeds)."""
    views = _resolve_views(con, source)
    if not views:
        return DQScanResult(issue_code="ZERO_PRICE", severity="WARNING", symbols=[])

    all_symbols: list[str] = []
    for view in views:
        rows = con.execute(
            f"""
            SELECT DISTINCT symbol
            FROM {view}
            WHERE open = 0 OR close = 0 OR high = 0
               {_date_filter(view, window_start, window_end)}
            """
        ).fetchall()
        all_symbols.extend(r[0] for r in rows)

    symbols = sorted(set(all_symbols))
    return DQScanResult(
        issue_code="ZERO_PRICE",
        severity="WARNING",
        symbols=symbols,
        details="open/close/high = 0",
    )


def scan_timestamp_invalid(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
) -> DQScanResult:
    """Detect 5-min candles outside 09:15-15:30 IST."""
    if not _view_exists(con, "v_5min"):
        return DQScanResult(issue_code="TIMESTAMP_INVALID", severity="CRITICAL", symbols=[])

    rows = con.execute(
        f"""
        SELECT DISTINCT symbol
        FROM v_5min
        WHERE candle_time::TIME < '09:15:00' OR candle_time::TIME > '15:30:00'
           {_date_filter("v_5min", window_start, window_end)}
        """
    ).fetchall()
    symbols = sorted(r[0] for r in rows)
    return DQScanResult(
        issue_code="TIMESTAMP_INVALID",
        severity="CRITICAL",
        symbols=symbols,
        details="candle_time outside 09:15-15:30 IST",
    )


def scan_extreme_candle(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    threshold: float = 0.50,
) -> DQScanResult:
    """Detect 5-min candles where (high-low)/open exceeds threshold."""
    if not _view_exists(con, "v_5min"):
        return DQScanResult(issue_code="EXTREME_CANDLE", severity="WARNING", symbols=[])

    rows = con.execute(
        f"""
        SELECT DISTINCT symbol
        FROM v_5min
        WHERE open > 0 AND (high - low) / open > {threshold}
           {_date_filter("v_5min", window_start, window_end)}
        """
    ).fetchall()
    symbols = sorted(r[0] for r in rows)
    return DQScanResult(
        issue_code="EXTREME_CANDLE",
        severity="WARNING",
        symbols=symbols,
        details=f"(high-low)/open > {threshold:.0%}",
    )


def scan_duplicate_candle(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
) -> DQScanResult:
    """Detect duplicate (symbol, date, candle_time) in 5-min data."""
    if not _view_exists(con, "v_5min"):
        return DQScanResult(issue_code="DUPLICATE_CANDLE", severity="CRITICAL", symbols=[])

    rows = con.execute(
        f"""
        SELECT DISTINCT symbol
        FROM (
            SELECT symbol, date, candle_time, COUNT(*) AS cnt
            FROM v_5min
            WHERE 1=1 {_date_filter("v_5min", window_start, window_end, prefix="AND")}
            GROUP BY symbol, date, candle_time
            HAVING cnt > 1
        )
        """
    ).fetchall()
    symbols = sorted(r[0] for r in rows)
    return DQScanResult(
        issue_code="DUPLICATE_CANDLE",
        severity="CRITICAL",
        symbols=symbols,
        details="duplicate (symbol, date, candle_time) rows",
    )


def scan_date_gap(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    gap_days: int = 7,
) -> DQScanResult:
    """Detect gaps > N calendar days between trading dates in daily data.

    Only flags gaps that occur *within* a symbol's active trading range
    (between its first and last candle). Pre-listing gaps are excluded.
    """
    if not _view_exists(con, "v_daily"):
        return DQScanResult(issue_code="DATE_GAP", severity="WARNING", symbols=[])

    rows = con.execute(
        f"""
        WITH symbol_range AS (
            SELECT symbol, MIN(date) AS first_date, MAX(date) AS last_date
            FROM v_daily
            WHERE 1=1 {_date_filter("v_daily", window_start, window_end, prefix="AND")}
            GROUP BY symbol
        ),
        gaps AS (
            SELECT d.symbol, d.date,
                   LAG(d.date) OVER (PARTITION BY d.symbol ORDER BY d.date) AS prev_date
            FROM v_daily d
            JOIN symbol_range sr ON d.symbol = sr.symbol
        )
        SELECT DISTINCT g.symbol
        FROM gaps g
        JOIN symbol_range sr ON g.symbol = sr.symbol
        WHERE g.prev_date IS NOT NULL
          AND DATEDIFF('day', g.prev_date, g.date) > {gap_days}
          AND g.prev_date > sr.first_date
        """
    ).fetchall()
    symbols = sorted(r[0] for r in rows)
    return DQScanResult(
        issue_code="DATE_GAP",
        severity="WARNING",
        symbols=symbols,
        details=f"gap > {gap_days} calendar days between trading dates (excludes pre-listing)",
    )


def scan_zero_volume_day(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
) -> DQScanResult:
    """Detect full trading days with zero volume."""
    if not _view_exists(con, "v_daily"):
        return DQScanResult(issue_code="ZERO_VOLUME_DAY", severity="INFO", symbols=[])

    rows = con.execute(
        f"""
        SELECT DISTINCT symbol
        FROM v_daily
        WHERE volume = 0
           {_date_filter("v_daily", window_start, window_end)}
        """
    ).fetchall()
    symbols = sorted(r[0] for r in rows)
    return DQScanResult(
        issue_code="ZERO_VOLUME_DAY",
        severity="INFO",
        symbols=symbols,
        details="full day with zero volume",
    )


def scan_extreme_move_daily(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    threshold: float = 0.30,
) -> DQScanResult:
    """Detect daily moves exceeding threshold."""
    if not _view_exists(con, "v_daily"):
        return DQScanResult(issue_code="EXTREME_MOVE_DAILY", severity="WARNING", symbols=[])

    rows = con.execute(
        f"""
        SELECT DISTINCT symbol
        FROM (
            SELECT symbol, date, close,
                   LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
            FROM v_daily
            WHERE 1=1 {_date_filter("v_daily", window_start, window_end, prefix="AND")}
        )
        WHERE prev_close IS NOT NULL
          AND prev_close > 0
          AND ABS(close / prev_close - 1) > {threshold}
        """
    ).fetchall()
    symbols = sorted(r[0] for r in rows)
    return DQScanResult(
        issue_code="EXTREME_MOVE_DAILY",
        severity="WARNING",
        symbols=symbols,
        details=f"|close/prev_close - 1| > {threshold:.0%}",
    )


def scan_missing_5min_coverage(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    collection_start: date | None = None,
) -> DQScanResult:
    """Detect symbol-date pairs with daily data but no 5-min data.

    Only checks dates on or after ``collection_start`` (default: 2025-04-01,
    the start of 5-min data collection). Dates before collection start are
    excluded since 5-min data was never collected for that era.
    """
    if not _view_exists(con, "v_daily") or not _view_exists(con, "v_5min"):
        return DQScanResult(issue_code="MISSING_5MIN_COVERAGE", severity="WARNING", symbols=[])

    effective_start = collection_start or date(2025, 4, 1)
    rows = con.execute(
        f"""
        WITH daily_dates AS (
            SELECT DISTINCT symbol, date
            FROM v_daily
            WHERE date >= '{effective_start.isoformat()}'
               {_date_filter("v_daily", window_start, window_end, prefix="AND")}
        ),
        fivemin_dates AS (
            SELECT DISTINCT symbol, date
            FROM v_5min
            WHERE date >= '{effective_start.isoformat()}'
               {_date_filter("v_5min", window_start, window_end, prefix="AND")}
        )
        SELECT DISTINCT d.symbol
        FROM daily_dates d
        LEFT JOIN fivemin_dates f ON d.symbol = f.symbol AND d.date = f.date
        WHERE f.date IS NULL
        ORDER BY d.symbol
        """
    ).fetchall()
    symbols = [r[0] for r in rows]
    return DQScanResult(
        issue_code="MISSING_5MIN_COVERAGE",
        severity="WARNING",
        symbols=symbols,
        details=f"daily data exists but no 5-min for same date (checked from {effective_start})",
    )


def scan_short_history(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
    min_dates: int = 252,
) -> DQScanResult:
    """Detect symbols with fewer than N trading dates."""
    if not _view_exists(con, "v_daily"):
        return DQScanResult(issue_code="SHORT_HISTORY", severity="INFO", symbols=[])

    rows = con.execute(
        f"""
        SELECT symbol
        FROM v_daily
        GROUP BY symbol
        HAVING COUNT(DISTINCT date) < {min_dates}
        ORDER BY symbol
        """
    ).fetchall()
    symbols = [r[0] for r in rows]
    return DQScanResult(
        issue_code="SHORT_HISTORY",
        severity="INFO",
        symbols=symbols,
        details=f"< {min_dates} distinct trading dates (newly listed)",
    )


# ---------------------------------------------------------------------------
# Orchestrators
# ---------------------------------------------------------------------------


_ALL_SCAN_FUNCS: list[Callable[..., DQScanResult]] = [
    scan_ohlc_violation,
    scan_null_price,
    scan_zero_price,
    scan_timestamp_invalid,
    scan_extreme_candle,
    scan_duplicate_candle,
    scan_date_gap,
    scan_zero_volume_day,
    scan_extreme_move_daily,
    scan_missing_5min_coverage,
    scan_short_history,
]

_FAST_SCAN_FUNCS: list[Callable[..., DQScanResult]] = [
    scan_missing_5min_coverage,
]


def run_full_scan(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
) -> list[DQScanResult]:
    """Run all DQ scans and return results."""
    results: list[DQScanResult] = []
    for scan_func in _ALL_SCAN_FUNCS:
        try:
            result = scan_func(con, window_start=window_start, window_end=window_end)
            results.append(result)
            if result.symbols:
                logger.info(
                    "DQ scan %s: %d symbols flagged", result.issue_code, len(result.symbols)
                )
        except Exception as exc:
            logger.error("DQ scan %s failed: %s", scan_func.__name__, exc)
    return results


def run_fast_scan(
    con: duckdb.DuckDBPyConnection,
    *,
    window_start: date | None = None,
    window_end: date | None = None,
) -> list[DQScanResult]:
    """Run only the fast (coverage) scans."""
    results: list[DQScanResult] = []
    for scan_func in _FAST_SCAN_FUNCS:
        try:
            result = scan_func(con, window_start=window_start, window_end=window_end)
            results.append(result)
        except Exception as exc:
            logger.error("DQ scan %s failed: %s", scan_func.__name__, exc)
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _view_exists(con: duckdb.DuckDBPyConnection, view_name: str) -> bool:
    """Check if a view or table exists in the DuckDB catalog."""
    try:
        con.execute(f"SELECT 1 FROM {view_name} LIMIT 0")
        return True
    except Exception:
        return False


def _resolve_views(con: duckdb.DuckDBPyConnection, source: str) -> list[str]:
    """Return list of views to scan based on source parameter."""
    views: list[str] = []
    if source in ("daily", "both") and _view_exists(con, "v_daily"):
        views.append("v_daily")
    if source in ("5min", "both") and _view_exists(con, "v_5min"):
        views.append("v_5min")
    return views


def _date_filter(
    view: str,
    window_start: date | None,
    window_end: date | None,
    *,
    prefix: str = "AND",
) -> str:
    """Build an optional date filter clause."""
    conditions: list[str] = []
    if window_start is not None:
        conditions.append(f"date >= '{window_start.isoformat()}'")
    if window_end is not None:
        conditions.append(f"date <= '{window_end.isoformat()}'")
    if not conditions:
        return ""
    joiner = f" {prefix} " if prefix else " AND "
    return joiner + " AND ".join(conditions)
