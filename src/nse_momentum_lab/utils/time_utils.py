"""
Time utility functions for NSE market operations.

Consolidates time handling logic from:
- services/backtest/duckdb_backtest_runner.py
"""

from datetime import time
from typing import Any

# NSE Trading Hours
NSE_OPEN_HOUR = 9
NSE_OPEN_MINUTE = 15
NSE_CLOSE_HOUR = 15
NSE_CLOSE_MINUTE = 30

# Pre-computed minutes from midnight for NSE open
NSE_OPEN_MINUTES_FROM_MIDNIGHT = NSE_OPEN_HOUR * 60 + NSE_OPEN_MINUTE
NSE_CLOSE_MINUTES_FROM_MIDNIGHT = NSE_CLOSE_HOUR * 60 + NSE_CLOSE_MINUTE


def normalize_candle_time(candle_time: Any) -> time | None:
    """Convert a raw DuckDB candle_time value to a datetime.time object.

    DuckDB TIME columns are returned as microseconds-since-midnight (int)
    when fetched via the Python API. This helper normalizes all three
    possible types: datetime, time, and int/float microseconds.

    Args:
        candle_time: A time value from DuckDB (datetime, time, int, or float).

    Returns:
        A datetime.time object, or None if conversion fails.

    Examples:
        >>> from datetime import datetime, time
        >>> normalize_candle_time(datetime(2024, 1, 1, 10, 30))
        datetime.time(10, 30)
        >>> normalize_candle_time(37800000000)  # 10:30 in microseconds
        datetime.time(10, 30)
    """
    import datetime as _dt

    if isinstance(candle_time, _dt.datetime):
        return candle_time.time()
    if isinstance(candle_time, _dt.time):
        return candle_time
    if isinstance(candle_time, (int, float)):
        # DuckDB stores TIME as microseconds since midnight
        total_seconds = int(candle_time) // 1_000_000
        h, remainder = divmod(total_seconds, 3600)
        m, s = divmod(remainder, 60)
        try:
            return _dt.time(h, m, s)
        except ValueError:
            return None
    return None


def minutes_from_nse_open(candle_time: Any) -> int | None:
    """Return minutes elapsed since NSE market open (09:15 IST) for a candle_time value.

    Handles datetime.time, datetime.datetime, and integer microseconds (DuckDB TIME64).

    Args:
        candle_time: A time value from DuckDB (datetime, time, int, or float).

    Returns:
        Minutes since 09:15 IST, or None if format is unrecognised.

    Examples:
        >>> from datetime import datetime
        >>> minutes_from_nse_open(datetime(2024, 1, 1, 9, 15))
        0
        >>> minutes_from_nse_open(datetime(2024, 1, 1, 10, 15))
        60
    """
    import datetime as _dt

    if isinstance(candle_time, _dt.datetime):
        return (candle_time.hour - NSE_OPEN_HOUR) * 60 + (candle_time.minute - NSE_OPEN_MINUTE)
    if isinstance(candle_time, _dt.time):
        return (candle_time.hour - NSE_OPEN_HOUR) * 60 + (candle_time.minute - NSE_OPEN_MINUTE)
    if isinstance(candle_time, (int, float)):
        # DuckDB stores TIME as microseconds since midnight
        total_minutes = int(candle_time) // 60_000_000
        return total_minutes - NSE_OPEN_MINUTES_FROM_MIDNIGHT
    return None


def nse_open_time() -> time:
    """Return NSE market open time (09:15 IST)."""
    return time(NSE_OPEN_HOUR, NSE_OPEN_MINUTE)


def nse_close_time() -> time:
    """Return NSE market close time (15:30 IST)."""
    return time(NSE_CLOSE_HOUR, NSE_CLOSE_MINUTE)
