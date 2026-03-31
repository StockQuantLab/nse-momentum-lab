"""
Constants and enums for NSE Momentum Lab.

Consolidates stringly-typed code found across:
- services/backtest/duckdb_backtest_runner.py
- services/scan/rules.py
- services/backtest/engine.py
"""

from datetime import time
from enum import StrEnum


class FilterName(StrEnum):
    """2LYNCH filter names for consistent reference across codebase."""

    H = "filter_h"  # High close in day's range
    N = "filter_n"  # Narrow/negative previous day
    TWO = "filter_2"  # Not up 2 days in a row
    Y = "filter_y"  # Young breakout (few prior breakouts)
    C = "filter_c"  # Volume compression
    L = "filter_l"  # Lynch trend


ALL_FILTERS = list(FilterName)


class ExperimentStatus(StrEnum):
    """Experiment run status values."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    COMPLETED = "completed"  # For backward compatibility


class EntryTimeframe(StrEnum):
    """Intraday entry timeframe options."""

    FIVE_MIN = "5min"
    ONE_MIN = "1min"
    DAILY = "daily"


class IngestionDataset(StrEnum):
    """Dataset identifiers used by the Kite ingestion workflow."""

    DAILY = "daily"
    FIVE_MIN = "5min"


class IngestionUniverse(StrEnum):
    """Symbol universe selection for Kite ingestion."""

    LOCAL_FIRST = "local-first"
    CURRENT_MASTER = "current-master"


# Exit time defaults for different exit reasons
# Maps exit reason categories to their typical exit times
EXIT_TIME_OPEN = time(9, 15)  # Market open
EXIT_TIME_CLOSE = time(15, 30)  # Market close


def get_exit_time_for_reason(exit_reason_value: str) -> time | None:
    """
    Get the typical exit time for a given exit reason.

    Args:
        exit_reason_value: The string value of an ExitReason enum.

    Returns:
        The exit time (09:15 or 15:30) or None if unknown.

    Examples:
        >>> get_exit_time_for_reason("ABNORMAL_GAP_EXIT")
        datetime.time(9, 15)
        >>> get_exit_time_for_reason("TIME_STOP")
        datetime.time(15, 30)
        >>> get_exit_time_for_reason("STOP_INITIAL") is None
        True
    """
    # Gap exits happen at open
    gap_exits = {"ABNORMAL_GAP_EXIT", "GAP_THROUGH_STOP"}
    if exit_reason_value in gap_exits:
        return EXIT_TIME_OPEN

    # Time-based exits happen at close
    close_exits = {
        "TIME_STOP",
        "ABNORMAL_PROFIT",
        "EXIT_EOD",
        "DELISTING",
        "SUSPENSION",
        "STOP_POST_DAY3",
        "WEAK_CLOSE_EXIT",
    }
    if exit_reason_value in close_exits:
        return EXIT_TIME_CLOSE

    # Stop exits have unknown intraday timing
    return None
