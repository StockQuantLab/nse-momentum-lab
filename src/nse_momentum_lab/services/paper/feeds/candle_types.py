"""Data types for paper trading feeds.

MarketSnapshot represents a single tick/snapshot from a live or replay source.
ClosedCandle represents a completed OHLCV bar ready for strategy evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """A single price snapshot from a feed source."""

    symbol: str
    ts: float  # epoch seconds
    last_price: float
    volume: float = 0.0
    source: str = "unknown"


@dataclass(frozen=True, slots=True)
class ClosedCandle:
    """A completed OHLCV candle ready for strategy evaluation."""

    symbol: str
    bar_start: float  # epoch seconds
    bar_end: float  # epoch seconds
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    first_snapshot_ts: float = 0.0
    last_snapshot_ts: float = 0.0
