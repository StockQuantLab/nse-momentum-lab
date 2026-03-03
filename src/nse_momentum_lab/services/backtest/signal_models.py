"""
Dataclasses for backtest signals.

Replaces tuple-based signals with type-safe, documented structures.
"""

from dataclasses import dataclass
from datetime import date, time


@dataclass(frozen=True)
class SignalMetadata:
    """Metadata associated with a backtest entry signal."""

    gap_pct: float
    """Gap percentage from previous close to breakout price."""

    atr: float
    """Average True Range (20-day) for volatility context."""

    filters_passed: int
    """Number of 2LYNCH filters that passed (0-6)."""

    entry_price: float | None = None
    """Actual intraday entry price from 5-minute data, if resolved."""

    same_day_stop_hit: bool = False
    """Whether the initial stop was hit on the entry day itself."""

    entry_time: time | None = None
    """Time of day when entry was triggered (from 5-minute candle)."""


@dataclass(frozen=True)
class BacktestSignal:
    """A single entry signal for backtesting.

    This represents a 2LYNCH breakout setup that has passed all filters
    and is ready for backtesting.

    Attributes:
        signal_date: The date when the breakout occurred.
        symbol_id: Internal numeric identifier for the symbol.
        symbol: The trading symbol (e.g., "RELIANCE", "TCS").
        initial_stop: The stop-loss price (low of the breakout day).
        metadata: Additional signal information for analysis.
    """

    signal_date: date
    symbol_id: int
    symbol: str
    initial_stop: float
    metadata: SignalMetadata

    def to_tuple(self) -> tuple:
        """Convert to legacy tuple format for backward compatibility.

        Returns:
            A 5-tuple: (signal_date, symbol_id, symbol, initial_stop, metadata_dict)
        """
        metadata_dict = {
            "gap_pct": self.metadata.gap_pct,
            "atr": self.metadata.atr,
            "filters_passed": self.metadata.filters_passed,
            "entry_price": self.metadata.entry_price,
            "same_day_stop_hit": self.metadata.same_day_stop_hit,
            "entry_time": self.metadata.entry_time,
        }
        return (self.signal_date, self.symbol_id, self.symbol, self.initial_stop, metadata_dict)

    @classmethod
    def from_tuple(cls, tpl: tuple) -> BacktestSignal:
        """Create from legacy tuple format.

        Args:
            tpl: A 5-tuple (signal_date, symbol_id, symbol, initial_stop, metadata_dict)

        Returns:
            A BacktestSignal instance.
        """
        signal_date, symbol_id, symbol, initial_stop, metadata_dict = tpl
        metadata = SignalMetadata(
            gap_pct=metadata_dict.get("gap_pct", 0.0),
            atr=metadata_dict.get("atr", 0.0),
            filters_passed=metadata_dict.get("filters_passed", 0),
            entry_price=metadata_dict.get("entry_price"),
            same_day_stop_hit=metadata_dict.get("same_day_stop_hit", False),
            entry_time=metadata_dict.get("entry_time"),
        )
        return cls(
            signal_date=signal_date,
            symbol_id=symbol_id,
            symbol=symbol,
            initial_stop=initial_stop,
            metadata=metadata,
        )
