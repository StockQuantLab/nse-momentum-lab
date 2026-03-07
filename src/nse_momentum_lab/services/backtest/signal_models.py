"""
Dataclasses for backtest signals.

Replaces tuple-based signals with type-safe, documented structures.
"""

from dataclasses import dataclass, field
from datetime import date, time
from typing import Any

from nse_momentum_lab.services.backtest.engine import PositionSide


@dataclass(frozen=True)
class SignalMetadata:
    """Metadata associated with a backtest entry signal."""

    gap_pct: float = 0.0
    atr: float = 0.0
    filters_passed: int = 0
    entry_price: float | None = None
    same_day_stop_hit: bool = False
    entry_time: time | None = None
    direction: PositionSide = PositionSide.LONG
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        result = {
            "gap_pct": self.gap_pct,
            "atr": self.atr,
            "filters_passed": self.filters_passed,
            "entry_price": self.entry_price,
            "same_day_stop_hit": self.same_day_stop_hit,
            "entry_time": self.entry_time.isoformat() if self.entry_time else None,
            "direction": self.direction.value,
        }
        result.update(self.extra)
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SignalMetadata:
        direction = PositionSide(data.get("direction", "LONG"))
        entry_time_str = data.get("entry_time")
        entry_time = time.fromisoformat(entry_time_str) if entry_time_str else None
        extra = {k: v for k, v in data.items() if k not in cls._fields()}
        return cls(
            gap_pct=data.get("gap_pct", 0.0),
            atr=data.get("atr", 0.0),
            filters_passed=data.get("filters_passed", 0),
            entry_price=data.get("entry_price"),
            same_day_stop_hit=data.get("same_day_stop_hit", False),
            entry_time=entry_time,
            direction=direction,
            extra=extra,
        )

    @staticmethod
    def _fields() -> set[str]:
        return {
            "gap_pct",
            "atr",
            "filters_passed",
            "entry_price",
            "same_day_stop_hit",
            "entry_time",
            "direction",
        }


@dataclass(frozen=True)
class BacktestSignal:
    """A single entry signal for backtesting.

    Strategy-agnostic signal that supports multiple strategies and directions.
    """

    signal_date: date
    symbol_id: int
    symbol: str
    initial_stop: float
    metadata: SignalMetadata = field(default_factory=SignalMetadata)
    target_price: float | None = None
    reference_price: float | None = None
    trigger_price: float | None = None

    @property
    def direction(self) -> PositionSide:
        return self.metadata.direction

    def to_tuple(self) -> tuple:
        """Convert to legacy tuple format for backward compatibility."""
        return (
            self.signal_date,
            self.symbol_id,
            self.symbol,
            self.initial_stop,
            self.metadata.to_dict(),
        )

    @classmethod
    def from_tuple(cls, tpl: tuple) -> BacktestSignal:
        """Create from legacy tuple format."""
        signal_date, symbol_id, symbol, initial_stop, metadata_dict = tpl
        if isinstance(metadata_dict, dict):
            metadata = SignalMetadata.from_dict(metadata_dict)
        else:
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
