"""
Dataclasses for backtest signals.

Replaces tuple-based signals with type-safe, documented structures.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, time
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
    entry_ts: datetime | None = None
    same_day_exit_ts: datetime | None = None
    carry_stop_next_session: float | None = None
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
            "entry_ts": self.entry_ts.isoformat() if self.entry_ts else None,
            "same_day_exit_ts": self.same_day_exit_ts.isoformat()
            if self.same_day_exit_ts
            else None,
            "carry_stop_next_session": self.carry_stop_next_session,
            "direction": self.direction.value,
        }
        result.update(self.extra)
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SignalMetadata:
        direction = PositionSide(data.get("direction", "LONG"))
        entry_time_str = data.get("entry_time")
        entry_time = time.fromisoformat(entry_time_str) if entry_time_str else None
        entry_ts_str = data.get("entry_ts")
        entry_ts = datetime.fromisoformat(entry_ts_str) if entry_ts_str else None
        exit_ts_str = data.get("same_day_exit_ts")
        same_day_exit_ts = datetime.fromisoformat(exit_ts_str) if exit_ts_str else None
        extra = {k: v for k, v in data.items() if k not in cls._fields()}
        return cls(
            gap_pct=data.get("gap_pct", 0.0),
            atr=data.get("atr", 0.0),
            filters_passed=data.get("filters_passed", 0),
            entry_price=data.get("entry_price"),
            same_day_stop_hit=data.get("same_day_stop_hit", False),
            entry_time=entry_time,
            entry_ts=entry_ts,
            same_day_exit_ts=same_day_exit_ts,
            carry_stop_next_session=data.get("carry_stop_next_session"),
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
            "entry_ts",
            "same_day_exit_ts",
            "carry_stop_next_session",
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
            metadata = SignalMetadata()
        return cls(
            signal_date=signal_date,
            symbol_id=symbol_id,
            symbol=symbol,
            initial_stop=initial_stop,
            metadata=metadata,
        )
