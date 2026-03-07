from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ExitReason(Enum):
    STOP = "STOP"
    TRAIL_STOP = "TRAIL_STOP"
    BREAKEVEN_STOP = "BREAKEVEN_STOP"
    TIME_EXIT = "TIME_EXIT"
    TARGET_EXIT = "TARGET_EXIT"
    GAP_STOP = "GAP_STOP"
    EVENT_INVALIDATION = "EVENT_INVALIDATION"
    RULE_EXIT = "RULE_EXIT"
    DELISTING = "DELISTING"
    DATA_INVALIDATION = "DATA_INVALIDATION"

    STOP_INITIAL = "STOP_INITIAL"
    STOP_BREAKEVEN = "STOP_BREAKEVEN"
    STOP_TRAIL = "STOP_TRAIL"
    STOP_POST_DAY3 = "STOP_POST_DAY3"
    TIME_STOP = "TIME_STOP"
    EXIT_EOD = "EXIT_EOD"
    GAP_THROUGH_STOP = "GAP_THROUGH_STOP"
    ABNORMAL_PROFIT = "ABNORMAL_PROFIT"
    ABNORMAL_GAP_EXIT = "ABNORMAL_GAP_EXIT"
    SUSPENSION = "SUSPENSION"


class PositionSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class ExitPolicyConfig:
    initial_stop_atr_mult: float = 2.0
    trail_activation_pct: float = 0.08
    trail_stop_pct: float = 0.02
    min_hold_days: int = 3
    time_stop_days: int = 5
    abnormal_profit_pct: float = 0.10
    abnormal_gap_exit_pct: float = 0.20
    follow_through_threshold: float = 0.0


class ExitPolicy(ABC):
    """Abstract base class for pluggable exit policies."""

    @abstractmethod
    def compute_initial_stop(
        self,
        entry_price: float,
        atr: float | None,
        direction: PositionSide,
        **kwargs: Any,
    ) -> float:
        """Compute the initial stop price based on entry and volatility."""
        pass

    @abstractmethod
    def should_exit(
        self,
        position: Any,
        current_date: int,
        current_price: float,
        high_price: float,
        low_price: float,
        open_price: float,
        max_price: float,
        direction: PositionSide,
    ) -> tuple[bool, ExitReason | None]:
        """Determine if position should exit. Returns (should_exit, reason)."""
        pass

    @abstractmethod
    def compute_trailing_stop(
        self,
        entry_price: float,
        max_price: float,
        current_stop: float,
        direction: PositionSide,
    ) -> float:
        """Compute updated trailing stop based on price movement."""
        pass


class DefaultBreakoutExitPolicy(ExitPolicy):
    """Default breakout-style exit policy (2LYNCH compatible)."""

    def __init__(self, config: ExitPolicyConfig | None = None) -> None:
        self.config = config or ExitPolicyConfig()

    def compute_initial_stop(
        self,
        entry_price: float,
        atr: float | None,
        direction: PositionSide,
        **kwargs: Any,
    ) -> float:
        if direction == PositionSide.SHORT:
            if atr is not None and atr > 0:
                return entry_price + (atr * self.config.initial_stop_atr_mult)
            return entry_price * 1.04
        else:
            if atr is not None and atr > 0:
                return entry_price - (atr * self.config.initial_stop_atr_mult)
            return entry_price * 0.96

    def should_exit(
        self,
        position: Any,
        current_date: int,
        current_price: float,
        high_price: float,
        low_price: float,
        open_price: float,
        max_price: float,
        direction: PositionSide,
    ) -> tuple[bool, ExitReason | None]:
        return False, None

    def compute_trailing_stop(
        self,
        entry_price: float,
        max_price: float,
        current_stop: float,
        direction: PositionSide,
    ) -> float:
        if direction == PositionSide.SHORT:
            trailing = max_price * (1 + self.config.trail_stop_pct)
            return min(current_stop, trailing)
        else:
            trailing = max_price * (1 - self.config.trail_stop_pct)
            return max(current_stop, trailing)


class SlippageModel:
    """
    Slippage model for Indian markets using INR value traded.

    Buckets based on 20-day average value traded (INR):
    - Large Cap: > ₹100 Crore → 5 bps
    - Mid Cap: ₹20-100 Crore → 10 bps
    - Small Cap: < ₹20 Crore → 20 bps
    """

    LARGE_BPS = 5.0
    MID_BPS = 10.0
    SMALL_BPS = 20.0

    LARGE_THRESHOLD_INR = 100_000_000.0
    SMALL_THRESHOLD_INR = 20_000_000.0

    def get_slippage_bps(
        self,
        value_traded_inr: float | None,
        entry_price: float,
        qty: int,
    ) -> float:
        if value_traded_inr is None:
            return self.MID_BPS

        avg_daily_value_inr = value_traded_inr
        if avg_daily_value_inr >= self.LARGE_THRESHOLD_INR:
            return self.LARGE_BPS
        if avg_daily_value_inr >= self.SMALL_THRESHOLD_INR:
            return self.MID_BPS
        return self.SMALL_BPS
