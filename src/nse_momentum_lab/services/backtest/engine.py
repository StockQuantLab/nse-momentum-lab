from __future__ import annotations

from enum import Enum


class ExitReason(Enum):
    STOP_INITIAL = "STOP_INITIAL"
    STOP_BREAKEVEN = "STOP_BREAKEVEN"
    STOP_TRAIL = "STOP_TRAIL"
    STOP_POST_DAY3 = "STOP_POST_DAY3"
    TIME_STOP = "TIME_STOP"
    EXIT_EOD = "EXIT_EOD"
    GAP_THROUGH_STOP = "GAP_THROUGH_STOP"
    ABNORMAL_PROFIT = "ABNORMAL_PROFIT"
    ABNORMAL_GAP_EXIT = "ABNORMAL_GAP_EXIT"
    DELISTING = "DELISTING"
    SUSPENSION = "SUSPENSION"


class PositionSide(Enum):
    LONG = "LONG"


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

    # Thresholds in INR (Indian Rupees)
    LARGE_THRESHOLD_INR = 100_000_000.0  # ₹100 Crore
    SMALL_THRESHOLD_INR = 20_000_000.0  # ₹20 Crore

    def get_slippage_bps(
        self,
        value_traded_inr: float | None,
        entry_price: float,
        qty: int,
    ) -> float:
        """
        Get slippage in basis points based on liquidity bucket.

        Args:
            value_traded_inr: 20-day average value traded in INR
            entry_price: Entry price (for fallback logic)
            qty: Quantity (for future size-aware logic)
        """
        if value_traded_inr is None:
            return self.MID_BPS

        avg_daily_value_inr = value_traded_inr
        if avg_daily_value_inr >= self.LARGE_THRESHOLD_INR:
            return self.LARGE_BPS
        if avg_daily_value_inr >= self.SMALL_THRESHOLD_INR:
            return self.MID_BPS
        return self.SMALL_BPS
