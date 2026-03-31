from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CostModel:
    """Transaction cost model for NSE equity backtests.

    Amount fields are rupees. Percentage fields are decimal fractions.
    """

    brokerage_per_order: float = 20.0
    stt_sell_pct: float = 0.00025
    exchange_txn_pct: float = 0.0000345
    sebi_pct: float = 0.000001
    gst_pct: float = 0.18
    stamp_duty_pct: float = 0.00003
    slippage_bps: float = 0.0

    @classmethod
    def zerodha(cls, *, slippage_bps: float = 0.0) -> CostModel:
        """Zerodha Equity Intraday cost model."""
        return cls(slippage_bps=slippage_bps)

    @classmethod
    def zero(cls) -> CostModel:
        """Zero-cost model for gross backtest comparisons."""
        return cls(
            brokerage_per_order=0.0,
            stt_sell_pct=0.0,
            exchange_txn_pct=0.0,
            sebi_pct=0.0,
            gst_pct=0.0,
            stamp_duty_pct=0.0,
            slippage_bps=0.0,
        )

    def round_trip_cost(
        self,
        entry_price: float,
        exit_price: float,
        qty: int,
        direction: str = "LONG",
    ) -> float:
        """Total round-trip transaction cost in rupees."""
        if qty <= 0 or entry_price <= 0 or exit_price <= 0:
            return 0.0
        entry_cost = self.entry_order_cost(entry_price, qty, direction=direction)
        exit_cost = self.exit_order_cost(exit_price, qty, direction=direction)
        return round(entry_cost + exit_cost, 2)

    def entry_order_cost(self, entry_price: float, qty: int, direction: str = "LONG") -> float:
        """Cost of the entry order only."""
        if qty <= 0 or entry_price <= 0:
            return 0.0

        direction = direction.upper().strip()
        order_value = entry_price * qty
        brokerage = self.brokerage_per_order
        stt = order_value * self.stt_sell_pct if direction == "SHORT" else 0.0
        exchange = order_value * self.exchange_txn_pct
        sebi = order_value * self.sebi_pct
        gst = (brokerage + exchange) * self.gst_pct
        slippage = order_value * (self.slippage_bps / 10_000)
        stamp = order_value * self.stamp_duty_pct if direction != "SHORT" else 0.0
        return round(brokerage + stt + exchange + sebi + gst + stamp + slippage, 2)

    def exit_order_cost(self, exit_price: float, qty: int, direction: str = "LONG") -> float:
        """Cost of the exit order only."""
        if qty <= 0 or exit_price <= 0:
            return 0.0

        direction = direction.upper().strip()
        order_value = exit_price * qty
        brokerage = self.brokerage_per_order
        stt = order_value * self.stt_sell_pct if direction != "SHORT" else 0.0
        exchange = order_value * self.exchange_txn_pct
        sebi = order_value * self.sebi_pct
        gst = (brokerage + exchange) * self.gst_pct
        slippage = order_value * (self.slippage_bps / 10_000)
        stamp = order_value * self.stamp_duty_pct if direction == "SHORT" else 0.0
        return round(brokerage + stt + exchange + sebi + gst + stamp + slippage, 2)

    def slippage_adjusted_prices(
        self,
        entry_price: float,
        exit_price: float,
        direction: str = "LONG",
    ) -> tuple[float, float]:
        """Return adjusted entry/exit prices after slippage."""
        if self.slippage_bps == 0:
            return entry_price, exit_price

        slip_frac = self.slippage_bps / 10_000
        direction = direction.upper().strip()
        if direction == "SHORT":
            adjusted_entry = entry_price * (1 - slip_frac)
            adjusted_exit = exit_price * (1 + slip_frac)
        else:
            adjusted_entry = entry_price * (1 + slip_frac)
            adjusted_exit = exit_price * (1 - slip_frac)
        return round(adjusted_entry, 2), round(adjusted_exit, 2)

    @property
    def is_zero(self) -> bool:
        return (
            self.brokerage_per_order == 0
            and self.stt_sell_pct == 0
            and self.exchange_txn_pct == 0
            and self.sebi_pct == 0
            and self.stamp_duty_pct == 0
            and self.slippage_bps == 0
        )


COST_MODELS: dict[str, Callable[..., CostModel]] = {
    "zerodha": CostModel.zerodha,
    "zero": CostModel.zero,
}


def cost_model_from_name(name: str, *, slippage_bps: float = 0.0) -> CostModel:
    """Create a cost model from a CLI-style model name."""
    factory = COST_MODELS.get(name.lower())
    if factory is None:
        raise ValueError(f"Unknown commission model: {name!r}. Choose from: {list(COST_MODELS)}")
    if name.lower() == "zerodha":
        return factory(slippage_bps=slippage_bps)
    return factory()
