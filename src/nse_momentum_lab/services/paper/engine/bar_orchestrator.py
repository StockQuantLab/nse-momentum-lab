"""Strategy-agnostic position book and capital management.

Tracks open positions, computes sizing, and manages slot allocation.
This module knows nothing about specific strategies — it is pure capital/slot accounting.

Adapted from cpr-pivot-lab's bar_orchestrator.py for NSE momentum strategies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


def slot_capital_for(
    *,
    max_positions: int,
    portfolio_value: float,
    max_position_pct: float = 0.0,
) -> float:
    """Compute per-slot capital cap."""
    slot_by_count = portfolio_value / max(1, max_positions)
    if max_position_pct > 0:
        pct_cap = portfolio_value * max_position_pct
        return min(slot_by_count, pct_cap)
    return slot_by_count


def minimum_trade_notional_for(
    *,
    max_positions: int,
    portfolio_value: float,
    max_position_pct: float = 0.0,
) -> float:
    """Dust threshold — reject allocations below this."""
    slot = slot_capital_for(
        max_positions=max_positions,
        portfolio_value=portfolio_value,
        max_position_pct=max_position_pct,
    )
    return max(1000.0, slot * 0.05)


@dataclass(slots=True)
class TrackedPosition:
    """In-memory position record for the session tracker."""

    position_id: str
    symbol: str
    direction: str  # "LONG" or "SHORT"
    entry_price: float
    stop_loss: float
    target_price: float | None
    entry_time: str
    quantity: int
    current_qty: int
    status: str = "OPEN"
    trail_state: dict[str, Any] = field(default_factory=dict)
    raw_position: dict[str, Any] | None = None
    days_held: int = 0  # nights carried since entry (0 = entry day, 1 = first overnight)


class SessionPositionTracker:
    """Strategy-agnostic position book with capital management."""

    def __init__(
        self,
        *,
        max_positions: int = 10,
        portfolio_value: float = 1_000_000.0,
        max_position_pct: float = 0.10,
    ) -> None:
        self.max_positions = max(1, max_positions)
        self.initial_capital = portfolio_value
        self.cash_available = portfolio_value
        self.max_position_pct = max_position_pct
        self.slot_capital = slot_capital_for(
            max_positions=self.max_positions,
            portfolio_value=portfolio_value,
            max_position_pct=max_position_pct,
        )
        self._open: dict[str, TrackedPosition] = {}
        self._closed_today: set[str] = set()

    @property
    def current_equity(self) -> float:
        """Cash + cost basis of open positions."""
        basis = sum(p.entry_price * p.current_qty for p in self._open.values())
        return self.cash_available + basis

    def credit_cash(self, amount: float) -> None:
        self.cash_available += amount

    @property
    def open_count(self) -> int:
        return len(self._open)

    def can_open_new(self) -> bool:
        return self.open_count < self.max_positions

    def slots_available(self) -> int:
        return max(0, self.max_positions - self.open_count)

    def has_open_position(self, symbol: str) -> bool:
        return symbol in self._open

    def get_open_position(self, symbol: str) -> TrackedPosition | None:
        tracked = self._open.get(symbol)
        return tracked

    def has_traded_today(self, symbol: str) -> bool:
        return symbol in self._closed_today

    def mark_traded(self, symbol: str) -> None:
        self._closed_today.add(symbol)

    def record_open(self, position: TrackedPosition, position_value: float) -> None:
        self._open[position.symbol] = position
        self.cash_available -= position_value

    def record_close(self, symbol: str, exit_value: float) -> None:
        tracked = self._open.pop(symbol, None)
        if tracked is not None:
            tracked.status = "CLOSED"
            self._closed_today.add(symbol)
        self.cash_available += exit_value

    def update_trail_state(self, symbol: str, trail_state: dict[str, Any]) -> None:
        tracked = self._open.get(symbol)
        if tracked is not None:
            tracked.trail_state = trail_state

    def partial_close(
        self,
        symbol: str,
        *,
        exit_qty: int,
        exit_value: float,
        new_stop: float,
        new_trail_state: dict[str, Any],
    ) -> None:
        """Reduce position qty after a partial exit, freeing capital for the exited portion."""
        tracked = self._open.get(symbol)
        if tracked is None:
            return
        tracked.current_qty = max(0, tracked.current_qty - exit_qty)
        tracked.quantity = tracked.current_qty
        tracked.stop_loss = new_stop
        tracked.trail_state = new_trail_state
        self.cash_available += exit_value

    def seed_open_positions(self, positions: list[dict[str, Any]]) -> None:
        """Pre-populate tracker with existing positions on session restart or carry-over."""
        for p in positions:
            qty = p.get("qty", 0) or 0
            sym = p.get("symbol", "")
            meta = p.get("metadata_json", {}) if isinstance(p.get("metadata_json"), dict) else {}
            days_held = int(meta.get("days_held", 0))
            # Restore the latest trail stop from metadata (updated each HOLD bar).
            # Fallback chain: current_sl (latest) → initial_sl (entry-day) → warn and use 0.
            # Never fall back to avg_entry — that silently sets stop to breakeven on resume.
            current_sl = meta.get("current_sl")
            initial_sl = meta.get("initial_sl")
            if current_sl is None and initial_sl is None:
                logger.warning(
                    "seed_open_positions: skipping %s (%s) because metadata has no stop",
                    sym,
                    p.get("position_id", ""),
                )
                continue
            stop_source = current_sl if current_sl is not None else initial_sl
            stop_loss = float(stop_source)
            trail_state = dict(meta)
            # Signal the runtime to apply post-day3 stop tightening on the first bar.
            if days_held >= 3:
                trail_state["pending_day_tighten"] = True
            tracked = TrackedPosition(
                position_id=p.get("position_id", ""),
                symbol=sym,
                direction=p.get("direction", "LONG"),
                entry_price=float(p.get("avg_entry", 0)),
                stop_loss=stop_loss,
                target_price=None,
                entry_time="",
                quantity=qty,
                current_qty=qty,
                status="OPEN",
                trail_state=trail_state,
                raw_position=p,
                days_held=days_held,
            )
            self._open[sym] = tracked
            self.cash_available -= tracked.entry_price * qty

    def open_symbols(self) -> set[str]:
        return set(self._open.keys())

    def compute_position_qty(
        self,
        *,
        entry_price: float,
        capital_base: float | None = None,
    ) -> int:
        """All-or-nothing sizing with minimum notional check."""
        if entry_price <= 0:
            return 0

        base = capital_base if capital_base is not None else self.current_equity
        slot = slot_capital_for(
            max_positions=self.max_positions,
            portfolio_value=base,
            max_position_pct=self.max_position_pct,
        )
        desired_notional = slot

        min_notional = minimum_trade_notional_for(
            max_positions=self.max_positions,
            portfolio_value=base,
            max_position_pct=self.max_position_pct,
        )
        if desired_notional < min_notional or self.cash_available < desired_notional:
            return 0

        return int(desired_notional / entry_price)


def should_process_symbol(
    *,
    bar_time_minutes: int,
    entry_cutoff_minutes: int,
    tracker: SessionPositionTracker,
    symbol: str,
    setup_status: str,
) -> bool:
    """Return True if symbol should be evaluated for entry."""
    if tracker.has_open_position(symbol):
        return True  # Always process for exit evaluation

    if bar_time_minutes >= entry_cutoff_minutes:
        return False

    if setup_status == "rejected":
        return False

    if tracker.has_traded_today(symbol):
        return False

    return True


def select_entries_for_bar(
    candidates: list[dict[str, Any]],
    tracker: SessionPositionTracker,
) -> list[dict[str, Any]]:
    """Take up to slots_available candidates, ranked by selection_score desc."""
    available = tracker.slots_available()
    if available <= 0 or not candidates:
        return []

    # Rank by selection_score from setup_row (backtest parity); fall back to symbol sort.
    sorted_candidates = sorted(
        candidates,
        key=lambda c: (
            -(c.get("setup_row") or {}).get("selection_score", 0.0),
            c.get("symbol", ""),
        ),
    )
    return sorted_candidates[:available]
