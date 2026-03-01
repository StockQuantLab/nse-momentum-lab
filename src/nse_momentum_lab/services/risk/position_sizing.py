"""Position sizing and portfolio risk management.

ATR-based Position Sizing:
    Risk per trade = Risk% of portfolio
    Position size = Risk / (Entry - Stop)

    Example:
        Portfolio = Rs 10,00,000
        Risk per trade = 1% = Rs 10,000
        Entry = Rs 104, Stop = Rs 98 (Rs 6 risk per share)
        Position size = 10000 / 6 = 1666 shares

Portfolio Risk Limits:
    - Max concurrent positions
    - Max drawdown halt (stop trading if exceeded)
    - Min days between new positions (cooling period)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PositionSizingConfig:
    """Configuration for ATR-based position sizing."""

    risk_per_trade_pct: float = 0.01  # 1% of portfolio per trade
    max_position_pct: float = 0.10  # Max 10% of portfolio in single position
    min_position_value_inr: float = 10000.0  # Min Rs 10k per position
    max_position_value_inr: float = 500000.0  # Max Rs 5L per position
    default_portfolio_value: float = 1000000.0  # Rs 10L default portfolio


@dataclass
class PortfolioRiskConfig:
    """Configuration for portfolio-level risk management."""

    max_positions: int = 10  # Max concurrent positions
    max_drawdown_pct: float = 0.15  # Halt trading at -15% drawdown
    max_new_positions_per_day: int = 3  # Limit new entries per day
    min_days_between_same_symbol: int = 5  # Cooling period for same stock
    max_sector_exposure_pct: float = 0.30  # Max 30% in single sector (if data available)


@dataclass
class PositionSize:
    """Result of position sizing calculation."""

    symbol_id: int
    symbol: str
    entry_price: float
    stop_price: float
    risk_per_share: float
    shares: int
    position_value: float
    risk_amount: float
    risk_pct: float


@dataclass
class PortfolioState:
    """Current state of the portfolio for risk management."""

    asof_date: date
    portfolio_value: float
    cash_available: float
    positions: dict[int, dict[str, Any]] = field(default_factory=dict)
    open_positions_count: int = 0
    current_drawdown_pct: float = 0.0
    trading_halted: bool = False
    halt_reason: str | None = None
    daily_new_positions: int = 0
    last_entry_dates: dict[int, date] = field(default_factory=dict)


class PositionSizer:
    """ATR-based position sizing calculator."""

    def __init__(self, config: PositionSizingConfig | None = None) -> None:
        self.config = config or PositionSizingConfig()

    def calculate_position_size(
        self,
        symbol_id: int,
        symbol: str,
        entry_price: float,
        stop_price: float,
        portfolio_value: float | None = None,
        atr_value: float | None = None,
    ) -> PositionSize:
        """Calculate position size based on risk per trade.

        Uses the formula:
            Risk Amount = Portfolio Value * Risk%
            Risk Per Share = Entry Price - Stop Price
            Shares = Risk Amount / Risk Per Share

        Args:
            symbol_id: Symbol ID
            symbol: Symbol string
            entry_price: Planned entry price
            stop_price: Initial stop loss price
            portfolio_value: Current portfolio value (uses default if None)
            atr_value: ATR value (can be used to validate stop distance)

        Returns:
            PositionSize with calculated shares and risk metrics
        """
        if portfolio_value is None:
            portfolio_value = self.config.default_portfolio_value

        # Calculate risk per trade
        risk_amount = portfolio_value * self.config.risk_per_trade_pct

        # Calculate risk per share
        risk_per_share = entry_price - stop_price

        if risk_per_share <= 0:
            logger.warning(f"Invalid stop price: {stop_price} >= entry {entry_price}")
            return PositionSize(
                symbol_id=symbol_id,
                symbol=symbol,
                entry_price=entry_price,
                stop_price=stop_price,
                risk_per_share=0.0,
                shares=0,
                position_value=0.0,
                risk_amount=0.0,
                risk_pct=0.0,
            )

        # Calculate shares
        shares = int(risk_amount / risk_per_share)

        # Apply position value limits
        position_value = shares * entry_price

        # Cap at max position value
        if position_value > self.config.max_position_value_inr:
            shares = int(self.config.max_position_value_inr / entry_price)
            position_value = shares * entry_price

        # Ensure minimum position value
        if position_value < self.config.min_position_value_inr:
            shares = int(self.config.min_position_value_inr / entry_price)
            position_value = shares * entry_price

        # Cap at max position % of portfolio
        max_position_value = portfolio_value * self.config.max_position_pct
        if position_value > max_position_value:
            shares = int(max_position_value / entry_price)
            position_value = shares * entry_price

        # Recalculate actual risk
        actual_risk = shares * risk_per_share
        actual_risk_pct = actual_risk / portfolio_value if portfolio_value > 0 else 0.0

        return PositionSize(
            symbol_id=symbol_id,
            symbol=symbol,
            entry_price=entry_price,
            stop_price=stop_price,
            risk_per_share=risk_per_share,
            shares=shares,
            position_value=position_value,
            risk_amount=actual_risk,
            risk_pct=actual_risk_pct,
        )


class PortfolioRiskManager:
    """Portfolio-level risk management."""

    def __init__(self, config: PortfolioRiskConfig | None = None) -> None:
        self.config = config or PortfolioRiskConfig()
        self._state: PortfolioState | None = None

    def initialize(
        self,
        portfolio_value: float,
        asof_date: date,
    ) -> PortfolioState:
        """Initialize portfolio state."""
        self._state = PortfolioState(
            asof_date=asof_date,
            portfolio_value=portfolio_value,
            cash_available=portfolio_value,
            positions={},
            open_positions_count=0,
            current_drawdown_pct=0.0,
            trading_halted=False,
            halt_reason=None,
            daily_new_positions=0,
            last_entry_dates={},
        )
        return self._state

    def update_state(
        self,
        asof_date: date,
        portfolio_value: float,
        positions: dict[int, dict[str, Any]] | None = None,
    ) -> PortfolioState:
        """Update portfolio state with current values."""
        if self._state is None:
            return self.initialize(portfolio_value, asof_date)

        # Reset daily counter on new date
        if asof_date != self._state.asof_date:
            self._state.daily_new_positions = 0

        self._state.asof_date = asof_date
        self._state.portfolio_value = portfolio_value

        if positions:
            self._state.positions = positions
            self._state.open_positions_count = len(positions)

        return self._state

    def can_open_position(
        self,
        symbol_id: int,
        asof_date: date,
    ) -> tuple[bool, str]:
        """Check if a new position can be opened.

        Returns:
            Tuple of (allowed, reason)
        """
        if self._state is None:
            return True, "No state initialized"

        # Check if trading is halted
        if self._state.trading_halted:
            return False, f"Trading halted: {self._state.halt_reason}"

        # Check max positions
        if self._state.open_positions_count >= self.config.max_positions:
            return False, f"Max positions reached ({self.config.max_positions})"

        # Check daily limit
        if self._state.daily_new_positions >= self.config.max_new_positions_per_day:
            return (
                False,
                f"Daily new position limit reached ({self.config.max_new_positions_per_day})",
            )

        # Check cooling period for same symbol
        if symbol_id in self._state.last_entry_dates:
            last_entry = self._state.last_entry_dates[symbol_id]
            days_since = (asof_date - last_entry).days
            if days_since < self.config.min_days_between_same_symbol:
                return (
                    False,
                    f"Cooling period: {days_since} days since last entry (min {self.config.min_days_between_same_symbol})",
                )

        # Check drawdown
        if self._state.current_drawdown_pct >= self.config.max_drawdown_pct:
            self._state.trading_halted = True
            self._state.halt_reason = (
                f"Max drawdown exceeded: {self._state.current_drawdown_pct:.1%}"
            )
            return False, self._state.halt_reason

        return True, "OK"

    def record_entry(
        self,
        symbol_id: int,
        asof_date: date,
        position_value: float,
    ) -> None:
        """Record a new position entry."""
        if self._state is None:
            return

        self._state.last_entry_dates[symbol_id] = asof_date
        self._state.daily_new_positions += 1
        self._state.open_positions_count += 1
        self._state.cash_available -= position_value

    def record_exit(
        self,
        symbol_id: int,
        exit_value: float,
    ) -> None:
        """Record a position exit."""
        if self._state is None:
            return

        if symbol_id in self._state.positions:
            del self._state.positions[symbol_id]
        self._state.open_positions_count = max(0, self._state.open_positions_count - 1)
        self._state.cash_available += exit_value

    def update_drawdown(
        self,
        peak_value: float,
        current_value: float,
    ) -> float:
        """Update and return current drawdown percentage."""
        if self._state is None or peak_value <= 0:
            return 0.0

        self._state.current_drawdown_pct = (peak_value - current_value) / peak_value

        # Check if we should halt
        if self._state.current_drawdown_pct >= self.config.max_drawdown_pct:
            self._state.trading_halted = True
            self._state.halt_reason = (
                f"Max drawdown exceeded: {self._state.current_drawdown_pct:.1%}"
            )
            logger.warning(f"Trading halted: {self._state.halt_reason}")

        return self._state.current_drawdown_pct

    def get_state(self) -> PortfolioState | None:
        """Get current portfolio state."""
        return self._state

    def reset_halt(self) -> None:
        """Reset trading halt (manual override)."""
        if self._state:
            self._state.trading_halted = False
            self._state.halt_reason = None
            logger.info("Trading halt reset manually")


def calculate_position_sizes(
    signals: list[tuple[date, int, str, float, float, dict]],
    portfolio_value: float,
    sizing_config: PositionSizingConfig | None = None,
) -> list[PositionSize]:
    """Calculate position sizes for multiple signals.

    Args:
        signals: List of (date, symbol_id, symbol, entry_price, stop_price, metadata)
        portfolio_value: Current portfolio value
        sizing_config: Position sizing configuration

    Returns:
        List of PositionSize objects
    """
    sizer = PositionSizer(sizing_config)
    results = []

    for _signal_date, symbol_id, symbol, entry_price, stop_price, _metadata in signals:
        position = sizer.calculate_position_size(
            symbol_id=symbol_id,
            symbol=symbol,
            entry_price=entry_price,
            stop_price=stop_price,
            portfolio_value=portfolio_value,
        )
        results.append(position)

    return results
