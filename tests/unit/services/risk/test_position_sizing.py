"""Tests for services/risk/position_sizing.py"""

from datetime import date

import pytest

from nse_momentum_lab.services.risk.position_sizing import (
    PortfolioRiskConfig,
    PortfolioRiskManager,
    PortfolioState,
    PositionSize,
    PositionSizer,
    PositionSizingConfig,
    calculate_position_sizes,
)


class TestPositionSizingConfig:
    def test_default_values(self) -> None:
        config = PositionSizingConfig()
        assert config.risk_per_trade_pct == 0.01
        assert config.max_position_pct == 0.10
        assert config.min_position_value_inr == 10000.0
        assert config.max_position_value_inr == 500000.0
        assert config.default_portfolio_value == 1000000.0

    def test_custom_values(self) -> None:
        config = PositionSizingConfig(
            risk_per_trade_pct=0.02,
            max_position_pct=0.15,
            min_position_value_inr=25000.0,
        )
        assert config.risk_per_trade_pct == 0.02
        assert config.max_position_pct == 0.15
        assert config.min_position_value_inr == 25000.0


class TestPortfolioRiskConfig:
    def test_default_values(self) -> None:
        config = PortfolioRiskConfig()
        assert config.max_positions == 10
        assert config.max_drawdown_pct == 0.15
        assert config.max_new_positions_per_day == 3
        assert config.min_days_between_same_symbol == 5
        assert config.max_sector_exposure_pct == 0.30


class TestPositionSize:
    def test_creation(self) -> None:
        size = PositionSize(
            symbol_id=1,
            symbol="TEST",
            entry_price=100.0,
            stop_price=95.0,
            risk_per_share=5.0,
            shares=200,
            position_value=20000.0,
            risk_amount=1000.0,
            risk_pct=0.01,
        )
        assert size.symbol_id == 1
        assert size.symbol == "TEST"
        assert size.entry_price == 100.0
        assert size.shares == 200
        assert size.risk_pct == 0.01


class TestPositionSizer:
    def test_init_default_config(self) -> None:
        sizer = PositionSizer()
        assert sizer.config.risk_per_trade_pct == 0.01
        assert sizer.config.default_portfolio_value == 1000000.0

    def test_init_custom_config(self) -> None:
        config = PositionSizingConfig(risk_per_trade_pct=0.02)
        sizer = PositionSizer(config)
        assert sizer.config.risk_per_trade_pct == 0.02

    def test_calculate_basic_position(self) -> None:
        sizer = PositionSizer()
        result = sizer.calculate_position_size(
            symbol_id=1,
            symbol="TEST",
            entry_price=100.0,
            stop_price=95.0,
            portfolio_value=1000000.0,
        )
        # Risk = 1% of 10L = 10,000
        # Risk per share = 100 - 95 = 5
        # Initial shares = 10,000 / 5 = 2000
        # Max position % (10%) = 100,000 / 100 = 1000 shares (this is the binding constraint)
        assert result.shares == 1000
        assert result.position_value == 100000.0
        assert result.risk_per_share == 5.0
        assert result.risk_amount == 5000.0  # 1000 * 5
        assert result.risk_pct == 0.005  # 0.5%

    def test_calculate_with_default_portfolio(self) -> None:
        sizer = PositionSizer()
        result = sizer.calculate_position_size(
            symbol_id=1,
            symbol="TEST",
            entry_price=100.0,
            stop_price=98.0,
            # portfolio_value=None should use default
        )
        assert result.shares > 0

    def test_invalid_stop_price(self) -> None:
        sizer = PositionSizer()
        result = sizer.calculate_position_size(
            symbol_id=1,
            symbol="TEST",
            entry_price=100.0,
            stop_price=105.0,  # Stop above entry
        )
        assert result.shares == 0
        assert result.position_value == 0.0

    def test_stop_equals_entry(self) -> None:
        sizer = PositionSizer()
        result = sizer.calculate_position_size(
            symbol_id=1,
            symbol="TEST",
            entry_price=100.0,
            stop_price=100.0,
        )
        assert result.shares == 0

    def test_max_position_value_cap(self) -> None:
        config = PositionSizingConfig(
            risk_per_trade_pct=0.10,  # 10% risk = 100,000
            max_position_value_inr=50000.0,  # Cap at 50k
        )
        sizer = PositionSizer(config)
        result = sizer.calculate_position_size(
            symbol_id=1,
            symbol="TEST",
            entry_price=100.0,
            stop_price=90.0,  # 10 risk per share
            portfolio_value=1000000.0,
        )
        # Risk = 100,000, risk per share = 10, shares = 10,000
        # But max position value is 50k, so shares capped at 500
        assert result.shares == 500
        assert result.position_value == 50000.0

    def test_min_position_value_enforced(self) -> None:
        config = PositionSizingConfig(
            risk_per_trade_pct=0.001,  # Very low risk
            min_position_value_inr=25000.0,
        )
        sizer = PositionSizer(config)
        result = sizer.calculate_position_size(
            symbol_id=1,
            symbol="TEST",
            entry_price=100.0,
            stop_price=98.0,
            portfolio_value=1000000.0,
        )
        # Risk would be only 1,000 (20 shares), but min is 25k
        # So shares should be at least 250
        assert result.shares >= 250
        assert result.position_value >= 25000.0

    def test_max_position_pct_cap(self) -> None:
        config = PositionSizingConfig(
            risk_per_trade_pct=0.20,  # 20% risk
            max_position_pct=0.05,  # Max 5% of portfolio
        )
        sizer = PositionSizer(config)
        result = sizer.calculate_position_size(
            symbol_id=1,
            symbol="TEST",
            entry_price=100.0,
            stop_price=90.0,
            portfolio_value=1000000.0,
        )
        # Max position value = 5% of 10L = 50,000
        # So shares max = 500
        assert result.position_value <= 50000.0


class TestPortfolioRiskManager:
    def test_init_default_config(self) -> None:
        manager = PortfolioRiskManager()
        assert manager.config.max_positions == 10
        assert manager._state is None

    def test_init_custom_config(self) -> None:
        config = PortfolioRiskConfig(max_positions=5)
        manager = PortfolioRiskManager(config)
        assert manager.config.max_positions == 5

    def test_initialize(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(
            portfolio_value=1000000.0,
            asof_date=date(2024, 1, 15),
        )
        assert state.portfolio_value == 1000000.0
        assert state.cash_available == 1000000.0
        assert state.open_positions_count == 0
        assert state.trading_halted is False
        assert state.daily_new_positions == 0

    def test_update_state(self) -> None:
        manager = PortfolioRiskManager()
        manager.initialize(1000000.0, date(2024, 1, 15))

        state = manager.update_state(
            asof_date=date(2024, 1, 16),
            portfolio_value=1020000.0,
            positions={1: {"entry_price": 100.0, "shares": 100}},
        )
        assert state.asof_date == date(2024, 1, 16)
        assert state.portfolio_value == 1020000.0
        assert state.open_positions_count == 1
        # Daily counter should reset on new date
        assert state.daily_new_positions == 0

    def test_can_open_position_allowed(self) -> None:
        manager = PortfolioRiskManager()
        manager.initialize(1000000.0, date(2024, 1, 15))

        allowed, reason = manager.can_open_position(
            symbol_id=1,
            asof_date=date(2024, 1, 15),
        )
        assert allowed is True
        assert reason == "OK"

    def test_can_open_position_halted_trading(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(1000000.0, date(2024, 1, 15))
        state.trading_halted = True
        state.halt_reason = "Max drawdown exceeded"

        allowed, reason = manager.can_open_position(
            symbol_id=1,
            asof_date=date(2024, 1, 15),
        )
        assert allowed is False
        assert "halted" in reason.lower()

    def test_can_open_position_max_positions(self) -> None:
        config = PortfolioRiskConfig(max_positions=2)
        manager = PortfolioRiskManager(config)
        state = manager.initialize(1000000.0, date(2024, 1, 15))
        state.open_positions_count = 2

        allowed, reason = manager.can_open_position(
            symbol_id=3,
            asof_date=date(2024, 1, 15),
        )
        assert allowed is False
        assert "Max positions" in reason

    def test_can_open_position_daily_limit(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(1000000.0, date(2024, 1, 15))
        state.daily_new_positions = 3

        allowed, reason = manager.can_open_position(
            symbol_id=1,
            asof_date=date(2024, 1, 15),
        )
        assert allowed is False
        assert "Daily" in reason

    def test_can_open_position_cooling_period(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(1000000.0, date(2024, 1, 15))
        state.last_entry_dates = {1: date(2024, 1, 12)}  # 3 days ago

        allowed, reason = manager.can_open_position(
            symbol_id=1,
            asof_date=date(2024, 1, 15),
        )
        assert allowed is False
        assert "Cooling period" in reason

    def test_can_open_position_after_cooling_period(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(1000000.0, date(2024, 1, 15))
        state.last_entry_dates = {1: date(2024, 1, 5)}  # 10 days ago

        allowed, reason = manager.can_open_position(
            symbol_id=1,
            asof_date=date(2024, 1, 15),
        )
        assert allowed is True

    def test_record_entry(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(1000000.0, date(2024, 1, 15))

        manager.record_entry(
            symbol_id=1,
            asof_date=date(2024, 1, 15),
            position_value=100000.0,
        )

        assert state.daily_new_positions == 1
        assert state.open_positions_count == 1
        assert state.last_entry_dates[1] == date(2024, 1, 15)
        assert state.cash_available == 900000.0

    def test_record_exit(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(1000000.0, date(2024, 1, 15))
        state.positions = {1: {"shares": 100}}
        state.open_positions_count = 1

        manager.record_exit(
            symbol_id=1,
            exit_value=105000.0,
        )

        assert state.open_positions_count == 0
        assert 1 not in state.positions
        assert state.cash_available == 1105000.0  # 1M + 105k - 0.5k entry (not tracked)

    def test_update_drawdown(self) -> None:
        manager = PortfolioRiskManager()
        manager.initialize(1000000.0, date(2024, 1, 15))

        dd = manager.update_drawdown(peak_value=1000000.0, current_value=900000.0)
        assert dd == 0.10  # 10% drawdown

    def test_update_drawdown_triggers_halt(self) -> None:
        config = PortfolioRiskConfig(max_drawdown_pct=0.10)
        manager = PortfolioRiskManager(config)
        state = manager.initialize(1000000.0, date(2024, 1, 15))

        dd = manager.update_drawdown(peak_value=1000000.0, current_value=850000.0)
        assert dd >= 0.15  # 15% drawdown
        assert state.trading_halted is True
        assert "Max drawdown" in state.halt_reason

    def test_reset_halt(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(1000000.0, date(2024, 1, 15))
        state.trading_halted = True
        state.halt_reason = "Test halt"

        manager.reset_halt()

        assert state.trading_halted is False
        assert state.halt_reason is None

    def test_get_state(self) -> None:
        manager = PortfolioRiskManager()
        state = manager.initialize(1000000.0, date(2024, 1, 15))
        assert manager.get_state() is state


class TestCalculatePositionSizes:
    def test_multiple_signals(self) -> None:
        signals = [
            (date(2024, 1, 15), 1, "RELIANCE", 1000.0, 950.0, {}),
            (date(2024, 1, 15), 2, "TCS", 3000.0, 2850.0, {}),
            (date(2024, 1, 15), 3, "INFY", 1500.0, 1425.0, {}),
        ]
        results = calculate_position_sizes(signals, portfolio_value=1000000.0)
        assert len(results) == 3
        assert all(isinstance(r, PositionSize) for r in results)

    def test_empty_signals(self) -> None:
        results = calculate_position_sizes([], portfolio_value=1000000.0)
        assert results == []

    def test_with_custom_config(self) -> None:
        config = PositionSizingConfig(risk_per_trade_pct=0.02)
        signals = [(date(2024, 1, 15), 1, "TEST", 100.0, 95.0, {})]
        results = calculate_position_sizes(signals, portfolio_value=1000000.0, sizing_config=config)
        # With 2% risk, shares should be double the 1% case
        assert results[0].shares > 0
