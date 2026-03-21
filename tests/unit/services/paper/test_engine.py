from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

from nse_momentum_lab.services.paper.engine import (
    KillSwitchState,
    PaperTrader,
    RiskConfig,
    RiskGovernance,
    SignalState,
)


class TestSignalState:
    def test_signal_states(self) -> None:
        assert SignalState.NEW.value == "NEW"
        assert SignalState.QUALIFIED.value == "QUALIFIED"
        assert SignalState.ALERTED.value == "ALERTED"
        assert SignalState.ENTERED.value == "ENTERED"
        assert SignalState.MANAGED.value == "MANAGED"
        assert SignalState.EXITED.value == "EXITED"
        assert SignalState.ARCHIVED.value == "ARCHIVED"


class TestKillSwitchState:
    def test_kill_switch_states(self) -> None:
        assert KillSwitchState.ACTIVE.value == "ACTIVE"
        assert KillSwitchState.PAUSED.value == "PAUSED"
        assert KillSwitchState.DISABLED.value == "DISABLED"


class TestRiskConfig:
    def test_default_config(self) -> None:
        config = RiskConfig()
        assert config.max_daily_loss_pct == 0.05
        assert config.max_drawdown_pct == 0.15
        assert config.max_positions == 10
        assert config.max_position_size_pct == 0.10
        assert config.kill_switch_threshold == 0.20

    def test_custom_config(self) -> None:
        config = RiskConfig(
            max_daily_loss_pct=0.10,
            max_drawdown_pct=0.20,
            max_positions=5,
        )
        assert config.max_daily_loss_pct == 0.10
        assert config.max_drawdown_pct == 0.20
        assert config.max_positions == 5

    def test_invalid_pct_raises(self) -> None:
        try:
            RiskConfig(max_daily_loss_pct=-0.1)
        except ValueError as exc:
            assert "max_daily_loss_pct" in str(exc)
        else:
            raise AssertionError("Expected ValueError for invalid max_daily_loss_pct")

    def test_invalid_max_positions_raises(self) -> None:
        try:
            RiskConfig(max_positions=0)
        except ValueError as exc:
            assert "max_positions" in str(exc)
        else:
            raise AssertionError("Expected ValueError for invalid max_positions")


class TestRiskGovernance:
    def setup_method(self) -> None:
        self.config = RiskConfig()
        self.risk = RiskGovernance(self.config)

    def test_can_enter_position_kill_switch_disabled(self) -> None:
        self.risk._kill_switch = KillSwitchState.DISABLED
        can_enter, reason = self.risk.can_enter_position(1000)
        assert can_enter is False
        assert "Kill switch disabled" in reason

    def test_can_enter_position_kill_switch_paused(self) -> None:
        self.risk._kill_switch = KillSwitchState.PAUSED
        can_enter, reason = self.risk.can_enter_position(1000)
        assert can_enter is False
        assert "Kill switch paused" in reason

    def test_can_enter_position_drawdown_limit(self) -> None:
        self.risk._daily_loss = 0.0
        self.risk._peak_equity = 100_000.0
        self.risk._total_equity = 84_000.0
        can_enter, reason = self.risk.can_enter_position(1000)
        assert can_enter is False
        assert "Max drawdown reached" in reason

    def test_can_enter_position_max_drawdown(self) -> None:
        self.risk._total_equity = 85_000.0
        self.risk._peak_equity = 100_000.0
        can_enter, reason = self.risk.can_enter_position(1000)
        assert can_enter is False
        assert "Max drawdown reached" in reason

    def test_can_enter_position_position_size_exceeded(self) -> None:
        self.risk._total_equity = 100_000.0
        self.risk._peak_equity = 100_000.0
        can_enter, reason = self.risk.can_enter_position(15000)
        assert can_enter is False
        assert "Position size exceeds limit" in reason

    def test_can_enter_position_ok(self) -> None:
        self.risk._total_equity = 100_000.0
        self.risk._peak_equity = 100_000.0
        can_enter, reason = self.risk.can_enter_position(5000)
        assert can_enter is True
        assert reason == "OK"

    def test_check_risk_profit(self) -> None:
        self.risk._daily_loss = 0.0
        self.risk._total_equity = 100_000.0
        self.risk._peak_equity = 100_000.0
        safe, reason = self.risk.check_risk(1000.0)
        assert safe is True
        assert reason == "OK"

    def test_check_risk_loss_breach(self) -> None:
        self.risk._daily_loss = 0.0
        self.risk._total_equity = 100_000.0
        self.risk._peak_equity = 100_000.0
        safe, reason = self.risk.check_risk(-6000.0)
        assert safe is False
        assert "Daily loss limit breached" in reason

    def test_check_risk_drawdown_breach(self) -> None:
        self.risk._daily_loss = 0.0
        self.risk._total_equity = 80_000.0
        self.risk._peak_equity = 100_000.0
        safe, reason = self.risk.check_risk(-2000.0)
        assert safe is False
        assert "Max drawdown breached" in reason

    def test_check_risk_peak_equity_update(self) -> None:
        self.risk._total_equity = 110_000.0
        self.risk._peak_equity = 100_000.0
        self.risk.check_risk(10000.0)
        assert self.risk._peak_equity == 120_000.0

    def test_get_kill_switch_state(self) -> None:
        self.risk._kill_switch = KillSwitchState.PAUSED
        assert self.risk.get_kill_switch_state() == KillSwitchState.PAUSED

    def test_reset_daily(self) -> None:
        self.risk._daily_loss = -5000.0
        self.risk.reset_daily()
        assert self.risk._daily_loss == 0.0


class TestPaperTrader:
    def setup_method(self) -> None:
        self.config = RiskConfig()
        self.trader = PaperTrader(self.config)

    def test_init(self) -> None:
        assert self.trader._risk is not None
        assert self.trader._slippage is not None
        assert self.trader._positions == {}

    def test_get_open_positions_empty(self) -> None:
        positions = self.trader.get_open_positions()
        assert positions == []

    def test_get_kill_switch_state(self) -> None:
        state = self.trader.get_kill_switch_state()
        assert state == KillSwitchState.ACTIVE

    @patch("nse_momentum_lab.services.paper.engine.PaperTrader._enter_position")
    @patch("nse_momentum_lab.services.paper.engine.PaperTrader._update_signal_state")
    async def test_process_signals_new_state_enters(
        self, mock_update: MagicMock, mock_enter: MagicMock
    ) -> None:
        mock_enter.return_value = None
        mock_update.return_value = None

        session = AsyncMock()
        signals = [
            {
                "signal_id": 1,
                "symbol_id": 100,
                "state": "NEW",
                "position_size": 5000,
            }
        ]
        prices = {100: {date.today(): {"close_adj": 100.0}}}

        results = await self.trader.process_signals(signals, prices, session)
        assert len(results) == 1
        assert results[0]["signal_id"] == 1
        assert results[0]["action"] == "processed"
        mock_enter.assert_called_once()

    @patch("nse_momentum_lab.services.paper.engine.PaperTrader._update_signal_state")
    async def test_process_signals_archived_when_rejected(self, mock_update: MagicMock) -> None:
        mock_update.return_value = None

        session = AsyncMock()
        signals = [
            {
                "signal_id": 1,
                "symbol_id": 100,
                "state": "NEW",
                "position_size": 50000,
            }
        ]
        prices = {}

        results = await self.trader.process_signals(signals, prices, session)
        assert len(results) == 0

    @patch("nse_momentum_lab.services.paper.engine.PaperTrader._manage_position")
    async def test_process_signals_entered_state_manages(self, mock_manage: MagicMock) -> None:
        mock_manage.return_value = None

        session = AsyncMock()
        signals = [
            {
                "signal_id": 1,
                "symbol_id": 100,
                "state": "ENTERED",
            }
        ]
        prices = {}

        results = await self.trader.process_signals(signals, prices, session)
        assert len(results) == 1
        mock_manage.assert_called_once()

    @patch("nse_momentum_lab.services.paper.engine.PaperTrader._enter_position")
    async def test_process_signals_rolls_back_on_error(self, mock_enter: MagicMock) -> None:
        mock_enter.side_effect = RuntimeError("boom")
        session = AsyncMock()
        signals = [{"signal_id": 1, "symbol_id": 100, "state": "NEW", "position_size": 5000}]

        try:
            await self.trader.process_signals(signals, {}, session)
        except RuntimeError as exc:
            assert str(exc) == "boom"
        else:
            raise AssertionError("Expected RuntimeError from _enter_position")

        session.rollback.assert_awaited_once()
