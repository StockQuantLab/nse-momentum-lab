from datetime import date

from nse_momentum_lab.services.backtest.engine import ExitReason, PositionSide, SlippageModel
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    Trade,
    VectorBTConfig,
    VectorBTEngine,
    VectorBTResult,
    run_vectorbt_backtest,
)


class TestExitReason:
    def test_exit_reasons(self) -> None:
        assert ExitReason.STOP_INITIAL.value == "STOP_INITIAL"
        assert ExitReason.STOP_BREAKEVEN.value == "STOP_BREAKEVEN"
        assert ExitReason.STOP_TRAIL.value == "STOP_TRAIL"
        assert ExitReason.STOP_POST_DAY3.value == "STOP_POST_DAY3"
        assert ExitReason.TIME_STOP.value == "TIME_STOP"
        assert ExitReason.EXIT_EOD.value == "EXIT_EOD"
        assert ExitReason.GAP_THROUGH_STOP.value == "GAP_THROUGH_STOP"
        assert ExitReason.ABNORMAL_PROFIT.value == "ABNORMAL_PROFIT"
        assert ExitReason.ABNORMAL_GAP_EXIT.value == "ABNORMAL_GAP_EXIT"
        assert ExitReason.DELISTING.value == "DELISTING"
        assert ExitReason.SUSPENSION.value == "SUSPENSION"


class TestPositionSide:
    def test_position_sides(self) -> None:
        assert PositionSide.LONG.value == "LONG"


class TestTrade:
    def test_trade_defaults(self) -> None:
        trade = Trade(
            symbol_id=1,
            symbol="TEST",
            entry_date=date(2024, 1, 1),
            entry_price=100.0,
            entry_mode="close",
            qty=100,
            initial_stop=95.0,
        )
        assert trade.exit_date is None
        assert trade.exit_price is None
        assert trade.pnl is None
        assert trade.pnl_r is None
        assert trade.fees == 0.0
        assert trade.slippage_bps == 0.0
        assert trade.mfe_r is None
        assert trade.mae_r is None
        assert trade.exit_reason is None
        assert trade.exit_rule_version == "v1"


class TestVectorBTConfig:
    def test_default_config(self) -> None:
        config = VectorBTConfig()
        assert config.initial_stop_atr_mult == 2.0
        assert config.trail_activation_pct == 0.08  # Stockbee: trailing stop at 8%+
        assert config.trail_stop_pct == 0.02
        assert config.min_hold_days == 3
        assert config.time_stop_days == 5  # Stockbee: exit on 3rd to 5th day
        assert config.abnormal_profit_pct == 0.10
        assert config.abnormal_gap_exit_pct == 0.20
        assert config.follow_through_threshold == 0.0  # Disabled: Stockbee holds 3-5 days
        assert config.fees_per_trade == 0.001
        assert config.slippage_large_bps == 5.0
        assert config.slippage_mid_bps == 10.0
        assert config.slippage_small_bps == 20.0
        assert config.large_bucket_threshold_inr == 100_000_000.0
        assert config.small_bucket_threshold_inr == 20_000_000.0


class TestVectorBTResult:
    def test_backtest_result_defaults(self) -> None:
        result = VectorBTResult(strategy_name="test", entry_mode="close", trades=[])
        assert result.total_return == 0.0
        assert result.sharpe_ratio == 0.0
        assert result.max_drawdown == 0.0
        assert result.win_rate == 0.0
        assert result.profit_factor == 0.0
        assert result.avg_r == 0.0


class TestSlippageModel:
    def setup_method(self) -> None:
        self.model = SlippageModel()

    def test_slippage_none_value_traded_inr(self) -> None:
        result = self.model.get_slippage_bps(None, 100.0, 100)
        assert result == self.model.MID_BPS

    def test_slippage_large_bucket(self) -> None:
        result = self.model.get_slippage_bps(150_000_000.0, 100.0, 100)
        assert result == self.model.LARGE_BPS

    def test_slippage_mid_bucket(self) -> None:
        result = self.model.get_slippage_bps(50_000_000.0, 100.0, 100)
        assert result == self.model.MID_BPS

    def test_slippage_small_bucket(self) -> None:
        result = self.model.get_slippage_bps(10_000_000.0, 100.0, 100)
        assert result == self.model.SMALL_BPS

    def test_slippage_boundaries(self) -> None:
        result_large = self.model.get_slippage_bps(self.model.LARGE_THRESHOLD_INR, 100.0, 100)
        result_mid = self.model.get_slippage_bps(self.model.SMALL_THRESHOLD_INR, 100.0, 100)
        result_small = self.model.get_slippage_bps(self.model.SMALL_THRESHOLD_INR - 1, 100.0, 100)
        assert result_large == self.model.LARGE_BPS
        assert result_mid == self.model.MID_BPS
        assert result_small == self.model.SMALL_BPS


class TestVectorBTEngine:
    def setup_method(self) -> None:
        self.config = VectorBTConfig()
        self.engine = VectorBTEngine(self.config)

    def test_init(self) -> None:
        assert self.engine.config is not None
        assert self.engine.config.slippage_large_bps == 5.0

    def test_run_backtest_no_signals(self) -> None:
        result = self.engine.run_backtest(
            strategy_name="test",
            signals=[],
            price_data={},
            value_traded_inr={},
        )
        assert result.strategy_name == "test"
        assert result.entry_mode == "gap_open"
        assert result.trades == []

    def test_run_backtest_no_price_data(self) -> None:
        signals = [(date(2024, 1, 1), 1, "TEST", 95.0, {})]
        result = self.engine.run_backtest(
            strategy_name="test",
            signals=signals,
            price_data={},
            value_traded_inr={},
        )
        assert len(result.trades) == 0

    def test_run_backtest_with_data(self) -> None:
        entry_date = date(2024, 1, 1)
        signals = [(entry_date, 1, "TEST", 95.0, {})]
        price_data = {
            1: {
                entry_date: {
                    "open_adj": 99.0,
                    "close_adj": 100.0,
                    "high_adj": 105.0,
                    "low_adj": 98.0,
                },
                date(2024, 1, 2): {
                    "open_adj": 101.0,
                    "close_adj": 102.0,
                    "high_adj": 106.0,
                    "low_adj": 99.0,
                },
                date(2024, 1, 3): {
                    "open_adj": 103.0,
                    "close_adj": 104.0,
                    "high_adj": 107.0,
                    "low_adj": 101.0,
                },
                date(2024, 1, 4): {
                    "open_adj": 97.0,
                    "close_adj": 96.0,
                    "high_adj": 105.0,
                    "low_adj": 95.0,
                },
                date(2024, 1, 5): {
                    "open_adj": 97.0,
                    "close_adj": 98.0,
                    "high_adj": 103.0,
                    "low_adj": 96.0,
                },
            }
        }
        value_traded_inr = {1: 50_000_000.0}
        result = self.engine.run_backtest(
            strategy_name="test",
            signals=signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
        )
        assert len(result.trades) == 1
        trade = result.trades[0]
        assert trade.symbol_id == 1
        assert result.entry_mode == "gap_open"

    def test_run_backtest_with_delisting(self) -> None:
        entry_date = date(2024, 1, 1)
        signals = [(entry_date, 1, "TEST", 95.0, {})]
        price_data = {
            1: {
                entry_date: {
                    "open_adj": 99.0,
                    "close_adj": 100.0,
                    "high_adj": 105.0,
                    "low_adj": 98.0,
                },
                date(2024, 1, 2): {
                    "open_adj": 101.0,
                    "close_adj": 102.0,
                    "high_adj": 106.0,
                    "low_adj": 99.0,
                },
                date(2024, 1, 3): {
                    "open_adj": 103.0,
                    "close_adj": 104.0,
                    "high_adj": 107.0,
                    "low_adj": 101.0,
                },
            }
        }
        value_traded_inr = {1: 50_000_000.0}
        delisting_dates = {1: date(2024, 1, 2)}
        result = self.engine.run_backtest(
            strategy_name="test",
            signals=signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
            delisting_dates=delisting_dates,
        )
        assert len(result.trades) == 1

    def test_slippage_applied(self) -> None:
        entry_date = date(2024, 1, 1)
        signals = [(entry_date, 1, "TEST", 95.0, {})]
        price_data = {
            1: {
                entry_date: {
                    "open_adj": 101.0,
                    "close_adj": 100.0,
                    "high_adj": 102.0,
                    "low_adj": 99.0,
                },
                date(2024, 1, 2): {
                    "open_adj": 103.0,
                    "close_adj": 102.0,
                    "high_adj": 104.0,
                    "low_adj": 101.0,
                },
                date(2024, 1, 3): {
                    "open_adj": 103.0,
                    "close_adj": 102.0,
                    "high_adj": 104.0,
                    "low_adj": 101.0,
                },
                date(2024, 1, 4): {
                    "open_adj": 103.0,
                    "close_adj": 102.0,
                    "high_adj": 104.0,
                    "low_adj": 101.0,
                },
            }
        }
        value_traded_inr = {1: 50_000_000.0}
        result = self.engine.run_backtest(
            strategy_name="test",
            signals=signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
        )
        assert len(result.trades) == 1
        assert result.trades[0].slippage_bps == 10.0


class TestRunBacktestConvenienceFunction:
    def test_run_backtest_function(self) -> None:
        result = run_vectorbt_backtest(
            strategy_name="test",
            signals=[],
            price_data={},
            value_traded_inr={},
        )
        assert result.strategy_name == "test"
        assert result.entry_mode == "gap_open"
        assert isinstance(result, VectorBTResult)
