from __future__ import annotations

from nse_momentum_lab.services.backtest.cost_model import CostModel, cost_model_from_name
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams


def test_zerodha_cost_model_round_trip_cost() -> None:
    model = CostModel.zerodha()
    cost = model.round_trip_cost(entry_price=100.0, exit_price=110.0, qty=10, direction="LONG")
    assert cost == 47.59


def test_zero_cost_model_is_zero() -> None:
    model = cost_model_from_name("zero")
    assert model.is_zero is True
    assert model.round_trip_cost(entry_price=100.0, exit_price=110.0, qty=10) == 0.0


def test_backtest_params_disable_vectorbt_costs() -> None:
    config = BacktestParams().to_vbt_config()
    assert config.fees_per_trade == 0.0
    assert config.slippage_large_bps == 0.0
    assert config.slippage_mid_bps == 0.0
    assert config.slippage_small_bps == 0.0
