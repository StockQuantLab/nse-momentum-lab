from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from nse_momentum_lab.services.backtest.sweep_runner import SweepResult, run_sweep


def test_sweep_result_records_all_combos():
    results = [
        SweepResult(exp_id="a", label="thresh=0.02", params={"breakout_threshold": 0.02}),
        SweepResult(exp_id="b", label="thresh=0.04", params={"breakout_threshold": 0.04}),
    ]
    assert len(results) == 2
    assert results[0].exp_id == "a"


@patch("nse_momentum_lab.services.backtest.sweep_runner.DuckDBBacktestRunner")
def test_run_sweep_generates_all_combos(mock_runner_cls):
    mock_runner = MagicMock()
    mock_runner_cls.return_value = mock_runner
    mock_runner.run.return_value = "test-exp-id"

    config = {
        "name": "test",
        "strategy": "thresholdbreakout",
        "base_params": {"universe_size": 500},
        "sweep": [{"param": "breakout_threshold", "values": [0.02, 0.04]}],
    }
    from nse_momentum_lab.services.backtest.sweep_schema import SweepConfig
    sweep_cfg = SweepConfig.from_dict(config)

    results = run_sweep(sweep_cfg, force=True, dry_run=False)
    assert len(results) == 2
    assert mock_runner.run.call_count == 2


@patch("nse_momentum_lab.services.backtest.sweep_runner.DuckDBBacktestRunner")
def test_run_sweep_dry_run_skips_runner(mock_runner_cls):
    config = {
        "name": "test",
        "strategy": "thresholdbreakout",
        "base_params": {},
        "sweep": [{"param": "breakout_threshold", "values": [0.02]}],
    }
    from nse_momentum_lab.services.backtest.sweep_schema import SweepConfig
    sweep_cfg = SweepConfig.from_dict(config)

    results = run_sweep(sweep_cfg, dry_run=True)
    assert len(results) == 1
    assert results[0].exp_id == "(dry-run)"
    mock_runner_cls().run.assert_not_called()
