from __future__ import annotations

import textwrap

import pytest

from nse_momentum_lab.services.backtest.sweep_schema import (
    SweepAxis,
    SweepCompare,
    SweepConfig,
    load_sweep_config,
)


def test_sweep_axis_single():
    axis = SweepAxis(param="breakout_threshold", values=[0.02, 0.04])
    assert axis.param == "breakout_threshold"
    assert len(axis.combinations()) == 2


def test_sweep_axis_cartesian():
    axes = [
        SweepAxis(param="breakout_threshold", values=[0.02, 0.04]),
        SweepAxis(param="trail_activation_pct", values=[0.06, 0.08]),
    ]
    combos = SweepConfig._cartesian(axes)
    assert len(combos) == 4
    assert combos[0] == {"breakout_threshold": 0.02, "trail_activation_pct": 0.06}


def test_sweep_config_from_dict():
    raw = {
        "name": "test-sweep",
        "strategy": "thresholdbreakout",
        "base_params": {"universe_size": 2000, "min_price": 10},
        "sweep": [{"param": "breakout_threshold", "values": [0.02, 0.04]}],
        "compare": {"metric": "calmar_ratio", "sort": "desc", "top_n": 3},
    }
    cfg = SweepConfig.from_dict(raw)
    assert cfg.name == "test-sweep"
    assert cfg.strategy == "thresholdbreakout"
    assert len(cfg.combinations()) == 2


def test_load_sweep_config_from_yaml(tmp_path):
    yaml_file = tmp_path / "sweep.yaml"
    yaml_file.write_text(textwrap.dedent("""\
        name: test
        strategy: thresholdbreakout
        base_params:
          universe_size: 2000
        sweep:
          - param: breakout_threshold
            values: [0.02, 0.04]
    """))
    cfg = load_sweep_config(yaml_file)
    assert cfg.name == "test"
    assert len(cfg.combinations()) == 2


def test_invalid_param_rejected():
    raw = {
        "name": "bad",
        "strategy": "thresholdbreakout",
        "base_params": {"universe_size": 2000},
        "sweep": [{"param": "nonexistent_param", "values": [1, 2]}],
    }
    with pytest.raises(ValueError, match="not a valid BacktestParams field"):
        SweepConfig.from_dict(raw)


def test_base_params_validated():
    """base_params keys must also be valid BacktestParams fields."""
    raw = {
        "name": "bad-base",
        "strategy": "thresholdbreakout",
        "base_params": {"universe_size": 2000, "fake_param": 99},
        "sweep": [{"param": "breakout_threshold", "values": [0.02]}],
    }
    with pytest.raises(ValueError, match="not a valid BacktestParams field"):
        SweepConfig.from_dict(raw)


def test_duplicate_axis_rejected():
    """Duplicate param names in sweep axes are rejected."""
    raw = {
        "name": "dup",
        "strategy": "thresholdbreakout",
        "base_params": {},
        "sweep": [
            {"param": "breakout_threshold", "values": [0.02]},
            {"param": "breakout_threshold", "values": [0.04]},
        ],
    }
    with pytest.raises(ValueError, match="Duplicate sweep axis"):
        SweepConfig.from_dict(raw)


def test_empty_values_rejected():
    """Empty values list in a sweep axis is rejected."""
    raw = {
        "name": "empty",
        "strategy": "thresholdbreakout",
        "base_params": {},
        "sweep": [{"param": "breakout_threshold", "values": []}],
    }
    with pytest.raises(ValueError, match="empty values"):
        SweepConfig.from_dict(raw)


def test_invalid_strategy_rejected():
    """Strategy name must resolve via strategy_registry."""
    raw = {
        "name": "bad-strat",
        "strategy": "nonexistent_strategy",
        "base_params": {},
        "sweep": [{"param": "breakout_threshold", "values": [0.02]}],
    }
    with pytest.raises(ValueError, match="Cannot resolve strategy"):
        SweepConfig.from_dict(raw)
