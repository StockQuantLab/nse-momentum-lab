"""Neutral bridge adapter: BacktestParams → PaperStrategyConfig.

This module is the ONLY place that imports from both the backtest and paper
config systems.  Neither ``backtest_presets`` nor ``strategy_presets`` import
from this module, so there is no circular dependency.

Usage
-----
    from nse_momentum_lab.services.paper.paper_backtest_bridge import (
        build_paper_config_from_preset,
    )

    config = build_paper_config_from_preset("BREAKOUT_4PCT", PositionSide.LONG)
    config = build_paper_config_from_preset(
        "BREAKDOWN_4PCT", PositionSide.SHORT,
        paper_overrides={"max_positions": 5},
    )
"""

from __future__ import annotations

import dataclasses
from typing import Any

from nse_momentum_lab.services.backtest.backtest_presets import build_params_from_preset
from nse_momentum_lab.services.backtest.engine import PositionSide
from nse_momentum_lab.services.paper.engine.strategy_presets import PaperStrategyConfig

# Fields that are execution/infrastructure knobs — safe to override via paper_overrides.
# Everything else is a strategy-defining field that must come from the preset.
_PAPER_INFRA_FIELDS: frozenset[str] = frozenset(
    {
        "max_positions",
        "max_position_pct",
        "flatten_time",
        "slippage_bps_large",
        "slippage_bps_mid",
        "slippage_bps_small",
        "extra_params",
    }
)


def build_paper_config_from_preset(
    preset_name: str,
    direction: PositionSide,
    *,
    paper_overrides: dict[str, Any] | None = None,
) -> PaperStrategyConfig:
    """Build a PaperStrategyConfig from a named backtest preset.

    Validates that ``direction`` matches the strategy encoded in the preset
    (e.g. BREAKDOWN_4PCT requires PositionSide.SHORT).  Applies optional
    ``paper_overrides`` after the bridge mapping — **only infrastructure fields**
    are accepted (``max_positions``, ``max_position_pct``, ``flatten_time``,
    ``slippage_bps_*``, ``extra_params``).  Strategy knobs must come from the
    preset; passing them via ``paper_overrides`` raises ``ValueError``.

    Args:
        preset_name: One of the ALL_PRESETS keys, e.g. ``"BREAKOUT_4PCT"``.
        direction: PositionSide.LONG or PositionSide.SHORT.
        paper_overrides: Optional mapping of infrastructure PaperStrategyConfig
            fields to override.  Strategy-defining fields are rejected.

    Raises:
        ValueError: If ``preset_name`` is unknown, direction mismatches the
            preset strategy, or ``paper_overrides`` contains strategy fields.
    """
    params = build_params_from_preset(preset_name)
    config = params.to_paper_config(direction)
    if paper_overrides:
        bad_keys = set(paper_overrides) - _PAPER_INFRA_FIELDS
        if bad_keys:
            raise ValueError(
                f"build_paper_config_from_preset: {sorted(bad_keys)!r} are strategy-defining "
                "fields and cannot be overridden via paper_overrides. "
                "Update the preset in backtest_presets.py instead."
            )
        config = dataclasses.replace(config, **paper_overrides)
    return config
