"""Named backtest run presets for the 2LYNCH operating point.

Each preset encodes ALL strategy-defining parameters explicitly so that
the canonical configuration is immune to future changes in BacktestParams
defaults.  Only "infrastructure" fields (universe size, date window,
parallelism) are left for the caller to supply.

Usage
-----
    from nse_momentum_lab.services.backtest.backtest_presets import (
        build_params_from_preset,
        list_preset_names,
    )

    params = build_params_from_preset(
        "BREAKOUT_4PCT",
        infra_overrides={
            "universe_size": 2000,
            "start_date": "2025-01-01",
            "end_date": "2026-04-17",
            "parallel_workers": 4,
        },
    )
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams

# ---------------------------------------------------------------------------
# Infrastructure fields — callers may override these without changing the
# canonical strategy identity of a preset.
# ---------------------------------------------------------------------------
INFRA_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "universe_size",
        "min_price",
        "min_filters",
        "min_value_traded_inr",
        "min_volume",
        "start_year",
        "end_year",
        "start_date",
        "end_date",
        "parallel_workers",
        "entry_timeframe",
    }
)

_VALID_PARAM_NAMES: frozenset[str] = frozenset(f.name for f in dataclasses.fields(BacktestParams))


# ---------------------------------------------------------------------------
# StrategyPreset dataclass
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StrategyPreset:
    """Immutable description of a canonical backtest configuration."""

    name: str
    label: str
    description: str
    overrides: Mapping[str, Any]  # always MappingProxyType at runtime


# ---------------------------------------------------------------------------
# Canonical preset definitions
# ---------------------------------------------------------------------------
def _preset(
    name: str,
    label: str,
    description: str,
    overrides: dict[str, Any],
) -> StrategyPreset:
    return StrategyPreset(
        name=name,
        label=label,
        description=description,
        overrides=MappingProxyType(overrides),
    )


# Shared engine defaults encoded explicitly so presets don't drift when
# BacktestParams default values change.
_ENGINE_DEFAULTS: dict[str, Any] = {
    "abnormal_gap_mode": "trail_after_gap",
    "same_day_r_ladder": True,
    "same_day_r_ladder_start_r": 2,
    "trail_activation_pct": 0.08,
    "trail_stop_pct": 0.02,
    "min_hold_days": 3,
    "time_stop_days": 5,
    "abnormal_profit_pct": 0.10,
    "abnormal_gap_exit_pct": 0.20,
    "entry_cutoff_minutes": 60,
    "max_stop_dist_pct": 0.08,
    "breakout_use_current_day_c_quality": True,
    # Skip the first 5-min candle (9:15-9:20): entries only after opening candle closes.
    "entry_start_minutes": 5,
    # H-carry logic: exit at close when position is not profitable AND didn't close near
    # high/low; tighten stop to at least breakeven when carrying. Enabled for all strategies.
    "h_carry_enabled": True,
}

ALL_PRESETS: dict[str, StrategyPreset] = {
    "BREAKOUT_4PCT": _preset(
        name="BREAKOUT_4PCT",
        label="4% Breakout (canonical)",
        description=(
            "Canonical 4% threshold breakout, no daily ranking budget cap. "
            "Unbudgeted canonical comparison baseline."
        ),
        overrides={
            **_ENGINE_DEFAULTS,
            "strategy": "thresholdbreakout",
            "breakout_threshold": 0.04,
            "breakout_daily_candidate_budget": 0,  # unbudgeted
        },
    ),
    "BREAKOUT_2PCT": _preset(
        name="BREAKOUT_2PCT",
        label="2% Breakout (canonical)",
        description=(
            "Canonical 2% threshold breakout, no daily ranking budget cap. "
            "Higher trade frequency baseline with lower per-trade expectancy."
        ),
        overrides={
            **_ENGINE_DEFAULTS,
            "strategy": "thresholdbreakout",
            "breakout_threshold": 0.02,
            "breakout_daily_candidate_budget": 0,  # unbudgeted
        },
    ),
    "BREAKDOWN_4PCT": _preset(
        name="BREAKDOWN_4PCT",
        label="4% Breakdown (Option-B short tuning)",
        description=(
            "4% threshold breakdown with Option-B short-side engine params: "
            "trail 4%, time-stop 3d, max-stop 5%, abnormal-profit 5%. "
            "Canonical short-side operating point for 4% breakdown."
        ),
        overrides={
            **_ENGINE_DEFAULTS,
            "strategy": "thresholdbreakdown",
            "breakout_threshold": 0.04,
            # Option-B short-side tuning
            "short_trail_activation_pct": 0.04,
            "short_time_stop_days": 3,
            "short_max_stop_dist_pct": 0.05,
            "short_abnormal_profit_pct": 0.05,
        },
    ),
    "BREAKDOWN_2PCT": _preset(
        name="BREAKDOWN_2PCT",
        label="2% Breakdown (Phase-1 canonical)",
        description=(
            "2% threshold breakdown with Phase-1 quality filters: "
            "strict filter_l (close<ma65), narrow-only filter_n, "
            "skip gap-down entries, rs_min=-0.10, budget=5. "
            "Canonical short-side operating point for 2% breakdown."
        ),
        overrides={
            **_ENGINE_DEFAULTS,
            "strategy": "thresholdbreakdown",
            "breakout_threshold": 0.02,
            # Phase-1 breakdown quality flags
            "breakdown_daily_candidate_budget": 5,
            "breakdown_rs_min": -0.10,
            "breakdown_strict_filter_l": True,
            "breakdown_filter_n_narrow_only": True,
            "breakdown_skip_gap_down": True,
        },
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def list_preset_names() -> list[str]:
    """Return sorted preset names (suitable for argparse choices)."""
    return sorted(ALL_PRESETS)


def get_preset(name: str) -> StrategyPreset:
    """Look up a preset by name (case-insensitive).

    Raises
    ------
    KeyError
        If the preset name is not registered.
    """
    key = name.upper()
    if key not in ALL_PRESETS:
        available = ", ".join(sorted(ALL_PRESETS))
        raise KeyError(f"Unknown preset {name!r}. Available: {available}")
    return ALL_PRESETS[key]


def build_params_from_preset(
    preset_name: str,
    *,
    infra_overrides: Mapping[str, Any] | None = None,
) -> BacktestParams:
    """Build a BacktestParams from a named preset plus optional infra overrides.

    The preset defines all strategy-specific fields.  ``infra_overrides``
    may only contain fields in ``INFRA_FIELD_NAMES`` (universe size, date
    range, parallelism, etc.) — the caller gets a ``ValueError`` otherwise.

    Parameters
    ----------
    preset_name:
        One of the keys in ``ALL_PRESETS`` (case-insensitive).
    infra_overrides:
        Infrastructure fields to apply on top of the preset.  These never
        affect strategy identity — only universe, date window, and execution
        concurrency.

    Returns
    -------
    BacktestParams
        Fully constructed parameter object ready to pass to
        ``DuckDBBacktestRunner.run()``.
    """
    preset = get_preset(preset_name)
    infra = dict(infra_overrides or {})

    # Reject any infra key that isn't a known infrastructure field or a valid
    # BacktestParams field — catches typos before they silently do nothing.
    unknown = infra.keys() - _VALID_PARAM_NAMES
    if unknown:
        raise ValueError(f"Unknown BacktestParams fields in infra_overrides: {sorted(unknown)}")

    strategy_fields = infra.keys() - INFRA_FIELD_NAMES
    if strategy_fields:
        raise ValueError(
            f"Strategy-defining fields must come from the preset, not infra_overrides: "
            f"{sorted(strategy_fields)}. "
            f"Choose a different preset or add a new preset to backtest_presets.py."
        )

    # Merge: preset overrides first, infra on top (infra wins for shared keys).
    merged = {**preset.overrides, **infra}

    # Validate merged keys against BacktestParams (belt-and-suspenders).
    bad = merged.keys() - _VALID_PARAM_NAMES
    if bad:
        raise ValueError(
            f"Preset {preset_name!r} contains unknown BacktestParams fields: {sorted(bad)}"
        )

    return BacktestParams(**merged)


def describe_preset(preset_name: str) -> str:
    """Return a human-readable description of a preset."""
    preset = get_preset(preset_name)
    lines = [
        f"Preset : {preset.name}",
        f"Label  : {preset.label}",
        f"Desc   : {preset.description}",
        "Params :",
    ]
    for k, v in sorted(preset.overrides.items()):
        lines.append(f"  {k} = {v!r}")
    return "\n".join(lines)
