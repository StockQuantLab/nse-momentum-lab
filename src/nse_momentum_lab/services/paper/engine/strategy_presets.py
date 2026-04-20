"""Strategy presets and alias resolution for paper trading.

Maps public strategy names (including legacy aliases) to canonical registry keys
and provides default paper trading parameters per strategy type.

Canonical names match the backtest strategy_registry.py:
  - 2lynchbreakout (LONG), 2lynchbreakdown (SHORT), episodicpivot (LONG)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Alias table: maps legacy/public names to canonical registry keys.
STRATEGY_ALIASES: dict[str, str] = {
    "thresholdbreakout": "2lynchbreakout",
    "indian2lynch": "2lynchbreakout",
    "thresholdbreakdown": "2lynchbreakdown",
    "epproxysameday": "episodicpivot",
}


def resolve_strategy_key(raw: str) -> str:
    """Resolve a strategy name (possibly an alias) to its canonical registry key."""
    key = raw.strip().lower()
    return STRATEGY_ALIASES.get(key, key)


@dataclass(frozen=True)
class PaperStrategyConfig:
    """Paper trading configuration for a specific strategy."""

    strategy_key: str
    direction: str  # "LONG" or "SHORT"
    breakout_threshold: float = 0.04
    breakout_reference: str = "prior_close"
    max_positions: int = 10
    max_position_pct: float = 0.10
    flatten_time: str = "15:15:00"
    entry_cutoff_minutes: int = 30  # minutes from open
    slippage_bps_large: float = 5.0
    slippage_bps_mid: float = 10.0
    slippage_bps_small: float = 20.0
    # Max allowed stop distance as a fraction of entry price (matches backtest defaults).
    # For SHORT, short_max_stop_dist_pct overrides max_stop_dist_pct when set.
    max_stop_dist_pct: float = 0.08
    short_max_stop_dist_pct: float | None = None
    extra_params: dict[str, Any] = field(default_factory=dict)


# Default paper trading configs per canonical strategy key.
_STRATEGY_DEFAULTS: dict[str, PaperStrategyConfig] = {
    "2lynchbreakout": PaperStrategyConfig(
        strategy_key="2lynchbreakout",
        direction="LONG",
        breakout_threshold=0.04,
        breakout_reference="prior_close",
    ),
    "2lynchbreakdown": PaperStrategyConfig(
        strategy_key="2lynchbreakdown",
        direction="SHORT",
        breakout_threshold=0.04,
        breakout_reference="prior_close",
    ),
    "episodicpivot": PaperStrategyConfig(
        strategy_key="episodicpivot",
        direction="LONG",
        breakout_threshold=0.0,  # EP uses gap-based detection, not threshold
        breakout_reference="open",
    ),
}


def get_paper_strategy_config(
    strategy: str,
    *,
    overrides: dict[str, Any] | None = None,
) -> PaperStrategyConfig:
    """Get paper trading config for a strategy, applying optional overrides."""
    canonical = resolve_strategy_key(strategy)
    base = _STRATEGY_DEFAULTS.get(canonical)
    if base is None:
        msg = f"Unknown strategy '{strategy}' (resolved to '{canonical}'). "
        msg += f"Available: {', '.join(sorted(_STRATEGY_DEFAULTS))}"
        raise ValueError(msg)

    if not overrides:
        return base

    # Apply overrides to a mutable copy.
    params = {**base.extra_params}
    fields = {
        "strategy_key": base.strategy_key,
        "direction": base.direction,
        "breakout_threshold": base.breakout_threshold,
        "breakout_reference": base.breakout_reference,
        "max_positions": base.max_positions,
        "max_position_pct": base.max_position_pct,
        "flatten_time": base.flatten_time,
        "entry_cutoff_minutes": base.entry_cutoff_minutes,
        "slippage_bps_large": base.slippage_bps_large,
        "slippage_bps_mid": base.slippage_bps_mid,
        "slippage_bps_small": base.slippage_bps_small,
        "max_stop_dist_pct": base.max_stop_dist_pct,
        "short_max_stop_dist_pct": base.short_max_stop_dist_pct,
    }
    for k, v in overrides.items():
        if k in fields:
            fields[k] = type(fields[k])(v)
        else:
            params[k] = v

    return PaperStrategyConfig(
        strategy_key=fields["strategy_key"],
        direction=fields["direction"],
        breakout_threshold=fields["breakout_threshold"],
        breakout_reference=fields["breakout_reference"],
        max_positions=fields["max_positions"],
        max_position_pct=fields["max_position_pct"],
        flatten_time=fields["flatten_time"],
        entry_cutoff_minutes=fields["entry_cutoff_minutes"],
        slippage_bps_large=fields["slippage_bps_large"],
        slippage_bps_mid=fields["slippage_bps_mid"],
        slippage_bps_small=fields["slippage_bps_small"],
        max_stop_dist_pct=fields["max_stop_dist_pct"],
        short_max_stop_dist_pct=fields["short_max_stop_dist_pct"],
        extra_params=params,
    )


def list_available_strategies() -> list[str]:
    """Return sorted list of canonical strategy keys."""
    return sorted(_STRATEGY_DEFAULTS)


def list_all_accepted_names() -> dict[str, str]:
    """Return mapping of all accepted names (canonical + aliases) to canonical keys."""
    result: dict[str, str] = {k: k for k in _STRATEGY_DEFAULTS}
    result.update(STRATEGY_ALIASES)
    return result
