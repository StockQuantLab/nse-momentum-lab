"""YAML sweep configuration schema and loader."""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams

_VALID_FIELDS = frozenset(f.name for f in BacktestParams.__dataclass_fields__.values())

_VALID_COMPARE_METRICS = frozenset(
    {
        "calmar_ratio",
        "win_rate",
        "annualised_return",
        "total_return",
        "max_drawdown",
        "profit_factor",
        "total_trades",
    }
)


def _validate_param_names(params: dict[str, Any], context: str = "params") -> None:
    """Validate that all param names are valid BacktestParams fields."""
    for key in params:
        if key not in _VALID_FIELDS:
            raise ValueError(f"{key!r} in {context} is not a valid BacktestParams field")


@dataclass(frozen=True)
class SweepAxis:
    """Single parameter axis to sweep."""

    param: str
    values: list[Any]

    def __post_init__(self) -> None:
        if self.param not in _VALID_FIELDS:
            raise ValueError(f"{self.param!r} is not a valid BacktestParams field")
        if not self.values:
            raise ValueError(f"Sweep axis {self.param!r} has empty values")

    def combinations(self) -> list[dict[str, Any]]:
        return [{self.param: v} for v in self.values]


@dataclass(frozen=True)
class SweepCompare:
    """Comparison and ranking configuration."""

    metric: str = "calmar_ratio"
    sort: str = "desc"  # "asc" or "desc"
    top_n: int = 5
    include_yearly: bool = True

    def __post_init__(self) -> None:
        if self.metric not in _VALID_COMPARE_METRICS:
            raise ValueError(f"Unknown compare metric: {self.metric!r}")
        if self.sort not in ("asc", "desc"):
            raise ValueError(f"Sort must be 'asc' or 'desc', got {self.sort!r}")


@dataclass
class SweepConfig:
    """Full sweep configuration."""

    name: str
    strategy: str
    base_params: dict[str, Any] = field(default_factory=dict)
    sweep: list[SweepAxis] = field(default_factory=list)
    compare: SweepCompare = field(default_factory=SweepCompare)
    tags: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> SweepConfig:
        # Validate strategy resolvability
        from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy

        strategy_name = raw.get("strategy", "")
        try:
            resolve_strategy(strategy_name)
        except (KeyError, ValueError) as e:
            raise ValueError(f"Cannot resolve strategy {strategy_name!r}: {e}") from e

        # Validate base_params
        base_params = raw.get("base_params", {})
        _validate_param_names(base_params, context="base_params")

        # Validate sweep axes (check for duplicates)
        sweep_axes = []
        seen_params: set[str] = set()
        for a in raw.get("sweep", []):
            axis = SweepAxis(param=a["param"], values=a["values"])
            if axis.param in seen_params:
                raise ValueError(f"Duplicate sweep axis: {axis.param!r}")
            seen_params.add(axis.param)
            sweep_axes.append(axis)

        compare_raw = raw.get("compare", {})
        compare = SweepCompare(**compare_raw) if compare_raw else SweepCompare()

        return cls(
            name=raw["name"],
            strategy=strategy_name,
            base_params=base_params,
            sweep=sweep_axes,
            compare=compare,
            tags=raw.get("tags", []),
        )

    @classmethod
    def _cartesian(cls, axes: list[SweepAxis]) -> list[dict[str, Any]]:
        """Compute cartesian product of all axis combinations."""
        if not axes:
            return [{}]
        keys = [a.param for a in axes]
        value_lists = [a.values for a in axes]
        return [dict(zip(keys, combo, strict=True)) for combo in itertools.product(*value_lists)]

    def combinations(self) -> list[dict[str, Any]]:
        return self._cartesian(self.sweep)

    def build_params_for(self, combo: dict[str, Any]) -> dict[str, Any]:
        """Merge base_params with a single sweep combination."""
        merged = {**self.base_params, "strategy": self.strategy, **combo}
        return merged


def load_sweep_config(path: Path) -> SweepConfig:
    """Load sweep configuration from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"YAML root must be a mapping, got {type(raw).__name__}")
    return SweepConfig.from_dict(raw)
