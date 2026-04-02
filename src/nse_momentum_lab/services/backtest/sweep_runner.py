"""Sweep orchestration engine — runs multiple backtests and auto-compares."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from nse_momentum_lab.services.backtest.comparison import (
    ExperimentSummary,
    compare_experiments,
)
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)
from nse_momentum_lab.services.backtest.sweep_schema import SweepConfig

logger = logging.getLogger(__name__)


@dataclass
class SweepResult:
    """Result of a single sweep combination."""

    exp_id: str
    label: str
    params: dict[str, Any]


def _build_label(combo: dict[str, Any]) -> str:
    """Build a human-readable label from a parameter combination."""
    parts = []
    for k, v in combo.items():
        short = k.replace("breakout_threshold", "thresh").replace("trail_activation_pct", "trail")
        parts.append(f"{short}={v}")
    return "-".join(parts)


def run_sweep(
    config: SweepConfig,
    force: bool = False,
    snapshot: bool = False,
    dry_run: bool = False,
) -> list[SweepResult]:
    """Execute all combinations in a sweep config.

    Args:
        config: SweepConfig with base params and sweep axes.
        force: Re-run even if cached.
        snapshot: Publish DuckDB snapshot per run.
        dry_run: Show combinations without running.

    Returns:
        List of SweepResult with exp_ids and labels.
    """
    combos = config.combinations()
    logger.info(
        "Sweep '%s': %d combinations for strategy '%s'", config.name, len(combos), config.strategy
    )

    if dry_run:
        for combo in combos:
            label = _build_label(combo)
            logger.info("  [DRY-RUN] %s -> %s", label, combo)
        return [SweepResult(exp_id="(dry-run)", label=_build_label(c), params=c) for c in combos]

    runner = DuckDBBacktestRunner()
    results: list[SweepResult] = []

    for combo in combos:
        label = _build_label(combo)
        params_dict = config.build_params_for(combo)
        params = BacktestParams(**params_dict)
        logger.info("Running %s …", label)
        exp_id = runner.run(params, force=force, snapshot=snapshot)
        results.append(SweepResult(exp_id=exp_id, label=label, params=combo))
        logger.info("  -> %s", exp_id)

    return results


def run_sweep_with_comparison(
    config: SweepConfig, **kwargs: Any
) -> tuple[list[SweepResult], list[ExperimentSummary]]:
    """Run sweep and auto-compare results."""
    results = run_sweep(config, **kwargs)
    experiments = [(r.exp_id, r.label) for r in results if r.exp_id != "(dry-run)"]
    if not experiments:
        return results, []
    ranked = compare_experiments(
        experiments,
        metric=config.compare.metric,
        sort=config.compare.sort,
        top_n=config.compare.top_n,
    )
    return results, ranked
