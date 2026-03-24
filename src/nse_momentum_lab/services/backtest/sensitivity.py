"""Parameter sensitivity analysis for strategy robustness testing.

This module provides one-at-a-time (OAT) sensitivity analysis to identify
which strategy parameters have the most impact on performance.

This is now strategy-agnostic and works with any registered strategy.

Usage:
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.sensitivity \
        --start-date 2024-01-01 --end-date 2024-12-31 --strategy indian2lynch

    doppler run -- uv run python -m nse_momentum_lab.services.backtest.sensitivity \
        --start-date 2024-01-01 --end-date 2024-12-31 --strategy thresholdbreakout
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import numpy as np

from nse_momentum_lab.services.backtest.protocols import (
    ProtocolConfig,
    ProtocolResult,
    SensitivityOATProtocol,
)
from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SensitivityResult:
    parameter: str
    base_value: Any
    tested_values: list[Any]
    metric_values: list[float]
    sensitivity_score: float
    optimal_value: Any
    metric_range: float


@dataclass
class SensitivityReport:
    results: list[SensitivityResult]
    most_sensitive: list[str]
    least_sensitive: list[str]
    base_metrics: dict[str, float]


DEFAULT_PARAM_RANGES: dict[str, list[Any]] = {
    "breakout_threshold": [0.02, 0.03, 0.04, 0.05, 0.06],
    "close_pos_threshold": [0.50, 0.60, 0.70, 0.80, 0.90],
    "nr_percentile": [0.10, 0.15, 0.20, 0.25, 0.30],
    "min_r2_l": [0.50, 0.60, 0.70, 0.80, 0.90],
    "max_down_days_l": [3, 5, 7, 10, 14],
    "atr_compress_ratio": [0.60, 0.70, 0.80, 0.90, 1.0],
    "range_percentile": [0.10, 0.15, 0.20, 0.25, 0.30],
    "vol_dryup_ratio": [0.60, 0.70, 0.80, 0.90, 1.0],
    "max_prior_breakouts": [0, 1, 2, 3, 4],
    "time_stop_days": [2, 3, 5, 7, 10],
    "trail_activation_pct": [0.03, 0.05, 0.07, 0.10],
    "trail_stop_pct": [0.01, 0.02, 0.03, 0.05],
    "initial_stop_atr_mult": [1.5, 2.0, 2.5, 3.0],
}


class SensitivityAnalyzer:
    """Strategy-agnostic sensitivity analyzer using the unified protocol framework."""

    def __init__(
        self,
        start_date: date,
        end_date: date,
        objective: str = "sharpe_ratio",
        strategy_name: str = "thresholdbreakout",
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.objective = objective
        self.strategy_name = strategy_name
        self._strategy = resolve_strategy(strategy_name)
        self._param_ranges = self._get_strategy_param_ranges()
        logger.info(f"Initialized sensitivity analyzer for strategy: {strategy_name}")

    def _get_strategy_param_ranges(self) -> dict[str, list[Any]]:
        """Get default parameter ranges based on strategy family."""
        strategy_family = self._strategy.family

        if strategy_family == "threshold_breakout":
            return {
                "breakout_threshold": [0.02, 0.03, 0.04, 0.05, 0.06],
                "close_pos_threshold": [0.50, 0.60, 0.70, 0.80, 0.90],
                "nr_percentile": [0.10, 0.15, 0.20, 0.25, 0.30],
                "min_r2_l": [0.50, 0.60, 0.70, 0.80, 0.90],
                "max_down_days_l": [3, 5, 7, 10, 14],
            }
        elif strategy_family in ("threshold_breakout", "threshold_breakdown"):
            return {
                "breakout_threshold": [0.02, 0.03, 0.04, 0.05, 0.06],
                "min_price": [5, 10, 20, 50],
                "min_value_traded_inr": [1_000_000, 3_000_000, 5_000_000, 10_000_000],
            }
        elif strategy_family == "episodic_pivot":
            return {
                "min_gap_pct": [0.02, 0.05, 0.07, 0.10, 0.15],
                "min_consolidation_days": [3, 5, 10, 15, 20],
            }

        return {}

    async def run_sensitivity_analysis(
        self,
        parameters: list[str] | None = None,
        param_ranges: dict[str, list[Any]] | None = None,
    ) -> SensitivityReport:
        """Run one-at-a-time sensitivity analysis.

        Args:
            parameters: List of parameter names to analyze
            param_ranges: Custom parameter ranges

        Returns:
            SensitivityReport with results
        """
        param_ranges = param_ranges or self._param_ranges
        parameters = parameters or list(param_ranges.keys())

        config = ProtocolConfig(
            strategy_name=self.strategy_name,
            start_date=self.start_date,
            end_date=self.end_date,
            objective_metric=self.objective,
        )

        protocol = SensitivityOATProtocol(config, param_ranges)

        async def backtest_fn(
            params: dict[str, Any],
            start_date: date,
            end_date: date,
        ) -> dict[str, Any]:
            return await self._evaluate_params(params)

        await protocol.run(backtest_fn)

        base_params = self._strategy.get_default_params()
        base_metrics = await self._evaluate_params(base_params)

        sensitivity_results: list[SensitivityResult] = []

        for param_name in parameters:
            if param_name not in param_ranges:
                logger.warning(f"Unknown parameter: {param_name}, skipping")
                continue

            param_values = param_ranges[param_name]
            base_value = base_params.get(param_name)
            metric_values = []

            for value in param_values:
                test_params = base_params.copy()
                test_params[param_name] = value

                try:
                    metrics = await self._evaluate_params(test_params)
                    metric_value = metrics.get(self.objective, 0.0)
                except Exception as e:
                    logger.error(f"  Failed: {e}")
                    metric_value = 0.0

                metric_values.append(metric_value)

            valid_metrics = [m for m in metric_values if m != 0.0]
            if valid_metrics:
                metric_range = max(valid_metrics) - min(valid_metrics)
                optimal_idx = metric_values.index(max(metric_values))
                optimal_value = param_values[optimal_idx]
            else:
                metric_range = 0.0
                optimal_value = base_value

            sensitivity_score = self._calculate_sensitivity_score(param_values, metric_values)

            sensitivity_results.append(
                SensitivityResult(
                    parameter=param_name,
                    base_value=base_value,
                    tested_values=param_values,
                    metric_values=metric_values,
                    sensitivity_score=sensitivity_score,
                    optimal_value=optimal_value,
                    metric_range=metric_range,
                )
            )

        sensitivity_results.sort(key=lambda x: x.sensitivity_score, reverse=True)

        most_sensitive = [r.parameter for r in sensitivity_results[:3]]
        least_sensitive = [r.parameter for r in sensitivity_results[-3:]]

        return SensitivityReport(
            results=sensitivity_results,
            most_sensitive=most_sensitive,
            least_sensitive=least_sensitive,
            base_metrics=base_metrics,
        )

    async def run_protocol(
        self,
        param_ranges: dict[str, list[Any]] | None = None,
    ) -> ProtocolResult:
        """Run sensitivity analysis using the unified protocol framework.

        Args:
            param_ranges: Custom parameter ranges

        Returns:
            ProtocolResult with all fold results
        """
        config = ProtocolConfig(
            strategy_name=self.strategy_name,
            start_date=self.start_date,
            end_date=self.end_date,
            objective_metric=self.objective,
        )

        protocol = SensitivityOATProtocol(config, param_ranges)

        async def backtest_fn(
            params: dict[str, Any],
            start_date: date,
            end_date: date,
        ) -> dict[str, Any]:
            return await self._evaluate_params(params)

        return await protocol.run(backtest_fn)

    async def _evaluate_params(self, params: dict[str, Any]) -> dict[str, float]:
        """Evaluate parameters with the strategy's candidate generation.

        This is a placeholder - in a full implementation, this would use
        the strategy's candidate generation and backtest logic.
        """
        logger.debug(f"Evaluating params: {params}")

        return {
            "sharpe_ratio": 0.0,
            "total_return": 0.0,
            "win_rate": 0.0,
            "trades": 0,
        }

    def _calculate_sensitivity_score(
        self,
        values: list[Any],
        metrics: list[float],
    ) -> float:
        """Calculate sensitivity score based on correlation and range."""
        if len(metrics) < 2:
            return 0.0

        valid_pairs = [(v, m) for v, m in zip(values, metrics, strict=True) if m != 0.0]
        if len(valid_pairs) < 2:
            return 0.0

        vals = np.array([p[0] for p in valid_pairs], dtype=np.float64)
        mets = np.array([p[1] for p in valid_pairs], dtype=np.float64)

        if np.std(vals) == 0:
            return 0.0

        correlation = np.corrcoef(vals, mets)[0, 1]
        if np.isnan(correlation):
            return 0.0

        range_score = (np.max(mets) - np.min(mets)) / (np.mean(np.abs(mets)) + 1e-10)

        return float(abs(correlation) * range_score)


def print_sensitivity_report(report: SensitivityReport, objective: str) -> None:
    print(f"\n{'=' * 80}")
    print("SENSITIVITY ANALYSIS REPORT")
    print(f"{'=' * 80}")
    print(f"Objective: {objective}")
    print(f"Base metrics: {report.base_metrics}")
    print(f"\nMost sensitive parameters: {', '.join(report.most_sensitive)}")
    print(f"Least sensitive parameters: {', '.join(report.least_sensitive)}")

    print(f"\n{'Parameter':<25} {'Base':<12} {'Optimal':<12} {'Range':<12} {'Score':<10}")
    print("-" * 80)

    for r in report.results:
        base_str = str(r.base_value)[:10]
        opt_str = str(r.optimal_value)[:10]
        range_str = f"{r.metric_range:.4f}"
        print(
            f"{r.parameter:<25} {base_str:<12} {opt_str:<12} {range_str:<12} {r.sensitivity_score:<10.4f}"
        )

    print("\nDetailed results per parameter:")
    print("-" * 80)

    for r in report.results:
        print(f"\n{r.parameter}:")
        for val, metric in zip(r.tested_values, r.metric_values, strict=True):
            marker = " <-- optimal" if val == r.optimal_value else ""
            print(f"  {val:>10} -> {metric:>10.4f}{marker}")

    print("=" * 80)


async def main_async(args):
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    analyzer = SensitivityAnalyzer(
        start_date=start_date,
        end_date=end_date,
        objective=args.objective,
        strategy_name=args.strategy,
    )

    if args.use_protocol:
        result = await analyzer.run_protocol()
        print(f"\nProtocol result: {result.status}")
        print(f"Total runs: {result.total_runs}")
        print(f"Best params: {result.best_params}")
    else:
        report = await analyzer.run_sensitivity_analysis()
        print_sensitivity_report(report, args.objective)


def main():
    parser = argparse.ArgumentParser(description="Parameter Sensitivity Analysis")
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="indian2lynch",
        help="Strategy name to analyze",
    )
    parser.add_argument(
        "--objective",
        type=str,
        default="sharpe_ratio",
        help="Optimization objective metric",
    )
    parser.add_argument(
        "--use-protocol",
        action="store_true",
        help="Use the unified protocol framework",
    )
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
