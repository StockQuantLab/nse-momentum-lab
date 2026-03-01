"""Parameter sensitivity analysis for strategy robustness testing.

This module provides one-at-a-time (OAT) sensitivity analysis to identify
which strategy parameters have the most impact on performance.

Usage:
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.sensitivity \
        --start-date 2024-01-01 --end-date 2024-12-31
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

from nse_momentum_lab.services.backtest.optimizer import ParameterOptimizer
from nse_momentum_lab.services.scan.rules import ScanConfig

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
    def __init__(
        self,
        start_date: date,
        end_date: date,
        objective: str = "sharpe_ratio",
        base_config: ScanConfig | None = None,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.objective = objective
        self.base_config = base_config or ScanConfig()
        self._optimizer = ParameterOptimizer(start_date, end_date, objective)

    async def run_sensitivity_analysis(
        self,
        parameters: list[str] | None = None,
        param_ranges: dict[str, list[Any]] | None = None,
    ) -> SensitivityReport:
        parameters = parameters or list(DEFAULT_PARAM_RANGES.keys())
        param_ranges = param_ranges or DEFAULT_PARAM_RANGES

        logger.info("Running sensitivity analysis...")
        logger.info(f"Base config: breakout_threshold={self.base_config.breakout_threshold}")

        base_params = self._config_to_params(self.base_config)
        base_metrics = await self._evaluate_params(base_params)
        logger.info(f"Base metrics: {base_metrics}")

        results: list[SensitivityResult] = []

        for param in parameters:
            if param not in param_ranges:
                logger.warning(f"Unknown parameter: {param}, skipping")
                continue

            logger.info(f"\nAnalyzing parameter: {param}")
            result = await self._analyze_parameter(
                param,
                param_ranges[param],
                base_params,
                base_metrics,
            )
            results.append(result)

        results.sort(key=lambda x: x.sensitivity_score, reverse=True)

        most_sensitive = [r.parameter for r in results[:3]]
        least_sensitive = [r.parameter for r in results[-3:]]

        return SensitivityReport(
            results=results,
            most_sensitive=most_sensitive,
            least_sensitive=least_sensitive,
            base_metrics=base_metrics,
        )

    async def _analyze_parameter(
        self,
        param: str,
        values: list[Any],
        base_params: dict[str, Any],
        base_metrics: dict[str, float],
    ) -> SensitivityResult:
        base_value = base_params.get(param)
        metric_values: list[float] = []

        for value in values:
            test_params = base_params.copy()
            test_params[param] = value

            logger.info(f"  Testing {param}={value}")
            try:
                metrics = await self._evaluate_params(test_params)
                metric_value = metrics.get(self.objective, 0.0)
            except Exception as e:
                logger.error(f"    Failed: {e}")
                metric_value = 0.0

            metric_values.append(metric_value)

        valid_metrics = [m for m in metric_values if m != 0.0]
        if valid_metrics:
            metric_range = max(valid_metrics) - min(valid_metrics)
            optimal_idx = metric_values.index(max(metric_values))
            optimal_value = values[optimal_idx]
        else:
            metric_range = 0.0
            optimal_value = base_value

        sensitivity_score = self._calculate_sensitivity_score(values, metric_values)

        return SensitivityResult(
            parameter=param,
            base_value=base_value,
            tested_values=values,
            metric_values=metric_values,
            sensitivity_score=sensitivity_score,
            optimal_value=optimal_value,
            metric_range=metric_range,
        )

    def _calculate_sensitivity_score(
        self,
        values: list[Any],
        metrics: list[float],
    ) -> float:
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

    async def _evaluate_params(self, params: dict[str, Any]) -> dict[str, float]:
        test_params = {
            "breakout_threshold": params.get("breakout_threshold", 0.04),
            "close_pos_threshold": params.get("close_pos_threshold", 0.70),
            "nr_percentile": params.get("nr_percentile", 0.20),
            "min_r2_l": params.get("min_r2_l", 0.70),
            "max_down_days_l": params.get("max_down_days_l", 7),
            "atr_compress_ratio": params.get("atr_compress_ratio", 0.80),
            "range_percentile": params.get("range_percentile", 0.20),
            "vol_dryup_ratio": params.get("vol_dryup_ratio", 0.80),
            "max_prior_breakouts": params.get("max_prior_breakouts", 2),
        }

        return await self._optimizer._evaluate_params(test_params)

    def _config_to_params(self, config: ScanConfig) -> dict[str, Any]:
        return {
            "breakout_threshold": config.breakout_threshold,
            "close_pos_threshold": config.close_pos_threshold,
            "nr_percentile": config.nr_percentile,
            "min_r2_l": config.min_r2_l,
            "max_down_days_l": config.max_down_days_l,
            "atr_compress_ratio": config.atr_compress_ratio,
            "range_percentile": config.range_percentile,
            "vol_dryup_ratio": config.vol_dryup_ratio,
            "max_prior_breakouts": config.max_prior_breakouts,
        }


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
    )

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
        "--objective",
        type=str,
        default="sharpe_ratio",
        help="Optimization objective metric",
    )
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
