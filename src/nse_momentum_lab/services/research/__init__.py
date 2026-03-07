"""
Research services for NSE Momentum Lab.

This module provides:
- Performance benchmarking
- Stale-run detection for dataset/feature changes
- Quality gates for backtest validation
"""

from nse_momentum_lab.services.research.benchmarks import (
    BASELINE_EXPECTATIONS,
    BacktestBenchmark,
    BenchmarkComparison,
    BenchmarkMetrics,
    benchmarked,
    check_baseline_expectations,
    estimate_full_run_duration,
)
from nse_momentum_lab.services.research.stale_detection import (
    DatasetVersionTracker,
    FeatureDependencyGraph,
    build_code_sha,
    find_cascading_stale_features,
    get_rebuild_plan,
    is_run_stale,
    list_stale_runs,
)
from nse_momentum_lab.services.research.validation import (
    STRATEGY_THRESHOLDS,
    QualityGateResult,
    QualityThresholds,
    validate_backtest_result,
    validate_benchmark,
    validate_performance_regressions,
    validate_research_run,
)

__all__ = [
    "BASELINE_EXPECTATIONS",
    "STRATEGY_THRESHOLDS",
    # Benchmarks
    "BacktestBenchmark",
    "BenchmarkComparison",
    "BenchmarkMetrics",
    # Stale detection
    "DatasetVersionTracker",
    "FeatureDependencyGraph",
    # Validation
    "QualityGateResult",
    "QualityThresholds",
    "benchmarked",
    "build_code_sha",
    "check_baseline_expectations",
    "estimate_full_run_duration",
    "find_cascading_stale_features",
    "get_rebuild_plan",
    "is_run_stale",
    "list_stale_runs",
    "validate_backtest_result",
    "validate_benchmark",
    "validate_performance_regressions",
    "validate_research_run",
]
