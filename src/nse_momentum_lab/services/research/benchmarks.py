"""
Performance benchmarking for backtest runs.

Tracks execution time, memory usage, and resource utilization to:
- Detect performance regressions
- Identify bottlenecks
- Establish baseline performance expectations
"""

from __future__ import annotations

import logging
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import wraps
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    import duckdb

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkMetrics:
    """Performance metrics from a single benchmark run."""

    # Timing metrics (seconds)
    total_duration_seconds: float
    candidate_generation_seconds: float = 0.0
    signal_resolution_seconds: float = 0.0
    execution_seconds: float = 0.0
    persistence_seconds: float = 0.0

    # Memory metrics (bytes)
    peak_memory_bytes: int = 0
    start_memory_bytes: int = 0
    end_memory_bytes: int = 0

    # Data metrics
    symbols_processed: int = 0
    signals_generated: int = 0
    trades_executed: int = 0
    years_processed: int = 0

    # Context
    benchmark_timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    strategy_name: str = ""
    dataset_hash: str = ""
    params_hash: str = ""

    @property
    def memory_delta_bytes(self) -> int:
        return self.end_memory_bytes - self.start_memory_bytes

    @property
    def memory_delta_mb(self) -> float:
        return self.memory_delta_bytes / (1024 * 1024)

    @property
    def peak_memory_mb(self) -> float:
        return self.peak_memory_bytes / (1024 * 1024)

    @property
    def signals_per_second(self) -> float:
        if self.candidate_generation_seconds > 0:
            return self.signals_generated / self.candidate_generation_seconds
        return 0.0

    @property
    def trades_per_second(self) -> float:
        if self.execution_seconds > 0:
            return self.trades_executed / self.execution_seconds
        return 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_duration_seconds": self.total_duration_seconds,
            "candidate_generation_seconds": self.candidate_generation_seconds,
            "signal_resolution_seconds": self.signal_resolution_seconds,
            "execution_seconds": self.execution_seconds,
            "persistence_seconds": self.persistence_seconds,
            "peak_memory_bytes": self.peak_memory_bytes,
            "memory_delta_bytes": self.memory_delta_bytes,
            "peak_memory_mb": self.peak_memory_mb,
            "memory_delta_mb": self.memory_delta_mb,
            "symbols_processed": self.symbols_processed,
            "signals_generated": self.signals_generated,
            "trades_executed": self.trades_executed,
            "years_processed": self.years_processed,
            "signals_per_second": self.signals_per_second,
            "trades_per_second": self.trades_per_second,
            "strategy_name": self.strategy_name,
            "dataset_hash": self.dataset_hash,
            "params_hash": self.params_hash,
            "benchmark_timestamp": self.benchmark_timestamp.isoformat(),
        }


@dataclass
class BenchmarkComparison:
    """Result of comparing two benchmarks."""

    baseline: BenchmarkMetrics
    current: BenchmarkMetrics

    # Computed deltas
    total_duration_delta_pct: float = 0.0
    memory_delta_pct: float = 0.0
    signals_per_sec_delta_pct: float = 0.0

    # Regression detection
    has_regression: bool = False
    regression_details: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.total_duration_delta_pct = self._pct_delta(
            self.baseline.total_duration_seconds,
            self.current.total_duration_seconds,
        )
        self.memory_delta_pct = self._pct_delta(
            self.baseline.peak_memory_bytes,
            self.current.peak_memory_bytes,
        )
        self.signals_per_sec_delta_pct = self._pct_delta(
            self.baseline.signals_per_second,
            self.current.signals_per_second,
        )
        self._detect_regressions()

    @staticmethod
    def _pct_delta(baseline: float, current: float) -> float:
        if baseline == 0:
            return 0.0
        return ((current - baseline) / baseline) * 100

    def _detect_regressions(self) -> None:
        """Detect performance regressions based on thresholds."""
        self.regression_details.clear()
        self.has_regression = False

        # Duration regression: +20% or more is a regression
        if self.total_duration_delta_pct > 20:
            self.has_regression = True
            self.regression_details.append(
                f"Duration increased by {self.total_duration_delta_pct:+.1f}% "
                f"({self.baseline.total_duration_seconds:.2f}s → {self.current.total_duration_seconds:.2f}s)"
            )

        # Memory regression: +30% or more is a regression
        if self.memory_delta_pct > 30:
            self.has_regression = True
            self.regression_details.append(
                f"Peak memory increased by {self.memory_delta_pct:+.1f}% "
                f"({self.baseline.peak_memory_mb:.1f}MB → {self.current.peak_memory_mb:.1f}MB)"
            )

        # Throughput regression: -15% or more is a regression
        if self.signals_per_sec_delta_pct < -15:
            self.has_regression = True
            self.regression_details.append(
                f"Signals/sec decreased by {self.signals_per_sec_delta_pct:+.1f}% "
                f"({self.baseline.signals_per_second:.1f} → {self.current.signals_per_second:.1f})"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline": self.baseline.to_dict(),
            "current": self.current.to_dict(),
            "total_duration_delta_pct": self.total_duration_delta_pct,
            "memory_delta_pct": self.memory_delta_pct,
            "signals_per_sec_delta_pct": self.signals_per_sec_delta_pct,
            "has_regression": self.has_regression,
            "regression_details": self.regression_details,
        }


class BacktestBenchmark:
    """
    Benchmarks backtest execution.

    Usage::

        benchmark = BacktestBenchmark()
        metrics = benchmark.measure(
            runner=runner,
            params=params,
            label="2lynch_baseline",
        )

        # Compare with previous
        comparison = benchmark.compare(metrics.label, metrics)
    """

    # Regression thresholds
    REGRESSION_DURATION_PCT = 20.0
    REGRESSION_MEMORY_PCT = 30.0
    REGRESSION_THROUGHPUT_PCT = -15.0

    def __init__(self, con: duckdb.DuckDBPyConnection | None = None):
        self.con = con
        self._history: dict[str, BenchmarkMetrics] = {}

    def measure(
        self,
        *,
        runner_fn: Callable[[], Any],
        strategy_name: str,
        dataset_hash: str,
        params_hash: str,
        label: str = "",
    ) -> BenchmarkMetrics:
        """
        Measure a backtest run's performance.

        Args:
            runner_fn: Function that executes the backtest
            strategy_name: Name of strategy being tested
            dataset_hash: Hash of input dataset
            params_hash: Hash of backtest parameters
            label: Optional label for this benchmark

        Returns:
            BenchmarkMetrics with timing and memory data
        """
        tracemalloc.start()
        start_time = time.perf_counter()
        start_memory = tracemalloc.get_traced_memory()[0]

        logger.info("Starting benchmark: %s", label or "unnamed")

        try:
            result = runner_fn()

            # Extract metrics from result if available
            symbols_processed = getattr(result, "symbols_processed", 0)
            signals_generated = getattr(result, "signals_generated", 0)
            trades_executed = getattr(result, "trades_executed", 0)
            years_processed = getattr(result, "years_processed", 0)

        except Exception as e:
            logger.error("Benchmark run failed: %s", e)
            raise
        finally:
            end_time = time.perf_counter()
            current_memory, peak_memory = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        metrics = BenchmarkMetrics(
            total_duration_seconds=end_time - start_time,
            peak_memory_bytes=peak_memory,
            start_memory_bytes=start_memory,
            end_memory_bytes=current_memory,
            symbols_processed=symbols_processed,
            signals_generated=signals_generated,
            trades_executed=trades_executed,
            years_processed=years_processed,
            strategy_name=strategy_name,
            dataset_hash=dataset_hash,
            params_hash=params_hash,
        )

        if label:
            self._history[label] = metrics
            self._persist_to_duckdb(metrics, label)

        logger.info(
            "Benchmark complete: %.2fs, peak %.1fMB, %d signals",
            metrics.total_duration_seconds,
            metrics.peak_memory_mb,
            metrics.signals_generated,
        )

        return metrics

    def compare(self, baseline_label: str, current: BenchmarkMetrics) -> BenchmarkComparison:
        """
        Compare current metrics against a stored baseline.

        Args:
            baseline_label: Label of the baseline benchmark
            current: Current benchmark metrics

        Returns:
            BenchmarkComparison with regression analysis
        """
        if baseline_label not in self._history:
            logger.warning("Baseline '%s' not found, using current as baseline", baseline_label)
            baseline = current
        else:
            baseline = self._history[baseline_label]

        comparison = BenchmarkComparison(baseline=baseline, current=current)

        if comparison.has_regression:
            logger.warning("Performance regression detected: %s", comparison.regression_details)
        else:
            logger.info("No performance regression detected")

        return comparison

    def compare_with_thresholds(
        self,
        baseline: BenchmarkMetrics,
        current: BenchmarkMetrics,
        *,
        duration_pct: float = REGRESSION_DURATION_PCT,
        memory_pct: float = REGRESSION_MEMORY_PCT,
        throughput_pct: float = REGRESSION_THROUGHPUT_PCT,
    ) -> BenchmarkComparison:
        """
        Compare with custom regression thresholds.

        Args:
            baseline: Baseline metrics
            current: Current metrics
            duration_pct: Duration increase threshold (%)
            memory_pct: Memory increase threshold (%)
            throughput_pct: Throughput decrease threshold (%)

        Returns:
            BenchmarkComparison with custom thresholds
        """
        comparison = BenchmarkComparison(baseline=baseline, current=current)

        # Override with custom thresholds
        comparison.has_regression = False
        comparison.regression_details.clear()

        if comparison.total_duration_delta_pct > duration_pct:
            comparison.has_regression = True
            comparison.regression_details.append(
                f"Duration +{comparison.total_duration_delta_pct:.1f}% exceeds threshold +{duration_pct:.0f}%"
            )

        if comparison.memory_delta_pct > memory_pct:
            comparison.has_regression = True
            comparison.regression_details.append(
                f"Memory +{comparison.memory_delta_pct:.1f}% exceeds threshold +{memory_pct:.0f}%"
            )

        if comparison.signals_per_sec_delta_pct < throughput_pct:
            comparison.has_regression = True
            comparison.regression_details.append(
                f"Throughput {comparison.signals_per_sec_delta_pct:.1f}% below threshold {throughput_pct:.0f}%"
            )

        return comparison

    def get_history(self, label: str | None = None) -> Sequence[BenchmarkMetrics]:
        """Get benchmark history, optionally filtered by label."""
        if label:
            return [self._history[label]] if label in self._history else []
        return list(self._history.values())

    def _persist_to_duckdb(self, metrics: BenchmarkMetrics, label: str) -> None:
        """Persist benchmark results to DuckDB."""
        if self.con is None:
            return

        try:
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS bt_benchmarks (
                    label TEXT,
                    benchmark_timestamp TIMESTAMP,
                    strategy_name TEXT,
                    dataset_hash TEXT,
                    params_hash TEXT,
                    total_duration_seconds DOUBLE,
                    candidate_generation_seconds DOUBLE,
                    signal_resolution_seconds DOUBLE,
                    execution_seconds DOUBLE,
                    persistence_seconds DOUBLE,
                    peak_memory_bytes INTEGER,
                    memory_delta_bytes INTEGER,
                    symbols_processed INTEGER,
                    signals_generated INTEGER,
                    trades_executed INTEGER,
                    years_processed INTEGER,
                    signals_per_second DOUBLE,
                    trades_per_second DOUBLE
                )
            """)

            self.con.execute(
                """
                INSERT INTO bt_benchmarks VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """,
                [
                    label,
                    metrics.benchmark_timestamp,
                    metrics.strategy_name,
                    metrics.dataset_hash,
                    metrics.params_hash,
                    metrics.total_duration_seconds,
                    metrics.candidate_generation_seconds,
                    metrics.signal_resolution_seconds,
                    metrics.execution_seconds,
                    metrics.persistence_seconds,
                    metrics.peak_memory_bytes,
                    metrics.memory_delta_bytes,
                    metrics.symbols_processed,
                    metrics.signals_generated,
                    metrics.trades_executed,
                    metrics.years_processed,
                    metrics.signals_per_second,
                    metrics.trades_per_second,
                ],
            )
        except Exception as e:
            logger.warning("Failed to persist benchmark: %s", e)

    @classmethod
    def load_baseline(cls, con: duckdb.DuckDBPyConnection, label: str) -> BenchmarkMetrics | None:
        """Load a baseline benchmark from DuckDB."""
        try:
            row = con.execute(
                "SELECT * FROM bt_benchmarks WHERE label = ? ORDER BY benchmark_timestamp DESC LIMIT 1",
                [label],
            ).fetchone()

            if row:
                return BenchmarkMetrics(
                    total_duration_seconds=row[5] or 0.0,
                    candidate_generation_seconds=row[6] or 0.0,
                    signal_resolution_seconds=row[7] or 0.0,
                    execution_seconds=row[8] or 0.0,
                    persistence_seconds=row[9] or 0.0,
                    peak_memory_bytes=row[10] or 0,
                    start_memory_bytes=(row[10] or 0) - (row[11] or 0),
                    end_memory_bytes=row[10] or 0,
                    symbols_processed=row[12] or 0,
                    signals_generated=row[13] or 0,
                    trades_executed=row[14] or 0,
                    years_processed=row[15] or 0,
                    benchmark_timestamp=datetime.fromisoformat(row[1])
                    if row[1]
                    else datetime.now(UTC),
                    strategy_name=row[2] or "",
                    dataset_hash=row[3] or "",
                    params_hash=row[4] or "",
                )
        except Exception as e:
            logger.warning("Failed to load baseline '%s': %s", label, e)
        return None


def benchmarked(label: str = "") -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to benchmark a function.

    Usage::

        @benchmarked("my_backtest")
        def run_backtest(params):
            ...
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            tracemalloc.start()
            start = time.perf_counter()

            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.perf_counter() - start
                _, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()

                logger.info(
                    "[BENCHMARK %s] %.2fs, peak %.1fMB",
                    label or func.__name__,
                    duration,
                    peak / (1024 * 1024),
                )

        return wrapper

    return decorator


def estimate_full_run_duration(
    sample_duration_seconds: float,
    sample_symbols: int,
    sample_years: int,
    target_symbols: int,
    target_years: int,
) -> float:
    """
    Estimate full backtest duration based on a sample run.

    Assumes O(n) scaling with symbols and years.
    """
    if sample_symbols == 0 or sample_years == 0:
        return 0.0

    symbol_factor = target_symbols / sample_symbols
    year_factor = target_years / sample_years

    return sample_duration_seconds * symbol_factor * year_factor


# Baseline performance expectations (10-year, 500-symbol run)
BASELINE_EXPECTATIONS = {
    "indian_2lynch": {
        "max_duration_seconds": 600,  # 10 minutes
        "max_memory_mb": 2048,  # 2GB
        "min_signals_per_second": 50.0,
    },
    "threshold_breakout": {
        "max_duration_seconds": 600,
        "max_memory_mb": 2048,
        "min_signals_per_second": 50.0,
    },
    "threshold_breakdown": {
        "max_duration_seconds": 600,
        "max_memory_mb": 2048,
        "min_signals_per_second": 50.0,
    },
    "episodic_pivot": {
        "max_duration_seconds": 300,  # Fewer signals
        "max_memory_mb": 1024,
        "min_signals_per_second": 10.0,
    },
}


def check_baseline_expectations(
    metrics: BenchmarkMetrics,
    strategy_name: str,
) -> dict[str, Any]:
    """
    Check if metrics meet baseline performance expectations.

    Returns dict with 'passed' bool and 'details' list.
    """
    expectations = BASELINE_EXPECTATIONS.get(strategy_name, BASELINE_EXPECTATIONS["indian_2lynch"])

    details: list[str] = []
    passed = True

    if metrics.total_duration_seconds > expectations["max_duration_seconds"]:
        passed = False
        details.append(
            f"Duration {metrics.total_duration_seconds:.1f}s exceeds "
            f"expectation {expectations['max_duration_seconds']:.0f}s"
        )

    if metrics.peak_memory_mb > expectations["max_memory_mb"]:
        passed = False
        details.append(
            f"Memory {metrics.peak_memory_mb:.0f}MB exceeds "
            f"expectation {expectations['max_memory_mb']:.0f}MB"
        )

    if metrics.signals_per_second < expectations["min_signals_per_second"]:
        passed = False
        details.append(
            f"Throughput {metrics.signals_per_second:.1f} signals/sec below "
            f"expectation {expectations['min_signals_per_second']:.0f}"
        )

    if not details:
        details.append("All baseline expectations met")

    return {"passed": passed, "details": details}
