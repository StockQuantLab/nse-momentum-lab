"""Unified research protocol framework.

This module provides a strategy-agnostic protocol system for running
research experiments including:
- single_run: Single backtest run
- grid_search: Parameter grid search optimization
- random_search: Random parameter search optimization
- walk_forward_anchored: Anchored walk-forward analysis
- walk_forward_rolling: Rolling window walk-forward analysis
- sensitivity_oat: One-at-a-time sensitivity analysis

All protocols are designed to work with any strategy registered in the strategy registry.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, TypeVar

from nse_momentum_lab.services.backtest.strategy_registry import (
    resolve_strategy,
)
from nse_momentum_lab.utils.hash_utils import compute_short_hash

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ProtocolType(Enum):
    SINGLE_RUN = "single_run"
    GRID_SEARCH = "grid_search"
    RANDOM_SEARCH = "random_search"
    WALK_FORWARD_ANCHORED = "walk_forward_anchored"
    WALK_FORWARD_ROLLING = "walk_forward_rolling"
    SENSITIVITY_OAT = "sensitivity_oat"


@dataclass
class ProtocolConfig:
    """Configuration shared across all research protocols."""

    strategy_name: str
    start_date: date
    end_date: date
    objective_metric: str = "sharpe_ratio"
    dataset_hash: str | None = None
    feature_hash: str | None = None
    code_sha: str | None = None
    checkpoint_enabled: bool = True
    max_combinations: int = 100
    random_seed: int = 42

    def __post_init__(self) -> None:
        if isinstance(self.strategy_name, str):
            self.strategy = resolve_strategy(self.strategy_name)
        else:
            self.strategy = self.strategy_name

    @property
    def protocol_hash(self) -> str:
        parts = [
            self.strategy_name,
            self.start_date.isoformat(),
            self.end_date.isoformat(),
            self.objective_metric,
        ]
        if self.dataset_hash:
            parts.append(self.dataset_hash)
        return compute_short_hash("".join(parts))


@dataclass
class FoldResult:
    """Result from a single fold in walk-forward or optimization."""

    fold_index: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    params: dict[str, Any]
    train_metrics: dict[str, float]
    test_metrics: dict[str, float]
    trade_count: int
    status: str = "SUCCEEDED"
    error_message: str | None = None
    checkpoint_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProtocolResult:
    """Result from a complete protocol run."""

    protocol_type: ProtocolType
    protocol_hash: str
    strategy_name: str
    strategy_version: str
    start_date: date
    end_date: date
    objective_metric: str
    folds: list[FoldResult] = field(default_factory=list)
    best_params: dict[str, Any] = field(default_factory=dict)
    best_metrics: dict[str, float] = field(default_factory=dict)
    status: str = "RUNNING"
    started_at: datetime | None = None
    finished_at: datetime | None = None
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "protocol_type": self.protocol_type.value,
            "protocol_hash": self.protocol_hash,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "objective_metric": self.objective_metric,
            "folds": [
                {
                    "fold_index": f.fold_index,
                    "train_start": f.train_start.isoformat(),
                    "train_end": f.train_end.isoformat(),
                    "test_start": f.test_start.isoformat(),
                    "test_end": f.test_end.isoformat(),
                    "params": f.params,
                    "train_metrics": f.train_metrics,
                    "test_metrics": f.test_metrics,
                    "trade_count": f.trade_count,
                    "status": f.status,
                    "error_message": f.error_message,
                }
                for f in self.folds
            ],
            "best_params": self.best_params,
            "best_metrics": self.best_metrics,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failed_runs": self.failed_runs,
        }


class ResearchProtocol(ABC):
    """Abstract base class for research protocols."""

    def __init__(self, config: ProtocolConfig) -> None:
        self.config = config
        self._result: ProtocolResult | None = None

    @property
    @abstractmethod
    def protocol_type(self) -> ProtocolType:
        """Return the protocol type."""
        pass

    @abstractmethod
    async def run(
        self,
        backtest_fn: Callable[..., Awaitable[dict[str, Any]]],
    ) -> ProtocolResult:
        """Run the protocol with a backtest function.

        Args:
            backtest_fn: Async callable: (params, start_date, end_date) -> dict[str, Any]

        Returns:
            ProtocolResult with all fold results and metrics.
        """
        pass

    def _create_result(self) -> ProtocolResult:
        """Create initial protocol result."""
        return ProtocolResult(
            protocol_type=self.protocol_type,
            protocol_hash=self.config.protocol_hash,
            strategy_name=self.config.strategy.name,
            strategy_version=self.config.strategy.version,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            objective_metric=self.config.objective_metric,
            started_at=datetime.utcnow(),
        )


class SingleRunProtocol(ResearchProtocol):
    """Single backtest run protocol."""

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.SINGLE_RUN

    async def run(
        self,
        backtest_fn: Callable[
            [dict[str, Any], date, date],
            Awaitable[dict[str, Any]],
        ],
    ) -> ProtocolResult:
        result = self._create_result()

        try:
            metrics = await backtest_fn(
                params=self.config.strategy.get_default_params(),
                start_date=self.config.start_date,
                end_date=self.config.end_date,
            )

            fold = FoldResult(
                fold_index=0,
                train_start=self.config.start_date,
                train_end=self.config.end_date,
                test_start=self.config.start_date,
                test_end=self.config.end_date,
                params=self.config.strategy.get_default_params(),
                train_metrics={},
                test_metrics=metrics,
                trade_count=metrics.get("trade_count", 0),
            )

            result.folds = [fold]
            result.best_params = self.config.strategy.get_default_params()
            result.best_metrics = metrics
            result.total_runs = 1
            result.successful_runs = 1
            result.status = "SUCCEEDED"

        except Exception as e:
            logger.error(f"Single run failed: {e}")
            result.status = "FAILED"
            result.failed_runs = 1
            if result.folds:
                result.folds[0].status = "FAILED"
                result.folds[0].error_message = str(e)

        result.finished_at = datetime.utcnow()
        self._result = result
        return result


class GridSearchProtocol(ResearchProtocol):
    """Grid search parameter optimization protocol."""

    def __init__(
        self,
        config: ProtocolConfig,
        param_grid: dict[str, list[Any]] | None = None,
    ) -> None:
        super().__init__(config)
        self.param_grid = param_grid or self._get_default_grid()

    def _get_default_grid(self) -> dict[str, list[Any]]:
        strategy_family = self.config.strategy.family

        if strategy_family == "indian_2lynch":
            return {
                "breakout_threshold": [0.03, 0.04, 0.05],
                "close_pos_threshold": [0.60, 0.70, 0.80],
                "nr_percentile": [0.15, 0.20, 0.25],
                "min_r2_l": [0.60, 0.70, 0.80],
                "max_down_days_l": [5, 7, 10],
            }
        elif strategy_family in ("threshold_breakout", "threshold_breakdown"):
            return {
                "breakout_threshold": [0.02, 0.03, 0.04, 0.05],
                "breakout_reference": ["prior_close", "prior_high", "multi_day_high"],
            }
        elif strategy_family == "episodic_pivot":
            return {
                "min_gap_pct": [0.03, 0.05, 0.07, 0.10],
                "min_consolidation_days": [5, 10, 15],
            }

        return {}

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.GRID_SEARCH

    def _generate_combinations(
        self,
        param_grid: dict[str, list[Any]],
    ) -> list[dict[str, Any]]:
        """Generate all parameter combinations from grid."""
        import itertools

        keys = list(param_grid.keys())
        values = [param_grid[k] for k in keys]
        combinations = list(itertools.product(*values))
        return [dict(zip(keys, combo, strict=True)) for combo in combinations]

    async def run(
        self,
        backtest_fn: Callable[..., Awaitable[dict[str, Any]]],
    ) -> ProtocolResult:
        result = self._create_result()

        combinations = self._generate_combinations(self.param_grid)

        if len(combinations) > self.config.max_combinations:
            logger.warning(
                f"Grid has {len(combinations)} combinations, limiting to {self.config.max_combinations}"
            )
            combinations = combinations[: self.config.max_combinations]

        logger.info(f"Running grid search with {len(combinations)} parameter combinations")

        fold_results: list[tuple[dict[str, Any], dict[str, float], int]] = []

        for i, params in enumerate(combinations):
            logger.info(f"  [{i + 1}/{len(combinations)}] Testing params: {params}")

            try:
                metrics = await backtest_fn(
                    params=params,
                    start_date=self.config.start_date,
                    end_date=self.config.end_date,
                )

                trade_count = metrics.get("trade_count", 0)
                fold_results.append((params, metrics, trade_count))
                result.successful_runs += 1

            except Exception as e:
                logger.error(f"    Failed: {e}")
                result.failed_runs += 1

            result.total_runs += 1

        if fold_results:
            fold_results.sort(
                key=lambda x: x[1].get(self.config.objective_metric, -999),
                reverse=True,
            )

            best_params, best_metrics, _best_trade_count = fold_results[0]

            for i, (params, metrics, trade_count) in enumerate(fold_results):
                fold = FoldResult(
                    fold_index=i,
                    train_start=self.config.start_date,
                    train_end=self.config.end_date,
                    test_start=self.config.start_date,
                    test_end=self.config.end_date,
                    params=params,
                    train_metrics={},
                    test_metrics=metrics,
                    trade_count=trade_count,
                )
                result.folds.append(fold)

            result.best_params = best_params
            result.best_metrics = best_metrics
            result.status = "SUCCEEDED"
        else:
            result.status = "FAILED"

        result.finished_at = datetime.utcnow()
        self._result = result
        return result


class RandomSearchProtocol(ResearchProtocol):
    """Random search parameter optimization protocol."""

    def __init__(
        self,
        config: ProtocolConfig,
        param_space: dict[str, tuple[Any, Any]] | None = None,
    ) -> None:
        super().__init__(config)
        self.param_space = param_space or self._get_default_space()

    def _get_default_space(self) -> dict[str, tuple[Any, Any]]:
        strategy_family = self.config.strategy.family

        if strategy_family == "indian_2lynch":
            return {
                "breakout_threshold": (0.02, 0.06),
                "close_pos_threshold": (0.50, 0.90),
                "nr_percentile": (0.10, 0.30),
                "min_r2_l": (0.50, 0.90),
                "max_down_days_l": (3, 14),
            }
        elif strategy_family in ("threshold_breakout", "threshold_breakdown"):
            return {
                "breakout_threshold": (0.01, 0.10),
                "min_price": (5, 100),
                "min_value_traded_inr": (1_000_000, 10_000_000),
            }
        elif strategy_family == "episodic_pivot":
            return {
                "min_gap_pct": (0.02, 0.15),
                "min_consolidation_days": (3, 20),
            }

        return {}

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.RANDOM_SEARCH

    def _sample_params(
        self,
        param_space: dict[str, tuple[Any, Any]],
        seed: int,
    ) -> dict[str, Any]:
        """Sample random parameters from the search space."""
        import random

        random.seed(seed)
        params = {}

        for key, (low, high) in param_space.items():
            if isinstance(low, int) and isinstance(high, int):
                params[key] = random.randint(low, high)
            elif isinstance(low, float) and isinstance(high, float):
                params[key] = random.uniform(low, high)
            else:
                params[key] = random.choice([low, high])

        return params

    async def run(
        self,
        backtest_fn: Callable[..., Awaitable[dict[str, Any]]],
    ) -> ProtocolResult:
        result = self._create_result()

        logger.info(f"Running random search with {self.config.max_combinations} iterations")

        import random

        random.seed(self.config.random_seed)

        fold_results: list[tuple[dict[str, Any], dict[str, float], int]] = []

        for i in range(self.config.max_combinations):
            seed = self.config.random_seed + i
            params = self._sample_params(self.param_space, seed)

            logger.info(f"  [{i + 1}/{self.config.max_combinations}] Testing params: {params}")

            try:
                metrics = await backtest_fn(
                    params=params,
                    start_date=self.config.start_date,
                    end_date=self.config.end_date,
                )

                trade_count = metrics.get("trade_count", 0)
                fold_results.append((params, metrics, trade_count))
                result.successful_runs += 1

            except Exception as e:
                logger.error(f"    Failed: {e}")
                result.failed_runs += 1

            result.total_runs += 1

        if fold_results:
            fold_results.sort(
                key=lambda x: x[1].get(self.config.objective_metric, -999),
                reverse=True,
            )

            best_params, best_metrics, _best_trade_count = fold_results[0]

            for i, (params, metrics, trade_count) in enumerate(fold_results):
                fold = FoldResult(
                    fold_index=i,
                    train_start=self.config.start_date,
                    train_end=self.config.end_date,
                    test_start=self.config.start_date,
                    test_end=self.config.end_date,
                    params=params,
                    train_metrics={},
                    test_metrics=metrics,
                    trade_count=trade_count,
                )
                result.folds.append(fold)

            result.best_params = best_params
            result.best_metrics = best_metrics
            result.status = "SUCCEEDED"
        else:
            result.status = "FAILED"

        result.finished_at = datetime.utcnow()
        self._result = result
        return result


class WalkForwardProtocol(ResearchProtocol):
    """Base class for walk-forward protocols."""

    def __init__(
        self,
        config: ProtocolConfig,
        train_days: int = 252,
        test_days: int = 63,
        roll_interval_days: int = 63,
        param_grid: dict[str, list[Any]] | None = None,
    ) -> None:
        super().__init__(config)
        self.train_days = train_days
        self.test_days = test_days
        self.roll_interval_days = roll_interval_days
        self.param_grid = param_grid

    @abstractmethod
    def _get_train_end(
        self,
        fold_index: int,
        test_start: date,
    ) -> date:
        """Calculate train end date based on protocol type."""
        pass

    def _generate_windows(
        self,
    ) -> list[tuple[date, date, date, date]]:
        """Generate all train/test windows."""
        from datetime import timedelta

        windows = []
        current_test_start = self.config.start_date + timedelta(days=self.train_days)

        while current_test_start + timedelta(days=self.test_days) <= self.config.end_date:
            train_end = self._get_train_end(0, current_test_start)
            test_end = current_test_start + timedelta(days=self.test_days - 1)

            if train_end <= self.config.start_date:
                break

            windows.append(
                (
                    self.config.start_date,
                    train_end,
                    current_test_start,
                    test_end,
                )
            )

            current_test_start = current_test_start + timedelta(days=self.roll_interval_days)

        return windows


class AnchoredWalkForwardProtocol(WalkForwardProtocol):
    """Anchored walk-forward protocol.

    In anchored walk-forward, the training window always starts from the
    same anchor point (config.start_date) and expands forward.
    """

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.WALK_FORWARD_ANCHORED

    def _get_train_end(
        self,
        fold_index: int,
        test_start: date,
    ) -> date:
        from datetime import timedelta

        return test_start - timedelta(days=1)

    async def run(
        self,
        backtest_fn: Callable[..., Awaitable[dict[str, Any]]],
    ) -> ProtocolResult:
        result = self._create_result()

        windows = self._generate_windows()
        logger.info(f"Running anchored walk-forward with {len(windows)} folds")

        best_overall_params = {}
        best_overall_score = float("-inf")

        import itertools

        param_combinations = (
            [{}]
            if not self.param_grid
            else [
                dict(zip(self.param_grid.keys(), combo, strict=True))
                for combo in itertools.product(
                    *[self.param_grid[k] for k in self.param_grid.keys()]
                )
            ]
        )

        if len(param_combinations) > 20:
            logger.warning("Limiting param combinations to 20 for walk-forward")
            param_combinations = param_combinations[:20]

        for fold_idx, (train_start, train_end, test_start, test_end) in enumerate(windows):
            logger.info(f"\n=== Fold {fold_idx + 1}/{len(windows)} ===")
            logger.info(f"  Train: {train_start} to {train_end}")
            logger.info(f"  Test: {test_start} to {test_end}")

            fold_best_params = {}
            fold_best_score = float("-inf")

            for params in param_combinations:
                try:
                    _train_metrics = await backtest_fn(
                        params=params,
                        start_date=train_start,
                        end_date=train_end,
                    )

                    test_metrics = await backtest_fn(
                        params=params,
                        start_date=test_start,
                        end_date=test_end,
                    )

                    test_score = test_metrics.get(self.config.objective_metric, 0)

                    if test_score > fold_best_score:
                        fold_best_score = test_score
                        fold_best_params = params

                    result.successful_runs += 1

                except Exception as e:
                    logger.error(f"    Failed: {e}")
                    result.failed_runs += 1

                result.total_runs += 1

            fold = FoldResult(
                fold_index=fold_idx,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                params=fold_best_params,
                train_metrics={},
                test_metrics={},
                trade_count=0,
            )
            result.folds.append(fold)

            if fold_best_score > best_overall_score:
                best_overall_score = fold_best_score
                best_overall_params = fold_best_params

        result.best_params = best_overall_params
        result.status = "SUCCEEDED" if result.successful_runs > 0 else "FAILED"
        result.finished_at = datetime.utcnow()
        self._result = result
        return result


class RollingWalkForwardProtocol(WalkForwardProtocol):
    """Rolling walk-forward protocol.

    In rolling walk-forward, both train and test windows slide forward
    by the roll interval, maintaining constant window sizes.
    """

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.WALK_FORWARD_ROLLING

    def _get_train_end(
        self,
        fold_index: int,
        test_start: date,
    ) -> date:
        from datetime import timedelta

        return test_start - timedelta(days=1) - timedelta(days=self.train_days)

    async def run(
        self,
        backtest_fn: Callable[..., Awaitable[dict[str, Any]]],
    ) -> ProtocolResult:
        result = self._create_result()

        windows = self._generate_windows()
        logger.info(f"Running rolling walk-forward with {len(windows)} folds")

        best_overall_params = {}
        best_overall_score = float("-inf")

        import itertools

        param_combinations = (
            [{}]
            if not self.param_grid
            else [
                dict(zip(self.param_grid.keys(), combo, strict=True))
                for combo in itertools.product(
                    *[self.param_grid[k] for k in self.param_grid.keys()]
                )
            ]
        )

        if len(param_combinations) > 20:
            logger.warning("Limiting param combinations to 20 for walk-forward")
            param_combinations = param_combinations[:20]

        for fold_idx, (train_start, train_end, test_start, test_end) in enumerate(windows):
            logger.info(f"\n=== Fold {fold_idx + 1}/{len(windows)} ===")
            logger.info(f"  Train: {train_start} to {train_end}")
            logger.info(f"  Test: {test_start} to {test_end}")

            fold_best_params = {}
            fold_best_score = float("-inf")

            for params in param_combinations:
                try:
                    _train_metrics = await backtest_fn(
                        params=params,
                        start_date=train_start,
                        end_date=train_end,
                    )

                    test_metrics = await backtest_fn(
                        params=params,
                        start_date=test_start,
                        end_date=test_end,
                    )

                    test_score = test_metrics.get(self.config.objective_metric, 0)

                    if test_score > fold_best_score:
                        fold_best_score = test_score
                        fold_best_params = params

                    result.successful_runs += 1

                except Exception as e:
                    logger.error(f"    Failed: {e}")
                    result.failed_runs += 1

                result.total_runs += 1

            fold = FoldResult(
                fold_index=fold_idx,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                params=fold_best_params,
                train_metrics={},
                test_metrics={},
                trade_count=0,
            )
            result.folds.append(fold)

            if fold_best_score > best_overall_score:
                best_overall_score = fold_best_score
                best_overall_params = fold_best_params

        result.best_params = best_overall_params
        result.status = "SUCCEEDED" if result.successful_runs > 0 else "FAILED"
        result.finished_at = datetime.utcnow()
        self._result = result
        return result


class SensitivityOATProtocol(ResearchProtocol):
    """One-at-a-time sensitivity analysis protocol."""

    def __init__(
        self,
        config: ProtocolConfig,
        param_ranges: dict[str, list[Any]] | None = None,
    ) -> None:
        super().__init__(config)
        self.param_ranges = param_ranges or self._get_default_ranges()

    def _get_default_ranges(self) -> dict[str, list[Any]]:
        strategy_family = self.config.strategy.family

        if strategy_family == "indian_2lynch":
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

    @property
    def protocol_type(self) -> ProtocolType:
        return ProtocolType.SENSITIVITY_OAT

    async def run(
        self,
        backtest_fn: Callable[..., Awaitable[dict[str, Any]]],
    ) -> ProtocolResult:
        result = self._create_result()

        base_params = self.config.strategy.get_default_params()

        logger.info("Running one-at-a-time sensitivity analysis...")

        base_metrics = await backtest_fn(
            params=base_params,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
        )

        logger.info(f"Base metrics: {base_metrics}")

        sensitivity_results: list[dict[str, Any]] = []

        for param_name, param_values in self.param_ranges.items():
            logger.info(f"\nAnalyzing parameter: {param_name}")

            base_value = base_params.get(param_name)
            metric_values = []

            for value in param_values:
                test_params = base_params.copy()
                test_params[param_name] = value

                try:
                    metrics = await backtest_fn(
                        params=test_params,
                        start_date=self.config.start_date,
                        end_date=self.config.end_date,
                    )

                    metric_value = metrics.get(self.config.objective_metric, 0.0)
                    metric_values.append(metric_value)

                    result.successful_runs += 1

                except Exception as e:
                    logger.error(f"  Failed: {e}")
                    metric_values.append(0.0)
                    result.failed_runs += 1

                result.total_runs += 1

            valid_metrics = [m for m in metric_values if m != 0.0]
            if valid_metrics:
                metric_range = max(valid_metrics) - min(valid_metrics)
                optimal_idx = metric_values.index(max(metric_values))
                optimal_value = param_values[optimal_idx]
            else:
                metric_range = 0.0
                optimal_value = base_value

            sensitivity_results.append(
                {
                    "parameter": param_name,
                    "base_value": base_value,
                    "optimal_value": optimal_value,
                    "tested_values": param_values,
                    "metric_values": metric_values,
                    "metric_range": metric_range,
                }
            )

        sensitivity_results.sort(key=lambda x: x["metric_range"], reverse=True)

        for i, sr in enumerate(sensitivity_results):
            fold = FoldResult(
                fold_index=i,
                train_start=self.config.start_date,
                train_end=self.config.end_date,
                test_start=self.config.start_date,
                test_end=self.config.end_date,
                params={"parameter": sr["parameter"], "base_value": sr["base_value"]},
                train_metrics={},
                test_metrics={
                    "sensitivity_range": sr["metric_range"],
                    "optimal_value": str(sr["optimal_value"]),
                },
                trade_count=0,
            )
            result.folds.append(fold)

        result.best_params = {sr["parameter"]: sr["optimal_value"] for sr in sensitivity_results}
        result.best_metrics = {
            "sensitivity_analysis": True,
            "parameters_tested": len(sensitivity_results),
        }
        result.status = "SUCCEEDED" if result.successful_runs > 0 else "FAILED"
        result.finished_at = datetime.utcnow()
        self._result = result
        return result


def create_protocol(
    protocol_type: ProtocolType,
    config: ProtocolConfig,
    **kwargs: Any,
) -> ResearchProtocol:
    """Factory function to create protocol instances.

    Args:
        protocol_type: Type of protocol to create
        config: Protocol configuration
        **kwargs: Additional protocol-specific arguments:
            - param_grid: dict for grid search
            - param_space: dict for random search
            - param_ranges: dict for sensitivity analysis
            - train_days: int for walk-forward
            - test_days: int for walk-forward
            - roll_interval_days: int for walk-forward

    Returns:
        ResearchProtocol instance

    Examples:
        >>> config = ProtocolConfig(
        ...     strategy_name="indian2lynch",
        ...     start_date=date(2020, 1, 1),
        ...     end_date=date(2024, 12, 31),
        ... )
        >>> protocol = create_protocol(ProtocolType.GRID_SEARCH, config)
    """
    protocols = {
        ProtocolType.SINGLE_RUN: SingleRunProtocol,
        ProtocolType.GRID_SEARCH: GridSearchProtocol,
        ProtocolType.RANDOM_SEARCH: RandomSearchProtocol,
        ProtocolType.WALK_FORWARD_ANCHORED: AnchoredWalkForwardProtocol,
        ProtocolType.WALK_FORWARD_ROLLING: RollingWalkForwardProtocol,
        ProtocolType.SENSITIVITY_OAT: SensitivityOATProtocol,
    }

    protocol_class = protocols.get(protocol_type)
    if not protocol_class:
        raise ValueError(f"Unknown protocol type: {protocol_type}")

    return protocol_class(config, **kwargs)
