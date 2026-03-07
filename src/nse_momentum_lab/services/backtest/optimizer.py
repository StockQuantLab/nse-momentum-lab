"""Parameter optimization worker for strategy parameter tuning.

This module provides automated parameter optimization for any registered strategy
using the unified research protocol framework.

Usage:
    # Run grid search optimization
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.optimizer \
        --start-date 2024-01-01 --end-date 2024-12-31 --mode grid --strategy indian2lynch

    # Run walk-forward optimization
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.optimizer \
        --start-date 2024-01-01 --end-date 2024-12-31 --mode walkforward --strategy indian2lynch

    # Run anchored walk-forward
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.optimizer \
        --start-date 2020-01-01 --end-date 2024-12-31 --mode walkforward-anchored --strategy thresholdbreakout

    # Run rolling walk-forward
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.optimizer \
        --start-date 2020-01-01 --end-date 2024-12-31 --mode walkforward-rolling --strategy thresholdbreakout

    # Run random search
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.optimizer \
        --start-date 2024-01-01 --end-date 2024-12-31 --mode random --strategy indian2lynch

    # Run sensitivity analysis
    doppler run -- uv run python -m nse_momentum_lab.services.backtest.optimizer \
        --start-date 2024-01-01 --end-date 2024-12-31 --mode sensitivity --strategy indian2lynch
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import (
    ExpMetric,
    ExpRun,
    MdOhlcvAdj,
    RefSymbol,
)
from nse_momentum_lab.services.backtest.protocols import (
    ProtocolConfig,
    ProtocolResult,
    ProtocolType,
    create_protocol,
)
from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy
from nse_momentum_lab.services.scan.features import FeatureEngine, PriceData
from nse_momentum_lab.services.scan.rules import ScanConfig, ScanRuleEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ParameterGrid:
    breakout_threshold: list[float] = field(default_factory=lambda: [0.03, 0.04, 0.05])
    close_pos_threshold: list[float] = field(default_factory=lambda: [0.60, 0.70, 0.80])
    nr_percentile: list[float] = field(default_factory=lambda: [0.15, 0.20, 0.25])
    min_r2_l: list[float] = field(default_factory=lambda: [0.60, 0.70, 0.80])
    max_down_days_l: list[int] = field(default_factory=lambda: [5, 7, 10])
    atr_compress_ratio: list[float] = field(default_factory=lambda: [0.70, 0.80, 0.90])
    range_percentile: list[float] = field(default_factory=lambda: [0.15, 0.20, 0.25])
    vol_dryup_ratio: list[float] = field(default_factory=lambda: [0.70, 0.80, 0.90])
    max_prior_breakouts: list[int] = field(default_factory=lambda: [1, 2, 3])

    def to_config(self, params: dict[str, Any]) -> ScanConfig:
        return ScanConfig(
            breakout_threshold=params.get("breakout_threshold", 0.04),
            close_pos_threshold=params.get("close_pos_threshold", 0.70),
            nr_percentile=params.get("nr_percentile", 0.20),
            min_r2_l=params.get("min_r2_l", 0.70),
            max_down_days_l=params.get("max_down_days_l", 7),
            atr_compress_ratio=params.get("atr_compress_ratio", 0.80),
            range_percentile=params.get("range_percentile", 0.20),
            vol_dryup_ratio=params.get("vol_dryup_ratio", 0.80),
            max_prior_breakouts=params.get("max_prior_breakouts", 2),
        )

    def generate_combinations(self) -> list[dict[str, Any]]:
        import itertools

        keys = [k for k in self.__dataclass_fields__.keys() if getattr(self, k)]
        values = [getattr(self, k) for k in keys]
        combinations = list(itertools.product(*values))
        return [dict(zip(keys, combo, strict=True)) for combo in combinations]


@dataclass
class OptimizationResult:
    params: dict[str, Any]
    metrics: dict[str, float]
    rank: int
    total_runs: int


class ParameterOptimizer:
    """Strategy-agnostic parameter optimizer using the unified protocol framework."""

    def __init__(
        self,
        start_date: date,
        end_date: date,
        objective: str = "sharpe_ratio",
        strategy_name: str = "indian2lynch",
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.objective = objective
        self.strategy_name = strategy_name
        self._strategy = resolve_strategy(strategy_name)
        self._sessionmaker = get_sessionmaker()
        self._feature_engine = FeatureEngine()
        logger.info(f"Initialized optimizer for strategy: {strategy_name}")

    async def run_grid_search(
        self,
        grid: ParameterGrid | None = None,
        max_combinations: int = 50,
    ) -> list[OptimizationResult]:
        grid = grid or ParameterGrid()
        combinations = grid.generate_combinations()

        if len(combinations) > max_combinations:
            logger.warning(
                f"Grid has {len(combinations)} combinations, limiting to {max_combinations}"
            )
            combinations = combinations[:max_combinations]

        logger.info(f"Running grid search with {len(combinations)} parameter combinations")

        results: list[OptimizationResult] = []
        for i, params in enumerate(combinations):
            logger.info(f"  [{i + 1}/{len(combinations)}] Testing params: {params}")
            try:
                metrics = await self._evaluate_params(params)
                results.append(
                    OptimizationResult(
                        params=params,
                        metrics=metrics,
                        rank=0,
                        total_runs=len(combinations),
                    )
                )
            except Exception as e:
                logger.error(f"    Failed: {e}")
                results.append(
                    OptimizationResult(
                        params=params,
                        metrics={"error": str(e)},
                        rank=0,
                        total_runs=len(combinations),
                    )
                )

        results.sort(key=lambda x: x.metrics.get(self.objective, -999), reverse=True)
        for i, r in enumerate(results):
            r.rank = i + 1

        return results

    async def run_walk_forward(
        self,
        train_days: int = 252,
        test_days: int = 63,
        grid: ParameterGrid | None = None,
    ) -> list[dict[str, Any]]:
        grid = grid or ParameterGrid()
        combinations = grid.generate_combinations()

        if len(combinations) > 20:
            logger.warning("Limiting grid to 20 combinations for walk-forward")
            combinations = combinations[:20]

        walk_forward_results: list[dict[str, Any]] = []

        current_date = self.start_date + timedelta(days=train_days)
        fold = 0

        while current_date + timedelta(days=test_days) <= self.end_date:
            train_end = current_date - timedelta(days=1)
            test_end = current_date + timedelta(days=test_days - 1)

            logger.info(f"\n=== Walk-Forward Fold {fold + 1} ===")
            logger.info(f"  Train: {self.start_date} to {train_end}")
            logger.info(f"  Test: {current_date} to {test_end}")

            fold_results: list[OptimizationResult] = []
            for params in combinations:
                try:
                    metrics = await self._evaluate_params(
                        params,
                        train_start=self.start_date,
                        train_end=train_end,
                        test_start=current_date,
                        test_end=test_end,
                    )
                    fold_results.append(
                        OptimizationResult(
                            params=params,
                            metrics=metrics,
                            rank=0,
                            total_runs=len(combinations),
                        )
                    )
                except Exception as e:
                    logger.error(f"    Failed: {e}")

            if fold_results:
                fold_results.sort(key=lambda x: x.metrics.get(self.objective, -999), reverse=True)
                best = fold_results[0]

                walk_forward_results.append(
                    {
                        "fold": fold,
                        "train_start": self.start_date.isoformat(),
                        "train_end": train_end.isoformat(),
                        "test_start": current_date.isoformat(),
                        "test_end": test_end.isoformat(),
                        "best_params": best.params,
                        "best_metrics": best.metrics,
                    }
                )

                logger.info(f"  Best: sharpe={best.metrics.get('sharpe_ratio', 0):.2f}")

            current_date += timedelta(days=test_days)
            fold += 1

        return walk_forward_results

    async def run_protocol(
        self,
        protocol_type: ProtocolType,
        param_grid: dict[str, list[Any]] | None = None,
        param_space: dict[str, tuple[Any, Any]] | None = None,
        param_ranges: dict[str, list[Any]] | None = None,
        train_days: int = 252,
        test_days: int = 63,
        roll_interval_days: int = 63,
        max_combinations: int = 100,
    ) -> ProtocolResult:
        """Run a research protocol using the unified framework.

        Args:
            protocol_type: Type of protocol to run
            param_grid: Parameter grid for grid search
            param_space: Parameter space for random search
            param_ranges: Parameter ranges for sensitivity analysis
            train_days: Training window days for walk-forward
            test_days: Test window days for walk-forward
            roll_interval_days: Roll interval for walk-forward
            max_combinations: Maximum combinations to test

        Returns:
            ProtocolResult with all fold results
        """
        config = ProtocolConfig(
            strategy_name=self.strategy_name,
            start_date=self.start_date,
            end_date=self.end_date,
            objective_metric=self.objective,
            max_combinations=max_combinations,
        )

        protocol_kwargs: dict[str, Any] = {}

        if protocol_type == ProtocolType.GRID_SEARCH:
            protocol_kwargs["param_grid"] = param_grid
        elif protocol_type == ProtocolType.RANDOM_SEARCH:
            protocol_kwargs["param_space"] = param_space
        elif protocol_type == ProtocolType.SENSITIVITY_OAT:
            protocol_kwargs["param_ranges"] = param_ranges
        elif protocol_type in (
            ProtocolType.WALK_FORWARD_ANCHORED,
            ProtocolType.WALK_FORWARD_ROLLING,
        ):
            protocol_kwargs["train_days"] = train_days
            protocol_kwargs["test_days"] = test_days
            protocol_kwargs["roll_interval_days"] = roll_interval_days
            protocol_kwargs["param_grid"] = param_grid

        protocol = create_protocol(protocol_type, config, **protocol_kwargs)

        async def backtest_fn(
            params: dict[str, Any],
            start_date: date,
            end_date: date,
        ) -> dict[str, Any]:
            return await self._evaluate_params(params, start_date, end_date)

        return await protocol.run(backtest_fn)

    async def _evaluate_params(
        self,
        params: dict[str, Any],
        train_start: date | None = None,
        train_end: date | None = None,
        test_start: date | None = None,
        test_end: date | None = None,
    ) -> dict[str, float]:
        train_start = train_start or self.start_date
        train_end = train_end or (self.end_date - timedelta(days=63))
        test_start = test_start or (train_end + timedelta(days=1))
        test_end = test_end or self.end_date

        config = ParameterGrid().to_config(params)

        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            universe = await self._load_universe(session)
            all_candidates: list[tuple] = []

            for symbol_id, symbol in universe:
                candidates = await self._scan_symbol(session, symbol_id, symbol, config, test_end)
                all_candidates.extend(candidates)

            if not all_candidates:
                return {"sharpe_ratio": 0, "total_return": 0, "win_rate": 0, "trades": 0}

            return await self._backtest_candidates(session, all_candidates, test_start, test_end)

    async def _load_universe(self, session) -> list[tuple[int, str]]:
        result = await session.execute(
            select(RefSymbol.symbol_id, RefSymbol.symbol).where(RefSymbol.status == "ACTIVE")
        )
        return list(result.fetchall())

    async def _scan_symbol(
        self,
        session,
        symbol_id: int,
        symbol: str,
        config: ScanConfig,
        asof_date: date,
    ) -> list[tuple]:
        result = await session.execute(
            select(MdOhlcvAdj)
            .where(
                MdOhlcvAdj.symbol_id == symbol_id,
                MdOhlcvAdj.trading_date <= asof_date,
            )
            .order_by(MdOhlcvAdj.trading_date.desc())
            .limit(300)
        )
        rows = list(result.scalars().all())
        rows.reverse()

        if len(rows) < 65:
            return []

        prices = [
            PriceData(
                trading_date=r.trading_date,
                open=float(r.open_adj),
                high=float(r.high_adj),
                low=float(r.low_adj),
                close=float(r.close_adj),
                volume=r.volume,
                value_traded=r.value_traded,
            )
            for r in rows
        ]

        features = self._feature_engine.compute_all(symbol_id, prices)
        rule_engine = ScanRuleEngine(config)
        candidates = rule_engine.run_scan(symbol_id, symbol, features, asof_date)

        return [(symbol_id, symbol, c.trading_date, c.passed) for c in candidates if c.passed]

    async def _backtest_candidates(
        self,
        session,
        candidates: list[tuple],
        test_start: date,
        test_end: date,
    ) -> dict[str, float]:
        if not candidates:
            return {"sharpe_ratio": 0, "total_return": 0, "win_rate": 0, "trades": 0}

        symbol_ids = {c[0] for c in candidates}

        price_result = await session.execute(
            select(MdOhlcvAdj)
            .where(
                MdOhlcvAdj.symbol_id.in_(symbol_ids),
                MdOhlcvAdj.trading_date >= test_start,
                MdOhlcvAdj.trading_date <= test_end,
            )
            .order_by(MdOhlcvAdj.symbol_id, MdOhlcvAdj.trading_date)
        )
        price_rows = price_result.scalars().all()

        price_data: dict[int, dict[date, dict[str, float]]] = {}
        for row in price_rows:
            if row.symbol_id not in price_data:
                price_data[row.symbol_id] = {}
            price_data[row.symbol_id][row.trading_date] = {
                "open": float(row.open_adj) if row.open_adj else 0,
                "high": float(row.high_adj) if row.high_adj else 0,
                "low": float(row.low_adj) if row.low_adj else 0,
                "close": float(row.close_adj) if row.close_adj else 0,
                "close_adj": float(row.close_adj) if row.close_adj else 0,
                "open_adj": float(row.open_adj) if row.open_adj else 0,
            }

        trades = []
        for symbol_id, _symbol, signal_date, passed in candidates:
            if not passed or symbol_id not in price_data:
                continue

            if signal_date not in price_data[symbol_id]:
                continue

            entry_price = price_data[symbol_id][signal_date].get("close_adj", 0)
            if entry_price <= 0:
                continue

            exit_date = signal_date + timedelta(days=20)
            exit_price = 0
            for d in sorted(price_data[symbol_id].keys()):
                if d > exit_date:
                    break
                if d > signal_date:
                    exit_price = price_data[symbol_id][d].get("close_adj", 0)

            if exit_price > 0:
                ret = (exit_price - entry_price) / entry_price
                trades.append(ret)

        if not trades:
            return {"sharpe_ratio": 0, "total_return": 0, "win_rate": 0, "trades": 0}

        total_return = sum(trades)
        wins = sum(1 for t in trades if t > 0)
        win_rate = wins / len(trades) if trades else 0

        returns_arr = trades
        mean_return = sum(returns_arr) / len(returns_arr)
        variance = sum((r - mean_return) ** 2 for r in returns_arr) / len(returns_arr)
        std_dev = variance**0.5
        sharpe = (mean_return / std_dev * (252**0.5)) if std_dev > 0 else 0

        return {
            "sharpe_ratio": sharpe,
            "total_return": total_return,
            "win_rate": win_rate,
            "trades": len(trades),
        }

    async def store_results(
        self,
        results: list[OptimizationResult],
        mode: str = "grid",
    ) -> int:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            exp_run = ExpRun(
                exp_hash=f"opt_{mode}_{self.strategy_name}_{self.start_date}_{self.end_date}",
                strategy_name=f"{self.strategy_name.upper()}_OPT_{mode.upper()}",
                strategy_hash=self._strategy.version,
                dataset_hash=f"{self.start_date}:{self.end_date}",
                params_json={
                    "optimization_mode": mode,
                    "objective": self.objective,
                    "start_date": self.start_date.isoformat(),
                    "end_date": self.end_date.isoformat(),
                    "strategy": self.strategy_name,
                },
                code_sha="",
                status="SUCCEEDED",
            )
            session.add(exp_run)
            await session.flush()

            for r in results[:10]:
                for metric_name, metric_value in r.metrics.items():
                    if isinstance(metric_value, (int, float)):
                        session.add(
                            ExpMetric(
                                exp_run_id=exp_run.exp_run_id,
                                metric_name=f"{metric_name}_rank{r.rank}",
                                metric_value=metric_value,
                            )
                        )

                params_str = json.dumps(r.params)
                session.add(
                    ExpMetric(
                        exp_run_id=exp_run.exp_run_id,
                        metric_name=f"params_rank{r.rank}",
                        metric_value=params_str,
                    )
                )

            await session.commit()
            return exp_run.exp_run_id

    async def store_protocol_result(
        self,
        result: ProtocolResult,
    ) -> int:
        """Store protocol result to database."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            exp_run = ExpRun(
                exp_hash=result.protocol_hash,
                strategy_name=f"{result.strategy_name.upper()}_{result.protocol_type.value.upper()}",
                strategy_hash=result.strategy_version,
                dataset_hash=f"{result.start_date}:{result.end_date}",
                params_json={
                    "protocol_type": result.protocol_type.value,
                    "objective": result.objective_metric,
                    "start_date": result.start_date.isoformat(),
                    "end_date": result.end_date.isoformat(),
                    "strategy": result.strategy_name,
                    "total_runs": result.total_runs,
                    "successful_runs": result.successful_runs,
                    "failed_runs": result.failed_runs,
                    "best_params": result.best_params,
                },
                code_sha="",
                status=result.status,
            )
            session.add(exp_run)
            await session.flush()

            for metric_name, metric_value in result.best_metrics.items():
                if isinstance(metric_value, (int, float)):
                    session.add(
                        ExpMetric(
                            exp_run_id=exp_run.exp_run_id,
                            metric_name=f"best_{metric_name}",
                            metric_value=metric_value,
                        )
                    )

            for fold in result.folds:
                for metric_name, metric_value in fold.test_metrics.items():
                    if isinstance(metric_value, (int, float)):
                        session.add(
                            ExpMetric(
                                exp_run_id=exp_run.exp_run_id,
                                metric_name=f"fold{fold.fold_index}_{metric_name}",
                                metric_value=metric_value,
                            )
                        )

            await session.commit()
            return exp_run.exp_run_id


def print_results(results: list[OptimizationResult]) -> None:
    print(f"\n{'=' * 80}")
    print(f"OPTIMIZATION RESULTS (Objective: {results[0].metrics.get('sharpe_ratio', 'N/A')})")
    print(f"{'=' * 80}")
    print(f"{'Rank':<6} {'Sharpe':<10} {'Return':<12} {'Win%':<10} {'Trades':<8} Parameters")
    print("-" * 80)

    for r in results[:10]:
        sharpe = r.metrics.get("sharpe_ratio", 0)
        ret = r.metrics.get("total_return", 0)
        win = r.metrics.get("win_rate", 0)
        trades = r.metrics.get("trades", 0)

        params_str = json.dumps(r.params)
        if len(params_str) > 40:
            params_str = params_str[:40] + "..."

        print(
            f"{r.rank:<6} {sharpe:<10.3f} {ret * 100:<11.2f}% {win * 100:<9.1f}% {trades:<8} {params_str}"
        )

    print("=" * 80)


def print_protocol_result(result: ProtocolResult) -> None:
    """Print protocol result in a readable format."""
    print(f"\n{'=' * 80}")
    print(f"PROTOCOL RESULTS: {result.protocol_type.value}")
    print(f"{'=' * 80}")
    print(f"Strategy: {result.strategy_name} (v{result.strategy_version})")
    print(f"Period: {result.start_date} to {result.end_date}")
    print(f"Objective: {result.objective_metric}")
    print(f"Status: {result.status}")
    print(f"Total runs: {result.total_runs}")
    print(f"Successful: {result.successful_runs}")
    print(f"Failed: {result.failed_runs}")
    print(f"\nBest parameters: {json.dumps(result.best_params, indent=2)}")
    print(f"Best metrics: {json.dumps(result.best_metrics, indent=2)}")
    print(f"\nFolds: {len(result.folds)}")
    for fold in result.folds:
        print(
            f"  Fold {fold.fold_index}: {fold.test_start} to {fold.test_end}, "
            f"trades={fold.trade_count}, status={fold.status}"
        )
    print("=" * 80)


async def main_async(args):
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    optimizer = ParameterOptimizer(
        start_date=start_date,
        end_date=end_date,
        objective=args.objective,
        strategy_name=args.strategy,
    )

    if args.mode == "grid":
        results = await optimizer.run_grid_search()
        if results:
            print_results(results)
            await optimizer.store_results(results, mode="grid")

    elif args.mode == "walkforward":
        results = await optimizer.run_walk_forward(
            train_days=args.train_days,
            test_days=args.test_days,
        )
        print(f"\nWalk-Forward Results: {len(results)} folds completed")
        for r in results:
            print(f"  Fold {r['fold']}: {r['best_metrics']}")

    elif args.mode == "walkforward-anchored":
        result = await optimizer.run_protocol(
            ProtocolType.WALK_FORWARD_ANCHORED,
            train_days=args.train_days,
            test_days=args.test_days,
            roll_interval_days=args.roll_interval,
        )
        print_protocol_result(result)
        await optimizer.store_protocol_result(result)

    elif args.mode == "walkforward-rolling":
        result = await optimizer.run_protocol(
            ProtocolType.WALK_FORWARD_ROLLING,
            train_days=args.train_days,
            test_days=args.test_days,
            roll_interval_days=args.roll_interval,
        )
        print_protocol_result(result)
        await optimizer.store_protocol_result(result)

    elif args.mode == "random":
        result = await optimizer.run_protocol(
            ProtocolType.RANDOM_SEARCH,
            max_combinations=args.max_combinations,
        )
        print_protocol_result(result)
        await optimizer.store_protocol_result(result)

    elif args.mode == "sensitivity":
        result = await optimizer.run_protocol(
            ProtocolType.SENSITIVITY_OAT,
        )
        print_protocol_result(result)
        await optimizer.store_protocol_result(result)

    else:
        print(f"Unknown mode: {args.mode}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Strategy Parameter Optimizer")
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
        help="Strategy name to optimize",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="grid",
        choices=[
            "grid",
            "walkforward",
            "walkforward-anchored",
            "walkforward-rolling",
            "random",
            "sensitivity",
        ],
        help="Optimization mode",
    )
    parser.add_argument(
        "--objective",
        type=str,
        default="sharpe_ratio",
        help="Optimization objective metric",
    )
    parser.add_argument(
        "--train-days",
        type=int,
        default=252,
        help="Walk-forward train period in days",
    )
    parser.add_argument(
        "--test-days",
        type=int,
        default=63,
        help="Walk-forward test period in days",
    )
    parser.add_argument(
        "--roll-interval",
        type=int,
        default=63,
        help="Walk-forward roll interval in days",
    )
    parser.add_argument(
        "--max-combinations",
        type=int,
        default=100,
        help="Maximum combinations for grid/random search",
    )
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
