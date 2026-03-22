"""Strategy-agnostic walk-forward framework.

This module provides walk-forward analysis capabilities that work with
any strategy registered in the strategy registry.

Supports both:
- Anchored walk-forward: training window starts from fixed anchor point
- Rolling walk-forward: both train and test windows slide forward
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from nse_momentum_lab.services.backtest.signal_models import BacktestSignal
from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    VectorBTConfig,
    VectorBTEngine,
    VectorBTResult,
    run_vectorbt_backtest,
)

logger = logging.getLogger(__name__)


@dataclass
class WalkForwardWindow:
    train_start: date
    train_end: date
    test_start: date
    test_end: date


@dataclass
class WalkForwardResult:
    window: WalkForwardWindow
    train_result: VectorBTResult | None
    test_result: VectorBTResult | None
    params_used: dict


class WalkForwardFramework:
    """Strategy-agnostic walk-forward framework.

    Supports anchored and rolling walk-forward analysis.

    Note: the window generators operate on calendar dates, not exchange-trading
    calendars. On a trading-day dataset this is usually close enough for research
    validation, but the actual number of sessions in a fold can vary with weekends
    and holidays.
    """

    DEFAULT_TRAIN_YEARS = 3
    DEFAULT_TEST_MONTHS = 6

    def __init__(
        self,
        train_years: int = DEFAULT_TRAIN_YEARS,
        test_months: int = DEFAULT_TEST_MONTHS,
        strategy_name: str | None = None,
    ) -> None:
        self.train_years = train_years
        self.test_months = test_months
        self._strategy = None

        if strategy_name:
            self._strategy = resolve_strategy(strategy_name)
            logger.info(f"Initialized walk-forward framework for strategy: {strategy_name}")

    @property
    def strategy_name(self) -> str:
        return self._strategy.name if self._strategy else "unknown"

    @staticmethod
    def _signal_date(signal: Any) -> date | None:
        if isinstance(signal, BacktestSignal):
            return signal.signal_date
        if isinstance(signal, tuple) and signal:
            candidate = signal[0]
            return candidate if isinstance(candidate, date) else None
        if isinstance(signal, dict):
            candidate = signal.get("signal_date") or signal.get("date")
            return candidate if isinstance(candidate, date) else None
        return getattr(signal, "signal_date", None)

    def generate_windows(
        self,
        data_start: date,
        data_end: date,
        roll_interval_days: int = 30,
    ) -> Iterator[WalkForwardWindow]:
        """Generate walk-forward windows.

        Args:
            data_start: Start date of the data
            data_end: End date of the data
            roll_interval_days: Days to roll forward between windows

        Yields:
            WalkForwardWindow for each fold
        """
        train_window_days = self.train_years * 365
        test_window_days = self.test_months * 30
        current_test_start = data_start + timedelta(days=train_window_days)

        while current_test_start + timedelta(days=test_window_days) <= data_end:
            test_end = current_test_start + timedelta(days=test_window_days) - timedelta(days=1)
            train_end = current_test_start - timedelta(days=1)

            if train_end <= data_start:
                break

            yield WalkForwardWindow(
                train_start=data_start,
                train_end=train_end,
                test_start=current_test_start,
                test_end=test_end,
            )

            current_test_start = current_test_start + timedelta(days=roll_interval_days)

    def generate_rolling_windows(
        self,
        data_start: date,
        data_end: date,
        train_days: int = 252,
        test_days: int = 63,
        roll_interval_days: int = 63,
    ) -> Iterator[WalkForwardWindow]:
        """Generate rolling walk-forward windows.

        Both train and test windows slide forward maintaining constant sizes.
        The day counts are calendar-day approximations applied to trading-day
        datasets.

        Args:
            data_start: Start date of the data
            data_end: End date of the data
            train_days: Training window size in days
            test_days: Test window size in days
            roll_interval_days: Days to roll forward between windows

        Yields:
            WalkForwardWindow for each fold
        """
        current_train_start = data_start

        while True:
            current_train_end = current_train_start + timedelta(days=train_days - 1)
            current_test_start = current_train_end + timedelta(days=1)
            current_test_end = current_test_start + timedelta(days=test_days - 1)

            if current_test_end > data_end:
                break

            if current_train_end >= current_test_start:
                current_train_start = current_train_start + timedelta(days=roll_interval_days)
                continue

            yield WalkForwardWindow(
                train_start=current_train_start,
                train_end=current_train_end,
                test_start=current_test_start,
                test_end=current_test_end,
            )

            current_train_start = current_train_start + timedelta(days=roll_interval_days)

    def run_walk_forward(
        self,
        strategy_name: str,
        signals: list,
        price_data: dict,
        value_traded_inr: dict,
        data_start: date,
        data_end: date,
        config: VectorBTConfig | None = None,
    ) -> list[WalkForwardResult]:
        """Run anchored walk-forward analysis.

        Research utility. The production CLI path uses DuckDBBacktestRunner
        for fold execution and only borrows window generation from this module.

        Args:
            strategy_name: Name of the strategy to use
            signals: List of signals from candidate generation
            price_data: Dictionary of price data by symbol
            value_traded_inr: Dictionary of value traded by symbol
            data_start: Start date for the analysis
            data_end: End date for the analysis
            config: Optional VectorBT configuration

        Returns:
            List of WalkForwardResult for each fold
        """
        results = []

        engine = VectorBTEngine(config) if config else None

        for window in self.generate_windows(data_start, data_end):
            train_signals = [
                s
                for s in signals
                if (signal_date := self._signal_date(s)) is not None
                and window.train_start <= signal_date <= window.train_end
            ]
            test_signals = [
                s
                for s in signals
                if (signal_date := self._signal_date(s)) is not None
                and window.test_start <= signal_date <= window.test_end
            ]

            if engine:
                train_result = engine.run_backtest(
                    strategy_name=f"{strategy_name}_train",
                    signals=train_signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                )
                test_result = engine.run_backtest(
                    strategy_name=f"{strategy_name}_test",
                    signals=test_signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                )
            else:
                train_result = run_vectorbt_backtest(
                    strategy_name=f"{strategy_name}_train",
                    signals=train_signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                )
                test_result = run_vectorbt_backtest(
                    strategy_name=f"{strategy_name}_test",
                    signals=test_signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                )

            results.append(
                WalkForwardResult(
                    window=window,
                    train_result=train_result,
                    test_result=test_result,
                    params_used={
                        "train_years": self.train_years,
                        "test_months": self.test_months,
                    },
                )
            )

            logger.info(
                f"Walk-forward window {window.train_start}->{window.train_end} "
                f"(train), {window.test_start}->{window.test_end} (test): "
                f"train trades={len(train_result.trades) if train_result else 0}, "
                f"test trades={len(test_result.trades) if test_result else 0}"
            )

        return results

    def run_rolling_walk_forward(
        self,
        strategy_name: str,
        signals: list,
        price_data: dict,
        value_traded_inr: dict,
        data_start: date,
        data_end: date,
        train_days: int = 252,
        test_days: int = 63,
        roll_interval_days: int = 63,
        config: VectorBTConfig | None = None,
    ) -> list[WalkForwardResult]:
        """Run rolling walk-forward analysis.

        Research utility. The production CLI path uses DuckDBBacktestRunner
        for fold execution and only borrows window generation from this module.

        Args:
            strategy_name: Name of the strategy to use
            signals: List of signals from candidate generation
            price_data: Dictionary of price data by symbol
            value_traded_inr: Dictionary of value traded by symbol
            data_start: Start date for the analysis
            data_end: End date for the analysis
            train_days: Training window size in days
            test_days: Test window size in days
            roll_interval_days: Days to roll forward between windows
            config: Optional VectorBT configuration

        Returns:
            List of WalkForwardResult for each fold
        """
        results = []

        engine = VectorBTEngine(config) if config else None

        for window in self.generate_rolling_windows(
            data_start,
            data_end,
            train_days,
            test_days,
            roll_interval_days,
        ):
            train_signals = [
                s
                for s in signals
                if (signal_date := self._signal_date(s)) is not None
                and window.train_start <= signal_date <= window.train_end
            ]
            test_signals = [
                s
                for s in signals
                if (signal_date := self._signal_date(s)) is not None
                and window.test_start <= signal_date <= window.test_end
            ]

            if engine:
                train_result = engine.run_backtest(
                    strategy_name=f"{strategy_name}_train",
                    signals=train_signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                )
                test_result = engine.run_backtest(
                    strategy_name=f"{strategy_name}_test",
                    signals=test_signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                )
            else:
                train_result = run_vectorbt_backtest(
                    strategy_name=f"{strategy_name}_train",
                    signals=train_signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                )
                test_result = run_vectorbt_backtest(
                    strategy_name=f"{strategy_name}_test",
                    signals=test_signals,
                    price_data=price_data,
                    value_traded_inr=value_traded_inr,
                )

            results.append(
                WalkForwardResult(
                    window=window,
                    train_result=train_result,
                    test_result=test_result,
                    params_used={
                        "train_days": train_days,
                        "test_days": test_days,
                        "roll_interval_days": roll_interval_days,
                    },
                )
            )

            logger.info(
                f"Rolling walk-forward window {window.train_start}->{window.train_end} "
                f"(train), {window.test_start}->{window.test_end} (test): "
                f"train trades={len(train_result.trades) if train_result else 0}, "
                f"test trades={len(test_result.trades) if test_result else 0}"
            )

        return results


def run_walk_forward(
    strategy_name: str,
    signals: list,
    price_data: dict,
    value_traded_inr: dict,
    config: VectorBTConfig | None = None,
    data_start: date | None = None,
    data_end: date | None = None,
) -> list[WalkForwardResult]:
    """Run anchored walk-forward analysis.

    Convenience function for running walk-forward analysis.

    Args:
        strategy_name: Name of the strategy to use
        signals: List of signals from candidate generation
        price_data: Dictionary of price data by symbol
        value_traded_inr: Dictionary of value traded by symbol
        config: Optional VectorBT configuration
        data_start: Start date for the analysis
        data_end: End date for the analysis

    Returns:
        List of WalkForwardResult for each fold
    """
    if data_start is None or data_end is None:
        raise ValueError("data_start and data_end must be provided")
    framework = WalkForwardFramework()
    return framework.run_walk_forward(
        strategy_name=strategy_name,
        signals=signals,
        price_data=price_data,
        value_traded_inr=value_traded_inr,
        config=config,
        data_start=data_start,
        data_end=data_end,
    )


def run_rolling_walk_forward(
    strategy_name: str,
    signals: list,
    price_data: dict,
    value_traded_inr: dict,
    train_days: int = 252,
    test_days: int = 63,
    roll_interval_days: int = 63,
    config: VectorBTConfig | None = None,
    data_start: date | None = None,
    data_end: date | None = None,
) -> list[WalkForwardResult]:
    """Run rolling walk-forward analysis.

    Convenience function for running rolling walk-forward analysis.

    Args:
        strategy_name: Name of the strategy to use
        signals: List of signals from candidate generation
        price_data: Dictionary of price data by symbol
        value_traded_inr: Dictionary of value traded by symbol
        train_days: Training window size in days
        test_days: Test window size in days
        roll_interval_days: Days to roll forward between windows
        config: Optional VectorBT configuration
        data_start: Start date for the analysis
        data_end: End date for the analysis

    Returns:
        List of WalkForwardResult for each fold
    """
    if data_start is None or data_end is None:
        raise ValueError("data_start and data_end must be provided")
    framework = WalkForwardFramework()
    return framework.run_rolling_walk_forward(
        strategy_name=strategy_name,
        signals=signals,
        price_data=price_data,
        value_traded_inr=value_traded_inr,
        config=config,
        data_start=data_start,
        data_end=data_end,
        train_days=train_days,
        test_days=test_days,
        roll_interval_days=roll_interval_days,
    )
