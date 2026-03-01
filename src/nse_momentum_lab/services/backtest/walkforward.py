from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta

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
    DEFAULT_TRAIN_YEARS = 3
    DEFAULT_TEST_MONTHS = 6

    def __init__(
        self,
        train_years: int = DEFAULT_TRAIN_YEARS,
        test_months: int = DEFAULT_TEST_MONTHS,
    ) -> None:
        self.train_years = train_years
        self.test_months = test_months

    def generate_windows(
        self,
        data_start: date,
        data_end: date,
        roll_interval_days: int = 30,
    ) -> Iterator[WalkForwardWindow]:
        current_test_start = data_start + timedelta(days=self.train_years * 365)

        while current_test_start + timedelta(days=self.test_months * 30) <= data_end:
            test_end = (
                current_test_start + timedelta(days=self.test_months * 30) - timedelta(days=1)
            )
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
        results = []

        engine = VectorBTEngine(config) if config else None

        for window in self.generate_windows(data_start, data_end):
            train_signals = [s for s in signals if window.train_start <= s[0] <= window.train_end]
            test_signals = [s for s in signals if window.test_start <= s[0] <= window.test_end]

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
                f"train trades={len(train_result.trades)}, test trades={len(test_result.trades)}"
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
