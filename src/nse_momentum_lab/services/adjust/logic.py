from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

logger = logging.getLogger(__name__)


@dataclass
class CorpAction:
    symbol_id: int
    ex_date: date
    action_type: str
    ratio_num: float | None
    ratio_den: float | None
    cash_amount: float | None


def compute_adjustment_factor(
    prev_close: float,
    action: CorpAction,
) -> float:
    if action.action_type == "SPLIT":
        if action.ratio_num is None or action.ratio_den is None:
            raise ValueError("SPLIT requires ratio_num and ratio_den")
        return action.ratio_den / action.ratio_num

    elif action.action_type == "BONUS":
        if action.ratio_num is None or action.ratio_den is None:
            raise ValueError("BONUS requires ratio_num and ratio_den")
        total_ratio = action.ratio_num + action.ratio_den
        return action.ratio_den / total_ratio

    elif action.action_type == "RIGHTS":
        if action.ratio_num is None or action.ratio_den is None:
            raise ValueError("RIGHTS requires ratio_num and ratio_den")
        return action.ratio_den / (action.ratio_num + action.ratio_den)

    elif action.action_type == "DIVIDEND":
        return 1.0

    else:
        raise ValueError(f"Unknown action type: {action.action_type}")


def apply_adjustment(
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    adj_factor: float,
) -> tuple[float, float, float, float]:
    return (
        open_price * adj_factor,
        high_price * adj_factor,
        low_price * adj_factor,
        close_price * adj_factor,
    )


def build_adjustment_series(
    trading_dates: list[date],
    close_prices: list[float],
    actions: list[tuple[date, CorpAction]],
) -> list[float]:
    n = len(trading_dates)
    adj_factors = [1.0] * n
    current_factor = 1.0

    action_map = dict(actions)

    for i in range(n - 1, -1, -1):
        trading_date = trading_dates[i]

        if trading_date in action_map:
            action = action_map[trading_date]
            factor = compute_adjustment_factor(close_prices[i], action)
            current_factor *= factor

        adj_factors[i] = current_factor

    return adj_factors


def reconcile_continuity(
    trading_dates: list[date],
    adjusted_closes: list[float],
    adj_factors: list[float],
    tolerance: float = 0.001,
) -> list[dict]:
    issues = []

    for i in range(1, len(adjusted_closes)):
        prev_adjusted = adjusted_closes[i - 1] * adj_factors[i] / adj_factors[i - 1]
        curr_adjusted = adjusted_closes[i]

        if abs(prev_adjusted - curr_adjusted) / prev_adjusted > tolerance:
            issues.append(
                {
                    "date": trading_dates[i],
                    "issue": "CONTINUITY_BREAK",
                    "details": f"Expected {prev_adjusted:.4f}, got {curr_adjusted:.4f}",
                }
            )

    return issues
