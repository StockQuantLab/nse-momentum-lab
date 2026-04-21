"""Shared intraday entry/stop execution utilities for 5-minute candle processing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time

import numpy as np
import polars as pl

from nse_momentum_lab.services.backtest.engine import ExitReason
from nse_momentum_lab.utils import minutes_from_nse_open, normalize_candle_time


@dataclass(frozen=True)
class IntradayExecutionResult:
    entry_price: float
    initial_stop: float
    entry_ts: datetime
    entry_time: time
    same_day_exit_price: float | None
    same_day_exit_ts: datetime | None
    same_day_exit_time: time | None
    same_day_exit_reason: ExitReason | None
    carry_stop_next_session: float | None


def _row_trading_date(row: dict[str, object]) -> date | None:
    raw = row.get("trading_date")
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if raw is None:
        raw = row.get("candle_time")
        if isinstance(raw, datetime):
            return raw.date()
        if isinstance(raw, str):
            try:
                return datetime.fromisoformat(raw).date()
            except ValueError:
                if " " in raw:
                    try:
                        return date.fromisoformat(raw.split(" ", maxsplit=1)[0])
                    except ValueError:
                        return None
            return None
        return None
    try:
        return date.fromisoformat(str(raw))
    except ValueError:
        return None


def _row_candle_time(row: dict[str, object]) -> time | None:
    raw = row.get("candle_time")
    normalized = normalize_candle_time(raw)
    if normalized is not None:
        return normalized
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw).time()
        except ValueError:
            if " " in raw:
                _, time_part = raw.split(" ", maxsplit=1)
                try:
                    return time.fromisoformat(time_part)
                except ValueError:
                    return None
            try:
                return time.fromisoformat(raw)
            except ValueError:
                return None
    return None


def _simulate_same_day_stop_execution(
    *,
    rows: list[dict[str, object]],
    entry_idx: int,
    entry_price: float,
    initial_stop: float,
    is_short: bool,
    same_day_r_ladder: bool,
    same_day_r_ladder_start_r: int = 2,
    short_same_day_take_profit_pct: float | None = None,
) -> tuple[bool, float | None, time | None, ExitReason | None, float]:
    """Simulate stop execution on bars after entry and return final carry stop."""
    stop_level = float(initial_stop)
    risk = (
        (float(initial_stop) - float(entry_price))
        if is_short
        else (float(entry_price) - float(initial_stop))
    )
    if risk <= 0:
        return False, None, None, None, stop_level

    for follow_row in rows[entry_idx + 1 :]:
        high_px = float(follow_row["high"])
        low_px = float(follow_row["low"])
        open_px = float(follow_row["open"])
        exit_time = normalize_candle_time(follow_row.get("candle_time"))

        # Optional short-only same-day profit-taking.
        if (
            is_short
            and short_same_day_take_profit_pct is not None
            and short_same_day_take_profit_pct > 0
        ):
            target_price = float(entry_price) * (1 - float(short_same_day_take_profit_pct))
            if low_px <= target_price:
                exit_price = open_px if open_px <= target_price else target_price
                return True, float(exit_price), exit_time, ExitReason.ABNORMAL_PROFIT, stop_level

        # Gap-through stop is filled at open.
        gap_through = open_px >= stop_level if is_short else open_px <= stop_level
        if gap_through:
            return True, open_px, exit_time, ExitReason.GAP_THROUGH_STOP, stop_level

        if same_day_r_ladder:
            if is_short:
                realized_r = (float(entry_price) - low_px) / risk
                r_steps = int(np.floor(realized_r))
                if r_steps >= same_day_r_ladder_start_r:
                    locked_r = float(max(0, r_steps - same_day_r_ladder_start_r))
                    candidate_stop = float(entry_price) - (locked_r * risk)
                    stop_level = min(stop_level, candidate_stop)
            else:
                realized_r = (high_px - float(entry_price)) / risk
                r_steps = int(np.floor(realized_r))
                if r_steps >= same_day_r_ladder_start_r:
                    locked_r = float(max(0, r_steps - same_day_r_ladder_start_r))
                    candidate_stop = float(entry_price) + (locked_r * risk)
                    stop_level = max(stop_level, candidate_stop)

        stop_hit = high_px >= stop_level if is_short else low_px <= stop_level
        if not stop_hit:
            continue

        if is_short:
            if stop_level < float(entry_price):
                reason = ExitReason.STOP_TRAIL
            elif np.isclose(stop_level, float(entry_price)):
                reason = ExitReason.STOP_BREAKEVEN
            else:
                reason = ExitReason.STOP_INITIAL
        else:
            if stop_level > float(entry_price):
                reason = ExitReason.STOP_TRAIL
            elif np.isclose(stop_level, float(entry_price)):
                reason = ExitReason.STOP_BREAKEVEN
            else:
                reason = ExitReason.STOP_INITIAL

        exit_time = normalize_candle_time(follow_row.get("candle_time"))
        return True, float(stop_level), exit_time, reason, stop_level

    return False, None, None, None, stop_level


def resolve_intraday_execution_from_5min(
    candles: pl.DataFrame,
    *,
    breakout_price: float,
    entry_cutoff_minutes: int = 30,
    is_short: bool = False,
    orh_window_minutes: int = 0,
    entry_start_minutes: int = 0,
    same_day_r_ladder: bool = False,
    same_day_r_ladder_start_r: int = 2,
    short_initial_stop_atr: float | None = None,
    short_initial_stop_atr_cap_mult: float | None = None,
    short_same_day_take_profit_pct: float | None = None,
) -> IntradayExecutionResult | None:
    """Resolve intraday entry and same-day stop behavior from 5-minute candles.

    ``entry_start_minutes`` skips entry-trigger checks for candles that open
    within the first N minutes of the session (session stats are still
    accumulated).  Use ``entry_start_minutes=5`` so the first 5-min candle
    (9:15-9:20) is observed but never traded - entries only happen once the
    full first candle is known at 9:20.
    """
    if candles.is_empty():
        return None

    rows = [dict(r) for r in candles.iter_rows(named=True)]
    entry_idx: int | None = None
    entry_price: float | None = None
    initial_stop: float | None = None
    entry_ts: datetime | None = None
    entry_time: time | None = None

    orh_high: float | None = None
    session_high: float | None = None
    session_low: float | None = None
    for idx, row in enumerate(rows):
        candle_time = _row_candle_time(row)
        if candle_time is None:
            continue
        minutes = minutes_from_nse_open(candle_time)
        if minutes is None or minutes < 0 or minutes > entry_cutoff_minutes:
            continue

        o = float(row["open"])
        h = float(row["high"])
        low_px = float(row["low"])
        session_high = h if session_high is None else max(session_high, h)
        session_low = low_px if session_low is None else min(session_low, low_px)

        # Skip entry-trigger check for early candles (e.g. the 9:15 candle when
        # entry_start_minutes=5).  Session stats above are still accumulated so
        # the initial stop always reflects the full opening range.
        if entry_start_minutes > 0 and minutes < entry_start_minutes:
            continue

        triggered = False
        trigger_price = float(breakout_price)

        if orh_window_minutes > 0 and not is_short:
            if minutes < orh_window_minutes:
                orh_high = h if orh_high is None else max(orh_high, h)
                continue
            if orh_high is None:
                continue
            if h >= orh_high:
                triggered = True
                trigger_price = float(orh_high)
        else:
            if is_short and low_px <= trigger_price:
                triggered = True
            elif (not is_short) and h >= trigger_price:
                triggered = True

        if not triggered:
            continue

        if is_short:
            entry_price = o if o <= trigger_price else trigger_price
            session_stop = float(session_high if session_high is not None else h)
            initial_stop = session_stop
            if (
                short_initial_stop_atr is not None
                and short_initial_stop_atr_cap_mult is not None
                and short_initial_stop_atr > 0
                and short_initial_stop_atr_cap_mult > 0
            ):
                capped_stop = float(entry_price) + (
                    float(short_initial_stop_atr_cap_mult) * float(short_initial_stop_atr)
                )
                initial_stop = min(session_stop, capped_stop)
        else:
            entry_price = o if o >= trigger_price else trigger_price
            initial_stop = float(session_low if session_low is not None else low_px)

        trading_day = _row_trading_date(row)
        if trading_day is None:
            break
        entry_time = candle_time
        entry_ts = datetime.combine(trading_day, candle_time)
        entry_idx = idx
        break

    if (
        entry_idx is None
        or entry_price is None
        or initial_stop is None
        or entry_ts is None
        or entry_time is None
    ):
        return None

    stop_hit, same_day_exit_price, same_day_exit_time, same_day_exit_reason, carry_stop = (
        _simulate_same_day_stop_execution(
            rows=rows,
            entry_idx=entry_idx,
            entry_price=entry_price,
            initial_stop=initial_stop,
            is_short=is_short,
            same_day_r_ladder=same_day_r_ladder,
            same_day_r_ladder_start_r=same_day_r_ladder_start_r,
            short_same_day_take_profit_pct=short_same_day_take_profit_pct,
        )
    )

    same_day_exit_ts = None
    if stop_hit and same_day_exit_time is not None:
        same_day_exit_ts = datetime.combine(entry_ts.date(), same_day_exit_time)

    return IntradayExecutionResult(
        entry_price=float(entry_price),
        initial_stop=float(initial_stop),
        entry_ts=entry_ts,
        entry_time=entry_time,
        same_day_exit_price=float(same_day_exit_price) if same_day_exit_price is not None else None,
        same_day_exit_ts=same_day_exit_ts,
        same_day_exit_time=same_day_exit_time,
        same_day_exit_reason=same_day_exit_reason,
        carry_stop_next_session=float(carry_stop) if carry_stop is not None else None,
    )
