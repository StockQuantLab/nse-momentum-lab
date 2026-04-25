"""Shared intraday entry/stop execution utilities for 5-minute candle processing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time

import numpy as np
import polars as pl

from nse_momentum_lab.services.backtest.engine import ExitReason
from nse_momentum_lab.services.paper.engine.shared_eval import evaluate_entry_trigger
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
    partial_exit_fraction: float | None = None


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
    same_day_partial_exit_pct: float | None = None,
    same_day_partial_exit_carry_stop_pct: float = 0.05,
) -> tuple[bool, float | None, time | None, ExitReason | None, float, float | None]:
    """Simulate stop execution on bars after entry and return final carry stop.

    Returns a 6-tuple: (did_exit, exit_price, exit_time, exit_reason, carry_stop, partial_fraction).
    partial_fraction is non-None only for PARTIAL_EXIT (0.80 = exit 80%).
    """
    stop_level = float(initial_stop)
    risk = (
        (float(initial_stop) - float(entry_price))
        if is_short
        else (float(entry_price) - float(initial_stop))
    )
    if risk <= 0:
        return False, None, None, None, stop_level, None

    for follow_row in rows[entry_idx + 1 :]:
        high_px = float(follow_row["high"])
        low_px = float(follow_row["low"])
        open_px = float(follow_row["open"])
        exit_time = normalize_candle_time(follow_row.get("candle_time"))

        # Same-day partial exit: large intraday move → exit 80%, carry 20% with tight stop.
        if same_day_partial_exit_pct and same_day_partial_exit_pct > 0:
            if is_short:
                target = float(entry_price) * (1.0 - float(same_day_partial_exit_pct))
                if low_px <= target:
                    exit_px = open_px if open_px <= target else target
                    carry = exit_px * (1.0 + float(same_day_partial_exit_carry_stop_pct))
                    return True, exit_px, exit_time, ExitReason.PARTIAL_EXIT, carry, 0.80
            else:
                target = float(entry_price) * (1.0 + float(same_day_partial_exit_pct))
                if high_px >= target:
                    exit_px = open_px if open_px >= target else target
                    carry = exit_px * (1.0 - float(same_day_partial_exit_carry_stop_pct))
                    return True, exit_px, exit_time, ExitReason.PARTIAL_EXIT, carry, 0.80

        # Optional short-only same-day profit-taking.
        if (
            is_short
            and short_same_day_take_profit_pct is not None
            and short_same_day_take_profit_pct > 0
        ):
            target_price = float(entry_price) * (1 - float(short_same_day_take_profit_pct))
            if low_px <= target_price:
                exit_price = open_px if open_px <= target_price else target_price
                return (
                    True,
                    float(exit_price),
                    exit_time,
                    ExitReason.ABNORMAL_PROFIT,
                    stop_level,
                    None,
                )

        # Gap-through stop is filled at open.
        gap_through = open_px >= stop_level if is_short else open_px <= stop_level
        if gap_through:
            return True, open_px, exit_time, ExitReason.GAP_THROUGH_STOP, stop_level, None

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
        return True, float(stop_level), exit_time, reason, stop_level, None

    return False, None, None, None, stop_level, None


# ---------------------------------------------------------------------------
# Shared held-position bar evaluator
# Called by paper-live, paper-replay, and (future) 5-min backtest hold-day loop.
# ---------------------------------------------------------------------------


def _classify_stop_reason_str(entry_price: float, stop_level: float, is_short: bool) -> str:
    """Return 'STOP_INITIAL' | 'STOP_BREAKEVEN' | 'STOP_TRAIL' for a stop hit."""
    eps = max(0.01, abs(entry_price) * 1e-5)
    if not is_short:
        if stop_level > entry_price + eps:
            return "STOP_TRAIL"
        if stop_level >= entry_price - eps:
            return "STOP_BREAKEVEN"
        return "STOP_INITIAL"
    else:
        if stop_level < entry_price - eps:
            return "STOP_TRAIL"
        if stop_level <= entry_price + eps:
            return "STOP_BREAKEVEN"
        return "STOP_INITIAL"


def evaluate_held_position_bar(
    *,
    open_px: float,
    high_px: float,
    low_px: float,
    close_px: float,
    entry_price: float,
    stop_level: float,
    direction: str,
    trail_state: dict,
    is_first_bar_of_session: bool = False,
    is_carried_position: bool = False,
    same_day_partial_exit_pct: float | None = None,
    same_day_partial_exit_carry_stop_pct: float = 0.05,
    is_entry_day: bool = False,
) -> dict:
    """Evaluate one 5-min bar for an already-open position.

    Canonical stop-management rules (shared across paper-live, paper-replay,
    and the future 5-min backtest hold-day loop):

    1. Post-day-3 stop tightening — applied once on the first bar of a new
       session when ``trail_state["pending_day_tighten"]`` is set.
    2. Gap-through stop — fires only on the first bar of a session for an
       overnight-carried position (open already through the stop).
    3. Stop hit — intraday low/high crosses stop_level → exit.
    4. Trail activation — if cumulative gain ≥ trail_activation_pct, tighten.

    NOT here: intraday breakeven promotion.
    Breakeven / stop tightening based on close-position-in-range is an
    EOD-only operation handled by the eod-carry step (H-carry rule).

    Returns
    -------
    dict with keys:
        action          "CLOSE" | "HOLD"
        exit_price      float (0.0 for HOLD)
        reason          str   (empty for HOLD)
        updated_stop    float
        updated_trail_state  dict
    """
    is_short = direction == "SHORT"
    trail_activation_pct = float(trail_state.get("trail_activation_pct", 0.08))
    trail_stop_pct = float(trail_state.get("trail_stop_pct", 0.02))
    updated = dict(trail_state)

    # ── 1. Post-day-3 stop tightening ────────────────────────────────────────
    if updated.pop("pending_day_tighten", False):
        prior_day_low = updated.get("prior_day_low")
        prior_day_high = updated.get("prior_day_high")
        if not is_short and prior_day_low:
            stop_level = max(stop_level, float(prior_day_low))
        elif is_short and prior_day_high:
            stop_level = min(stop_level, float(prior_day_high))
        updated["current_sl"] = stop_level

    # ── 2. Gap-through stop (overnight carries, first bar only) ──────────────
    if is_first_bar_of_session and is_carried_position:
        if not is_short and open_px <= stop_level:
            updated["current_sl"] = stop_level
            return {
                "action": "CLOSE",
                "exit_price": open_px,
                "reason": "GAP_THROUGH_STOP",
                "updated_stop": stop_level,
                "updated_trail_state": updated,
            }
        if is_short and open_px >= stop_level:
            updated["current_sl"] = stop_level
            return {
                "action": "CLOSE",
                "exit_price": open_px,
                "reason": "GAP_THROUGH_STOP",
                "updated_stop": stop_level,
                "updated_trail_state": updated,
            }

    # ── 2b. Same-day partial exit ─────────────────────────────────────────────
    # Fires on entry day only when a large move hits the partial exit threshold.
    # One-shot guard: only fires once per position.
    if (
        is_entry_day
        and same_day_partial_exit_pct
        and same_day_partial_exit_pct > 0
        and not trail_state.get("partial_exit_taken")
    ):
        if not is_short:
            target = entry_price * (1.0 + float(same_day_partial_exit_pct))
            if high_px >= target:
                exit_px = open_px if open_px >= target else target
                carry = exit_px * (1.0 - float(same_day_partial_exit_carry_stop_pct))
                updated["current_sl"] = carry
                updated["partial_exit_taken"] = True
                return {
                    "action": "PARTIAL_EXIT",
                    "exit_price": exit_px,
                    "reason": "PARTIAL_EXIT",
                    "partial_fraction": 0.80,
                    "carry_stop": carry,
                    "updated_stop": carry,
                    "updated_trail_state": updated,
                }
        else:
            target = entry_price * (1.0 - float(same_day_partial_exit_pct))
            if low_px <= target:
                exit_px = open_px if open_px <= target else target
                carry = exit_px * (1.0 + float(same_day_partial_exit_carry_stop_pct))
                updated["current_sl"] = carry
                updated["partial_exit_taken"] = True
                return {
                    "action": "PARTIAL_EXIT",
                    "exit_price": exit_px,
                    "reason": "PARTIAL_EXIT",
                    "partial_fraction": 0.80,
                    "carry_stop": carry,
                    "updated_stop": carry,
                    "updated_trail_state": updated,
                }

    # ── 3. Stop hit ───────────────────────────────────────────────────────────
    if not is_short and low_px <= stop_level:
        reason = _classify_stop_reason_str(entry_price, stop_level, is_short)
        updated["current_sl"] = stop_level
        return {
            "action": "CLOSE",
            "exit_price": stop_level,
            "reason": reason,
            "updated_stop": stop_level,
            "updated_trail_state": updated,
        }
    if is_short and high_px >= stop_level:
        reason = _classify_stop_reason_str(entry_price, stop_level, is_short)
        updated["current_sl"] = stop_level
        return {
            "action": "CLOSE",
            "exit_price": stop_level,
            "reason": reason,
            "updated_stop": stop_level,
            "updated_trail_state": updated,
        }

    # ── 4. Trail activation ───────────────────────────────────────────────────
    if not is_short:
        gain = (high_px - entry_price) / entry_price if entry_price > 0 else 0.0
        if gain >= trail_activation_pct:
            stop_level = max(stop_level, high_px * (1 - trail_stop_pct))
            updated["phase"] = "TRAIL"
        updated["highest_since_entry"] = max(
            float(updated.get("highest_since_entry", high_px)), high_px
        )
    else:
        gain = (entry_price - low_px) / entry_price if entry_price > 0 else 0.0
        if gain >= trail_activation_pct:
            stop_level = min(stop_level, low_px * (1 + trail_stop_pct))
            updated["phase"] = "TRAIL"
        updated["lowest_since_entry"] = min(
            float(updated.get("lowest_since_entry", low_px)), low_px
        )

    updated["current_sl"] = stop_level
    return {
        "action": "HOLD",
        "exit_price": 0.0,
        "reason": "",
        "updated_stop": stop_level,
        "updated_trail_state": updated,
    }


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
    same_day_partial_exit_pct: float | None = None,
    same_day_partial_exit_carry_stop_pct: float = 0.05,
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

        if orh_window_minutes > 0 and not is_short:
            if minutes < orh_window_minutes:
                orh_high = h if orh_high is None else max(orh_high, h)
                continue
            if orh_high is None:
                continue
            # ORH trigger: use orh_high as the trigger price (LONG-only mode).
            _trigger = evaluate_entry_trigger(
                candle_high=h,
                candle_low=low_px,
                candle_open=o,
                session_low=float(session_low if session_low is not None else low_px),
                session_high=float(session_high if session_high is not None else h),
                trigger_price=float(orh_high),
                is_short=False,
                max_stop_dist_pct=999.0,
            )
            if _trigger is None:
                continue
            entry_price = _trigger.entry_price
            initial_stop = _trigger.initial_stop
        else:
            # Standard threshold trigger — delegate to shared pure helper.
            # max_stop_dist_pct=999.0: the backtest runner applies this guard externally;
            # passing a large value avoids double-gating.
            _trigger = evaluate_entry_trigger(
                candle_high=h,
                candle_low=low_px,
                candle_open=o,
                session_low=float(session_low if session_low is not None else low_px),
                session_high=float(session_high if session_high is not None else h),
                trigger_price=float(breakout_price),
                is_short=is_short,
                max_stop_dist_pct=999.0,
                short_initial_stop_atr_cap_mult=short_initial_stop_atr_cap_mult,
                atr=float(short_initial_stop_atr) if short_initial_stop_atr is not None else 0.0,
            )
            if _trigger is None:
                continue
            entry_price = _trigger.entry_price
            initial_stop = _trigger.initial_stop

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

    (
        did_exit,
        same_day_exit_price,
        same_day_exit_time,
        same_day_exit_reason,
        carry_stop,
        partial_fraction,
    ) = _simulate_same_day_stop_execution(
        rows=rows,
        entry_idx=entry_idx,
        entry_price=entry_price,
        initial_stop=initial_stop,
        is_short=is_short,
        same_day_r_ladder=same_day_r_ladder,
        same_day_r_ladder_start_r=same_day_r_ladder_start_r,
        short_same_day_take_profit_pct=short_same_day_take_profit_pct,
        same_day_partial_exit_pct=same_day_partial_exit_pct,
        same_day_partial_exit_carry_stop_pct=same_day_partial_exit_carry_stop_pct,
    )

    same_day_exit_ts = None
    if did_exit and same_day_exit_time is not None:
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
        partial_exit_fraction=partial_fraction,
    )
