"""Shared pure-function evaluation helpers for entry triggers and carry/stop updates.

Used identically by the paper runtime (bar-by-bar streaming) and the backtest
runner (batch 5-min candle loop). Any change here affects both paths
simultaneously — run parity tests after every edit.

Held-position stop management is already shared via
``evaluate_held_position_bar()`` in ``services.backtest.intraday_execution``.
This module covers the entry-trigger decision plus the EOD H-carry decision:
threshold check, stop placement, max-stop-dist guard, and overnight carry
clamping / weak-close exit selection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time

from nse_momentum_lab.services.backtest.engine import ExitReason


@dataclass(frozen=True)
class EntryTriggerResult:
    """Outcome of a successful entry trigger evaluation."""

    entry_price: float
    initial_stop: float


@dataclass(frozen=True)
class HoldQualityCarryResult:
    """Outcome of the H-carry / EOD carry decision."""

    same_day_exit_price: float | None
    same_day_exit_reason: str | None
    same_day_exit_ts: datetime | None
    same_day_exit_time: time | None
    carry_stop_next_session: float | None
    carry_action: str


def evaluate_entry_trigger(
    *,
    candle_high: float,
    candle_low: float,
    candle_open: float,
    session_low: float,
    session_high: float,
    trigger_price: float,
    is_short: bool,
    max_stop_dist_pct: float = 0.08,
    short_initial_stop_atr_cap_mult: float | None = None,
    atr: float = 0.0,
) -> EntryTriggerResult | None:
    """Pure entry-trigger decision for one 5-min candle.

    ``trigger_price`` is already-computed: ``prev_close * (1 + threshold)``
    for LONG, ``prev_close * (1 - threshold)`` for SHORT.

    Returns ``None`` if the candle does not trigger an entry, or if the
    resulting stop distance exceeds ``max_stop_dist_pct``.

    LONG: triggers when ``candle_high >= trigger_price``.
          Entry = max(candle_open, trigger_price).
          Stop  = session_low (ATR fallback for degenerate bars where
                  session_low >= entry).

    SHORT: triggers when ``candle_low <= trigger_price``.
           Entry = min(candle_open, trigger_price).
           Stop  = session_high, optionally capped at
                   entry + short_initial_stop_atr_cap_mult * atr.

    Callers that apply the max-stop-dist gate externally (e.g. the batch
    backtest loop) should pass ``max_stop_dist_pct=999.0`` to suppress the
    guard here and avoid double-gating.
    """
    if not is_short:
        if candle_high < trigger_price:
            return None
        entry_price = max(float(candle_open), trigger_price)
        # session_low is below entry in all normal cases; ATR fallback for degenerate bars.
        if float(session_low) < entry_price:
            initial_stop = float(session_low)
        else:
            initial_stop = entry_price - atr * 2.0 if atr > 0 else entry_price * 0.96
        if entry_price > 0 and initial_stop < entry_price * (1.0 - max_stop_dist_pct):
            return None
        return EntryTriggerResult(entry_price=entry_price, initial_stop=initial_stop)
    else:
        if candle_low > trigger_price:
            return None
        entry_price = min(float(candle_open), trigger_price)
        # session_high is above entry in all normal cases; buffer fallback for degenerate bars.
        if float(session_high) > entry_price:
            initial_stop = float(session_high)
        else:
            initial_stop = entry_price * 1.04
        # Optional ATR-based cap on short initial stop distance.
        if (
            short_initial_stop_atr_cap_mult is not None
            and float(short_initial_stop_atr_cap_mult) > 0
            and atr > 0
        ):
            capped = entry_price + float(short_initial_stop_atr_cap_mult) * atr
            initial_stop = min(initial_stop, capped)
        if entry_price > 0 and initial_stop > entry_price * (1.0 + max_stop_dist_pct):
            return None
        return EntryTriggerResult(entry_price=entry_price, initial_stop=initial_stop)


def evaluate_hold_quality_carry_rule(
    *,
    hold_quality_passed: bool,
    entry_price: float | None,
    close_price: float | None,
    carry_stop_next_session: float | None,
    same_day_exit_price: float | None,
    same_day_exit_reason: str | None,
    same_day_exit_ts: datetime | None,
    same_day_exit_time: time | None,
    signal_date: date,
    is_short: bool,
) -> HoldQualityCarryResult:
    """Apply the shared H-carry / EOD carry rule.

    Mirrors the backtest and paper-live overnight carry behavior:
    - if a same-day intraday exit already occurred, preserve it
    - if H=True, tighten carry stop to at least breakeven
    - if H=False and the position is losing/flat, exit at the close
    - if H=False and the position is profitable, carry with a breakeven stop
    """
    if same_day_exit_reason is not None:
        return HoldQualityCarryResult(
            same_day_exit_price=same_day_exit_price,
            same_day_exit_reason=same_day_exit_reason,
            same_day_exit_ts=same_day_exit_ts,
            same_day_exit_time=same_day_exit_time,
            carry_stop_next_session=carry_stop_next_session,
            carry_action="normal",
        )

    if entry_price is None or close_price is None:
        return HoldQualityCarryResult(
            same_day_exit_price=same_day_exit_price,
            same_day_exit_reason=same_day_exit_reason,
            same_day_exit_ts=same_day_exit_ts,
            same_day_exit_time=same_day_exit_time,
            carry_stop_next_session=carry_stop_next_session,
            carry_action="normal",
        )

    if hold_quality_passed:
        base = carry_stop_next_session if carry_stop_next_session is not None else entry_price
        tightened = (
            min(float(base), float(entry_price))
            if is_short
            else max(float(base), float(entry_price))
        )
        return HoldQualityCarryResult(
            same_day_exit_price=same_day_exit_price,
            same_day_exit_reason=same_day_exit_reason,
            same_day_exit_ts=same_day_exit_ts,
            same_day_exit_time=same_day_exit_time,
            carry_stop_next_session=tightened,
            carry_action="normal",
        )

    if is_short:
        close_failed_entry = close_price >= entry_price
    else:
        close_failed_entry = close_price <= entry_price

    if close_failed_entry:
        return HoldQualityCarryResult(
            same_day_exit_price=float(close_price),
            same_day_exit_reason=ExitReason.WEAK_CLOSE_EXIT.value,
            same_day_exit_ts=datetime.combine(signal_date, time(15, 30)),
            same_day_exit_time=time(15, 30),
            carry_stop_next_session=carry_stop_next_session,
            carry_action="weak_close_exit",
        )

    base_carry_stop = (
        carry_stop_next_session if carry_stop_next_session is not None else entry_price
    )
    tightened_stop = (
        min(float(base_carry_stop), float(entry_price))
        if is_short
        else max(float(base_carry_stop), float(entry_price))
    )
    return HoldQualityCarryResult(
        same_day_exit_price=same_day_exit_price,
        same_day_exit_reason=same_day_exit_reason,
        same_day_exit_ts=same_day_exit_ts,
        same_day_exit_time=same_day_exit_time,
        carry_stop_next_session=tightened_stop,
        carry_action="breakeven_carry",
    )
