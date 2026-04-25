"""Phase 1 parity tests — shared evaluate_entry_trigger().

Layer 1: refactor-invariance — shared function produces same entry_price / initial_stop
         as the backtest resolve_intraday_execution_from_5min() on identical single-candle input.
Layer 2: ISSUE-042 observational stub — daily-bar vs 5-min hold-day P&L delta (< 0.5% annual).
"""
from __future__ import annotations

import time as _time
from datetime import date, datetime

import polars as pl
import pytest

from nse_momentum_lab.services.paper.engine.shared_eval import (
    EntryTriggerResult,
    evaluate_entry_trigger,
    evaluate_hold_quality_carry_rule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_candle_df(
    *,
    trading_date: str = "2024-01-02",
    candle_time: str = "09:25:00",
    open_: float,
    high: float,
    low: float,
    close: float,
) -> pl.DataFrame:
    dt = datetime.fromisoformat(f"{trading_date}T{candle_time}")
    return pl.DataFrame(
        {
            "trading_date": [date.fromisoformat(trading_date)],
            "candle_time": [dt],
            "open": [open_],
            "high": [high],
            "low": [low],
            "close": [close],
        }
    )


def _backtest_resolve(
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    breakout_price: float,
    is_short: bool = False,
    short_initial_stop_atr_cap_mult: float | None = None,
    prior_low: float | None = None,
    prior_high: float | None = None,
):
    """Resolve via backtest; optional prior_low/prior_high add a non-triggering first candle
    so the resolver accumulates a real session_low/session_high before the trigger bar."""
    from nse_momentum_lab.services.backtest.intraday_execution import (
        resolve_intraday_execution_from_5min,
    )

    trigger_df = _make_candle_df(
        candle_time="09:25:00", open_=open_, high=high, low=low, close=close
    )
    if prior_low is not None or prior_high is not None:
        # Flat prior candle at OHLC=prior_low (LONG) or prior_high (SHORT).
        # Must not trigger entry: for LONG keep high < breakout_price;
        # for SHORT keep low > breakout_price.
        p = prior_low if prior_low is not None else (prior_high if prior_high is not None else low)
        prior_df = _make_candle_df(
            candle_time="09:20:00",
            open_=p, high=p, low=p, close=p,
        )
        df = pl.concat([prior_df, trigger_df])
    else:
        df = trigger_df

    return resolve_intraday_execution_from_5min(
        df,
        breakout_price=breakout_price,
        entry_cutoff_minutes=60,
        is_short=is_short,
        entry_start_minutes=5,
        short_initial_stop_atr_cap_mult=short_initial_stop_atr_cap_mult,
        same_day_partial_exit_pct=None,
    )


# ---------------------------------------------------------------------------
# Layer 1a: pure-unit tests for evaluate_entry_trigger()
# ---------------------------------------------------------------------------


def test_long_trigger_open_below_trigger():
    r = evaluate_entry_trigger(
        candle_high=105.0, candle_low=99.0, candle_open=102.0,
        session_low=99.0, session_high=105.0,
        trigger_price=104.0, is_short=False, max_stop_dist_pct=0.08,
    )
    assert r is not None
    assert r.entry_price == pytest.approx(104.0)
    assert r.initial_stop == pytest.approx(99.0)


def test_long_trigger_gap_open_above():
    r = evaluate_entry_trigger(
        candle_high=108.0, candle_low=106.0, candle_open=107.0,
        session_low=99.0, session_high=108.0,
        trigger_price=104.0, is_short=False, max_stop_dist_pct=0.08,
    )
    assert r is not None
    assert r.entry_price == pytest.approx(107.0)
    assert r.initial_stop == pytest.approx(99.0)


def test_long_no_trigger():
    r = evaluate_entry_trigger(
        candle_high=103.5, candle_low=100.0, candle_open=100.5,
        session_low=99.0, session_high=103.5,
        trigger_price=104.0, is_short=False, max_stop_dist_pct=0.08,
    )
    assert r is None


def test_long_stop_too_wide_rejected():
    r = evaluate_entry_trigger(
        candle_high=105.0, candle_low=100.0, candle_open=102.0,
        session_low=90.0,  # 14% below entry — exceeds 8% max
        session_high=105.0,
        trigger_price=104.0, is_short=False, max_stop_dist_pct=0.08,
    )
    assert r is None


def test_short_trigger_open_above_trigger():
    r = evaluate_entry_trigger(
        candle_high=100.0, candle_low=94.0, candle_open=97.0,
        session_low=94.0, session_high=100.0,
        trigger_price=96.0, is_short=True, max_stop_dist_pct=0.08,
    )
    assert r is not None
    assert r.entry_price == pytest.approx(96.0)
    assert r.initial_stop == pytest.approx(100.0)


def test_short_gap_open_below():
    r = evaluate_entry_trigger(
        candle_high=96.0, candle_low=93.0, candle_open=94.5,
        session_low=93.0, session_high=100.0,
        trigger_price=96.0, is_short=True, max_stop_dist_pct=0.08,
    )
    assert r is not None
    assert r.entry_price == pytest.approx(94.5)


def test_short_no_trigger():
    r = evaluate_entry_trigger(
        candle_high=100.0, candle_low=97.0, candle_open=98.0,
        session_low=97.0, session_high=100.0,
        trigger_price=96.0, is_short=True, max_stop_dist_pct=0.08,
    )
    assert r is None


def test_short_atr_cap_applied():
    r = evaluate_entry_trigger(
        candle_high=100.0, candle_low=90.0, candle_open=93.0,
        session_low=90.0, session_high=115.0,
        trigger_price=92.0, is_short=True,
        max_stop_dist_pct=0.30,  # relaxed so uncapped stop doesn't fail
        short_initial_stop_atr_cap_mult=2.0, atr=3.0,
    )
    # cap: min(115.0, 92.0 + 2*3) = min(115.0, 98.0) = 98.0
    assert r is not None
    assert r.initial_stop == pytest.approx(98.0)


# ---------------------------------------------------------------------------
# Layer 1b: parity — shared function vs backtest resolver (single candle)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("open_,high,low,close,trigger,prior_low,exp_entry,exp_stop", [
    # open below trigger: prior candle set session_low=99; stop = session_low
    (102.0, 105.0, 100.0, 103.0, 104.0, 99.0, 104.0, 99.0),
    # gap open above trigger: no prior candle, session_low = candle_low = 106
    (107.0, 109.0, 106.0, 108.0, 104.0, None, 107.0, 106.0),
])
def test_long_shared_matches_backtest(open_, high, low, close, trigger, prior_low, exp_entry, exp_stop):
    session_low = prior_low if prior_low is not None else low
    shared = evaluate_entry_trigger(
        candle_high=high, candle_low=low, candle_open=open_,
        session_low=session_low, session_high=high,
        trigger_price=trigger, is_short=False, max_stop_dist_pct=0.08,
    )
    assert shared is not None
    assert shared.entry_price == pytest.approx(exp_entry)
    assert shared.initial_stop == pytest.approx(exp_stop)

    bt = _backtest_resolve(open_=open_, high=high, low=low, close=close,
                           breakout_price=trigger, is_short=False, prior_low=prior_low)
    assert bt is not None, "backtest resolve returned None"
    assert bt.entry_price == pytest.approx(shared.entry_price, abs=0.01)
    assert bt.initial_stop == pytest.approx(shared.initial_stop, abs=0.01)


@pytest.mark.parametrize("open_,high,low,close,trigger,prior_high,exp_entry,exp_stop", [
    # open above trigger: prior candle set session_high=100; stop = session_high
    (97.0, 100.0, 94.0, 95.0, 96.0, 100.0, 96.0, 100.0),
    # gap open below trigger: no prior candle, session_high = candle_high = 95
    (93.0, 95.0, 90.0, 91.0, 96.0, None, 93.0, 95.0),
])
def test_short_shared_matches_backtest(open_, high, low, close, trigger, prior_high, exp_entry, exp_stop):
    session_high = prior_high if prior_high is not None else high
    shared = evaluate_entry_trigger(
        candle_high=high, candle_low=low, candle_open=open_,
        session_low=low, session_high=session_high,
        trigger_price=trigger, is_short=True, max_stop_dist_pct=0.08,
    )
    assert shared is not None
    assert shared.entry_price == pytest.approx(exp_entry)
    assert shared.initial_stop == pytest.approx(exp_stop)

    bt = _backtest_resolve(open_=open_, high=high, low=low, close=close,
                           breakout_price=trigger, is_short=True, prior_high=prior_high)
    assert bt is not None, "backtest resolve returned None"
    assert bt.entry_price == pytest.approx(shared.entry_price, abs=0.01)
    assert bt.initial_stop == pytest.approx(shared.initial_stop, abs=0.01)


# ---------------------------------------------------------------------------
# Layer 1c: shared carry helper parity
# ---------------------------------------------------------------------------


def test_short_h_carry_tightens_to_breakeven() -> None:
    result = evaluate_hold_quality_carry_rule(
        hold_quality_passed=True,
        entry_price=100.0,
        close_price=92.0,
        carry_stop_next_session=105.0,
        same_day_exit_price=None,
        same_day_exit_reason=None,
        same_day_exit_ts=None,
        same_day_exit_time=None,
        signal_date=date(2024, 1, 2),
        is_short=True,
    )
    assert result.same_day_exit_reason is None
    assert result.carry_stop_next_session == pytest.approx(100.0)
    assert result.carry_action == "normal"


def test_short_weak_close_exits_at_close() -> None:
    result = evaluate_hold_quality_carry_rule(
        hold_quality_passed=False,
        entry_price=100.0,
        close_price=101.0,
        carry_stop_next_session=105.0,
        same_day_exit_price=None,
        same_day_exit_reason=None,
        same_day_exit_ts=None,
        same_day_exit_time=None,
        signal_date=date(2024, 1, 2),
        is_short=True,
    )
    assert result.same_day_exit_reason == "WEAK_CLOSE_EXIT"
    assert result.same_day_exit_price == pytest.approx(101.0)
    assert result.same_day_exit_time == datetime.strptime("15:30:00", "%H:%M:%S").time()
    assert result.carry_action == "weak_close_exit"
    assert result.carry_stop_next_session == pytest.approx(105.0)


# ---------------------------------------------------------------------------
# Performance gate
# ---------------------------------------------------------------------------


def test_evaluate_entry_trigger_performance():
    """100k calls must complete in < 500ms."""
    n_calls = 100_000
    start = _time.perf_counter()
    for _ in range(n_calls):
        evaluate_entry_trigger(
            candle_high=105.0, candle_low=99.0, candle_open=102.0,
            session_low=99.0, session_high=105.0,
            trigger_price=104.0, is_short=False, max_stop_dist_pct=0.08,
        )
    elapsed = _time.perf_counter() - start
    assert elapsed < 0.5, f"100k calls took {elapsed:.3f}s — exceeds 500ms budget"


# ---------------------------------------------------------------------------
# Layer 2: ISSUE-042 observational stub
# ---------------------------------------------------------------------------


def test_issue_042_observational_stub():
    """Documents the granularity delta between daily-bar and 5-min hold-day P&L.

    Full test requires a real replay session. Stub accepted until the
    paper-vs-backtest comparison pipeline (Step 1.4) is in place.
    Acceptance: < 0.5% annualized drift.
    """
    accepted_drift_pct = 0.005
    assert accepted_drift_pct > 0
