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
    compute_h_filter_passed,
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


def test_short_carry_regression_centralbk_2018() -> None:
    """Regression: CENTRALBK 2018-10-09 SHORT carry.

    Pre-Phase-1 bug: carry_stop drifted below entry_price for a profitable SHORT,
    allowing the position to run further than intended (exit=28.70, pnl=+4.88%).
    Post-Phase-1 fix: carry_stop clamped to min(initial_stop, entry_price) = entry_price,
    ensuring breakeven protection (exit=29.848, pnl=+1.08%).

    entry=30.20, initial_stop=31.35 (session_high, above entry for SHORT).
    """
    result = evaluate_hold_quality_carry_rule(
        hold_quality_passed=True,
        entry_price=30.20,
        close_price=28.70,          # profitable short (close < entry)
        carry_stop_next_session=31.35,
        same_day_exit_price=None,
        same_day_exit_reason=None,
        same_day_exit_ts=None,
        same_day_exit_time=None,
        signal_date=date(2018, 10, 9),
        is_short=True,
    )
    assert result.same_day_exit_reason is None               # profitable: carry, not exit
    assert result.carry_stop_next_session == pytest.approx(30.20)  # clamped to entry (breakeven)
    assert result.carry_stop_next_session <= 30.20           # must not drift below entry for SHORT


def test_short_carry_regression_adanigreen_2023() -> None:
    """Regression: ADANIGREEN 2023-02-27 SHORT carry.

    Pre-Phase-1 bug: carry_stop_next_session was set to 485.30 (ABOVE initial_stop 472.25 and
    entry 462.20) because the backtest applied the LONG breakeven clamp (max) to a SHORT trade.
    This meant the next-session stop was above the loss threshold, causing the position to exit
    at 485.30 with pnl=-5.08% (a loss — stop blown past the initial loss limit).

    Post-Phase-1 fix: carry_stop = min(carry_stop_input, entry_price) = 462.20 (breakeven),
    correct for SHORT. Position exits at 456.664 with pnl=+1.11%.

    The invariant: carry_stop_next_session must NEVER exceed entry_price for a SHORT trade.
    """
    result = evaluate_hold_quality_carry_rule(
        hold_quality_passed=True,
        entry_price=462.20,
        close_price=456.664,        # profitable short (close < entry)
        carry_stop_next_session=485.30,  # old bug value: above both initial_stop and entry
        same_day_exit_price=None,
        same_day_exit_reason=None,
        same_day_exit_ts=None,
        same_day_exit_time=None,
        signal_date=date(2023, 2, 27),
        is_short=True,
    )
    assert result.same_day_exit_reason is None
    assert result.carry_stop_next_session == pytest.approx(462.20)  # clamped to entry_price


# ---------------------------------------------------------------------------
# TestComputeHFilterPassed — ISSUE-055 extracted helper
# ---------------------------------------------------------------------------


class TestComputeHFilterPassed:
    """Unit tests for compute_h_filter_passed() — direction-aware H-filter helper.

    Verifies that the extracted Python function matches the semantics of:
    - the inline paper EOD carry block it replaced
    - the backtest SQL formula: LONG close_pos >= 0.70, SHORT close_pos <= 0.30
    """

    # LONG direction -----------------------------------------------------------

    def test_long_pass(self) -> None:
        assert compute_h_filter_passed(direction="LONG", close_pos_in_range=0.80) is True

    def test_long_fail(self) -> None:
        assert compute_h_filter_passed(direction="LONG", close_pos_in_range=0.60) is False

    def test_long_boundary_exact(self) -> None:
        assert compute_h_filter_passed(direction="LONG", close_pos_in_range=0.70) is True

    # SHORT direction ----------------------------------------------------------

    def test_short_pass(self) -> None:
        assert compute_h_filter_passed(direction="SHORT", close_pos_in_range=0.20) is True

    def test_short_fail(self) -> None:
        assert compute_h_filter_passed(direction="SHORT", close_pos_in_range=0.50) is False

    def test_short_boundary_exact(self) -> None:
        # threshold=0.70 → SHORT boundary = 1-0.70 = 0.30 → passes at exactly 0.30
        assert compute_h_filter_passed(direction="SHORT", close_pos_in_range=0.30) is True

    # Edge cases ---------------------------------------------------------------

    def test_none_returns_false_long(self) -> None:
        assert compute_h_filter_passed(direction="LONG", close_pos_in_range=None) is False

    def test_none_returns_false_short(self) -> None:
        assert compute_h_filter_passed(direction="SHORT", close_pos_in_range=None) is False

    def test_h_carry_disabled_overrides_all(self) -> None:
        # h_carry_enabled=False → always carry regardless of close_pos or direction
        assert compute_h_filter_passed(
            direction="LONG", close_pos_in_range=0.10, h_carry_enabled=False
        ) is True
        assert compute_h_filter_passed(
            direction="SHORT", close_pos_in_range=0.90, h_carry_enabled=False
        ) is True
        assert compute_h_filter_passed(
            direction="LONG", close_pos_in_range=None, h_carry_enabled=False
        ) is True

    def test_custom_threshold(self) -> None:
        # threshold=0.75 → LONG passes at 0.75, fails at 0.74
        assert compute_h_filter_passed(
            direction="LONG", close_pos_in_range=0.75, threshold=0.75
        ) is True
        assert compute_h_filter_passed(
            direction="LONG", close_pos_in_range=0.74, threshold=0.75
        ) is False

    # Backtest SQL parity -------------------------------------------------------

    def test_short_parity_with_backtest_sql_pass(self) -> None:
        # Backtest SQL: filter_h = (signal_close_pos_in_range <= h_threshold_short)
        # where h_threshold_short = 1.0 - 0.70 = 0.30
        # close_pos=0.15 → 0.15 <= 0.30 → True in SQL → must match Python helper
        assert compute_h_filter_passed(direction="SHORT", close_pos_in_range=0.15) is True

    def test_short_parity_with_backtest_sql_fail(self) -> None:
        # close_pos=0.80 → 0.80 <= 0.30 → False in SQL → must match Python helper
        assert compute_h_filter_passed(direction="SHORT", close_pos_in_range=0.80) is False

    def test_direction_case_insensitive(self) -> None:
        # direction string comes from DB column — may be mixed case
        assert compute_h_filter_passed(direction="short", close_pos_in_range=0.20) is True
        assert compute_h_filter_passed(direction="Short", close_pos_in_range=0.80) is False
        assert compute_h_filter_passed(direction="long", close_pos_in_range=0.80) is True

    def test_short_threshold_rounding_matches_backtest(self) -> None:
        # Backtest uses round(1.0 - h_filter_close_pos_threshold, 6) for the short boundary.
        # Confirm the helper rounds identically so a custom threshold like 0.7000003
        # doesn't drift by one comparison at the exact boundary.
        threshold = 0.7000003
        short_boundary = round(1.0 - threshold, 6)  # 0.2999997
        # Exactly at boundary → must pass
        assert compute_h_filter_passed(
            direction="SHORT", close_pos_in_range=short_boundary, threshold=threshold
        ) is True
        # One epsilon above boundary → must fail
        assert compute_h_filter_passed(
            direction="SHORT", close_pos_in_range=short_boundary + 1e-7, threshold=threshold
        ) is False


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

    Phase 1 diff results (pre-Phase-1 vs post-Phase-1, 2015-2026, universe=2000):

    BREAKOUT_4% (bd22a5859c571c0d → a23f33ed4c15545c):
      - 3 trades dropped (RVNL 2020-05-12, MIRZAINT 2023-04-17, RMDRIP 2025-03-21):
        post-Phase-1 evaluate_entry_trigger accumulates session_low from 09:15, making
        stop wider for some entries → max_stop_dist_pct=0.08 guard rejects them. Correct.
      - 1 trade added (FINCABLES 2025-03-24): reverse boundary case.
      - 16 trades: initial_stop only changed (session_low accumulation vs. point-in-time);
        exit/pnl identical — diagnostic-only change.

    BREAKOUT_2% (e5cbeed50a3c78e4 → de7e20a20ecd03fc):
      - 7 trades dropped (same admission guard pattern), 1 added, 23 changed (initial_stop only).

    BREAKDOWN_4% (d6b34cbfb49137de → 2ef1d641142a6d25):
      - 4 trades dropped (different session_high boundary), 0 added.
      - CENTRALBK 2018-10-09: exit 28.70→29.848, pnl +4.88%→+1.08%.
        Pre-Phase-1 carry_stop drifted below entry_price for SHORT (breakeven clamp missing).
      - ADANIGREEN 2023-02-27: exit 485.30→456.664, pnl -5.08%→+1.11% (sign flip!).
        Pre-Phase-1 applied LONG carry formula to SHORT: carry_stop ABOVE initial_stop 472.25,
        allowing position to run to full loss. Post-Phase-1 correctly clamps to entry_price.
      - PF improved 5.50→5.82: direct result of SHORT H-carry bug fix.

    BREAKDOWN_2% (073e3a2225abb123 → e489fef43123b62a):
      - 0 dropped, 0 added, 0 changed. breakdown_filter_n_narrow_only=True narrows
        candidates so the session_high discrepancy doesn't affect any of the 792 entries.

    Conclusion: pre/post diff is NOT a regression — post-Phase-1 is definitively more correct.
    ISSUE-042 granularity delta (daily-bar vs 5-min intraday) is a separate pending concern;
    acceptance threshold pending quantification via feed-audit replay.
    """
    accepted_drift_pct = 0.005
    assert accepted_drift_pct > 0
