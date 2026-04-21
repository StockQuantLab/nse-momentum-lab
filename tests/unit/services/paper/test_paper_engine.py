"""Unit tests for the paper trading v2 engine modules.

Covers:
- bar_orchestrator: slot sizing, position tracker, entry selection ranking
- candle_builder: OHLCV aggregation, bar boundaries, drain/flush
- paper_runtime: _minutes_from_open, evaluate_candle (LONG/SHORT), stop classification,
  enforce_session_risk_controls
- paper_db: transaction rollback, insert_signal idempotency, flatten SHORT P&L,
  patch_position_metadata
- alert_dispatcher: _redact_url bot-token redaction
"""

from __future__ import annotations

import time as _time
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_IST = timezone(timedelta(hours=5, minutes=30))


def _epoch_at_ist(hour: int, minute: int, *, day: int = 1) -> float:
    """Return epoch seconds for the given IST wall-clock time on a fixed date."""
    dt = datetime(2026, 4, day, hour, minute, 0, tzinfo=_IST)
    return dt.timestamp()


def _make_candle(
    *,
    open: float = 100.0,
    high: float = 105.0,
    low: float = 98.0,
    close: float = 103.0,
    bar_end: float | None = None,
    bar_start: float | None = None,
) -> dict:
    if bar_end is None:
        bar_end = _epoch_at_ist(9, 20)
    if bar_start is None:
        bar_start = bar_end - 300
    return {
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "bar_end": bar_end,
        "bar_start": bar_start,
        "ts": bar_end,
    }


def _make_session(
    *,
    direction: str = "LONG",
    flatten_time: str = "15:15:00",
    max_daily_loss_pct: float = 0.05,
    max_drawdown_pct: float = 0.15,
) -> dict:
    return {
        "risk_config": {
            "flatten_time": flatten_time,
            "max_daily_loss_pct": max_daily_loss_pct,
            "max_drawdown_pct": max_drawdown_pct,
        },
    }


# ---------------------------------------------------------------------------
# bar_orchestrator tests
# ---------------------------------------------------------------------------


class TestSlotCapital:
    def test_slot_by_count(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import slot_capital_for

        slot = slot_capital_for(max_positions=10, portfolio_value=1_000_000.0)
        assert slot == 100_000.0

    def test_pct_cap_wins_when_smaller(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import slot_capital_for

        # 1% of 1_000_000 = 10_000, count-slot = 100_000
        slot = slot_capital_for(
            max_positions=10, portfolio_value=1_000_000.0, max_position_pct=0.01
        )
        assert slot == 10_000.0

    def test_count_slot_wins_when_pct_is_larger(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import slot_capital_for

        # 50% cap on 10 positions is larger than count-slot — count slot wins
        slot = slot_capital_for(
            max_positions=10, portfolio_value=1_000_000.0, max_position_pct=0.50
        )
        assert slot == 100_000.0

    def test_minimum_notional_floor(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            minimum_trade_notional_for,
        )

        # With tiny portfolio, floor at 1000
        notional = minimum_trade_notional_for(max_positions=10, portfolio_value=100.0)
        assert notional >= 1000.0


class TestSessionPositionTracker:
    def test_initial_state(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker

        t = SessionPositionTracker(max_positions=5, portfolio_value=500_000.0)
        assert t.open_count == 0
        assert t.slots_available() == 5
        assert not t.has_open_position("RELIANCE")

    def test_record_open_and_close(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
            TrackedPosition,
        )

        t = SessionPositionTracker(max_positions=5, portfolio_value=500_000.0)
        pos = TrackedPosition(
            position_id="p1",
            symbol="RELIANCE",
            direction="LONG",
            entry_price=2500.0,
            stop_loss=2400.0,
            target_price=None,
            entry_time="",
            quantity=10,
            current_qty=10,
        )
        t.record_open(pos, 25_000.0)
        assert t.open_count == 1
        assert t.has_open_position("RELIANCE")
        assert t.cash_available == 500_000.0 - 25_000.0

        t.record_close("RELIANCE", 26_000.0)
        assert t.open_count == 0
        assert not t.has_open_position("RELIANCE")
        assert t.cash_available == 500_000.0 - 25_000.0 + 26_000.0

    def test_seed_open_positions_skips_rows_without_stop_metadata(self, caplog):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker

        tracker = SessionPositionTracker(max_positions=5, portfolio_value=500_000.0)
        positions = [
            {
                "position_id": "bad-1",
                "symbol": "BAD",
                "direction": "LONG",
                "avg_entry": 100.0,
                "qty": 10,
                "metadata_json": {"signal_id": "sig-bad"},
            },
            {
                "position_id": "ok-1",
                "symbol": "OK",
                "direction": "LONG",
                "avg_entry": 200.0,
                "qty": 5,
                "metadata_json": {"initial_sl": 180.0, "signal_id": "sig-ok"},
            },
        ]

        with caplog.at_level("WARNING"):
            tracker.seed_open_positions(positions)

        ok = tracker.get_open_position("OK")
        assert tracker.open_count == 1
        assert tracker.get_open_position("BAD") is None
        assert ok is not None
        assert ok.stop_loss == 180.0
        assert tracker.cash_available == 499_000.0
        assert "skipping BAD (bad-1) because metadata has no stop" in caplog.text

    def test_slots_available_decrements(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
            TrackedPosition,
        )

        t = SessionPositionTracker(max_positions=3, portfolio_value=300_000.0)
        for i, sym in enumerate(["A", "B", "C"]):
            pos = TrackedPosition(
                position_id=f"p{i}",
                symbol=sym,
                direction="LONG",
                entry_price=100.0,
                stop_loss=90.0,
                target_price=None,
                entry_time="",
                quantity=10,
                current_qty=10,
            )
            t.record_open(pos, 1_000.0)
        assert t.slots_available() == 0
        assert not t.can_open_new()

    def test_compute_position_qty_basic(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker

        t = SessionPositionTracker(max_positions=10, portfolio_value=1_000_000.0)
        qty = t.compute_position_qty(entry_price=100.0)
        assert qty == 1000  # slot=100_000/100=1000

    def test_compute_position_qty_zero_when_price_zero(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker

        t = SessionPositionTracker(max_positions=10, portfolio_value=1_000_000.0)
        assert t.compute_position_qty(entry_price=0.0) == 0

    def test_has_traded_today(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
            TrackedPosition,
        )

        t = SessionPositionTracker()
        pos = TrackedPosition(
            position_id="p1",
            symbol="TCS",
            direction="LONG",
            entry_price=3500.0,
            stop_loss=3400.0,
            target_price=None,
            entry_time="",
            quantity=5,
            current_qty=5,
        )
        t.record_open(pos, 17_500.0)
        t.record_close("TCS", 18_000.0)
        assert t.has_traded_today("TCS")


class TestSelectEntriesForBar:
    def test_selects_up_to_slots_available(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
            select_entries_for_bar,
        )

        t = SessionPositionTracker(max_positions=2)
        candidates = [
            {"symbol": "A", "setup_row": {"selection_score": 1.0}},
            {"symbol": "B", "setup_row": {"selection_score": 2.0}},
            {"symbol": "C", "setup_row": {"selection_score": 3.0}},
        ]
        chosen = select_entries_for_bar(candidates, t)
        assert len(chosen) == 2
        # Highest selection_score first
        assert chosen[0]["symbol"] == "C"
        assert chosen[1]["symbol"] == "B"

    def test_returns_empty_when_no_slots(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
            TrackedPosition,
            select_entries_for_bar,
        )

        t = SessionPositionTracker(max_positions=1)
        pos = TrackedPosition(
            position_id="p1",
            symbol="X",
            direction="LONG",
            entry_price=100.0,
            stop_loss=90.0,
            target_price=None,
            entry_time="",
            quantity=1,
            current_qty=1,
        )
        t.record_open(pos, 100.0)
        candidates = [{"symbol": "Y", "setup_row": {"selection_score": 5.0}}]
        assert select_entries_for_bar(candidates, t) == []

    def test_falls_back_to_symbol_sort_when_score_tied(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
            select_entries_for_bar,
        )

        t = SessionPositionTracker(max_positions=1)
        candidates = [
            {"symbol": "Z", "setup_row": {"selection_score": 1.0}},
            {"symbol": "A", "setup_row": {"selection_score": 1.0}},
        ]
        chosen = select_entries_for_bar(candidates, t)
        assert len(chosen) == 1
        assert chosen[0]["symbol"] == "A"  # alphabetically first on tie


# ---------------------------------------------------------------------------
# candle_builder tests
# ---------------------------------------------------------------------------


class TestFiveMinuteCandleBuilder:
    def _make_snapshot(self, symbol: str, ts: float, price: float, volume: float = 0.0):
        from nse_momentum_lab.services.paper.feeds.candle_types import MarketSnapshot

        return MarketSnapshot(symbol=symbol, ts=ts, last_price=price, volume=volume)

    def test_single_bar_no_close(self):
        from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder

        builder = FiveMinuteCandleBuilder(interval_minutes=5)
        ts = _epoch_at_ist(9, 16)
        builder.ingest(self._make_snapshot("RELIANCE", ts, 2500.0))
        assert builder.drain_closed() == []

    def test_bar_closes_on_next_interval(self):
        from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder

        builder = FiveMinuteCandleBuilder(interval_minutes=5)
        ts1 = _epoch_at_ist(9, 16)  # inside 09:15 bucket
        ts2 = _epoch_at_ist(9, 21)  # inside 09:20 bucket → closes previous bar
        builder.ingest(self._make_snapshot("TCS", ts1, 3500.0, volume=100.0))
        builder.ingest(self._make_snapshot("TCS", ts2, 3510.0, volume=200.0))
        closed = builder.drain_closed()
        assert len(closed) == 1
        candle = closed[0]
        assert candle.symbol == "TCS"
        assert candle.open == 3500.0
        assert candle.close == 3500.0  # only one tick in first bar
        assert candle.high == 3500.0
        assert candle.low == 3500.0

    def test_ohlcv_aggregation_within_bar(self):
        from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder
        from nse_momentum_lab.services.paper.feeds.candle_types import MarketSnapshot

        builder = FiveMinuteCandleBuilder(interval_minutes=5)
        base_ts = _epoch_at_ist(9, 16)
        ticks = [
            MarketSnapshot("INF", base_ts + 0, 100.0, 0.0),
            MarketSnapshot("INF", base_ts + 30, 105.0, 50.0),
            MarketSnapshot("INF", base_ts + 60, 98.0, 100.0),
            MarketSnapshot("INF", base_ts + 90, 103.0, 150.0),
        ]
        builder.ingest_many(ticks)
        # No bar closed yet (all in same bucket)
        assert builder.drain_closed() == []

        # Force close with flush
        flushed = builder.flush("INF")
        assert len(flushed) == 1
        c = flushed[0]
        assert c.open == 100.0
        assert c.high == 105.0
        assert c.low == 98.0
        assert c.close == 103.0

    def test_volume_delta_cumulative(self):
        from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder
        from nse_momentum_lab.services.paper.feeds.candle_types import MarketSnapshot

        builder = FiveMinuteCandleBuilder(interval_minutes=5)
        base_ts = _epoch_at_ist(9, 16)
        builder.ingest(MarketSnapshot("X", base_ts, 100.0, volume=1000.0))
        builder.ingest(MarketSnapshot("X", base_ts + 30, 102.0, volume=1200.0))

        # Cross bar boundary
        ts2 = _epoch_at_ist(9, 21)
        builder.ingest(MarketSnapshot("X", ts2, 103.0, volume=1500.0))
        closed = builder.drain_closed()
        assert len(closed) == 1
        # Volume delta from second tick only (first has no prev vol)
        assert closed[0].volume == pytest.approx(200.0)

    def test_flush_all_symbols(self):
        from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder
        from nse_momentum_lab.services.paper.feeds.candle_types import MarketSnapshot

        builder = FiveMinuteCandleBuilder(interval_minutes=5)
        ts = _epoch_at_ist(9, 16)
        builder.ingest(MarketSnapshot("A", ts, 100.0))
        builder.ingest(MarketSnapshot("B", ts, 200.0))
        flushed = builder.flush()
        assert len(flushed) == 2
        symbols = {c.symbol for c in flushed}
        assert symbols == {"A", "B"}

    def test_out_of_order_tick_dropped(self):
        from nse_momentum_lab.services.paper.feeds.candle_builder import FiveMinuteCandleBuilder
        from nse_momentum_lab.services.paper.feeds.candle_types import MarketSnapshot

        builder = FiveMinuteCandleBuilder(interval_minutes=5)
        ts_new = _epoch_at_ist(9, 21)
        ts_old = _epoch_at_ist(9, 16)
        builder.ingest(MarketSnapshot("Z", ts_new, 300.0))
        builder.ingest(MarketSnapshot("Z", ts_old, 290.0))  # out of order — dropped
        flushed = builder.flush("Z")
        assert len(flushed) == 1
        assert flushed[0].close == 300.0  # old tick did not overwrite


# ---------------------------------------------------------------------------
# paper_runtime tests
# ---------------------------------------------------------------------------


class TestMinutesFromOpen:
    def test_exactly_at_open(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _minutes_from_open

        ts = _epoch_at_ist(9, 15)
        assert _minutes_from_open(ts) == 0

    def test_five_minutes_after_open(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _minutes_from_open

        ts = _epoch_at_ist(9, 20)
        assert _minutes_from_open(ts) == 5

    def test_before_open_clamps_to_zero(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _minutes_from_open

        ts = _epoch_at_ist(9, 0)
        assert _minutes_from_open(ts) == 0

    def test_accepts_datetime_object(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _minutes_from_open

        dt = datetime(2026, 4, 1, 9, 25, 0, tzinfo=_IST)
        assert _minutes_from_open(dt) == 10


class TestClassifyStopReason:
    def test_long_initial_stop(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _classify_stop_reason

        # Stop well below entry → INITIAL
        assert _classify_stop_reason(100.0, 95.0, "LONG") == "STOP_INITIAL"

    def test_long_breakeven_stop(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _classify_stop_reason

        assert _classify_stop_reason(100.0, 100.0, "LONG") == "STOP_BREAKEVEN"

    def test_long_trail_stop(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _classify_stop_reason

        # Stop above entry → TRAIL
        assert _classify_stop_reason(100.0, 105.0, "LONG") == "STOP_TRAIL"

    def test_short_initial_stop(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _classify_stop_reason

        # Stop well above entry → INITIAL
        assert _classify_stop_reason(100.0, 106.0, "SHORT") == "STOP_INITIAL"

    def test_short_breakeven_stop(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _classify_stop_reason

        assert _classify_stop_reason(100.0, 100.0, "SHORT") == "STOP_BREAKEVEN"

    def test_short_trail_stop(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import _classify_stop_reason

        # Stop below entry → TRAIL
        assert _classify_stop_reason(100.0, 94.0, "SHORT") == "STOP_TRAIL"


class TestEvaluateCandle:
    """Tests for evaluate_candle using a minimal strategy_config stub."""

    def _make_strategy_config(
        self,
        *,
        direction: str = "LONG",
        threshold: float = 0.04,
        entry_cutoff_minutes: int = 360,
        extra_params: dict | None = None,
    ):
        class _Config:
            pass

        cfg = _Config()
        cfg.direction = direction
        cfg.breakout_threshold = threshold
        cfg.entry_cutoff_minutes = entry_cutoff_minutes
        cfg.extra_params = extra_params or {}
        return cfg

    def _runtime_with_candidate(
        self,
        symbol: str,
        prev_close: float,
        direction: str = "LONG",
    ):
        from nse_momentum_lab.services.paper.engine.paper_runtime import PaperRuntimeState

        runtime = PaperRuntimeState()
        state = runtime.for_symbol(symbol)
        state.setup_status = "candidate"
        state.setup_row = {
            "symbol": symbol,
            "prev_close": prev_close,
            "atr_20": prev_close * 0.02,
            "selection_score": 1.0,
            "signal_id": "sig-001",
        }
        state.trade_date = "2026-04-01"
        return runtime

    def test_long_entry_triggered(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
        )
        from nse_momentum_lab.services.paper.engine.paper_runtime import evaluate_candle

        tracker = SessionPositionTracker()
        runtime = self._runtime_with_candidate("RELIANCE", prev_close=2500.0)
        cfg = self._make_strategy_config(threshold=0.04)

        # High must reach 2500 * 1.04 = 2600
        candle = _make_candle(open=2560.0, high=2610.0, low=2540.0, close=2600.0)
        result = evaluate_candle(
            symbol="RELIANCE",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=_make_session(),
            strategy_config=cfg,
        )
        assert result["action"] == "ENTRY_CANDIDATE"
        assert result["direction"] == "LONG"
        assert result["signal_id"] == "sig-001"
        assert result["entry_price"] >= 2600.0

    def test_long_no_trigger_when_high_below_breakout(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
        )
        from nse_momentum_lab.services.paper.engine.paper_runtime import evaluate_candle

        tracker = SessionPositionTracker()
        runtime = self._runtime_with_candidate("TCS", prev_close=3000.0)
        cfg = self._make_strategy_config(threshold=0.04)

        candle = _make_candle(open=3000.0, high=3100.0, low=2990.0, close=3050.0)
        result = evaluate_candle(
            symbol="TCS",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=_make_session(),
            strategy_config=cfg,
        )
        assert result["action"] == "SKIP"

    def test_skip_when_setup_pending(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
        )
        from nse_momentum_lab.services.paper.engine.paper_runtime import (
            PaperRuntimeState,
            evaluate_candle,
        )

        tracker = SessionPositionTracker()
        runtime = PaperRuntimeState()
        state = runtime.for_symbol("INFOSYS")
        state.setup_status = "pending"
        cfg = self._make_strategy_config()

        candle = _make_candle(open=1500.0, high=1600.0, low=1490.0, close=1580.0)
        result = evaluate_candle(
            symbol="INFOSYS",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=_make_session(),
            strategy_config=cfg,
        )
        assert result["action"] == "SKIP"
        assert "setup_pending" in result["reason"]

    def test_skip_after_entry_window_closes(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
        )
        from nse_momentum_lab.services.paper.engine.paper_runtime import evaluate_candle

        tracker = SessionPositionTracker()
        runtime = self._runtime_with_candidate("HDFC", prev_close=1700.0)
        cfg = self._make_strategy_config(entry_cutoff_minutes=30)

        # Bar at 09:50 → 35 minutes since open > cutoff of 30
        ts = _epoch_at_ist(9, 50)
        candle = _make_candle(high=1800.0, bar_end=ts)
        result = evaluate_candle(
            symbol="HDFC",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=_make_session(),
            strategy_config=cfg,
        )
        assert result["action"] == "SKIP"
        assert result["reason"] == "entry_window_closed"

    def test_short_entry_triggered(self):
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
        )
        from nse_momentum_lab.services.paper.engine.paper_runtime import evaluate_candle

        tracker = SessionPositionTracker()
        runtime = self._runtime_with_candidate("WIPRO", prev_close=500.0, direction="SHORT")
        cfg = self._make_strategy_config(direction="SHORT", threshold=0.02)

        # Breakdown: low must reach 500 * 0.98 = 490
        candle = _make_candle(open=495.0, high=497.0, low=488.0, close=491.0)
        result = evaluate_candle(
            symbol="WIPRO",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=_make_session(),
            strategy_config=cfg,
        )
        assert result["action"] == "ENTRY_CANDIDATE"
        assert result["direction"] == "SHORT"

    def test_eod_time_stop_on_open_position(self):
        """Bars past 15:15 are held (not force-closed) — EOD carry is post-market via eod-carry."""
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
            TrackedPosition,
        )
        from nse_momentum_lab.services.paper.engine.paper_runtime import evaluate_candle

        tracker = SessionPositionTracker()
        pos = TrackedPosition(
            position_id="p1",
            symbol="SBIN",
            direction="LONG",
            entry_price=500.0,
            stop_loss=480.0,
            target_price=None,
            entry_time="",
            quantity=10,
            current_qty=10,
            trail_state={
                "current_sl": 480.0,
                "trail_activation_pct": 0.08,
                "trail_stop_pct": 0.02,
                "phase": "PROTECT",
                "highest_since_entry": 500.0,
            },
        )
        tracker.record_open(pos, 5_000.0)

        runtime = self._runtime_with_candidate("SBIN", prev_close=490.0)
        runtime.for_symbol("SBIN").setup_status = "pending"

        ts = _epoch_at_ist(15, 16)  # After 15:15 — no longer force-closed by bar engine
        candle = _make_candle(open=510.0, high=515.0, low=508.0, close=512.0, bar_end=ts)
        cfg = self._make_strategy_config()

        result = evaluate_candle(
            symbol="SBIN",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=_make_session(flatten_time="15:15:00"),
            strategy_config=cfg,
        )
        # EXIT_EOD was removed: positions carry overnight via eod-carry post-market command.
        assert result["action"] == "HOLD"

    def test_gap_through_stop_long(self):
        """Bar opens below active stop → gap-through-stop close at open price."""
        from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
            SessionPositionTracker,
            TrackedPosition,
        )
        from nse_momentum_lab.services.paper.engine.paper_runtime import evaluate_candle

        tracker = SessionPositionTracker()
        pos = TrackedPosition(
            position_id="p2",
            symbol="TATAMOTORS",
            direction="LONG",
            entry_price=800.0,
            stop_loss=780.0,
            target_price=None,
            entry_time="",
            quantity=5,
            current_qty=5,
            trail_state={"current_sl": 790.0, "trail_activation_pct": 0.08, "trail_stop_pct": 0.02},
        )
        tracker.record_open(pos, 4_000.0)

        runtime = self._runtime_with_candidate("TATAMOTORS", prev_close=800.0)
        cfg = self._make_strategy_config()

        # Open at 785, which is BELOW the 790 stop
        candle = _make_candle(open=785.0, high=788.0, low=782.0, close=786.0)
        result = evaluate_candle(
            symbol="TATAMOTORS",
            candle=candle,
            runtime_state=runtime,
            tracker=tracker,
            session=_make_session(),
            strategy_config=cfg,
        )
        assert result["action"] == "CLOSE"
        assert result["reason"] == "GAP_THROUGH_STOP"
        assert result["exit_price"] == pytest.approx(785.0)


class TestEnforceSessionRiskControls:
    def test_no_breach_when_pnl_positive(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import (
            enforce_session_risk_controls,
        )

        result = enforce_session_risk_controls(
            session=_make_session(max_daily_loss_pct=0.05),
            positions=[],
            as_of=datetime(2026, 4, 1, 10, 0, 0, tzinfo=_IST),
            portfolio_value=1_000_000.0,
            realized_pnl=5_000.0,
        )
        assert not result["triggered"]

    def test_daily_loss_limit_triggered_by_realized_pnl(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import (
            enforce_session_risk_controls,
        )

        # 5% of 1_000_000 = 50_000 → -51_000 should trigger
        result = enforce_session_risk_controls(
            session=_make_session(max_daily_loss_pct=0.05),
            positions=[],
            as_of=datetime(2026, 4, 1, 10, 0, 0, tzinfo=_IST),
            portfolio_value=1_000_000.0,
            realized_pnl=-51_000.0,
        )
        assert result["triggered"]
        assert any("daily_loss_limit" in r for r in result["reasons"])

    def test_daily_loss_triggered_by_unrealized_pnl(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import (
            enforce_session_risk_controls,
        )

        # Realized safe, but unrealized tips over the limit
        result = enforce_session_risk_controls(
            session=_make_session(max_daily_loss_pct=0.05),
            positions=[{"pnl": None}],  # open position, pnl unknown
            as_of=datetime(2026, 4, 1, 10, 0, 0, tzinfo=_IST),
            portfolio_value=1_000_000.0,
            realized_pnl=-20_000.0,
            unrealized_pnl=-35_000.0,  # total -55_000 > -50_000 limit
        )
        assert result["triggered"]

    def test_flatten_time_triggers(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import (
            enforce_session_risk_controls,
        )

        result = enforce_session_risk_controls(
            session=_make_session(flatten_time="15:15:00"),
            positions=[],
            as_of=datetime(2026, 4, 1, 15, 16, 0, tzinfo=_IST),
            portfolio_value=1_000_000.0,
        )
        assert result["triggered"]
        assert any("flatten_time" in r for r in result["reasons"])

    def test_no_trigger_before_flatten_time(self):
        from nse_momentum_lab.services.paper.engine.paper_runtime import (
            enforce_session_risk_controls,
        )

        result = enforce_session_risk_controls(
            session=_make_session(flatten_time="15:15:00"),
            positions=[],
            as_of=datetime(2026, 4, 1, 14, 0, 0, tzinfo=_IST),
            portfolio_value=1_000_000.0,
        )
        assert not result["triggered"]


# ---------------------------------------------------------------------------
# paper_db tests (in-memory DuckDB)
# ---------------------------------------------------------------------------


@pytest.fixture
def paper_db(tmp_path):
    """Open an in-memory PaperDB for the duration of the test."""
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    db = PaperDB(":memory:")
    yield db
    db.close()


@pytest.fixture
def session_id(paper_db):
    """Create a minimal session and return its id."""
    session = paper_db.create_session(
        strategy_name="2lynchbreakout",
        mode="replay",
        trade_date=date(2026, 4, 1),
    )
    return session["session_id"]


class TestPaperDBLifecycle:
    def test_connect_on_init(self):
        """PaperDB should be usable immediately after construction."""
        from nse_momentum_lab.services.paper.db.paper_db import PaperDB

        db = PaperDB(":memory:")
        sessions = db.list_sessions()
        assert isinstance(sessions, list)
        db.close()

    def test_context_manager(self):
        from nse_momentum_lab.services.paper.db.paper_db import PaperDB

        with PaperDB(":memory:") as db:
            sessions = db.list_sessions()
            assert isinstance(sessions, list)


class TestPaperDBTransaction:
    def test_transaction_rollback_on_exception(self, paper_db, session_id):
        """A transaction that raises should leave the DB unchanged."""
        before = paper_db.list_positions_by_session(session_id)
        try:
            with paper_db.transaction():
                paper_db.insert_position(
                    session_id=session_id,
                    symbol="RELIANCE",
                    direction="LONG",
                    avg_entry=2500.0,
                    qty=10,
                    state="OPEN",
                )
                raise RuntimeError("deliberate rollback")
        except RuntimeError:
            pass
        after = paper_db.list_positions_by_session(session_id)
        assert len(after) == len(before)  # nothing committed

    def test_transaction_commits_on_success(self, paper_db, session_id):
        with paper_db.transaction():
            paper_db.insert_position(
                session_id=session_id,
                symbol="TCS",
                direction="LONG",
                avg_entry=3500.0,
                qty=5,
                state="OPEN",
            )
        after = paper_db.list_positions_by_session(session_id)
        assert any(p["symbol"] == "TCS" for p in after)


class TestInsertSignalIdempotency:
    def test_duplicate_insert_does_not_create_second_row(self, paper_db, session_id):
        asof = date(2026, 4, 1)
        sig1 = paper_db.insert_signal(
            session_id=session_id, symbol="RELIANCE", asof_date=asof, state="NEW"
        )
        sig2 = paper_db.insert_signal(
            session_id=session_id, symbol="RELIANCE", asof_date=asof, state="QUALIFIED"
        )
        # Should return the same signal_id (upsert semantics)
        assert sig1["signal_id"] == sig2["signal_id"]

    def test_different_symbols_get_different_ids(self, paper_db, session_id):
        asof = date(2026, 4, 1)
        sig_a = paper_db.insert_signal(
            session_id=session_id, symbol="A", asof_date=asof, state="NEW"
        )
        sig_b = paper_db.insert_signal(
            session_id=session_id, symbol="B", asof_date=asof, state="NEW"
        )
        assert sig_a["signal_id"] != sig_b["signal_id"]


class TestFlattenOpenPositions:
    def _open_position(self, paper_db, session_id, symbol, direction, entry, qty):
        return paper_db.insert_position(
            session_id=session_id,
            symbol=symbol,
            direction=direction,
            avg_entry=entry,
            qty=qty,
            state="OPEN",
        )

    def test_long_pnl_positive_when_exit_above_entry(self, paper_db, session_id):
        self._open_position(paper_db, session_id, "RELIANCE", "LONG", 2500.0, 10)
        closed = paper_db.flatten_open_positions(session_id, mark_prices={"RELIANCE": 2600.0})
        assert len(closed) == 1
        assert closed[0]["pnl"] == pytest.approx(1000.0)  # (2600-2500)*10

    def test_short_pnl_positive_when_exit_below_entry(self, paper_db, session_id):
        self._open_position(paper_db, session_id, "WIPRO", "SHORT", 500.0, 20)
        closed = paper_db.flatten_open_positions(session_id, mark_prices={"WIPRO": 480.0})
        assert len(closed) == 1
        assert closed[0]["pnl"] == pytest.approx(400.0)  # (500-480)*20

    def test_short_pnl_negative_when_exit_above_entry(self, paper_db, session_id):
        self._open_position(paper_db, session_id, "TCS", "SHORT", 3000.0, 5)
        closed = paper_db.flatten_open_positions(session_id, mark_prices={"TCS": 3100.0})
        assert closed[0]["pnl"] == pytest.approx(-500.0)  # (3000-3100)*5

    def test_fallback_to_avg_entry_when_no_mark_price(self, paper_db, session_id):
        self._open_position(paper_db, session_id, "INFY", "LONG", 1400.0, 10)
        # No mark_prices → falls back to avg_entry → zero P&L
        closed = paper_db.flatten_open_positions(session_id)
        assert closed[0]["pnl"] == pytest.approx(0.0)

    def test_positions_are_closed_after_flatten(self, paper_db, session_id):
        self._open_position(paper_db, session_id, "HDFCBANK", "LONG", 1600.0, 5)
        paper_db.flatten_open_positions(session_id, mark_prices={"HDFCBANK": 1650.0})
        open_after = paper_db.list_open_positions(session_id)
        assert open_after == []

    def test_close_side_is_buy_for_short(self, paper_db, session_id):
        self._open_position(paper_db, session_id, "AXISBANK", "SHORT", 900.0, 10)
        paper_db.flatten_open_positions(session_id, mark_prices={"AXISBANK": 880.0})
        # Inspect the order created — should be a BUY to close the short
        orders = paper_db.list_orders_by_session(session_id)
        close_orders = [o for o in orders if o.get("symbol") == "AXISBANK"]
        assert any(o["side"] == "BUY" for o in close_orders)


class TestPatchPositionMetadata:
    def test_merge_without_overwriting_other_keys(self, paper_db, session_id):
        pos = paper_db.insert_position(
            session_id=session_id,
            symbol="SBIN",
            direction="LONG",
            avg_entry=500.0,
            qty=20,
            state="OPEN",
            metadata_json={"signal_id": "sig-abc", "phase": "PROTECT"},
        )
        paper_db.patch_position_metadata(pos["position_id"], last_mark_price=510.0)
        updated = paper_db.get_position(pos["position_id"])
        meta = updated["metadata_json"]
        # Existing keys preserved
        assert meta.get("signal_id") == "sig-abc"
        assert meta.get("phase") == "PROTECT"
        # New key merged in
        assert meta.get("last_mark_price") == pytest.approx(510.0)


# ---------------------------------------------------------------------------
# alert_dispatcher: _redact_url
# ---------------------------------------------------------------------------


class TestRedactUrl:
    def test_redacts_bot_token_in_url(self):
        from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import _redact_url

        url = "https://api.telegram.org/bot1234567890:ABC-xyz_TOKEN/sendMessage"
        redacted = _redact_url(url)
        assert "1234567890" not in redacted
        assert "ABC-xyz_TOKEN" not in redacted
        assert "<REDACTED>" in redacted
        assert "api.telegram.org" in redacted

    def test_preserves_non_telegram_urls(self):
        from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import _redact_url

        url = "https://example.com/api/v1/data"
        assert _redact_url(url) == url

    def test_redacts_in_longer_error_string(self):
        from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import _redact_url

        msg = "HTTPError: POST https://api.telegram.org/bot9876:TOKEN123/sendMessage failed"
        redacted = _redact_url(msg)
        assert "TOKEN123" not in redacted

    def test_empty_string_safe(self):
        from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import _redact_url

        assert _redact_url("") == ""

    def test_case_insensitive(self):
        from nse_momentum_lab.services.paper.notifiers.alert_dispatcher import _redact_url

        url = "HTTPS://API.TELEGRAM.ORG/BOT12345:SECRET/sendMessage"
        redacted = _redact_url(url)
        assert "SECRET" not in redacted
