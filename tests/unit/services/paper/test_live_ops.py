"""Unit tests for paper live-ops wiring.

These tests cover the ops-layer fixes added for live websocket runs:
- retry wrapper keeps alert dedup state across attempts
- CLI live command uses the retry wrapper
- EOD carry CLI forwards --no-alerts
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


def test_should_retry_excludes_risk_breach_and_no_symbols(monkeypatch):
    from datetime import timezone, timedelta
    from nse_momentum_lab.services.paper.scripts import paper_live

    # Freeze time at 10:00 IST so the time-of-day gate never fires.
    _IST = timezone(timedelta(hours=5, minutes=30))
    _before_cutoff = datetime(2026, 4, 25, 10, 0, 0, tzinfo=_IST)

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return _before_cutoff.astimezone(tz) if tz else _before_cutoff

    monkeypatch.setattr(paper_live, "datetime", _FakeDT)

    assert paper_live._should_retry({"status": "FAILED"}, 1, 5)
    assert paper_live._should_retry({"status": "STALE"}, 1, 5)
    assert not paper_live._should_retry({"status": "RISK_BREACH"}, 1, 5)
    assert not paper_live._should_retry({"status": "NO_SYMBOLS"}, 1, 5)
    assert not paper_live._should_retry({"error": "Session sess-1 not found"}, 1, 5)
    assert not paper_live._should_retry({"error": "No symbols in session"}, 1, 5)


@pytest.mark.asyncio
async def test_run_live_session_with_retry_reuses_alert_dedup(monkeypatch):
    from datetime import timezone, timedelta
    from nse_momentum_lab.services.paper.scripts import paper_live

    # Freeze time at 10:00 IST so _should_retry's time-of-day gate never fires.
    _IST = timezone(timedelta(hours=5, minutes=30))
    _before_cutoff = datetime(2026, 4, 25, 10, 0, 0, tzinfo=_IST)

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return _before_cutoff.astimezone(tz) if tz else _before_cutoff

    monkeypatch.setattr(paper_live, "datetime", _FakeDT)

    call_state: list[tuple[int, bool, set[str]]] = []

    async def fake_run_live_session(*, alerts_sent=None, auto_flatten_on_error=True, **kwargs):
        assert alerts_sent is not None
        call_state.append((id(alerts_sent), auto_flatten_on_error, set(alerts_sent)))
        if len(call_state) == 1:
            alerts_sent.add("SESSION_STARTED:sess-1")
            return {"session_id": "sess-1", "status": "FAILED"}
        return {"session_id": "sess-1", "status": "COMPLETED"}

    monkeypatch.setattr(paper_live, "run_live_session", fake_run_live_session)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await paper_live.run_live_session_with_retry(
            session_id="sess-1",
            max_retries=2,
        )

    assert result["status"] == "COMPLETED"
    assert len(call_state) == 2
    assert call_state[0][0] == call_state[1][0]
    assert call_state[0][1] is False
    assert call_state[1][1] is True
    assert call_state[1][2] == {"SESSION_STARTED:sess-1"}


def test_resolve_kite_credentials_falls_back_to_env(monkeypatch):
    from nse_momentum_lab.services.paper.scripts import paper_live

    def broken_get_settings():
        raise ValueError("settings unavailable")

    monkeypatch.setattr(paper_live, "get_settings", broken_get_settings)
    monkeypatch.setenv("KITE_API_KEY", "env-api-key")
    monkeypatch.setenv("KITE_ACCESS_TOKEN", "env-access-token")

    assert paper_live._resolve_kite_credentials() == ("env-api-key", "env-access-token")


def test_resolve_kite_instrument_map_uses_kite_auth(monkeypatch):
    from nse_momentum_lab.services.paper.scripts import paper_live

    class FakeAuth:
        def get_instrument_token(self, symbol, exchange="NSE"):
            assert exchange == "NSE"
            return {"ABC": 111, "XYZ": None}.get(symbol)

    monkeypatch.setattr(paper_live, "KiteAuth", FakeAuth)

    assert paper_live._resolve_kite_instrument_map(["XYZ", "ABC", "ABC"]) == {"ABC": 111}


def test_terminal_session_status_maps_terminal_states():
    from types import SimpleNamespace

    from nse_momentum_lab.services.paper.scripts import paper_live

    assert paper_live._terminal_session_status("NO_SYMBOLS", None) == "COMPLETED"
    assert paper_live._terminal_session_status("COMPLETED", None) == "COMPLETED"
    assert paper_live._terminal_session_status("RISK_BREACH", None) == "FAILED"
    assert paper_live._terminal_session_status(
        "STOPPING", SimpleNamespace(open_count=0)
    ) == "COMPLETED"
    assert paper_live._terminal_session_status(
        "STOPPING", SimpleNamespace(open_count=3)
    ) == "STOPPING"


def test_session_alert_lookup_uses_alert_log(monkeypatch, tmp_path):
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB
    from nse_momentum_lab.services.paper.scripts import paper_live

    db = PaperDB(str(tmp_path / "paper.duckdb"))
    try:
        db.create_session(
            session_id="sess-1",
            trade_date=None,
            strategy_name="2lynchbreakout",
            mode="paper",
            status="ACTIVE",
        )
        assert not paper_live._session_alert_already_sent(
            db, "sess-1", paper_live.AlertType.SESSION_STARTED
        )
        db.insert_alert_log(
            session_id="sess-1",
            alert_type=paper_live.AlertType.SESSION_STARTED.value,
            channel="TelegramNotifier",
            status="sent",
            payload={"subject": "started"},
        )
        assert paper_live._session_alert_already_sent(
            db, "sess-1", paper_live.AlertType.SESSION_STARTED
        )
    finally:
        db.close()


def test_log_direction_readiness_reads_seeded_symbols(caplog):
    from nse_momentum_lab.services.paper.scripts import paper_live

    runtime_state = SimpleNamespace(
        symbols={
            "AAA": SimpleNamespace(setup_status="candidate"),
            "BBB": SimpleNamespace(setup_status="skipped"),
        }
    )

    with caplog.at_level("INFO"):
        paper_live._log_direction_readiness(runtime_state, "sess-1")

    assert "LIVE_STARTUP_READY: 1/2" in caplog.text


def test_feed_transition_alert_state_dedups_across_calls():
    from nse_momentum_lab.services.paper.scripts import paper_live

    class DummyDB:
        def __init__(self):
            self.feed_state = {
                "session_id": "sess-1",
                "source": "kite",
                "mode": "paper",
                "status": "OK",
                "is_stale": False,
                "subscription_count": 10,
                "raw_state": {"alert_state": {"last_emitted_state": "OK"}},
            }

        def get_feed_state(self, session_id):
            return self.feed_state

        def upsert_feed_state(self, **kwargs):
            self.feed_state = {**self.feed_state, **kwargs}
            return self.feed_state

    class DummyDispatcher:
        def __init__(self):
            self.events = []

        def enqueue(self, event):
            self.events.append(event)

    db = DummyDB()
    dispatcher = DummyDispatcher()
    alert_state = {"last_emitted_state": "OK"}

    emitted = paper_live._maybe_emit_feed_transition(
        paper_db=db,
        session_id="sess-1",
        alert_dispatcher=dispatcher,
        alert_state=alert_state,
        next_state="STALE",
        details="No data for 3 cycles",
    )
    assert emitted is True
    assert dispatcher.events[-1].alert_type == paper_live.AlertType.FEED_STALE
    assert db.feed_state["raw_state"]["alert_state"]["last_emitted_state"] == "STALE"

    emitted_again = paper_live._maybe_emit_feed_transition(
        paper_db=db,
        session_id="sess-1",
        alert_dispatcher=dispatcher,
        alert_state=alert_state,
        next_state="STALE",
        details="No data for 4 cycles",
    )
    assert emitted_again is False
    assert len(dispatcher.events) == 1

    recovered = paper_live._maybe_emit_feed_transition(
        paper_db=db,
        session_id="sess-1",
        alert_dispatcher=dispatcher,
        alert_state=alert_state,
        next_state="OK",
        details="Live data flow restored",
    )
    assert recovered is True
    assert dispatcher.events[-1].alert_type == paper_live.AlertType.FEED_RECOVERED
    assert db.feed_state["raw_state"]["alert_state"]["last_emitted_state"] == "OK"


def test_feed_transition_stale_cooldown_suppresses_flappy_realert():
    from nse_momentum_lab.services.paper.scripts import paper_live

    class DummyDB:
        def __init__(self):
            self.feed_state = {
                "session_id": "sess-1",
                "source": "kite",
                "mode": "paper",
                "status": "OK",
                "is_stale": False,
                "subscription_count": 10,
                "raw_state": {"alert_state": {"last_emitted_state": "OK"}},
            }

        def get_feed_state(self, session_id):
            return self.feed_state

        def upsert_feed_state(self, **kwargs):
            self.feed_state = {**self.feed_state, **kwargs}
            return self.feed_state

    class DummyDispatcher:
        def __init__(self):
            self.events = []

        def enqueue(self, event):
            self.events.append(event)

    db = DummyDB()
    dispatcher = DummyDispatcher()
    alert_state = {"last_emitted_state": "OK"}
    t0 = datetime(2026, 4, 22, 14, 14, 0, tzinfo=UTC)

    assert paper_live._maybe_emit_feed_transition(
        paper_db=db,
        session_id="sess-1",
        alert_dispatcher=dispatcher,
        alert_state=alert_state,
        next_state="STALE",
        details="transport=websocket last_tick_age=301s",
        now_ts=t0,
    )
    assert paper_live._maybe_emit_feed_transition(
        paper_db=db,
        session_id="sess-1",
        alert_dispatcher=dispatcher,
        alert_state=alert_state,
        next_state="OK",
        details="Live data flow restored",
        now_ts=t0.replace(minute=15),
    )
    suppressed = paper_live._maybe_emit_feed_transition(
        paper_db=db,
        session_id="sess-1",
        alert_dispatcher=dispatcher,
        alert_state=alert_state,
        next_state="STALE",
        details="transport=websocket last_tick_age=301s",
        now_ts=t0.replace(minute=16),
    )

    assert suppressed is False
    assert [event.alert_type for event in dispatcher.events] == [
        paper_live.AlertType.FEED_STALE,
        paper_live.AlertType.FEED_RECOVERED,
    ]


def test_live_tick_feed_status_uses_tick_age_not_bar_gap():
    from nse_momentum_lab.services.paper.scripts import paper_live

    assert paper_live._live_tick_feed_status(now_ts=100.0, last_tick_ts=None) == ("OK", False, None)
    assert paper_live._live_tick_feed_status(now_ts=100.0, last_tick_ts=95.0) == ("OK", False, 5.0)
    assert paper_live._live_tick_feed_status(now_ts=700.0, last_tick_ts=399.0) == (
        "STALE",
        True,
        301.0,
    )


def test_format_feed_stale_details_includes_manual_position_lines():
    from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
        SessionPositionTracker,
        TrackedPosition,
    )
    from nse_momentum_lab.services.paper.scripts import paper_live

    tracker = SessionPositionTracker()
    tracker.record_open(
        TrackedPosition(
            position_id="pos-1",
            symbol="GTPL",
            direction="SHORT",
            entry_price=68.36,
            stop_loss=68.45,
            target_price=67.94,
            entry_time="09:45",
            quantity=1462,
            current_qty=1462,
        ),
        68.36 * 1462,
    )

    details = paper_live._format_feed_stale_details(
        transport="websocket",
        streak=4,
        tick_age_sec=305.0,
        last_tick_ts=1713774972.0,
        tracker=tracker,
    )

    assert "Feed stale" in details
    assert "Transport: <code>websocket</code>" in details
    assert "⚡ <b>Open positions</b> — place manual SL orders now:" in details
    assert "GTPL" in details
    assert "Entry: <code>₹68.36</code>" in details
    assert "SL: <code>₹68.45</code>" in details
    assert "Target: <code>₹67.94</code>" in details
    assert "Qty: <code>1,462</code>" in details


def test_format_feed_recovered_details_matches_operator_summary():
    from nse_momentum_lab.services.paper.engine.bar_orchestrator import SessionPositionTracker
    from nse_momentum_lab.services.paper.scripts import paper_live

    tracker = SessionPositionTracker()
    details = paper_live._format_feed_recovered_details(
        stale_cycles=1,
        tracker=tracker,
        reconnect_count=0,
        down_duration_sec=52,
    )

    assert "Feed recovered" in details
    assert "Stale cycles: <code>1</code>" in details
    assert "WebSocket down: <code>52s</code>" in details
    assert "Reconnects: <code>0</code>" in details
    assert "Monitoring: <code>0</code> open position(s)" in details


def test_cmd_live_uses_retry_wrapper_and_forwards_no_alerts(monkeypatch):
    from nse_momentum_lab.cli import paper_v2

    captured: dict[str, object] = {}

    async def fake_retry(**kwargs):
        captured.update(kwargs)
        return {"session_id": "sess-1", "status": "COMPLETED"}

    monkeypatch.setattr(
        "nse_momentum_lab.services.paper.scripts.paper_live.run_live_session_with_retry",
        fake_retry,
    )
    monkeypatch.setattr(paper_v2, "_resolve_session_id", lambda *a, **k: "sess-1")
    monkeypatch.setattr(paper_v2, "_run_async", lambda coro: asyncio.run(coro))

    args = SimpleNamespace(
        paper_db="paper.duckdb",
        market_db="market.duckdb",
        poll_interval=1.25,
        max_cycles=3,
        no_alerts=True,
    )

    with pytest.raises(SystemExit) as excinfo:
        paper_v2._cmd_live(args)

    assert excinfo.value.code == 0
    assert captured["session_id"] == "sess-1"
    assert captured["no_alerts"] is True


def test_cmd_multi_live_resolves_multiple_sessions(monkeypatch):
    from nse_momentum_lab.cli import paper_v2

    captured: dict[str, object] = {}

    async def fake_group(**kwargs):
        captured.update(kwargs)
        return {
            sid: {"session_id": sid, "status": "COMPLETED"}
            for sid in kwargs["session_ids"]
        }

    def fake_resolve_session_id(args, db_path, *, mode, trade_date=None):
        assert db_path == "paper.duckdb"
        assert mode == "live"
        return f"{args.strategy}-session"

    monkeypatch.setattr(
        "nse_momentum_lab.services.paper.scripts.paper_live.run_live_session_group",
        fake_group,
    )
    monkeypatch.setattr(paper_v2, "_resolve_session_id", fake_resolve_session_id)
    monkeypatch.setattr(paper_v2, "_run_async", lambda coro: asyncio.run(coro))

    args = SimpleNamespace(
        paper_db="paper.duckdb",
        market_db="market.duckdb",
        poll_interval=1.25,
        max_cycles=3,
        no_alerts=False,
        session_ids=[],
        strategies=["2lynchbreakout", "2lynchbreakdown"],
        trade_date="2026-04-22",
    )

    with pytest.raises(SystemExit) as excinfo:
        paper_v2._cmd_multi_live(args)

    assert excinfo.value.code == 0
    assert captured["session_ids"] == ["2lynchbreakout-session", "2lynchbreakdown-session"]
    assert captured["poll_interval"] == 1.25


def test_find_matching_resumable_session_filters_by_preset():
    from datetime import date

    from nse_momentum_lab.cli import paper_v2

    class FakeDB:
        def list_sessions(self, limit=200):
            return [
                {
                    "session_id": "sess-4pct",
                    "status": "PLANNED",
                    "strategy_name": "2lynchbreakout",
                    "mode": "live",
                    "trade_date": "2026-04-22",
                    "strategy_params": {"preset_name": "BREAKOUT_4PCT"},
                },
                {
                    "session_id": "sess-2pct",
                    "status": "PAUSED",
                    "strategy_name": "2lynchbreakout",
                    "mode": "live",
                    "trade_date": "2026-04-22",
                    "strategy_params": {"preset_name": "BREAKOUT_2PCT"},
                },
            ]

    match = paper_v2._find_matching_resumable_session(
        FakeDB(),
        strategy_name="2lynchbreakout",
        trade_date=date(2026, 4, 22),
        mode="live",
        preset_name="BREAKOUT_2PCT",
    )

    assert match is not None
    assert match["session_id"] == "sess-2pct"


def test_cmd_prepare_with_preset_persists_preset_config(monkeypatch, capsys):
    from nse_momentum_lab.cli import paper_v2

    created: dict[str, object] = {}

    class FakeDB:
        def __init__(self, path):
            self.path = path

        def list_sessions(self, limit=200):
            return []

        def create_session(self, **kwargs):
            created.update(kwargs)
            return {"session_id": "sess-1"}

        def close(self):
            return None

    monkeypatch.setattr(
        "nse_momentum_lab.services.paper.db.paper_db.PaperDB",
        FakeDB,
    )
    monkeypatch.setattr(paper_v2, "_load_default_symbols", lambda *a, **k: ["AAA", "BBB"])

    args = SimpleNamespace(
        paper_db="paper.duckdb",
        market_db="market.duckdb",
        strategy="2lynchbreakout",
        preset="BREAKOUT_2PCT",
        mode="live",
        trade_date="2026-04-22",
        symbols="",
        portfolio_value=1_000_000,
        risk_config=None,
        metadata='{"owner":"ops"}',
    )

    paper_v2._cmd_prepare(args)

    output = capsys.readouterr().out
    assert '"preset": "BREAKOUT_2PCT"' in output
    assert created["strategy_name"] == "2lynchbreakout"
    assert created["symbols"] == ["AAA", "BBB"]
    assert created["strategy_params"]["preset_name"] == "BREAKOUT_2PCT"
    assert created["strategy_params"]["breakout_threshold"] == pytest.approx(0.02)
    assert created["strategy_params"]["extra_params"]["owner"] == "ops"


def test_cmd_eod_carry_forwards_no_alerts(monkeypatch):
    from nse_momentum_lab.cli import paper_v2

    captured: dict[str, object] = {}

    def fake_run_eod_carry(**kwargs):
        captured.update(kwargs)
        return {"session_id": "sess-1", "status": "COMPLETED"}

    monkeypatch.setattr(
        "nse_momentum_lab.services.paper.scripts.paper_eod_carry.run_eod_carry",
        fake_run_eod_carry,
    )
    monkeypatch.setattr(paper_v2, "_resolve_session_id", lambda *a, **k: "sess-1")

    args = SimpleNamespace(
        paper_db="paper.duckdb",
        market_db="market.duckdb",
        trade_date="2026-04-22",
        session_id=None,
        strategy="2lynchbreakout",
        no_alerts=True,
    )

    with pytest.raises(SystemExit) as excinfo:
        paper_v2._cmd_eod_carry(args)

    assert excinfo.value.code == 0
    assert captured["no_alerts"] is True
    assert captured["paper_db_path"] == "paper.duckdb"


def test_get_paper_strategy_config_allows_none_optional_override():
    from nse_momentum_lab.services.paper.engine.strategy_presets import get_paper_strategy_config

    cfg = get_paper_strategy_config(
        "2lynchbreakdown",
        overrides={"short_trail_activation_pct": None},
    )

    assert cfg.short_trail_activation_pct is None


def test_seed_candidates_uses_session_strategy_threshold(monkeypatch):
    import polars as pl

    from nse_momentum_lab.services.paper.engine import paper_runtime

    captured: dict[str, object] = {}

    def fake_build_candidate_query(params, symbols, start, end):
        captured["threshold"] = params.breakout_threshold
        return "select 1 where 1 = 0", []

    monkeypatch.setattr(
        "nse_momentum_lab.services.backtest.strategy_registry.resolve_strategy",
        lambda strategy_name: SimpleNamespace(build_candidate_query=fake_build_candidate_query),
    )

    class FakeResult:
        def pl(self):
            return pl.DataFrame()

    class FakeConn:
        def execute(self, query, params):
            return FakeResult()

    market_db = SimpleNamespace(con=FakeConn())
    runtime_state = paper_runtime.PaperRuntimeState()
    strategy_config = SimpleNamespace(
        strategy_key="2lynchbreakout",
        breakout_threshold=0.02,
        extra_params={},
    )

    seeded = paper_runtime.seed_candidates_from_market_db(
        market_db,
        runtime_state,
        ["AAA"],
        "2026-04-22",
        direction="LONG",
        strategy_config=strategy_config,
    )

    assert seeded == 0
    assert captured["threshold"] == pytest.approx(0.02)
