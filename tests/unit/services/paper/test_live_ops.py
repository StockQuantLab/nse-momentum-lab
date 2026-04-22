"""Unit tests for paper live-ops wiring.

These tests cover the ops-layer fixes added for live websocket runs:
- retry wrapper keeps alert dedup state across attempts
- CLI live command uses the retry wrapper
- EOD carry CLI forwards --no-alerts
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


def test_should_retry_excludes_risk_breach_and_no_symbols():
    from nse_momentum_lab.services.paper.scripts.paper_live import _should_retry

    assert _should_retry({"status": "FAILED"}, 1, 5)
    assert _should_retry({"status": "STALE"}, 1, 5)
    assert not _should_retry({"status": "RISK_BREACH"}, 1, 5)
    assert not _should_retry({"status": "NO_SYMBOLS"}, 1, 5)
    assert not _should_retry({"error": "Session sess-1 not found"}, 1, 5)
    assert not _should_retry({"error": "No symbols in session"}, 1, 5)


@pytest.mark.asyncio
async def test_run_live_session_with_retry_reuses_alert_dedup(monkeypatch):
    from nse_momentum_lab.services.paper.scripts import paper_live

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
