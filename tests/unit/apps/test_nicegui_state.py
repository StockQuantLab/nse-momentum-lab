from __future__ import annotations

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[3]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import apps.nicegui.state as state  # noqa: E402


class _DummyExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, bool]] = []

    def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
        self.calls.append((wait, cancel_futures))


def test_shutdown_dashboard_resources_is_idempotent(monkeypatch) -> None:
    executor = _DummyExecutor()
    pg_executor = _DummyExecutor()
    closers: list[str] = []

    monkeypatch.setattr(state, "_executor", executor)
    monkeypatch.setattr(state, "_pg_executor", pg_executor)
    monkeypatch.setattr(state, "close_market_db", lambda: closers.append("market"))
    monkeypatch.setattr(state, "close_backtest_db", lambda: closers.append("backtest"))
    monkeypatch.setattr(state, "_dashboard_resources_closed", False)

    state.shutdown_dashboard_resources()
    state.shutdown_dashboard_resources()

    assert executor.calls == [(False, True)]
    assert pg_executor.calls == [(False, True)]
    assert closers == ["market", "backtest"]
