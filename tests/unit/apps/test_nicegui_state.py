from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

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


def test_build_experiment_options_includes_breakout_threshold() -> None:
    experiments_df = pl.DataFrame(
        [
            {
                "exp_id": "exp-1",
                "strategy_name": "2LYNCHBreakout",
                "params_json": '{"breakout_threshold": 0.02, "start_date": "2025-01-01", "end_date": "2026-03-27"}',
                "total_trades": 123,
                "total_return_pct": 45.6,
                "created_at": datetime(2026, 3, 29, 22, 25, tzinfo=UTC),
                "start_year": 2025,
                "end_year": 2026,
            }
        ]
    )

    options = state.build_experiment_options(experiments_df)
    label = next(iter(options))

    assert "2LYNCHBreakout 2%" in label
    assert "2025-01-01 to 2026-03-27" in label
    assert options[label] == "exp-1"


def test_market_monitor_latest_uses_cache(monkeypatch) -> None:
    calls: list[str] = []
    sample = pl.DataFrame([{"trading_date": datetime(2026, 3, 30, tzinfo=UTC), "posture": "aggressive"}])

    class _DummyMarketDb:
        def get_market_monitor_latest(self) -> pl.DataFrame:
            calls.append("latest")
            return sample

    monkeypatch.setattr(state, "get_db", lambda: _DummyMarketDb())
    monkeypatch.setattr(state, "_market_monitor_cache", {})

    first = state.get_market_monitor_latest()
    second = state.get_market_monitor_latest()

    assert calls == ["latest"]
    assert first.to_dicts() == second.to_dicts()
    assert first is not second


def test_market_monitor_latest_uses_ttl_cache(monkeypatch) -> None:
    calls: list[str] = []
    frame = pl.DataFrame([{"trade_date": "2026-03-30", "posture": "aggressive"}])

    class _DummyDB:
        def get_market_monitor_latest(self) -> pl.DataFrame:
            calls.append("latest")
            return frame

    monkeypatch.setattr(state, "get_db", lambda: _DummyDB())
    monkeypatch.setattr(state, "_market_monitor_cache", {})

    first = state.get_market_monitor_latest()
    second = state.get_market_monitor_latest()

    assert calls == ["latest"]
    assert first.to_dicts() == second.to_dicts() == frame.to_dicts()
