from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import nse_momentum_lab.services.kite.scheduler as scheduler
from nse_momentum_lab.services.kite.scheduler import CheckpointState, KiteScheduler
from nse_momentum_lab.utils.constants import IngestionDataset, IngestionUniverse


def test_daily_ingestion_refreshes_features_incrementally(monkeypatch) -> None:
    calls: list[date] = []

    monkeypatch.setattr(
        KiteScheduler,
        "_resolve_symbols",
        lambda self, **kwargs: ["AAA"],
    )
    monkeypatch.setattr(
        KiteScheduler,
        "_load_checkpoint",
        lambda self, **kwargs: CheckpointState(path=Path("ignored.json"), completed_symbols=set()),
    )

    scheduler_obj = KiteScheduler(
        auth=SimpleNamespace(get_instrument_token=lambda symbol: 101),
        writer=SimpleNamespace(
            fetch_and_write_daily=lambda **kwargs: 1,
            fetch_and_write_5min=lambda **kwargs: 1,
        ),
    )

    def _refresh_features(summary, *, start_date):
        calls.append(start_date)
        summary["features_refreshed"] = True

    monkeypatch.setattr(scheduler_obj, "_refresh_features", _refresh_features)

    summary = scheduler_obj._run_ingestion(
        dataset=IngestionDataset.DAILY,
        symbols=None,
        start_date=date(2026, 3, 21),
        end_date=date(2026, 3, 24),
        save_raw=False,
        resume=False,
        mode="append",
        update_features=True,
        universe=IngestionUniverse.LOCAL_FIRST,
    )

    assert calls == [date(2026, 3, 21)]
    assert summary["succeeded"] == 1
    assert summary["features_refreshed"] is True
    assert summary["checkpoint_cleared"] is False


def test_refresh_features_uses_incremental_market_monitor(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class DummyDB:
        def _build_modular_features(self, *, force: bool = False, since_date=None) -> None:
            calls.append(("features", force, since_date))

        def build_market_monitor_incremental(self, since_date=None, *, force: bool = False) -> int:
            calls.append(("monitor", since_date, force))
            return 17

    monkeypatch.setattr(
        scheduler,
        "get_market_db",
        lambda: DummyDB(),
    )

    scheduler_obj = KiteScheduler(auth=SimpleNamespace(), writer=SimpleNamespace())
    summary: dict[str, object] = {}

    scheduler_obj._refresh_features(summary, start_date=date(2026, 3, 21))

    assert calls == [
        ("features", False, date(2026, 3, 21)),
        ("monitor", date(2026, 3, 21), False),
    ]
    assert summary == {
        "features_refreshed": True,
        "market_monitor_refreshed": True,
        "market_monitor_rows": 17,
    }


def test_get_ingestion_status_reads_available_ranges(monkeypatch) -> None:
    class DummyCon:
        def execute(self, query: str):
            if "FROM v_daily" in query:
                return SimpleNamespace(
                    fetchone=lambda: (date(2026, 3, 21), date(2026, 3, 25), 1435)
                )
            if "FROM v_5min" in query:
                return SimpleNamespace(
                    fetchone=lambda: (date(2026, 3, 10), date(2026, 3, 20), 1435)
                )
            raise AssertionError(query)

    class DummyDB:
        _has_daily = True
        _has_5min = True
        con = DummyCon()

    monkeypatch.setattr(scheduler, "get_market_db", lambda read_only=True: DummyDB())

    scheduler_obj = KiteScheduler(
        auth=SimpleNamespace(
            is_authenticated=lambda: True,
            get_instrument_master_path=lambda exchange: Path("data/raw/kite/instruments/NSE.csv"),
        ),
        writer=SimpleNamespace(),
    )

    status = scheduler_obj.get_ingestion_status()

    assert status["authenticated"] is True
    assert status["daily"] == {
        "min_date": "2026-03-21",
        "max_date": "2026-03-25",
        "symbols": 1435,
    }
    assert status["5min"] == {
        "min_date": "2026-03-10",
        "max_date": "2026-03-20",
        "symbols": 1435,
    }


def test_resolve_symbols_uses_local_kite_intersection_for_daily(monkeypatch) -> None:
    scheduler_obj = KiteScheduler(auth=SimpleNamespace(), writer=SimpleNamespace())
    monkeypatch.setattr(
        scheduler_obj, "get_symbols_from_local_parquet", lambda: ["AAA", "BBB", "CCC"]
    )
    monkeypatch.setattr(
        scheduler_obj, "get_symbols_from_kite", lambda **kwargs: ["BBB", "CCC", "DDD"]
    )

    resolved = scheduler_obj._resolve_symbols(
        symbols=None,
        dataset=IngestionDataset.DAILY,
        start_date=date(2026, 3, 21),
        end_date=date(2026, 3, 21),
    )

    assert resolved == ["BBB", "CCC"]


def test_resolve_symbols_uses_daily_window_intersection_for_5min(monkeypatch) -> None:
    scheduler_obj = KiteScheduler(auth=SimpleNamespace(), writer=SimpleNamespace())
    monkeypatch.setattr(
        scheduler_obj,
        "get_symbols_from_daily_range",
        lambda **kwargs: ["AAA", "BBB", "CCC"],
    )
    monkeypatch.setattr(
        scheduler_obj, "get_symbols_from_kite", lambda **kwargs: ["BBB", "CCC", "DDD"]
    )

    resolved = scheduler_obj._resolve_symbols(
        symbols=None,
        dataset=IngestionDataset.FIVE_MIN,
        start_date=date(2026, 3, 23),
        end_date=date(2026, 3, 23),
    )

    assert resolved == ["BBB", "CCC"]


def test_resolve_symbols_uses_current_master_when_requested(monkeypatch) -> None:
    scheduler_obj = KiteScheduler(auth=SimpleNamespace(), writer=SimpleNamespace())
    calls: list[bool] = []

    def _get_symbols_from_kite(**kwargs):
        calls.append(bool(kwargs.get("refresh")))
        return ["AAA", "BBB"]

    monkeypatch.setattr(scheduler_obj, "get_symbols_from_kite", _get_symbols_from_kite)

    resolved = scheduler_obj._resolve_symbols(
        symbols=None,
        dataset=IngestionDataset.DAILY,
        start_date=date(2026, 3, 21),
        end_date=date(2026, 3, 21),
        universe=IngestionUniverse.CURRENT_MASTER,
    )

    assert resolved == ["AAA", "BBB"]
    assert calls == [True]


def test_checkpoint_path_is_namespaced_by_universe() -> None:
    scheduler_obj = KiteScheduler(auth=SimpleNamespace(), writer=SimpleNamespace())

    local_path = scheduler_obj._checkpoint_path(
        dataset=IngestionDataset.DAILY,
        start_date=date(2026, 3, 21),
        end_date=date(2026, 3, 21),
        universe=IngestionUniverse.LOCAL_FIRST,
    )
    master_path = scheduler_obj._checkpoint_path(
        dataset=IngestionDataset.DAILY,
        start_date=date(2026, 3, 21),
        end_date=date(2026, 3, 21),
        universe=IngestionUniverse.CURRENT_MASTER,
    )

    assert local_path != master_path
    assert "local-first" in local_path.name
    assert "current-master" in master_path.name
