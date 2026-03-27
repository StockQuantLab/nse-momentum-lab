from __future__ import annotations

from argparse import Namespace
from datetime import date

import nse_momentum_lab.cli.market_monitor as market_monitor


def test_market_monitor_status_uses_read_only_db(monkeypatch) -> None:
    calls: list[bool] = []

    class DummyDB:
        def get_status(self):
            return {"data_source": "local", "dataset_hash": "abc", "tables": {}}

        def get_market_monitor_latest(self):
            return type(
                "LatestFrame",
                (),
                {
                    "empty": True,
                    "is_empty": lambda self: True,
                },
            )()

    monkeypatch.setattr(
        market_monitor.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=True,
            allow_full_rebuild=False,
            status=True,
            incremental=False,
            since=None,
        ),
    )

    def _get_market_db(*, read_only: bool = False) -> DummyDB:
        calls.append(read_only)
        return DummyDB()

    monkeypatch.setattr(
        market_monitor,
        "get_market_db",
        _get_market_db,
    )

    exit_code = market_monitor.main()

    assert exit_code == 0
    assert calls == [True]


def test_market_monitor_incremental_uses_incremental_build(monkeypatch) -> None:
    calls: list[tuple[date | None, bool]] = []

    class DummyDB:
        def build_market_monitor_incremental(self, since_date=None, *, force: bool = False):
            calls.append((since_date, force))
            return 123

        def get_market_monitor_latest(self):
            return type(
                "LatestFrame",
                (),
                {
                    "is_empty": lambda self: True,
                },
            )()

    monkeypatch.setattr(
        market_monitor.argparse.ArgumentParser,
        "parse_args",
        lambda self: Namespace(
            force=True,
            allow_full_rebuild=False,
            status=False,
            incremental=True,
            since=None,
        ),
    )
    monkeypatch.setattr(
        market_monitor,
        "get_market_db",
        lambda *, read_only=False: DummyDB(),
    )

    exit_code = market_monitor.main()

    assert exit_code == 0
    assert calls == [(None, True)]
