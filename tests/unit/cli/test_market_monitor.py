from __future__ import annotations

from argparse import Namespace

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
        lambda self: Namespace(force=False, status=True),
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
