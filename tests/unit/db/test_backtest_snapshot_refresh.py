from __future__ import annotations

from pathlib import Path

from nse_momentum_lab.db.market_db import MarketDataDB


class _LockedConnection:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, sql: str):
        self.sql.append(sql)
        if sql.startswith("ATTACH"):
            raise RuntimeError("file is locked")
        return self


def test_refresh_backtest_read_snapshot_skips_locked_target(monkeypatch, tmp_path: Path) -> None:
    db = MarketDataDB.__new__(MarketDataDB)
    db._read_only = False
    db.db_path = tmp_path / "backtest.duckdb"
    db.con = _LockedConnection()

    monkeypatch.setenv(
        "BACKTEST_DASHBOARD_DUCKDB_PATH",
        str(tmp_path / "backtest_dashboard.duckdb"),
    )

    db.refresh_backtest_read_snapshot()

    assert len(db.con.sql) == 1
    assert db.con.sql[0].startswith("ATTACH")
