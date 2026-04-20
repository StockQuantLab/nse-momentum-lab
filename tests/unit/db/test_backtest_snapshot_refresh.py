from __future__ import annotations

from pathlib import Path

from nse_momentum_lab.db.market_db import MarketDataDB


class _LockedConnection:
    """Simulates a DuckDB connection that raises on ATTACH (locked target file)."""

    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, sql: str):
        self.sql.append(sql)
        if sql.startswith("ATTACH"):
            raise RuntimeError("file is locked")
        return self


def test_refresh_backtest_read_snapshot_handles_locked_target(monkeypatch, tmp_path: Path) -> None:
    db = MarketDataDB.__new__(MarketDataDB)
    db._read_only = False
    db.db_path = tmp_path / "backtest.duckdb"
    db.con = _LockedConnection()

    monkeypatch.setenv(
        "BACKTEST_DASHBOARD_DUCKDB_PATH",
        str(tmp_path / "backtest_dashboard.duckdb"),
    )

    # Should not raise — locked-file errors are caught internally by VersionedReplicaSync.
    db.refresh_backtest_read_snapshot()

    # At minimum, CHECKPOINT is issued; the ATTACH from versioned replica sync fails gracefully.
    assert any(s.startswith("CHECKPOINT") for s in db.con.sql)
