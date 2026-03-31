from __future__ import annotations

from types import SimpleNamespace

import nse_momentum_lab.cli.db_verify as db_verify


def test_verify_duckdb_fails_on_legacy_timestamp_files(monkeypatch) -> None:
    class DummyDB:
        def get_status(self):
            return {
                "parquet_5min": True,
                "parquet_daily": True,
                "tables": {"feat_daily": 123},
                "symbols": 10,
                "total_candles": 100,
                "date_range": "2025-01-01 to 2026-03-27",
            }

    monkeypatch.setattr(
        db_verify,
        "get_market_db",
        lambda *, read_only=False: DummyDB(),
    )
    monkeypatch.setattr(
        db_verify,
        "scan_5min_timestamp_alignment",
        lambda path: [SimpleNamespace(symbol="RELIANCE", year=2025, path="bad.parquet")],
    )

    assert db_verify.verify_duckdb() is False


def test_verify_duckdb_passes_when_timestamps_are_clean(monkeypatch) -> None:
    class DummyDB:
        def get_status(self):
            return {
                "parquet_5min": True,
                "parquet_daily": True,
                "tables": {"feat_daily": 123},
                "symbols": 10,
                "total_candles": 100,
                "date_range": "2025-01-01 to 2026-03-27",
            }

    monkeypatch.setattr(
        db_verify,
        "get_market_db",
        lambda *, read_only=False: DummyDB(),
    )
    monkeypatch.setattr(db_verify, "scan_5min_timestamp_alignment", lambda path: [])

    assert db_verify.verify_duckdb() is True
