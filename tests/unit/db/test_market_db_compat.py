from __future__ import annotations

from types import MethodType

import duckdb

from nse_momentum_lab.db.market_db import MarketDataDB


def _make_db() -> MarketDataDB:
    db = MarketDataDB.__new__(MarketDataDB)
    db.con = duckdb.connect(":memory:")
    db._read_only = False
    db._data_source = "local"
    db._daily_glob = ""
    db._five_min_glob = ""
    db._has_5min = False
    db._has_daily = False
    db.lake = None
    db.get_dataset_snapshot = MethodType(lambda self: {"dataset_hash": "test-hash"}, db)
    db._get_materialization_state = MethodType(lambda self, table: None, db)
    db._upsert_materialization_state = MethodType(lambda self, **kwargs: None, db)
    db.register_dataset_snapshot = MethodType(lambda self, snapshot: None, db)
    return db


def test_get_features_reads_legacy_feat_daily_date_column() -> None:
    db = _make_db()
    db.con.execute(
        """
        CREATE TABLE feat_daily (
            symbol VARCHAR,
            date DATE,
            ret_1d DOUBLE,
            ret_5d DOUBLE,
            atr_20 DOUBLE,
            range_pct DOUBLE,
            close_pos_in_range DOUBLE,
            ma_20 DOUBLE,
            ma_65 DOUBLE,
            ma_7 DOUBLE,
            ma_65_sma DOUBLE,
            rs_252 DOUBLE,
            vol_20 DOUBLE,
            dollar_vol_20 DOUBLE
        )
        """
    )
    db.con.execute(
        """
        INSERT INTO feat_daily VALUES
            ('AAA', '2026-03-27', 0.05, 0.10, 4.0, 0.12, 0.70, 95.0, 96.0, 94.0, 97.0, 1.5, 100.0, 200.0)
        """
    )

    row = db.get_features("AAA", "2026-03-27")

    assert row is not None
    assert row["symbol"] == "AAA"
    assert str(row["trading_date"]) == "2026-03-27"
    assert row["dollar_vol_20"] == 200.0


def test_get_avg_dollar_vol_20_uses_legacy_feat_daily_date_column() -> None:
    db = _make_db()
    db.con.execute(
        """
        CREATE TABLE feat_daily (
            symbol VARCHAR,
            date DATE,
            ret_1d DOUBLE,
            ret_5d DOUBLE,
            atr_20 DOUBLE,
            range_pct DOUBLE,
            close_pos_in_range DOUBLE,
            ma_20 DOUBLE,
            ma_65 DOUBLE,
            ma_7 DOUBLE,
            ma_65_sma DOUBLE,
            rs_252 DOUBLE,
            vol_20 DOUBLE,
            dollar_vol_20 DOUBLE
        )
        """
    )
    db.con.execute(
        """
        INSERT INTO feat_daily VALUES
            ('AAA', '2026-03-27', 0.05, 0.10, 4.0, 0.12, 0.70, 95.0, 96.0, 94.0, 97.0, 1.5, 100.0, 200.0),
            ('AAA', '2026-03-28', 0.06, 0.12, 4.1, 0.13, 0.72, 96.0, 97.0, 95.0, 98.0, 1.6, 110.0, 220.0),
            ('BBB', '2026-03-27', 0.01, 0.02, 3.0, 0.08, 0.55, 45.0, 46.0, 44.0, 47.0, 0.9, 90.0, 180.0)
        """
    )

    result = db.get_avg_dollar_vol_20_by_symbol(["AAA", "BBB"], "2026-03-27", "2026-03-28")
    rows = result.sort("symbol").to_dicts()

    assert rows == [
        {"symbol": "AAA", "avg_dollar_vol_20": 210.0},
        {"symbol": "BBB", "avg_dollar_vol_20": 180.0},
    ]
