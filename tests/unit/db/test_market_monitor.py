from __future__ import annotations

import sys
from pathlib import Path
from types import MethodType

import duckdb

_project_root = Path(__file__).resolve().parents[3]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from apps.nicegui.pages.market_monitor import _year_table_row  # noqa: E402
from nse_momentum_lab.db.market_db import MarketDataDB  # noqa: E402
from nse_momentum_lab.features.daily_core import build_feat_daily_core  # noqa: E402


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


def test_build_market_monitor_table_uses_feat_daily_core() -> None:
    db = _make_db()
    db.con.execute(
        """
        CREATE TABLE feat_daily_core (
            symbol VARCHAR,
            trading_date DATE,
            close DOUBLE,
            ma_20 DOUBLE,
            ma_40 DOUBLE,
            atr_20 DOUBLE,
            vol_20 DOUBLE,
            dollar_vol_20 DOUBLE,
            ret_1d DOUBLE,
            ret_5d DOUBLE,
            atr_compress_ratio DOUBLE,
            range_percentile_252 DOUBLE,
            close_pos_in_range DOUBLE
        )
        """
    )
    db.con.execute(
        """
        INSERT INTO feat_daily_core VALUES
            ('AAA', '2026-03-17', 100.0, 95.0, 94.0, 4.0, 1000000.0, 4000000.0, 0.05, 0.10, 0.80, 0.60, 0.75),
            ('BBB', '2026-03-17',  50.0, 55.0, 56.0, 3.0,  900000.0, 4500000.0, -0.06, -0.12, 0.90, 0.30, 0.25),
            ('AAA', '2026-03-18', 102.0, 96.0, 95.0, 4.1, 1100000.0, 4200000.0, 0.02, 0.06, 0.78, 0.65, 0.72),
            ('BBB', '2026-03-18',  47.0, 54.0, 55.0, 3.1,  920000.0, 4600000.0, -0.04, -0.08, 0.88, 0.28, 0.22)
        """
    )

    row_count = db.build_market_monitor_table(force=True)

    assert row_count == 2

    latest = db.get_market_monitor_latest()
    assert not latest.is_empty()
    latest_row = latest.to_dicts()[0]
    assert str(latest_row["trading_date"]) == "2026-03-18"
    assert latest_row["universe_size"] == 0
    assert latest_row["up_4pct_count"] == 0
    assert latest_row["down_4pct_count"] == 0
    assert latest_row["pct_above_ma40"] is None
    assert latest_row["t2108_equivalent_pct"] is None
    assert latest_row["alert_flags_json"] == '["mixed"]'

    history = db.get_market_monitor_history(days=10)
    assert history.height == 2


def test_build_feat_daily_core_uses_duckdb_scalar_functions() -> None:
    db = _make_db()
    db.con.execute(
        """
        CREATE TABLE v_daily (
            symbol VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE
        )
        """
    )
    db.con.execute(
        """
        INSERT INTO v_daily VALUES
            ('AAA', '2026-03-17',  99.0, 105.0,  98.0, 102.0, 1000000.0),
            ('AAA', '2026-03-18', 101.0, 106.0, 100.0, 104.0, 1100000.0),
            ('AAA', '2026-03-19', 103.0, 107.0, 101.0, 105.0, 1200000.0),
            ('AAA', '2026-03-20', 104.0, 108.0, 102.0, 106.0, 1300000.0)
        """
    )
    db.con.execute(
        """
        CREATE TABLE bt_materialization_state (
            table_name VARCHAR,
            dataset_hash VARCHAR,
            query_version VARCHAR,
            row_count BIGINT,
            updated_at TIMESTAMP,
            PRIMARY KEY(table_name)
        )
        """
    )

    row_count = build_feat_daily_core(db.con, force=True)

    assert row_count == 4
    row = db.con.execute(
        "SELECT upper_wick_ratio, lower_wick_ratio FROM feat_daily_core ORDER BY trading_date DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row[0] is not None
    assert row[1] is not None


def test_year_table_row_includes_full_breadth_family() -> None:
    row = _year_table_row(
        {
            "weekday": "Wed",
            "date_display": "Mar 18",
            "primary_display": "Bearish",
            "primary_regime": "bearish",
            "up_4pct_count": 11,
            "down_4pct_count": 7,
            "ratio_5d": 1.25,
            "ratio_10d": 0.88,
            "t2108_equivalent_pct": 48.3,
            "up_25q_count": 120,
            "down_25q_count": 80,
            "up_25m_count": 14,
            "down_25m_count": 9,
            "up_50m_count": 6,
            "down_50m_count": 3,
            "up_13_34_count": 18,
            "down_13_34_count": 11,
            "pct_above_ma20": 52.1,
        }
    )

    assert row["regime_badge_class"] == "bearish-badge"
    assert row["up_25m_val"] == 14
    assert row["down_25m_val"] == 9
    assert row["up_50m_val"] == 6
    assert row["down_50m_val"] == 3
    assert row["up_13_34_val"] == 18
    assert row["down_13_34_val"] == 11
    assert row["t2108_val"] == 48.3
    assert row["date_key"] == ""
    assert row["up_4pct_display"] == "11"
    assert row["ratio_5d_display"] == "1.25"
    assert row["ma20_display"] == "52%"
