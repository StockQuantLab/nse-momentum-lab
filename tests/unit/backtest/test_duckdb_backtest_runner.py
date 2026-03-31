from __future__ import annotations

from datetime import date, time

import duckdb
import polars as pl
import pytest

from nse_momentum_lab.db.market_db import MarketDataDB
from nse_momentum_lab.services.backtest import duckdb_backtest_runner as runner_module
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)
from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy


def _make_in_memory_market_db() -> MarketDataDB:
    db = MarketDataDB.__new__(MarketDataDB)
    db.con = duckdb.connect(":memory:")
    db._data_source = "local"
    db._has_daily = False
    db._has_5min = False
    db._daily_glob = "local://daily"
    db._five_min_glob = "local://5min"
    db._ensure_backtest_tables()
    return db


def _make_legacy_in_memory_market_db() -> MarketDataDB:
    db = MarketDataDB.__new__(MarketDataDB)
    db.con = duckdb.connect(":memory:")
    db._data_source = "local"
    db._has_daily = False
    db._has_5min = False
    db._daily_glob = "local://daily"
    db._five_min_glob = "local://5min"
    db.con.execute(
        """
        CREATE TABLE bt_experiment (
            exp_id          VARCHAR PRIMARY KEY,
            strategy_name   VARCHAR NOT NULL,
            params_json     VARCHAR NOT NULL,
            params_hash     VARCHAR,
            dataset_hash    VARCHAR,
            code_hash       VARCHAR,
            data_source     VARCHAR DEFAULT 'local',
            dataset_snapshot_json VARCHAR DEFAULT '{}',
            start_year      INTEGER NOT NULL,
            end_year        INTEGER NOT NULL,
            total_return_pct    DOUBLE DEFAULT 0,
            annualized_return_pct DOUBLE DEFAULT 0,
            total_trades    INTEGER DEFAULT 0,
            win_rate_pct    DOUBLE DEFAULT 0,
            max_drawdown_pct DOUBLE DEFAULT 0,
            profit_factor   DOUBLE DEFAULT 0,
            status          VARCHAR DEFAULT 'running',
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    db.con.execute(
        """
        INSERT INTO bt_experiment (
            exp_id, strategy_name, params_json, start_year, end_year, created_at
        ) VALUES (?, ?, ?, ?, ?, current_timestamp)
        """,
        ["exp-legacy", "thresholdbreakout", "{}", 2025, 2025],
    )
    return db


def test_backtest_params_hash_is_deterministic() -> None:
    assert BacktestParams().to_hash() == BacktestParams().to_hash()


def test_backtest_params_hash_changes_with_inputs() -> None:
    p1 = BacktestParams(min_price=10)
    p2 = BacktestParams(min_price=50)
    assert p1.to_hash() != p2.to_hash()


def test_backtest_params_hash_changes_with_cost_model() -> None:
    p1 = BacktestParams(commission_model="zerodha", slippage_bps=0.0)
    p2 = BacktestParams(commission_model="zero", slippage_bps=0.0)
    assert p1.to_hash() != p2.to_hash()


def test_experiment_id_depends_on_dataset_hash() -> None:
    params_hash = BacktestParams().to_hash()
    exp1 = DuckDBBacktestRunner.build_experiment_id(params_hash, "dataset_a")
    exp2 = DuckDBBacktestRunner.build_experiment_id(params_hash, "dataset_b")
    assert exp1 != exp2


def test_resolve_same_day_r_ladder_start_r_uses_default_for_long() -> None:
    params = BacktestParams(same_day_r_ladder_start_r=2, short_same_day_r_ladder_start_r=1)
    strategy = resolve_strategy("thresholdbreakout")
    assert strategy.direction.value == "LONG"
    assert DuckDBBacktestRunner._resolve_same_day_r_ladder_start_r(params, strategy) == 2


def test_resolve_same_day_r_ladder_start_r_uses_short_override() -> None:
    params = BacktestParams(same_day_r_ladder_start_r=2, short_same_day_r_ladder_start_r=1)
    strategy = resolve_strategy("thresholdbreakdown")
    assert strategy.direction.value == "SHORT"
    assert DuckDBBacktestRunner._resolve_same_day_r_ladder_start_r(params, strategy) == 1


def test_symbol_id_map_is_collision_free_and_stable() -> None:
    symbols = ["TCS", "RELIANCE", "INFY", "TCS", "HDFCBANK"]
    symbol_map = DuckDBBacktestRunner._build_symbol_id_map(symbols)

    assert list(symbol_map.keys()) == ["HDFCBANK", "INFY", "RELIANCE", "TCS"]
    assert list(symbol_map.values()) == [1, 2, 3, 4]
    assert len(symbol_map.values()) == len(set(symbol_map.values()))


def test_resolve_intraday_entry_from_5min_finds_first_touch() -> None:
    candles = pl.DataFrame(
        {
            "candle_time": [
                "2024-01-02 09:15:00",
                "2024-01-02 09:20:00",
                "2024-01-02 09:25:00",
            ],
            "open": [100.0, 101.0, 103.0],
            "high": [101.0, 103.5, 105.0],
            "low": [99.5, 100.5, 102.0],
            "close": [100.8, 103.0, 104.5],
            "volume": [1000, 1200, 1500],
        }
    )
    result = DuckDBBacktestRunner._resolve_intraday_entry_from_5min(candles, breakout_price=103.0)
    assert result is not None
    assert float(result["entry_price"]) == 103.0
    assert float(result["initial_stop"]) == 99.5


def test_resolve_intraday_entry_from_5min_returns_none_when_no_breakout() -> None:
    candles = pl.DataFrame(
        {
            "candle_time": ["2024-01-02 09:15:00", "2024-01-02 09:20:00"],
            "open": [100.0, 100.2],
            "high": [100.8, 100.9],
            "low": [99.8, 100.0],
            "close": [100.2, 100.4],
            "volume": [1000, 900],
        }
    )
    result = DuckDBBacktestRunner._resolve_intraday_entry_from_5min(candles, breakout_price=103.0)
    assert result is None


def test_resolve_intraday_entry_from_5min_short_applies_atr_stop_cap() -> None:
    candles = pl.DataFrame(
        {
            "candle_time": [
                "2024-01-02 09:15:00",
                "2024-01-02 09:20:00",
            ],
            "open": [105.0, 100.0],
            "high": [110.0, 101.0],
            "low": [104.0, 97.0],
            "close": [104.5, 98.5],
            "volume": [1000, 900],
        }
    )
    uncapped = DuckDBBacktestRunner._resolve_intraday_entry_from_5min(
        candles, breakout_price=98.0, is_short=True
    )
    capped = DuckDBBacktestRunner._resolve_intraday_entry_from_5min(
        candles,
        breakout_price=98.0,
        is_short=True,
        short_initial_stop_atr=4.0,
        short_initial_stop_atr_cap_mult=1.5,
    )

    assert uncapped is not None
    assert capped is not None
    assert float(uncapped["entry_price"]) == 98.0
    assert float(uncapped["initial_stop"]) == 110.0
    assert float(capped["initial_stop"]) == 104.0


def test_resolve_intraday_entry_from_5min_short_same_day_take_profit() -> None:
    candles = pl.DataFrame(
        {
            "trading_date": [date(2024, 1, 2), date(2024, 1, 2)],
            "candle_time": [time(9, 15), time(9, 20)],
            "open": [100.0, 99.5],
            "high": [104.0, 101.0],
            "low": [99.0, 97.0],
            "close": [100.0, 98.0],
            "volume": [1000, 900],
        }
    )

    no_tp = DuckDBBacktestRunner._resolve_intraday_entry_from_5min(
        candles,
        breakout_price=100.0,
        is_short=True,
    )
    with_tp = DuckDBBacktestRunner._resolve_intraday_entry_from_5min(
        candles,
        breakout_price=100.0,
        is_short=True,
        short_same_day_take_profit_pct=0.02,
    )

    assert no_tp is not None
    assert with_tp is not None
    assert bool(no_tp["same_day_stop_hit"]) is False
    assert bool(with_tp["same_day_stop_hit"]) is True
    assert float(with_tp["same_day_exit_price"]) == 98.0
    assert with_tp["same_day_exit_reason"] == "ABNORMAL_PROFIT"


def test_market_db_persists_trade_filter_count() -> None:
    db = _make_in_memory_market_db()
    try:
        exp_id = "exp_trade_filters"
        db.save_experiment(
            exp_id=exp_id,
            strategy_name="thresholdbreakout",
            params_json="{}",
            start_year=2020,
            end_year=2020,
        )

        db.save_trades(
            exp_id,
            [
                {
                    "symbol": "TCS",
                    "entry_date": date(2020, 1, 2),
                    "exit_date": date(2020, 1, 7),
                    "entry_price": 100.0,
                    "exit_price": 108.0,
                    "pnl_pct": 8.0,
                    "r_multiple": 1.5,
                    "exit_reason": "TIME_STOP",
                    "holding_days": 5,
                    "gap_pct": 0.041,
                    "filters_passed": 5,
                    "year": 2020,
                }
            ],
        )

        trades = db.get_experiment_trades(exp_id)
        assert trades.height == 1
        assert trades["filters_passed"][0] == 5
        assert trades["symbol"][0] == "TCS"
    finally:
        db.con.close()


def test_threshold_breakout_candidate_query_uses_prior_day_watchlist_features() -> None:
    db = _make_in_memory_market_db()
    try:
        db.con.execute(
            """
            CREATE TABLE v_daily(
                symbol VARCHAR,
                date DATE,
                close DOUBLE,
                high DOUBLE,
                low DOUBLE,
                open DOUBLE,
                volume DOUBLE
            )
            """
        )
        db.con.execute(
            """
            CREATE TABLE feat_daily(
                symbol VARCHAR,
                date DATE,
                close_pos_in_range DOUBLE,
                ma_20 DOUBLE,
                ret_5d DOUBLE,
                atr_20 DOUBLE,
                vol_dryup_ratio DOUBLE,
                atr_compress_ratio DOUBLE,
                range_percentile DOUBLE,
                prior_breakouts_30d INTEGER,
                prior_breakdowns_90d INTEGER,
                r2_65 DOUBLE,
                ma_7 DOUBLE,
                ma_65_sma DOUBLE
            )
            """
        )
        v_daily_rows = [
            ("HILTON", date(2026, 3, 19), 12.0, 12.2, 11.8, 12.1, 100000.0),
            ("HILTON", date(2026, 3, 20), 12.5, 12.6, 12.3, 12.4, 120000.0),
            ("HILTON", date(2026, 3, 21), 13.0, 13.1, 12.8, 12.9, 130000.0),
            ("HILTON", date(2026, 3, 22), 13.5, 13.6, 13.2, 13.3, 140000.0),
            ("HILTON", date(2026, 3, 23), 13.2, 13.4, 13.0, 13.35, 150000.0),
            ("HILTON", date(2026, 3, 24), 14.38, 14.6, 13.9, 15.12, 700000.0),
            ("HILTON", date(2026, 3, 25), 15.0, 15.5, 14.9, 14.95, 800000.0),
        ]
        db.con.executemany("INSERT INTO v_daily VALUES (?, ?, ?, ?, ?, ?, ?)", v_daily_rows)

        feat_rows = [
            ("HILTON", date(2026, 3, 19), 0.9, 11.8, 0.2, 1.2, 0.8, 1.0, 0.2, 0, 0, 0.7, 12.0, 12.5),
            ("HILTON", date(2026, 3, 20), 0.9, 12.0, 0.2, 1.1, 0.8, 1.0, 0.2, 0, 0, 0.7, 12.1, 12.6),
            ("HILTON", date(2026, 3, 21), 0.9, 12.4, 0.2, 1.0, 0.8, 1.0, 0.2, 0, 0, 0.7, 12.2, 12.7),
            ("HILTON", date(2026, 3, 22), 0.9, 12.8, 0.2, 1.0, 0.8, 1.0, 0.2, 0, 0, 0.7, 12.3, 12.8),
            ("HILTON", date(2026, 3, 23), 0.9, 13.1, 0.2, 0.9, 0.8, 1.0, 0.2, 0, 0, 0.7, 12.4, 12.9),
            ("HILTON", date(2026, 3, 24), 0.5277777777777781, 18.69, -0.10516490354698194, 0.936,
             1.6567735285516751, 1.0, 0.0, 1, 0, 0.9609702193230579, 15.0, 24.90107692307693),
            ("HILTON", date(2026, 3, 25), 0.98, 10.0, 0.5, 1.5, 0.5, 1.0, 0.9, 0, 0, 0.99, 16.0, 20.0),
        ]
        db.con.executemany("INSERT INTO feat_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", feat_rows)

        strategy = resolve_strategy("thresholdbreakout")
        query, params_tuple = strategy.build_candidate_query(
            BacktestParams(breakout_threshold=0.04),
            ["HILTON"],
            date(2026, 3, 19),
            date(2026, 3, 25),
        )
        result = db.con.execute(query, params_tuple).pl()

        row = result.filter(pl.col("trading_date") == date(2026, 3, 25))
        assert row.height == 1
        filter_cols = ["filter_h", "filter_n", "filter_y", "filter_c", "filter_l", "filter_2"]
        filters_passed = sum(int(row[col][0]) for col in filter_cols)
        assert filters_passed == 3
        assert row["watch_date"][0] == date(2026, 3, 24)
    finally:
        db.con.close()


def test_to_equity_df_uses_net_pnl_without_alias_collision() -> None:
    runner = DuckDBBacktestRunner.__new__(DuckDBBacktestRunner)
    trades = pl.DataFrame(
        {
            "entry_date": [date(2026, 3, 27), date(2026, 3, 28)],
            "symbol": ["AAA", "BBB"],
            "net_pnl": [100.0, -25.0],
            "pnl_pct": [1.0, -0.25],
        }
    )

    equity = runner._to_equity_df(trades, portfolio_value=1000.0)

    assert equity["pnl_pct"].to_list() == [10.0, -2.5]
    assert equity["cumulative_return_pct"].to_list() == [10.0, 7.5]
    assert equity["drawdown_pct"].to_list() == [0.0, -2.5]


def test_delete_experiment_removes_trades_and_yearly_metrics() -> None:
    db = _make_in_memory_market_db()
    try:
        exp_id = "exp_delete_test"
        db.save_experiment(
            exp_id=exp_id,
            strategy_name="thresholdbreakout",
            params_json="{}",
            start_year=2021,
            end_year=2021,
        )
        db.save_trades(
            exp_id,
            [
                {
                    "symbol": "INFY",
                    "entry_date": date(2021, 1, 4),
                    "year": 2021,
                }
            ],
        )
        db.save_yearly_metric(
            exp_id,
            {
                "year": 2021,
                "signals": 10,
                "trades": 1,
                "wins": 1,
                "losses": 0,
                "return_pct": 3.5,
                "win_rate_pct": 100.0,
                "avg_r": 0.5,
                "max_dd_pct": 1.0,
                "profit_factor": 2.0,
                "avg_holding_days": 3.0,
                "exit_reasons": {"TIME_STOP": 1},
            },
        )
        db.save_execution_diagnostics(
            exp_id,
            [
                {
                    "year": 2021,
                    "signal_date": date(2021, 1, 4),
                    "symbol": "INFY",
                    "status": "executed",
                    "reason": "entry",
                }
            ],
        )

        db.delete_experiment(exp_id)

        assert db.get_experiment(exp_id) is None
        assert db.get_experiment_trades(exp_id).is_empty()
        assert db.get_experiment_yearly_metrics(exp_id).is_empty()
        assert db.get_experiment_execution_diagnostics(exp_id).is_empty()
    finally:
        db.con.close()


def test_market_db_persists_wf_run_id_and_cleanup_summary() -> None:
    db = _make_in_memory_market_db()
    try:
        exp_id = "exp_wf_linked"
        db.save_experiment(
            exp_id=exp_id,
            strategy_name="thresholdbreakout",
            params_json="{}",
            start_year=2022,
            end_year=2022,
            wf_run_id="wf-2026-03-21",
        )
        db.save_trades(
            exp_id,
            [
                {
                    "symbol": "TCS",
                    "entry_date": date(2022, 2, 1),
                    "exit_date": date(2022, 2, 4),
                    "year": 2022,
                }
            ],
        )
        db.save_yearly_metric(
            exp_id,
            {
                "year": 2022,
                "signals": 1,
                "trades": 1,
                "wins": 1,
                "losses": 0,
                "return_pct": 2.0,
            },
        )
        db.save_execution_diagnostics(
            exp_id,
            [
                {
                    "year": 2022,
                    "signal_date": date(2022, 2, 1),
                    "symbol": "TCS",
                    "status": "executed",
                    "reason": "entry",
                }
            ],
        )

        experiment = db.get_experiment(exp_id)
        summary = db.get_experiment_cleanup_summary(exp_id)

        assert experiment is not None
        assert experiment["wf_run_id"] == "wf-2026-03-21"
        assert summary is not None
        assert summary["wf_run_id"] == "wf-2026-03-21"
        assert summary["experiment_rows"] == 1
        assert summary["trade_rows"] == 1
        assert summary["yearly_metric_rows"] == 1
        assert summary["diagnostic_rows"] == 1
        assert summary["total_rows"] == 4
    finally:
        db.con.close()


def test_list_experiments_backfills_missing_wf_run_id_column() -> None:
    db = _make_legacy_in_memory_market_db()
    try:
        experiments = db.list_experiments()

        assert "wf_run_id" in experiments.columns
        assert experiments["wf_run_id"].to_list() == [None]
        assert db.list_experiments_for_wf_run_id("wf-legacy") == []
    finally:
        db.con.close()


def test_validate_required_lineage_dependencies_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummySettings:
        database_url = "postgresql://test:test@localhost:5432/test_db"
        minio_endpoint = "http://localhost:9003"
        minio_access_key = "minio"
        minio_secret_key = "secret"

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def execute(self, _query: str) -> None:
            return None

        def fetchone(self):
            return (1,)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def cursor(self) -> FakeCursor:
            return FakeCursor()

    called = {"publisher": False}

    class FakePublisher:
        def __init__(self) -> None:
            called["publisher"] = True

    monkeypatch.setattr(runner_module, "get_settings", lambda: DummySettings())
    monkeypatch.setattr(runner_module.psycopg, "connect", lambda _url: FakeConn())
    monkeypatch.setattr(runner_module, "BacktestArtifactPublisher", FakePublisher)

    DuckDBBacktestRunner._validate_required_lineage_dependencies()
    assert called["publisher"] is True


def test_validate_required_lineage_dependencies_raises_when_minio_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummySettings:
        database_url = "postgresql://test:test@localhost:5432/test_db"
        minio_endpoint = "http://localhost:9003"
        minio_access_key = "minio"
        minio_secret_key = "secret"

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def execute(self, _query: str) -> None:
            return None

        def fetchone(self):
            return (1,)

    class FakeConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def cursor(self) -> FakeCursor:
            return FakeCursor()

    monkeypatch.setattr(runner_module, "get_settings", lambda: DummySettings())
    monkeypatch.setattr(runner_module.psycopg, "connect", lambda _url: FakeConn())

    def _boom() -> None:
        raise RuntimeError("minio down")

    monkeypatch.setattr(runner_module, "BacktestArtifactPublisher", _boom)

    with pytest.raises(RuntimeError, match="MinIO artifacts store is unreachable"):
        DuckDBBacktestRunner._validate_required_lineage_dependencies()


def test_backtest_runtime_guard_blocks_when_backtest_db_locked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_locked(*_args, **_kwargs):
        raise RuntimeError("used by another process")

    monkeypatch.setattr(runner_module.duckdb, "connect", _raise_locked)

    with pytest.raises(RuntimeError, match=r"backtest\.duckdb is not writable"):
        DuckDBBacktestRunner._assert_no_conflicting_backtest_runtime()
