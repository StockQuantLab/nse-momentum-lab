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


def test_backtest_params_hash_is_deterministic() -> None:
    assert BacktestParams().to_hash() == BacktestParams().to_hash()


def test_backtest_params_hash_changes_with_inputs() -> None:
    p1 = BacktestParams(min_price=10)
    p2 = BacktestParams(min_price=50)
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

        db.delete_experiment(exp_id)

        assert db.get_experiment(exp_id) is None
        assert db.get_experiment_trades(exp_id).is_empty()
        assert db.get_experiment_yearly_metrics(exp_id).is_empty()
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
