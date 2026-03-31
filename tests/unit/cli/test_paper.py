from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import polars as pl
import pytest

from nse_momentum_lab.cli.paper import (
    _session_to_json,
    _snapshot_hash,
    build_parser,
    main,
)
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams
from nse_momentum_lab.services.paper.runtime import PaperRuntimePlan


def _runtime_symbols(
    *,
    prepare_result: dict | None = None,
    execute_live_result: dict | None = None,
    execute_replay_result: dict | None = None,
):
    class _FakeRuntimeScaffold:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    _FakeRuntimeScaffold.prepare_session = AsyncMock(return_value=prepare_result or {})
    _FakeRuntimeScaffold.execute_live_cycle = AsyncMock(return_value=execute_live_result)
    _FakeRuntimeScaffold.execute_replay_cycle = AsyncMock(return_value=execute_replay_result)
    return (MagicMock(name="PaperRuntimePlan"), _FakeRuntimeScaffold, lambda payload: payload)


class TestPaperCLI:
    def test_build_parser_has_commands(self) -> None:
        parser = build_parser()
        commands = parser._subparsers._group_actions[0].choices.keys()  # type: ignore[attr-defined]
        assert "daily-prepare" in commands
        assert "daily-sim" in commands
        assert "daily-replay" in commands
        assert "daily-live" in commands
        assert "walk-forward" not in commands
        assert "walk-forward-cleanup" not in commands
        assert "cleanup-walk-forward" not in commands

    def test_prepare_parser_rejects_walk_forward_mode(self) -> None:
        parser = build_parser()
        try:
            parser.parse_args(
                [
                    "prepare",
                    "--mode",
                    "walk_forward",
                ]
            )
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError("Expected parser failure for unsupported prepare mode")

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.create_or_update_paper_session", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_prepare_command_executes(
        self,
        mock_sm: MagicMock,
        mock_create: AsyncMock,
        mock_warn: AsyncMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_create.return_value = {
            "session_id": "paper-indian-2lynch-2026-03-21-replay",
            "status": "PLANNING",
        }

        with patch("sys.argv", ["nseml-paper", "prepare", "--trade-date", "2026-03-21"]):
            main()

        mock_create.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper._resolve_all_local_symbols", return_value=["ABC", "XYZ"])
    def test_daily_prepare_defaults_to_all_symbols(self, mock_symbols: MagicMock) -> None:
        with (
            patch(
                "nse_momentum_lab.cli.paper._build_daily_prepare_report",
                return_value={"coverage_ready": True, "trade_date": "2026-03-27"},
            ),
            patch("sys.argv", ["nseml-paper", "daily-prepare", "--mode", "live"]),
        ):
            main()

        mock_symbols.assert_called_once()

    def test_session_to_json_supports_orm_like_objects(self) -> None:
        session = SimpleNamespace(
            session_id="paper-live",
            status="ACTIVE",
            _sa_instance_state=object(),
        )

        assert _session_to_json(session) == {
            "session_id": "paper-live",
            "status": "ACTIVE",
        }

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_settings")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_live_command_executes(
        self,
        mock_sm: MagicMock,
        mock_settings: MagicMock,
        mock_warn: AsyncMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_settings.return_value = MagicMock(
            kite_ws_max_tokens=3000,
            kite_api_key="kite-key",
            kite_access_token="kite-token",
            has_kite_credentials=lambda: True,
        )
        runtime_symbols = _runtime_symbols(
            prepare_result={
                "session": {"session_id": "paper-live"},
                "feed_state": {"session_id": "paper-live"},
                "signals": [],
                "feed_plan": {},
                "resolved_instrument_tokens": [12345],
            },
            execute_live_result={"session_id": "paper-live", "processed_signals": 1},
        )
        runtime_cls = runtime_symbols[1]
        mock_kite_client = MagicMock()
        mock_kite_client_cls = MagicMock()
        mock_kite_client_cls.return_value.__enter__.return_value = mock_kite_client
        mock_kite_client_cls.return_value.__exit__.return_value = None
        mock_runner = MagicMock()
        mock_runner.run = AsyncMock()

        with (
            patch("nse_momentum_lab.cli.paper._get_paper_runtime_symbols", return_value=runtime_symbols),
            patch(
                "nse_momentum_lab.cli.paper._build_runtime_plan",
                return_value=SimpleNamespace(session_id="paper-live"),
            ),
            patch("nse_momentum_lab.cli.paper._build_stream_runner", return_value=mock_runner),
            patch(
                "nse_momentum_lab.cli.paper._get_kite_connect_client_cls",
                return_value=mock_kite_client_cls,
            ),
            patch("sys.argv", ["nseml-paper", "live", "--execute", "--run", "--symbols", "ABC"]),
        ):
            main()

        runtime_cls.prepare_session.assert_awaited_once()
        runtime_cls.execute_live_cycle.assert_awaited_once()
        mock_runner.run.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper._print_fast_sim_summary")
    @patch("nse_momentum_lab.cli.paper._resolve_daily_symbols", return_value=["ABC"])
    def test_daily_sim_command_executes(
        self,
        mock_symbols: MagicMock,
        mock_print_summary: MagicMock,
    ) -> None:
        mock_runner_cls = MagicMock()
        mock_runner = MagicMock()
        mock_runner.run.return_value = "exp-fast-sim"
        mock_runner_cls.return_value = mock_runner

        with (
            patch(
                "nse_momentum_lab.cli.paper._build_daily_prepare_report",
                return_value={"coverage_ready": True, "trade_date": "2026-03-27"},
            ),
            patch("nse_momentum_lab.cli.paper._get_backtest_runner_cls", return_value=mock_runner_cls),
            patch("sys.argv", ["nseml-paper", "daily-sim", "--trade-date", "2026-03-27"]),
        ):
            main()

        mock_runner.run.assert_called_once()
        mock_symbols.assert_called_once()
        mock_print_summary.assert_called_once_with("exp-fast-sim")

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_settings")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_stream_command_executes(
        self,
        mock_sm: MagicMock,
        mock_settings: MagicMock,
        mock_warn: AsyncMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_settings.return_value = MagicMock(
            kite_ws_max_tokens=3000,
            kite_api_key="kite-key",
            kite_access_token="kite-token",
            has_kite_credentials=lambda: True,
        )
        runtime_symbols = _runtime_symbols(
            prepare_result={
                "session": {"session_id": "paper-stream"},
                "feed_state": {"session_id": "paper-stream"},
                "signals": [],
                "feed_plan": {},
                "resolved_instrument_tokens": [12345],
            }
        )
        runtime_cls = runtime_symbols[1]
        mock_runner = MagicMock()
        mock_runner.run = AsyncMock()

        with (
            patch("nse_momentum_lab.cli.paper._get_paper_runtime_symbols", return_value=runtime_symbols),
            patch(
                "nse_momentum_lab.cli.paper._build_runtime_plan",
                return_value=SimpleNamespace(session_id="paper-stream"),
            ),
            patch("nse_momentum_lab.cli.paper._build_stream_runner", return_value=mock_runner),
            patch("sys.argv", ["nseml-paper", "stream", "--symbols", "ABC"]),
        ):
            main()

        runtime_cls.prepare_session.assert_awaited_once()
        mock_runner.run.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_settings")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_replay_day_execute_command(
        self,
        mock_sm: MagicMock,
        mock_settings: MagicMock,
        mock_warn: AsyncMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_settings.return_value = MagicMock(
            kite_ws_max_tokens=3000,
            kite_api_key=None,
            kite_access_token=None,
            has_kite_credentials=lambda: False,
        )
        runtime_symbols = _runtime_symbols(
            prepare_result={
                "session": {"session_id": "paper-replay"},
                "feed_state": {"session_id": "paper-replay"},
                "signals": [],
                "feed_plan": {},
            },
            execute_replay_result={"session_id": "paper-replay", "processed_signals": 2},
        )
        runtime_cls = runtime_symbols[1]

        with (
            patch("nse_momentum_lab.cli.paper._get_paper_runtime_symbols", return_value=runtime_symbols),
            patch(
                "nse_momentum_lab.cli.paper._build_runtime_plan",
                return_value=SimpleNamespace(session_id="paper-replay"),
            ),
            patch(
                "sys.argv",
                ["nseml-paper", "replay-day", "--trade-date", "2026-03-21", "--execute"],
            ),
        ):
            main()

        runtime_cls.prepare_session.assert_awaited_once()
        runtime_cls.execute_replay_cycle.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_settings")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_daily_live_command_executes(
        self,
        mock_sm: MagicMock,
        mock_settings: MagicMock,
        mock_warn: AsyncMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_settings.return_value = MagicMock(
            kite_ws_max_tokens=3000,
            kite_api_key="kite-key",
            kite_access_token="kite-token",
            has_kite_credentials=lambda: True,
        )
        runtime_symbols = _runtime_symbols(
            prepare_result={
                "session": {"session_id": "paper-daily-live"},
                "feed_state": {"session_id": "paper-daily-live"},
                "signals": [],
                "feed_plan": {},
            }
        )
        runtime_cls = runtime_symbols[1]

        with (
            patch("nse_momentum_lab.cli.paper._get_paper_runtime_symbols", return_value=runtime_symbols),
            patch(
                "nse_momentum_lab.cli.paper._build_runtime_plan",
                return_value=SimpleNamespace(
                    session_id="paper-daily-live",
                    strategy_params={},
                    observe_only=False,
                ),
            ),
            patch(
                "nse_momentum_lab.cli.paper._build_daily_prepare_report",
                return_value={"coverage_ready": True, "trade_date": "2026-03-27"},
            ),
            patch("sys.argv", ["nseml-paper", "daily-live", "--symbols", "ABC"]),
        ):
            main()

        runtime_cls.prepare_session.assert_awaited_once()

    def test_daily_replay_skips_runtime_when_preparation_is_not_ready(
        self,
    ) -> None:
        runtime_symbols = _runtime_symbols()
        runtime_cls = runtime_symbols[1]
        with (
            patch("nse_momentum_lab.cli.paper._get_paper_runtime_symbols", return_value=runtime_symbols),
            patch(
                "nse_momentum_lab.cli.paper._build_daily_prepare_report",
                return_value={"coverage_ready": False, "trade_date": "2026-03-27"},
            ),
            patch("sys.argv", ["nseml-paper", "daily-replay", "--symbols", "ABC"]),
        ):
            main()

        runtime_cls.prepare_session.assert_not_awaited()

    def test_daily_replay_short_circuits_on_empty_watchlist(
        self,
    ) -> None:
        runtime_symbols = _runtime_symbols()
        runtime_cls = runtime_symbols[1]
        with (
            patch("nse_momentum_lab.cli.paper._get_paper_runtime_symbols", return_value=runtime_symbols),
            patch(
                "nse_momentum_lab.cli.paper._build_daily_prepare_report",
                return_value={"coverage_ready": True, "trade_date": "2026-03-27"},
            ),
            patch(
                "nse_momentum_lab.services.paper.live_watchlist.build_prior_day_watchlist",
                return_value=pl.DataFrame(),
            ),
            patch("sys.argv", ["nseml-paper", "daily-replay", "--watchlist", "--symbols", "ABC"]),
        ):
            main()

        runtime_cls.prepare_session.assert_not_awaited()


# ---------------------------------------------------------------------------
# 1.3 Tighten symbol universe for daily live sessions
# ---------------------------------------------------------------------------


class TestBuildOperationalUniverse:
    """Tests for build_operational_universe in live_watchlist.py."""

    @patch("nse_momentum_lab.services.paper.live_watchlist.get_market_db")
    def test_returns_intersection_of_daily_and_5min(self, mock_get_db: MagicMock) -> None:
        from nse_momentum_lab.services.paper.live_watchlist import build_operational_universe

        mock_con = MagicMock()
        mock_get_db.return_value = MagicMock(con=mock_con)

        # MAX(date) query returns 2026-03-26
        mock_con.execute.return_value.fetchone.return_value = (date(2026, 3, 26),)

        # First call: findone (MAX date), second: daily symbols, third: 5min symbols
        daily_rows = [("RELIANCE",), ("TCS",), ("INFY",), ("STALE",)]
        five_min_rows = [("RELIANCE",), ("TCS",), ("INFY",)]
        mock_con.execute.return_value.fetchall.side_effect = [daily_rows, five_min_rows]

        result = build_operational_universe(trade_date=date(2026, 3, 27))

        # STALE excluded (no 5-min data)
        assert result == ["INFY", "RELIANCE", "TCS"]

    @patch("nse_momentum_lab.services.paper.live_watchlist.get_market_db")
    def test_returns_empty_when_no_prior_date(self, mock_get_db: MagicMock) -> None:
        from nse_momentum_lab.services.paper.live_watchlist import build_operational_universe

        mock_con = MagicMock()
        mock_get_db.return_value = MagicMock(con=mock_con)
        mock_con.execute.return_value.fetchone.return_value = (None,)

        result = build_operational_universe(trade_date=date(2026, 3, 27))
        assert result == []

    @patch("nse_momentum_lab.services.paper.live_watchlist.get_market_db")
    def test_returns_empty_on_none_trade_date(self, mock_get_db: MagicMock) -> None:
        from nse_momentum_lab.services.paper.live_watchlist import build_operational_universe

        result = build_operational_universe(trade_date=None)
        assert result == []
        mock_get_db.assert_not_called()

    @patch("nse_momentum_lab.services.paper.live_watchlist.get_market_db")
    def test_returns_empty_on_exception(self, mock_get_db: MagicMock) -> None:
        from nse_momentum_lab.services.paper.live_watchlist import build_operational_universe

        mock_get_db.side_effect = RuntimeError("db error")

        result = build_operational_universe(trade_date=date(2026, 3, 27))
        assert result == []


class TestBuildPriorDayWatchlist:
    @patch("nse_momentum_lab.services.paper.live_watchlist.get_market_db")
    def test_long_watchlist_query_executes_and_returns_rows(
        self,
        mock_get_db: MagicMock,
    ) -> None:
        from nse_momentum_lab.services.paper.live_watchlist import build_prior_day_watchlist

        con = duckdb.connect(":memory:")
        con.execute(
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
        con.execute(
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
                ma_65_sma DOUBLE
            )
            """
        )
        con.executemany(
            "INSERT INTO v_daily VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("ABC", date(2026, 3, 25), 102.0, 103.0, 100.0, 101.0, 100000.0),
                ("ABC", date(2026, 3, 26), 100.0, 101.0, 98.0, 101.0, 100000.0),
                ("ABC", date(2026, 3, 27), 101.0, 102.0, 99.0, 100.0, 100000.0),
            ],
        )
        con.execute(
            "INSERT INTO feat_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "ABC",
                date(2026, 3, 27),
                0.8,
                95.0,
                0.1,
                2.0,
                1.0,
                1.0,
                0.4,
                1,
                0,
                0.8,
                90.0,
            ],
        )
        mock_get_db.return_value = SimpleNamespace(con=con)

        result = build_prior_day_watchlist(
            symbols=["ABC"],
            trade_date=date(2026, 3, 30),
            strategy="thresholdbreakout",
            threshold=0.04,
            min_filters=5,
        )

        assert result.height == 1
        assert result["symbol"].to_list() == ["ABC"]
        assert result["filters_passed"].to_list() == [6]

    @patch("nse_momentum_lab.services.paper.live_watchlist._build_latest_bar_fallback_watchlist")
    @patch("nse_momentum_lab.services.paper.live_watchlist.get_market_db")
    def test_primary_query_failure_uses_fallback_without_filtering_it_away(
        self,
        mock_get_db: MagicMock,
        mock_fallback: MagicMock,
    ) -> None:
        from nse_momentum_lab.services.paper import live_watchlist
        from nse_momentum_lab.services.paper.live_watchlist import build_prior_day_watchlist

        live_watchlist._WATCHLIST_CACHE.clear()

        mock_get_db.return_value = SimpleNamespace(
            con=SimpleNamespace(execute=MagicMock(side_effect=RuntimeError("binder error")))
        )
        mock_fallback.return_value = pl.DataFrame(
            {
                "symbol": ["ABC"],
                "watch_date": [date(2026, 3, 27)],
                "last_close": [101.0],
                "close_pos_in_range": [0.8],
                "atr_20": [2.0],
                "range_percentile": [0.4],
                "vol_dryup_ratio": [1.0],
                "r2_65": [0.8],
                "value_traded_inr": [10100000.0],
                "filter_h": [False],
                "filter_n": [False],
                "filter_y": [False],
                "filter_c": [False],
                "filter_l": [False],
                "filter_2": [False],
                "filters_passed": [0],
            }
        )

        result = build_prior_day_watchlist(
            symbols=["ABC"],
            trade_date=date(2026, 3, 30),
            min_filters=5,
        )

        assert result.height == 1
        assert result["symbol"].to_list() == ["ABC"]
        mock_fallback.assert_called_once()


class TestLivePaperOperationalHelpers:
    @patch("nse_momentum_lab.cli.paper.get_settings")
    def test_build_runtime_plan_encodes_threshold_and_watchlist_in_session_id(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        from nse_momentum_lab.cli.paper import _build_runtime_plan

        mock_get_settings.return_value = SimpleNamespace(
            kite_api_key=None,
            kite_access_token=None,
        )
        args = SimpleNamespace(
            session_id=None,
            strategy="thresholdbreakout",
            trade_date=date(2026, 3, 30),
            experiment_id=None,
            notes=None,
            strategy_params='{"breakout_threshold": 0.02}',
            risk_config=None,
            feed_mode="full",
            instrument_tokens="",
            observe=False,
            watchlist=True,
        )

        plan = _build_runtime_plan(
            args,
            mode="live",
            feed_source="kite",
            trade_date=date(2026, 3, 30),
            symbols=["TCS"],
        )

        assert plan.session_id == "paper-thresholdbreakout-thr-0p02-watchlist-2026-03-30-live"

    @patch("nse_momentum_lab.services.paper.live_watchlist.build_prior_day_watchlist")
    def test_build_watchlist_report_flags_empty_watchlist_as_not_ready(
        self,
        mock_build_watchlist: MagicMock,
    ) -> None:
        from nse_momentum_lab.cli.paper import _build_watchlist_report

        mock_build_watchlist.return_value = pl.DataFrame()
        args = SimpleNamespace(
            strategy="thresholdbreakout",
            strategy_params=None,
            watchlist=True,
        )

        watchlist_df, report = _build_watchlist_report(
            args,
            trade_date=date(2026, 3, 30),
            symbols=["TCS", "INFY"],
        )

        assert watchlist_df is not None
        assert report["enabled"] is True
        assert report["ready"] is False
        assert report["count"] == 0
        assert report["reasons"] == ["empty_watchlist"]

    @patch("nse_momentum_lab.services.paper.live_watchlist.build_prior_day_watchlist")
    def test_build_watchlist_report_passes_short_direction_for_breakdown(
        self,
        mock_build_watchlist: MagicMock,
    ) -> None:
        from nse_momentum_lab.cli.paper import _build_watchlist_report

        mock_build_watchlist.return_value = pl.DataFrame(
            {"symbol": ["SBIN"], "filters_passed": [5]}
        )
        args = SimpleNamespace(
            strategy="thresholdbreakdown",
            strategy_params='{"breakout_threshold": 0.02}',
            watchlist=True,
        )

        _build_watchlist_report(
            args,
            trade_date=date(2026, 3, 30),
            symbols=["SBIN"],
        )

        assert mock_build_watchlist.call_args.kwargs["direction"] == "short"

    def test_compact_paper_session_summary_trims_large_payloads(self) -> None:
        from nse_momentum_lab.cli.paper import _compact_paper_session_summary

        payload = _compact_paper_session_summary(
            {
                "session": {
                    "session_id": "paper-thresholdbreakout-thr-0p04-watchlist-2026-03-30-live",
                    "trade_date": "2026-03-30",
                    "strategy_name": "thresholdbreakout",
                    "experiment_id": None,
                    "mode": "live",
                    "status": "ACTIVE",
                    "symbols": ["TCS", "INFY"],
                    "strategy_params": {"breakout_threshold": 0.04},
                },
                "counts": {
                    "signals": 3,
                    "open_signals": 2,
                    "open_positions": 1,
                    "orders": 1,
                    "fills": 1,
                    "queue_signals": 3,
                },
                "feed_state": {
                    "source": "kite",
                    "mode": "full",
                    "status": "CONNECTED",
                    "is_stale": False,
                    "subscription_count": 2,
                    "last_quote_at": "2026-03-30T09:31:00+05:30",
                    "last_tick_at": "2026-03-30T09:31:00+05:30",
                    "heartbeat_at": "2026-03-30T09:31:01+05:30",
                    "metadata_json": {"instrument_tokens": [101, 202], "observe_only": False},
                },
            }
        )

        assert payload["session"]["symbol_count"] == 2
        assert payload["feed_state"]["token_count"] == 2
        assert "symbols" not in payload["session"]

    @patch("builtins.print")
    @patch("nse_momentum_lab.cli.paper.get_paper_session_summary_compact", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_status_summary_uses_compact_session_query(
        self,
        mock_sm: MagicMock,
        mock_compact_summary: AsyncMock,
        mock_print: MagicMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_compact_summary.return_value = {
            "session": {"session_id": "paper-live", "symbol_count": 33},
            "counts": {"signals": 33},
            "feed_state": {"token_count": 33},
        }

        with patch("sys.argv", ["nseml-paper", "status", "--session-id", "paper-live", "--summary"]):
            main()

        mock_compact_summary.assert_awaited_once_with(mock_session, "paper-live")
        printed = json.loads(mock_print.call_args.args[0])
        assert printed["session"]["symbol_count"] == 33
        assert printed["feed_state"]["token_count"] == 33

    @patch("builtins.print")
    @patch("nse_momentum_lab.cli.paper.list_paper_sessions_compact", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_status_summary_list_uses_compact_session_list(
        self,
        mock_sm: MagicMock,
        mock_list_compact: AsyncMock,
        mock_print: MagicMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_list_compact.return_value = [
            {"session_id": "paper-live", "symbol_count": 33, "status": "ACTIVE"}
        ]

        with patch("sys.argv", ["nseml-paper", "status", "--summary"]):
            main()

        mock_list_compact.assert_awaited_once()
        printed = json.loads(mock_print.call_args.args[0])
        assert printed["sessions"][0]["symbol_count"] == 33


class TestResolveDailySymbolsLive:
    """Tests for _resolve_daily_symbols with live=True operational universe."""

    @patch("nse_momentum_lab.cli.paper._resolve_all_local_symbols")
    @patch("nse_momentum_lab.services.paper.live_watchlist.build_operational_universe")
    def test_live_all_symbols_uses_operational_universe(
        self,
        mock_build: MagicMock,
        mock_all_local: MagicMock,
    ) -> None:
        from nse_momentum_lab.cli.paper import _resolve_daily_symbols

        mock_build.return_value = ["RELIANCE", "TCS", "INFY"]
        args = SimpleNamespace(symbols=None, all_symbols=True, experiment_id=None)

        result = _resolve_daily_symbols(args, date(2026, 3, 27), live=True)
        assert result == ["RELIANCE", "TCS", "INFY"]
        mock_build.assert_called_once_with(trade_date=date(2026, 3, 27))
        mock_all_local.assert_not_called()

    @patch("nse_momentum_lab.cli.paper._resolve_all_local_symbols")
    @patch("nse_momentum_lab.services.paper.live_watchlist.build_operational_universe")
    def test_live_all_symbols_falls_back_when_empty(
        self,
        mock_build: MagicMock,
        mock_all_local: MagicMock,
    ) -> None:
        from nse_momentum_lab.cli.paper import _resolve_daily_symbols

        mock_build.return_value = []
        mock_all_local.return_value = ["RELIANCE", "TCS"]
        args = SimpleNamespace(symbols=None, all_symbols=True, experiment_id=None)

        result = _resolve_daily_symbols(args, date(2026, 3, 27), live=True)
        assert result == ["RELIANCE", "TCS"]
        mock_all_local.assert_called_once()

    @patch("nse_momentum_lab.cli.paper._resolve_all_local_symbols")
    def test_replay_does_not_use_operational_universe(
        self,
        mock_all_local: MagicMock,
    ) -> None:
        from nse_momentum_lab.cli.paper import _resolve_daily_symbols

        mock_all_local.return_value = ["RELIANCE", "TCS"]
        args = SimpleNamespace(symbols=None, all_symbols=True, experiment_id=None)

        # live=False (default) should use all local symbols
        result = _resolve_daily_symbols(args, date(2026, 3, 27), live=False)
        assert result == ["RELIANCE", "TCS"]

    def test_explicit_symbols_ignore_live_flag(self) -> None:
        from nse_momentum_lab.cli.paper import _resolve_daily_symbols

        args = SimpleNamespace(symbols="RELIANCE,TCS", all_symbols=False, experiment_id=None)

        result = _resolve_daily_symbols(args, date(2026, 3, 27), live=True)
        assert result == ["RELIANCE", "TCS"]


# ---------------------------------------------------------------------------
# 2.1 First-class daily readiness report
# ---------------------------------------------------------------------------


class TestComposeLiveReadinessVerdict:
    """Tests for _compose_live_readiness_verdict structured readiness output."""

    def test_ready_when_data_coverage_complete(self) -> None:
        from nse_momentum_lab.cli.paper import _compose_live_readiness_verdict

        result = _compose_live_readiness_verdict(
            data_report={
                "coverage_ready": True,
                "requested_symbol_count": 100,
                "missing_symbol_count": 0,
                "matched_prev_trade_dates": {"2026-03-26": 100},
                "missing_symbol_sample": [],
            },
            trade_date=date(2026, 3, 27),
        )

        assert result["verdict"] == "READY"
        assert "data_ready" in result["checks"]
        assert result["reasons"] == []
        assert result["remediation"] == []

    def test_blocked_when_data_coverage_missing(self) -> None:
        from nse_momentum_lab.cli.paper import _compose_live_readiness_verdict

        result = _compose_live_readiness_verdict(
            data_report={
                "coverage_ready": False,
                "requested_symbol_count": 50,
                "missing_symbol_count": 50,
                "matched_prev_trade_dates": {},
                "missing_symbol_sample": [{"symbol": "STALE", "reasons": ["v_daily", "v_5min"]}],
            },
            trade_date=date(2026, 3, 27),
        )

        assert result["verdict"] == "BLOCKED"
        assert "data_coverage_gap" in result["reasons"]
        assert len(result["remediation"]) > 0

    def test_observe_only_when_partial_coverage(self) -> None:
        from nse_momentum_lab.cli.paper import _compose_live_readiness_verdict

        result = _compose_live_readiness_verdict(
            data_report={
                "coverage_ready": False,
                "requested_symbol_count": 100,
                "missing_symbol_count": 5,
                "matched_prev_trade_dates": {"2026-03-26": 95},
                "missing_symbol_sample": [],
            },
            trade_date=date(2026, 3, 27),
        )

        assert result["verdict"] == "OBSERVE_ONLY"
        assert "partial_data_coverage" in result["reasons"]

    def test_backward_compat_coverage_ready(self) -> None:
        from nse_momentum_lab.cli.paper import _compose_live_readiness_verdict

        result = _compose_live_readiness_verdict(
            data_report={
                "coverage_ready": True,
                "requested_symbol_count": 80,
                "missing_symbol_count": 0,
                "matched_prev_trade_dates": {"2026-03-26": 80},
                "missing_symbol_sample": [],
            },
            trade_date=date(2026, 3, 27),
        )

        assert result["coverage_ready"] is True

    def test_data_readiness_section_present(self) -> None:
        from nse_momentum_lab.cli.paper import _compose_live_readiness_verdict

        result = _compose_live_readiness_verdict(
            data_report={
                "coverage_ready": True,
                "requested_symbol_count": 200,
                "missing_symbol_count": 0,
                "matched_prev_trade_dates": {"2026-03-26": 200},
                "missing_symbol_sample": [],
            },
            trade_date=date(2026, 3, 27),
        )

        assert "data_readiness" in result
        dr = result["data_readiness"]
        assert dr["ready"] is True
        assert dr["requested_symbol_count"] == 200
        assert dr["matched_symbol_count"] == 200
        assert dr["missing_symbol_count"] == 0


# ---------------------------------------------------------------------------
# 2.2 Session cleanup and archive
# ---------------------------------------------------------------------------


class TestSessionCleanup:
    """Tests for list_stale_sessions and archive_sessions in db/paper.py."""

    @patch("nse_momentum_lab.db.paper._utc_now")
    @patch("nse_momentum_lab.db.paper.get_paper_session", new_callable=AsyncMock)
    async def test_archive_sessions_archives_found(
        self,
        mock_get: AsyncMock,
        mock_now: MagicMock,
    ) -> None:
        from nse_momentum_lab.db.paper import archive_sessions

        mock_session = AsyncMock()
        mock_session.status = "ACTIVE"
        mock_session.finished_at = None
        mock_session.archived_at = None
        mock_session.updated_at = None
        mock_get.return_value = mock_session
        mock_now.return_value = datetime(2026, 3, 27, 12, 0, 0)

        result = await archive_sessions(mock_session, ["sess-1", "sess-2"])

        assert result["archived"] == 2
        assert result["not_found"] == 0
        assert mock_session.status == "ARCHIVED"

    @patch("nse_momentum_lab.db.paper.get_paper_session", new_callable=AsyncMock)
    async def test_archive_sessions_skips_not_found(
        self,
        mock_get: AsyncMock,
    ) -> None:
        from nse_momentum_lab.db.paper import archive_sessions

        mock_get.return_value = None

        result = await archive_sessions(AsyncMock(), ["nonexistent"])

        assert result["archived"] == 0
        assert result["not_found"] == 1


class TestCleanupCLI:
    """Tests for the cleanup CLI command."""

    @patch("nse_momentum_lab.db.paper.archive_sessions", new_callable=AsyncMock)
    @patch("nse_momentum_lab.db.paper.list_stale_sessions", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    @pytest.mark.asyncio
    async def test_cleanup_dry_run_does_not_archive(
        self,
        mock_sessionmaker: MagicMock,
        mock_list_stale: AsyncMock,
        mock_archive: AsyncMock,
    ) -> None:
        from nse_momentum_lab.cli.paper import _cmd_cleanup

        mock_db = AsyncMock()
        mock_sessionmaker.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_sessionmaker.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_list_stale.return_value = [
            {"session_id": "old-1", "status": "ACTIVE", "created_at": "2026-03-25T09:00:00"},
        ]

        args = SimpleNamespace(mode="live", max_age_hours=48, dry_run=True)
        await _cmd_cleanup(args)

        mock_archive.assert_not_called()

    @patch("nse_momentum_lab.db.paper.archive_sessions", new_callable=AsyncMock)
    @patch("nse_momentum_lab.db.paper.list_stale_sessions", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    @pytest.mark.asyncio
    async def test_cleanup_no_stale_sessions(
        self,
        mock_sessionmaker: MagicMock,
        mock_list_stale: AsyncMock,
        mock_archive: AsyncMock,
    ) -> None:
        from nse_momentum_lab.cli.paper import _cmd_cleanup

        mock_db = AsyncMock()
        mock_sessionmaker.return_value.__aenter__ = AsyncMock(return_value=mock_db)
        mock_sessionmaker.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_list_stale.return_value = []

        args = SimpleNamespace(mode=None, max_age_hours=48, dry_run=False)
        await _cmd_cleanup(args)

        mock_archive.assert_not_called()
