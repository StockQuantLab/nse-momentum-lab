from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest

from nse_momentum_lab.cli.paper import (
    _build_walk_forward_runtime_coverage_report,
    _check_daily_walk_forward_gate,
    _check_walk_forward_gate,
    _evaluate_walk_forward,
    _snapshot_hash,
    _summarize_folds,
    _validate_walk_forward_runtime_coverage,
    build_parser,
    main,
)
from nse_momentum_lab.db.market_db import FEAT_DAILY_QUERY_VERSION, MarketDataDB
from nse_momentum_lab.features import (
    FEAT_2LYNCH_DERIVED_VERSION,
    FEAT_DAILY_CORE_VERSION,
    FEAT_INTRADAY_CORE_VERSION,
)
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams
from nse_momentum_lab.services.paper.runtime import PaperRuntimePlan


def _make_walk_forward_runtime_db(
    *,
    daily_dates: list[date],
    five_min_dates: list[date],
    feat_daily_core_dates: list[date],
    feat_2lynch_derived_dates: list[date],
    feat_intraday_core_dates: list[date],
) -> MarketDataDB:
    db = MarketDataDB.__new__(MarketDataDB)
    db.con = duckdb.connect(":memory:")
    db._data_source = "local"
    db._daily_glob = "daily"
    db._five_min_glob = "five_min"
    db._has_daily = True
    db._has_5min = True
    db.lake = SimpleNamespace(
        mode="local",
        local_parquet_dir=None,
        bucket="",
        daily_prefix="",
        five_min_prefix="",
        endpoint=None,
        access_key=None,
        secret_key=None,
        secure=False,
    )
    db.con.execute("CREATE TABLE v_daily(date DATE, symbol VARCHAR)")
    db.con.execute("CREATE TABLE v_5min(date DATE, symbol VARCHAR)")
    for trade_date in daily_dates:
        db.con.execute("INSERT INTO v_daily VALUES (?, ?)", [trade_date, "ABC"])
    for trade_date in five_min_dates:
        db.con.execute("INSERT INTO v_5min VALUES (?, ?)", [trade_date, "ABC"])

    db.con.execute(
        """
        CREATE TABLE bt_materialization_state (
            table_name VARCHAR PRIMARY KEY,
            dataset_hash VARCHAR NOT NULL,
            query_version VARCHAR NOT NULL,
            row_count BIGINT DEFAULT 0,
            updated_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )

    snapshot = db.get_dataset_snapshot()
    daily_hash = _snapshot_hash(snapshot["daily"])  # type: ignore[arg-type]
    five_min_hash = _snapshot_hash(snapshot["five_min"])  # type: ignore[arg-type]

    def seed_feature_table(
        table_name: str,
        dates: list[date],
        *,
        dataset_hash: str,
        query_version: str,
    ) -> None:
        db.con.execute(f"CREATE TABLE {table_name}(trading_date DATE)")
        for trade_date in dates:
            db.con.execute(f"INSERT INTO {table_name} VALUES (?)", [trade_date])
        db.con.execute(
            """
            INSERT INTO bt_materialization_state (table_name, dataset_hash, query_version, row_count)
            VALUES (?, ?, ?, ?)
            """,
            [table_name, dataset_hash, query_version, len(dates)],
        )

    seed_feature_table(
        "feat_daily_core",
        feat_daily_core_dates,
        dataset_hash=daily_hash,
        query_version=FEAT_DAILY_CORE_VERSION,
    )
    seed_feature_table(
        "feat_2lynch_derived",
        feat_2lynch_derived_dates,
        dataset_hash=daily_hash,
        query_version=FEAT_2LYNCH_DERIVED_VERSION,
    )
    seed_feature_table(
        "feat_intraday_core",
        feat_intraday_core_dates,
        dataset_hash=five_min_hash,
        query_version=FEAT_INTRADAY_CORE_VERSION,
    )
    return db


class TestPaperCLI:
    def test_build_parser_has_commands(self) -> None:
        parser = build_parser()
        commands = parser._subparsers._group_actions[0].choices.keys()  # type: ignore[attr-defined]
        assert "walk-forward" in commands
        assert "walk-forward-cleanup" in commands
        assert "cleanup-walk-forward" in commands
        assert "daily-prepare" in commands
        assert "daily-replay" in commands
        assert "daily-live" in commands

    def test_walk_forward_cleanup_parser_supports_run_ids(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "walk-forward-cleanup",
                "--wf-run-id",
                "wf-1",
                "--run-id",
                "exp-1",
                "--apply",
            ]
        )

        assert args.wf_run_ids == ["wf-1"]
        assert args.run_ids == ["exp-1"]
        assert args.apply is True

    def test_build_parser_validates_positive_days(self) -> None:
        parser = build_parser()
        try:
            parser.parse_args(
                [
                    "walk-forward",
                    "--start-date",
                    "2026-03-01",
                    "--end-date",
                    "2026-03-09",
                    "--train-days",
                    "-1",
                ]
            )
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError("Expected parser failure for negative --train-days")

    def test_main_rejects_walk_forward_end_before_start(self) -> None:
        with patch(
            "sys.argv",
            [
                "nseml-paper",
                "walk-forward",
                "--start-date",
                "2026-03-09",
                "--end-date",
                "2026-03-01",
            ],
        ):
            try:
                main()
            except SystemExit as exc:
                assert exc.code == 2
            else:
                raise AssertionError("Expected parser failure for end date before start date")

    def test_walk_forward_decision_requires_performance(self) -> None:
        passing_summary = _summarize_folds(
            [
                {
                    "status": "completed",
                    "total_return_pct": 1.5,
                    "max_drawdown_pct": 4.0,
                    "total_trades": 10,
                },
                {
                    "status": "completed",
                    "total_return_pct": 2.0,
                    "max_drawdown_pct": 3.5,
                    "total_trades": 12,
                },
            ]
        )
        failing_summary = _summarize_folds(
            [
                {
                    "status": "completed",
                    "total_return_pct": -1.0,
                    "max_drawdown_pct": 20.0,
                    "total_trades": 10,
                }
            ]
        )

        assert _evaluate_walk_forward(passing_summary)["status"] == "PASS"
        assert _evaluate_walk_forward(failing_summary)["status"] == "FAIL"

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
        with patch(
            "nse_momentum_lab.cli.paper._build_daily_prepare_report",
            return_value={"coverage_ready": True, "trade_date": "2026-03-27"},
        ), patch("sys.argv", ["nseml-paper", "daily-prepare", "--mode", "live"]):
            main()

        mock_symbols.assert_called_once()

    def test_walk_forward_cleanup_command_dry_run_does_not_delete(self) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_plan = AsyncMock(
            return_value={
                "requested_wf_run_ids": ["wf-1"],
                "requested_run_ids": [],
                "missing_wf_run_ids": [],
                "missing_run_ids": [],
                "postgres": {
                    "sessions": [
                        {
                            "session": {"session_id": "wf-1"},
                            "folds": [{"fold_id": 1}],
                            "fold_count": 1,
                        }
                    ],
                    "session_count": 1,
                    "fold_row_count": 1,
                },
                "duckdb": {
                    "experiments": [
                        {
                            "exp_id": "exp-1",
                            "wf_run_id": "wf-1",
                            "trade_rows": 2,
                            "yearly_metric_rows": 1,
                            "diagnostic_rows": 1,
                            "experiment_rows": 1,
                            "total_rows": 5,
                        }
                    ],
                    "run_ids_to_delete": ["exp-1"],
                    "experiment_count": 1,
                    "row_count": 5,
                },
                "summary": {
                    "requested_wf_run_ids": 1,
                    "requested_run_ids": 0,
                    "postgres_sessions": 1,
                    "postgres_fold_rows": 1,
                    "duckdb_experiments": 1,
                    "duckdb_rows": 5,
                },
            }
        )
        mock_backtest_db = MagicMock()

        with (
            patch("nse_momentum_lab.cli.paper.get_sessionmaker") as mock_sm,
            patch("nse_momentum_lab.cli.paper.get_backtest_db", return_value=mock_backtest_db),
            patch("nse_momentum_lab.cli.paper._build_walk_forward_cleanup_plan", new=mock_plan),
        ):
            mock_sm.return_value.return_value = mock_context
            with patch(
                "sys.argv",
                ["nseml-paper", "walk-forward-cleanup", "--wf-run-id", "wf-1"],
            ):
                main()

        mock_plan.assert_awaited_once()
        assert mock_session.execute.await_count == 0
        mock_session.commit.assert_not_awaited()

    def test_walk_forward_cleanup_command_apply_deletes_matching_rows(self) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_plan = AsyncMock(
            return_value={
                "requested_wf_run_ids": ["wf-1"],
                "requested_run_ids": ["exp-legacy"],
                "missing_wf_run_ids": [],
                "missing_run_ids": [],
                "postgres": {
                    "sessions": [
                        {
                            "session": {"session_id": "wf-1"},
                            "folds": [{"fold_id": 1}],
                            "fold_count": 1,
                        }
                    ],
                    "session_count": 1,
                    "fold_row_count": 1,
                },
                "duckdb": {
                    "experiments": [
                        {
                            "exp_id": "exp-1",
                            "wf_run_id": "wf-1",
                            "trade_rows": 2,
                            "yearly_metric_rows": 1,
                            "diagnostic_rows": 1,
                            "experiment_rows": 1,
                            "total_rows": 5,
                        },
                        {
                            "exp_id": "exp-legacy",
                            "wf_run_id": None,
                            "trade_rows": 1,
                            "yearly_metric_rows": 0,
                            "diagnostic_rows": 0,
                            "experiment_rows": 1,
                            "total_rows": 2,
                        },
                    ],
                    "run_ids_to_delete": ["exp-1", "exp-legacy"],
                    "experiment_count": 2,
                    "row_count": 7,
                },
                "summary": {
                    "requested_wf_run_ids": 1,
                    "requested_run_ids": 1,
                    "postgres_sessions": 1,
                    "postgres_fold_rows": 1,
                    "duckdb_experiments": 2,
                    "duckdb_rows": 7,
                },
            }
        )
        mock_backtest_db = MagicMock()
        mock_backtest_db.experiment_exists.return_value = True

        with (
            patch("nse_momentum_lab.cli.paper.get_sessionmaker") as mock_sm,
            patch("nse_momentum_lab.cli.paper.get_backtest_db", return_value=mock_backtest_db),
            patch("nse_momentum_lab.cli.paper._build_walk_forward_cleanup_plan", new=mock_plan),
        ):
            mock_sm.return_value.return_value = mock_context
            with patch(
                "sys.argv",
                [
                    "nseml-paper",
                    "walk-forward-cleanup",
                    "--wf-run-id",
                    "wf-1",
                    "--run-id",
                    "exp-legacy",
                    "--apply",
                ],
            ):
                main()

        mock_plan.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper._check_walk_forward_gate", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.KiteStreamRunner.run", new_callable=AsyncMock)
    @patch(
        "nse_momentum_lab.cli.paper.PaperRuntimeScaffold.execute_live_cycle", new_callable=AsyncMock
    )
    @patch(
        "nse_momentum_lab.cli.paper.PaperRuntimeScaffold.prepare_session", new_callable=AsyncMock
    )
    @patch("nse_momentum_lab.cli.paper.KiteConnectClient")
    @patch("nse_momentum_lab.cli.paper.get_settings")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_live_command_executes(
        self,
        mock_sm: MagicMock,
        mock_settings: MagicMock,
        mock_kite_client_cls: MagicMock,
        mock_prepare: AsyncMock,
        mock_execute: AsyncMock,
        mock_run: AsyncMock,
        mock_gate: AsyncMock,
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
        mock_prepare.return_value = {
            "session": {"session_id": "paper-live"},
            "feed_state": {"session_id": "paper-live"},
            "signals": [],
            "feed_plan": {},
            "resolved_instrument_tokens": [12345],
        }
        mock_execute.return_value = {"session_id": "paper-live", "processed_signals": 1}
        mock_kite_client = MagicMock()
        mock_kite_client_cls.return_value.__enter__.return_value = mock_kite_client
        mock_kite_client_cls.return_value.__exit__.return_value = None

        with patch("sys.argv", ["nseml-paper", "live", "--execute", "--run", "--symbols", "ABC"]):
            main()

        mock_prepare.assert_awaited_once()
        mock_execute.assert_awaited_once()
        mock_run.assert_awaited_once()
        mock_gate.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper._check_walk_forward_gate", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.KiteStreamRunner.run", new_callable=AsyncMock)
    @patch(
        "nse_momentum_lab.cli.paper.PaperRuntimeScaffold.prepare_session", new_callable=AsyncMock
    )
    @patch("nse_momentum_lab.cli.paper.get_settings")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_stream_command_executes(
        self,
        mock_sm: MagicMock,
        mock_settings: MagicMock,
        mock_prepare: AsyncMock,
        mock_run: AsyncMock,
        mock_gate: AsyncMock,
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
        mock_prepare.return_value = {
            "session": {"session_id": "paper-stream"},
            "feed_state": {"session_id": "paper-stream"},
            "signals": [],
            "feed_plan": {},
            "resolved_instrument_tokens": [12345],
        }

        with patch("sys.argv", ["nseml-paper", "stream", "--symbols", "ABC"]):
            main()

        mock_prepare.assert_awaited_once()
        mock_run.assert_awaited_once()
        mock_gate.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper._check_walk_forward_gate", new_callable=AsyncMock)
    @patch(
        "nse_momentum_lab.cli.paper.PaperRuntimeScaffold.execute_replay_cycle",
        new_callable=AsyncMock,
    )
    @patch(
        "nse_momentum_lab.cli.paper.PaperRuntimeScaffold.prepare_session", new_callable=AsyncMock
    )
    @patch("nse_momentum_lab.cli.paper.get_settings")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_replay_day_execute_command(
        self,
        mock_sm: MagicMock,
        mock_settings: MagicMock,
        mock_prepare: AsyncMock,
        mock_execute: AsyncMock,
        mock_gate: AsyncMock,
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
        mock_prepare.return_value = {
            "session": {"session_id": "paper-replay"},
            "feed_state": {"session_id": "paper-replay"},
            "signals": [],
            "feed_plan": {},
        }
        mock_execute.return_value = {"session_id": "paper-replay", "processed_signals": 2}

        with patch(
            "sys.argv",
            ["nseml-paper", "replay-day", "--trade-date", "2026-03-21", "--execute"],
        ):
            main()

        mock_prepare.assert_awaited_once()
        mock_execute.assert_awaited_once()
        mock_gate.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper._check_daily_walk_forward_gate", new_callable=AsyncMock)
    @patch(
        "nse_momentum_lab.cli.paper.PaperRuntimeScaffold.prepare_session", new_callable=AsyncMock
    )
    @patch("nse_momentum_lab.cli.paper.get_settings")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_daily_live_command_executes(
        self,
        mock_sm: MagicMock,
        mock_settings: MagicMock,
        mock_prepare: AsyncMock,
        mock_gate: AsyncMock,
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
        mock_prepare.return_value = {
            "session": {"session_id": "paper-daily-live"},
            "feed_state": {"session_id": "paper-daily-live"},
            "signals": [],
            "feed_plan": {},
        }

        with patch(
            "nse_momentum_lab.cli.paper._build_daily_prepare_report",
            return_value={"coverage_ready": True, "trade_date": "2026-03-27"},
        ), patch("sys.argv", ["nseml-paper", "daily-live", "--symbols", "ABC"]):
            main()

        mock_prepare.assert_awaited_once()
        mock_gate.assert_awaited_once()

    @patch("nse_momentum_lab.cli.paper.PaperRuntimeScaffold.prepare_session", new_callable=AsyncMock)
    def test_daily_replay_skips_runtime_when_preparation_is_not_ready(
        self,
        mock_prepare: AsyncMock,
    ) -> None:
        with patch(
            "nse_momentum_lab.cli.paper._build_daily_prepare_report",
            return_value={"coverage_ready": False, "trade_date": "2026-03-27"},
        ), patch("sys.argv", ["nseml-paper", "daily-replay", "--symbols", "ABC"]):
            main()

        mock_prepare.assert_not_awaited()

    @patch("nse_momentum_lab.cli.paper._warn_if_session_exists", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper._load_market_trading_sessions")
    @patch("nse_momentum_lab.cli.paper.set_paper_session_status", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.update_paper_session", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.insert_walk_forward_fold", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.reset_walk_forward_folds", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.create_or_update_paper_session", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.DuckDBBacktestRunner")
    @patch("nse_momentum_lab.cli.paper.WalkForwardFramework")
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_walk_forward_resets_existing_fold_rows_before_rerun(
        self,
        mock_sm: MagicMock,
        mock_framework_cls: MagicMock,
        mock_runner_cls: MagicMock,
        mock_create: AsyncMock,
        mock_reset_folds: AsyncMock,
        mock_insert_fold: AsyncMock,
        mock_update_session: AsyncMock,
        mock_set_status: AsyncMock,
        mock_load_sessions: MagicMock,
        mock_warn: AsyncMock,
    ) -> None:
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_load_sessions.return_value = [
            date(2025, 4, 1),
            date(2025, 4, 2),
            date(2025, 4, 3),
            date(2025, 4, 4),
        ]

        mock_framework = MagicMock()
        mock_framework.generate_rolling_windows_from_sessions.return_value = [
            SimpleNamespace(
                train_start=date(2025, 4, 1),
                train_end=date(2025, 12, 8),
                test_start=date(2025, 12, 9),
                test_end=date(2026, 2, 9),
            )
        ]
        mock_framework_cls.return_value = mock_framework

        mock_runner = MagicMock()
        mock_runner.run.return_value = "exp-1"
        mock_runner.results_db.get_experiment.return_value = {
            "status": "completed",
            "total_return_pct": 2.5,
            "max_drawdown_pct": 3.0,
            "profit_factor": 1.4,
            "total_trades": 11,
        }
        mock_runner_cls.return_value = mock_runner

        with patch(
            "nse_momentum_lab.cli.paper._validate_walk_forward_runtime_coverage",
            return_value={
                "runtime_mode": "modular",
                "trade_dates": [date(2025, 4, 1), date(2025, 4, 2), date(2025, 4, 3)],
                "coverage_ready": True,
            },
        ):
            with patch(
                "sys.argv",
                [
                    "nseml-paper",
                    "walk-forward",
                    "--start-date",
                    "2025-04-01",
                    "--end-date",
                    "2026-03-09",
                    "--max-folds",
                    "1",
                ],
            ):
                main()

        mock_reset_folds.assert_awaited_once_with(
            mock_session, "wf-thresholdbreakout-2025-04-01-2026-03-09"
        )
        mock_insert_fold.assert_awaited_once()
        mock_set_status.assert_awaited()
        assert mock_runner.run.call_args is not None
        assert mock_runner.run.call_args.kwargs["wf_run_id"] == "wf-thresholdbreakout-2025-04-01-2026-03-09"

    def test_walk_forward_runtime_preflight_passes_when_runtime_tables_are_current(self) -> None:
        trade_dates = [date(2026, 3, 17), date(2026, 3, 18), date(2026, 3, 19)]
        market_db = _make_walk_forward_runtime_db(
            daily_dates=trade_dates,
            five_min_dates=trade_dates,
            feat_daily_core_dates=trade_dates,
            feat_2lynch_derived_dates=trade_dates,
            feat_intraday_core_dates=trade_dates,
        )

        report = _build_walk_forward_runtime_coverage_report(
            market_db,
            trade_dates,
            start_date=trade_dates[0],
            end_date=trade_dates[-1],
        )

        assert report["runtime_mode"] == "modular"
        assert report["coverage_ready"] is True
        assert report["missing_by_date"] == []
        assert report["tables"]["market_day_state"]["stale_reasons"] == []
        assert report["tables"]["strategy_day_state"]["stale_reasons"] == []
        assert report["tables"]["intraday_day_pack"]["stale_reasons"] == []

    def test_walk_forward_runtime_preflight_fails_on_stale_intraday_pack(self) -> None:
        trade_dates = [date(2026, 3, 17), date(2026, 3, 18), date(2026, 3, 19)]
        market_db = _make_walk_forward_runtime_db(
            daily_dates=trade_dates,
            five_min_dates=trade_dates,
            feat_daily_core_dates=trade_dates,
            feat_2lynch_derived_dates=trade_dates,
            feat_intraday_core_dates=trade_dates[:-1],
        )

        with patch("nse_momentum_lab.cli.paper.get_market_db", return_value=market_db):
            with pytest.raises(SystemExit) as exc_info:
                _validate_walk_forward_runtime_coverage(
                    trade_dates,
                    start_date=trade_dates[0],
                    end_date=trade_dates[-1],
                )

        assert "intraday_day_pack" in str(exc_info.value)
        assert "missing_trade_dates" in str(exc_info.value)

    @patch("nse_momentum_lab.cli.paper._validate_walk_forward_runtime_coverage")
    @patch("nse_momentum_lab.cli.paper._load_market_trading_sessions")
    @patch("nse_momentum_lab.cli.paper.DuckDBBacktestRunner")
    @patch("nse_momentum_lab.cli.paper.WalkForwardFramework")
    def test_walk_forward_aborts_before_runner_when_runtime_is_stale(
        self,
        mock_framework_cls: MagicMock,
        mock_runner_cls: MagicMock,
        mock_load_sessions: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        mock_load_sessions.return_value = [
            date(2026, 3, 17),
            date(2026, 3, 18),
            date(2026, 3, 19),
        ]
        mock_framework = MagicMock()
        mock_framework.generate_rolling_windows_from_sessions.return_value = [
            SimpleNamespace(
                train_start=date(2026, 3, 17),
                train_end=date(2026, 3, 17),
                test_start=date(2026, 3, 18),
                test_end=date(2026, 3, 18),
            )
        ]
        mock_framework_cls.return_value = mock_framework
        mock_validate.side_effect = SystemExit("stale runtime coverage")

        with patch(
            "sys.argv",
            [
                "nseml-paper",
                "walk-forward",
                "--start-date",
                "2026-03-17",
                "--end-date",
                "2026-03-19",
                "--train-days",
                "1",
                "--test-days",
                "1",
                "--roll-interval-days",
                "1",
                "--max-folds",
                "1",
            ],
        ):
            with pytest.raises(SystemExit, match="stale runtime coverage"):
                main()

        mock_runner_cls.assert_not_called()

    @patch("nse_momentum_lab.cli.paper.get_backtest_db")
    @patch("nse_momentum_lab.cli.paper.list_passed_walk_forward_sessions", new_callable=AsyncMock)
    async def test_check_walk_forward_gate_requires_trade_date_coverage(
        self,
        mock_list_sessions: AsyncMock,
        mock_get_backtest_db: MagicMock,
    ) -> None:
        mock_get_backtest_db.return_value = MagicMock()
        mock_list_sessions.return_value = [
            {
                "session_id": "wf-1",
                "strategy_name": "threshold_breakout",
                "finished_at": "2026-03-20T10:00:00+00:00",
                "strategy_params": {
                    "walk_forward": {
                        "test_ranges": [{"start": "2026-03-10", "end": "2026-03-20"}]
                    }
                },
            }
        ]
        plan = PaperRuntimePlan(
            session_id="paper-1",
            strategy_name="threshold_breakout",
            trade_date=date(2026, 3, 21),
            mode="replay",
        )

        try:
            await _check_walk_forward_gate(AsyncMock(), plan)
        except SystemExit as exc:
            assert "outside validated test coverage" in str(exc)
        else:
            raise AssertionError("Expected walk-forward gate failure for uncovered trade date")

    @patch("nse_momentum_lab.cli.paper.get_backtest_db")
    @patch("nse_momentum_lab.cli.paper.list_passed_walk_forward_sessions", new_callable=AsyncMock)
    async def test_check_walk_forward_gate_validates_experiment_lineage(
        self,
        mock_list_sessions: AsyncMock,
        mock_get_backtest_db: MagicMock,
    ) -> None:
        base_params = asdict(BacktestParams(strategy="thresholdbreakout"))
        mock_backtest_db = MagicMock()
        mock_backtest_db.get_experiment.return_value = {
            "strategy_name": "threshold_breakout",
            "dataset_hash": "dataset-1",
            "code_hash": "code-1",
            "params_json": json.dumps(base_params),
        }
        mock_get_backtest_db.return_value = mock_backtest_db
        mock_list_sessions.return_value = [
            {
                "session_id": "wf-1",
                "strategy_name": "threshold_breakout",
                "finished_at": "2026-03-20T10:00:00+00:00",
                "strategy_params": {
                    "walk_forward": {
                        "base_params": base_params,
                        "test_ranges": [{"start": "2026-03-10", "end": "2026-03-20"}],
                        "lineage": {
                            "dataset_hashes": ["dataset-2"],
                            "code_hashes": ["code-1"],
                        },
                    }
                },
            }
        ]
        plan = PaperRuntimePlan(
            session_id="paper-1",
            strategy_name="threshold_breakout",
            trade_date=date(2026, 3, 20),
            mode="replay",
            experiment_id="exp-1",
        )

        try:
            await _check_walk_forward_gate(AsyncMock(), plan)
        except SystemExit as exc:
            assert "dataset hash is outside the validated walk-forward lineage" in str(exc)
        else:
            raise AssertionError("Expected walk-forward gate failure for mismatched lineage")

    @patch("nse_momentum_lab.cli.paper.get_backtest_db")
    @patch("nse_momentum_lab.cli.paper.list_passed_walk_forward_sessions", new_callable=AsyncMock)
    async def test_daily_gate_ignores_trade_date_window(
        self,
        mock_list_sessions: AsyncMock,
        mock_get_backtest_db: MagicMock,
    ) -> None:
        mock_get_backtest_db.return_value = MagicMock()
        mock_list_sessions.return_value = [
            {
                "session_id": "wf-1",
                "strategy_name": "threshold_breakout",
                "finished_at": "2026-03-20T10:00:00+00:00",
                "strategy_params": {
                    "walk_forward": {
                        "test_ranges": [{"start": "2026-03-10", "end": "2026-03-20"}]
                    }
                },
            }
        ]
        plan = PaperRuntimePlan(
            session_id="paper-1",
            strategy_name="threshold_breakout",
            trade_date=date(2026, 3, 27),
            mode="live",
            symbols=["ABC"],
        )

        await _check_daily_walk_forward_gate(AsyncMock(), plan)


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
        self, mock_all_local: MagicMock,
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
                "missing_symbol_sample": [
                    {"symbol": "STALE", "reasons": ["v_daily", "v_5min"]}
                ],
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
        self, mock_get: AsyncMock, mock_now: MagicMock,
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
        self, mock_get: AsyncMock,
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
