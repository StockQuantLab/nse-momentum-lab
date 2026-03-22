from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from nse_momentum_lab.cli.paper import _evaluate_walk_forward, _summarize_folds, build_parser, main


class TestPaperCLI:
    def test_build_parser_has_commands(self) -> None:
        parser = build_parser()
        commands = sorted(parser._subparsers._group_actions[0].choices.keys())  # type: ignore[attr-defined]
        assert commands == [
            "alert",
            "archive",
            "flatten",
            "live",
            "pause",
            "prepare",
            "qualify",
            "replay-day",
            "resume",
            "status",
            "stop",
            "stream",
            "walk-forward",
        ]

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

    @patch("nse_momentum_lab.cli.paper.create_or_update_paper_session", new_callable=AsyncMock)
    @patch("nse_momentum_lab.cli.paper.get_sessionmaker")
    def test_prepare_command_executes(self, mock_sm: MagicMock, mock_create: AsyncMock) -> None:
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
        }

        with patch("sys.argv", ["nseml-paper", "stream", "--symbols", "ABC"]):
            main()

        mock_prepare.assert_awaited_once()
        mock_run.assert_awaited_once()

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
    ) -> None:
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        mock_framework = MagicMock()
        mock_framework.generate_rolling_windows.return_value = [
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

        mock_reset_folds.assert_awaited_once_with(mock_session, "wf-indian_2lynch-2025-04-01-2026-03-09")
        mock_insert_fold.assert_awaited_once()
        mock_set_status.assert_awaited()
