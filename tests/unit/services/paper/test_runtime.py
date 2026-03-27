from __future__ import annotations

import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from nse_momentum_lab.services.paper.runtime import PaperRuntimePlan, PaperRuntimeScaffold


class _FakeDiagnosticsFrame:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def is_empty(self) -> bool:
        return not self._rows

    def to_dicts(self) -> list[dict]:
        return list(self._rows)


def _sessionmaker_mock(session: AsyncMock | None = None) -> MagicMock:
    db_session = session or AsyncMock()
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=db_session)
    context.__aexit__ = AsyncMock()
    sessionmaker = MagicMock()
    sessionmaker.return_value = context
    return sessionmaker


class TestPaperRuntimeScaffold:
    def test_build_feed_plan_without_tokens(self) -> None:
        runtime = PaperRuntimeScaffold(feed_batch_size=100)
        plan = PaperRuntimePlan(
            session_id="paper-1",
            strategy_name="thresholdbreakout",
            trade_date=None,
            mode="live",
        )

        feed_plan = runtime.build_feed_plan(plan)

        assert feed_plan["feed_source"] == "kite"
        assert feed_plan["batches"] == []
        assert feed_plan["connection_url"] is None

    @patch("nse_momentum_lab.services.paper.runtime.get_backtest_db")
    @patch(
        "nse_momentum_lab.services.paper.runtime.reset_session_signal_queue", new_callable=AsyncMock
    )
    @patch("nse_momentum_lab.services.paper.runtime.upsert_signal", new_callable=AsyncMock)
    @patch(
        "nse_momentum_lab.services.paper.runtime.upsert_paper_session_signal",
        new_callable=AsyncMock,
    )
    @patch(
        "nse_momentum_lab.services.paper.runtime.upsert_paper_feed_state", new_callable=AsyncMock
    )
    @patch(
        "nse_momentum_lab.services.paper.runtime.list_paper_session_signals", new_callable=AsyncMock
    )
    @patch(
        "nse_momentum_lab.services.paper.runtime.get_paper_session_summary", new_callable=AsyncMock
    )
    @patch(
        "nse_momentum_lab.services.paper.runtime.create_or_update_paper_session",
        new_callable=AsyncMock,
    )
    def test_prepare_session_loads_experiment_queue(
        self,
        mock_create: AsyncMock,
        mock_summary: AsyncMock,
        mock_list_session_signals: AsyncMock,
        mock_feed_state: AsyncMock,
        mock_upsert_session_signal: AsyncMock,
        mock_upsert_signal: AsyncMock,
        mock_reset_queue: AsyncMock,
        mock_get_backtest_db: MagicMock,
    ) -> None:
        runtime = PaperRuntimeScaffold(feed_batch_size=100)
        plan = PaperRuntimePlan(
            session_id="paper-1",
            strategy_name="thresholdbreakout",
            trade_date=date(2026, 3, 23),
            mode="live",
            experiment_id="exp-1",
        )
        db_session = AsyncMock()
        execute_result = MagicMock()
        execute_result.scalars.return_value.all.return_value = [
            SimpleNamespace(symbol="ABC", symbol_id=101),
            SimpleNamespace(symbol="XYZ", symbol_id=202),
        ]
        db_session.execute = AsyncMock(return_value=execute_result)
        sessionmaker = _sessionmaker_mock(db_session)
        mock_get_backtest_db.return_value.get_experiment_execution_diagnostics.return_value = (
            _FakeDiagnosticsFrame(
                [
                    {
                        "signal_date": date(2026, 3, 23),
                        "symbol": "ABC",
                        "status": "queued_for_execution",
                        "reason": "eligible",
                        "initial_stop": 95.0,
                        "selection_rank": 1,
                        "selection_score": 8.2,
                    },
                    {
                        "signal_date": date(2026, 3, 23),
                        "symbol": "XYZ",
                        "status": "skipped_stop_too_wide",
                        "reason": "long_stop_below_max_distance",
                        "initial_stop": 88.0,
                        "selection_rank": 2,
                        "selection_score": 7.4,
                    },
                ]
            )
        )
        mock_upsert_signal.side_effect = [
            SimpleNamespace(signal_id=1),
            SimpleNamespace(signal_id=2),
        ]
        mock_feed_state.return_value = SimpleNamespace(
            session_id="paper-1",
            source="kite",
            mode="full",
            status="READY",
            subscription_count=2,
            is_stale=False,
        )
        mock_list_session_signals.return_value = [{"signal_id": 1}, {"signal_id": 2}]
        mock_summary.return_value = {"session": {"session_id": "paper-1"}}

        result = asyncio.run(runtime.prepare_session(sessionmaker, plan, status="ACTIVE"))

        assert result["queue_size"] == 2
        assert result["actionable_queue_size"] == 1
        assert result["feed_plan"]["batches"] == []
        assert mock_reset_queue.await_count == 1
        assert mock_upsert_signal.await_count == 2
        assert mock_upsert_session_signal.await_count == 2
        assert mock_create.await_count == 1

    @patch(
        "nse_momentum_lab.services.paper.runtime.upsert_paper_feed_state", new_callable=AsyncMock
    )
    @patch(
        "nse_momentum_lab.services.paper.runtime.list_paper_session_signals", new_callable=AsyncMock
    )
    @patch(
        "nse_momentum_lab.services.paper.runtime.get_paper_session_summary", new_callable=AsyncMock
    )
    @patch(
        "nse_momentum_lab.services.paper.runtime.create_or_update_paper_session",
        new_callable=AsyncMock,
    )
    def test_prepare_session_loads_runtime_queue_without_experiment(
        self,
        mock_create: AsyncMock,
        mock_summary: AsyncMock,
        mock_list_session_signals: AsyncMock,
        mock_feed_state: AsyncMock,
    ) -> None:
        runtime = PaperRuntimeScaffold(feed_batch_size=100)
        plan = PaperRuntimePlan(
            session_id="paper-rt",
            strategy_name="thresholdbreakout",
            trade_date=date(2026, 3, 23),
            mode="replay",
            symbols=["ABC", "XYZ"],
        )
        sessionmaker = _sessionmaker_mock()
        mock_feed_state.return_value = SimpleNamespace(
            session_id="paper-rt",
            source="duckdb",
            mode="full",
            status="READY",
            subscription_count=1,
            is_stale=False,
        )
        mock_list_session_signals.return_value = [{"signal_id": 1}]
        mock_summary.return_value = {"session": {"session_id": "paper-rt"}}

        with patch.object(runtime, "_fetch_queue_from_runtime", new_callable=AsyncMock) as mock_fetch:
            with patch.object(
                runtime, "_persist_queue_from_experiment", new_callable=AsyncMock
            ) as mock_persist:
                mock_fetch.return_value = {
                    "rows": [{"symbol": "ABC", "status": "queued_for_execution"}],
                    "queue_size": 1,
                    "actionable_queue_size": 1,
                    "symbols": ["ABC"],
                    "ref_symbols": {"ABC": SimpleNamespace(symbol="ABC", symbol_id=101)},
                    "missing_symbols": [],
                }
                result = asyncio.run(runtime.prepare_session(sessionmaker, plan, status="RUNNING"))

        mock_fetch.assert_awaited_once()
        mock_persist.assert_awaited_once()
        mock_create.assert_awaited_once()
        assert result["queue_size"] == 1
        assert result["actionable_queue_size"] == 1

    def test_strategy_hash_for_runtime_plan_is_deterministic(self) -> None:
        runtime = PaperRuntimeScaffold()
        plan = PaperRuntimePlan(
            session_id="paper-hash",
            strategy_name="thresholdbreakout",
            trade_date=date(2026, 3, 23),
            mode="replay",
            symbols=["ABC"],
            strategy_params={"breakout_threshold": 0.02},
        )

        first = runtime._strategy_hash_for_plan(plan)
        second = runtime._strategy_hash_for_plan(plan)

        assert first == second
        assert len(first) == 16

    @patch(
        "nse_momentum_lab.services.paper.runtime.get_paper_session_summary", new_callable=AsyncMock
    )
    @patch("nse_momentum_lab.services.paper.runtime.list_session_signals", new_callable=AsyncMock)
    @patch("nse_momentum_lab.services.paper.runtime.PaperTrader")
    def test_execute_replay_cycle_normalizes_signal_dates(
        self,
        mock_trader_cls: MagicMock,
        mock_list_signals: AsyncMock,
        mock_summary: AsyncMock,
    ) -> None:
        runtime = PaperRuntimeScaffold()
        sessionmaker = _sessionmaker_mock()
        mock_summary.side_effect = [
            {"session": {"session_id": "paper-1", "risk_config": {}}},
            {"session": {"session_id": "paper-1", "risk_config": {}}},
        ]
        mock_list_signals.return_value = [
            {
                "signal_id": 1,
                "symbol_id": 101,
                "state": "NEW",
                "planned_entry_date": "2026-03-23",
                "asof_date": "2026-03-22",
            }
        ]
        trader = MagicMock()
        trader.process_signals = AsyncMock(return_value=[{"signal_id": 1}])
        mock_trader_cls.return_value = trader

        with patch.object(runtime, "_load_eod_prices", new_callable=AsyncMock) as mock_prices:
            mock_prices.return_value = {101: {date(2026, 3, 23): {"close_adj": 100.0}}}
            result = asyncio.run(runtime.execute_replay_cycle(sessionmaker, "paper-1"))

        processed_signals = trader.process_signals.await_args.args[0]
        assert processed_signals[0]["planned_entry_date"] == date(2026, 3, 23)
        assert processed_signals[0]["asof_date"] == date(2026, 3, 22)
        assert result["processed_signals"] == 1

    def test_fetch_queue_from_watchlist_persists_entry_cutoff_minutes(self) -> None:
        runtime = PaperRuntimeScaffold()
        plan = PaperRuntimePlan(
            session_id="paper-watchlist",
            strategy_name="thresholdbreakout",
            trade_date=date(2026, 3, 27),
            mode="live",
            strategy_params={
                "_live_watchlist_rows": [
                    {
                        "symbol": "TCS",
                        "last_close": 100.0,
                        "atr_20": 2.0,
                        "filters_passed": 3,
                    }
                ],
                "breakout_threshold": 0.001,
                "entry_cutoff_minutes": 180,
            },
        )
        db_session = AsyncMock()

        with patch.object(
            runtime,
            "_ensure_ref_symbols",
            new=AsyncMock(return_value={"TCS": SimpleNamespace(symbol_id=7, symbol="TCS")}),
        ):
            with patch.object(runtime, "_resolve_instrument_token_map", return_value={"TCS": 12345}):
                result = asyncio.run(runtime._fetch_queue_from_watchlist(db_session, plan))

        assert result["queue_size"] == 1
        assert result["rows"][0]["entry_cutoff_minutes"] == 180
        assert result["rows"][0]["instrument_token"] == 12345
        metadata = runtime._build_signal_metadata(plan, result["rows"][0])
        assert metadata["entry_cutoff_minutes"] == 180

    def test_live_watchlist_rows_start_as_new_signals(self) -> None:
        runtime = PaperRuntimeScaffold()
        plan = PaperRuntimePlan(
            session_id="paper-watchlist-live",
            strategy_name="thresholdbreakout",
            trade_date=date(2026, 3, 27),
            mode="live",
            strategy_params={
                "_watchlist_mode": True,
                "_live_watchlist_rows": [
                    {
                        "symbol": "TCS",
                        "last_close": 100.0,
                        "atr_20": 2.0,
                        "filters_passed": 3,
                    }
                ],
                "breakout_threshold": 0.02,
                "entry_cutoff_minutes": 180,
            },
        )
        row = {
            "symbol": "TCS",
            "status": "watching_intraday_trigger",
        }

        assert runtime._signal_state_from_runtime_row(plan=plan, row=row) == "NEW"

    def test_fetch_queue_from_watchlist_returns_all_seed_symbols(self) -> None:
        runtime = PaperRuntimeScaffold()
        plan = PaperRuntimePlan(
            session_id="paper-watchlist-symbols",
            strategy_name="thresholdbreakout",
            trade_date=date(2026, 3, 27),
            mode="live",
            strategy_params={
                "_live_watchlist_rows": [
                    {
                        "symbol": "TCS",
                        "last_close": 100.0,
                        "atr_20": 2.0,
                        "filters_passed": 3,
                    },
                    {
                        "symbol": "INFY",
                        "last_close": 200.0,
                        "atr_20": 4.0,
                        "filters_passed": 4,
                    },
                ],
                "breakout_threshold": 0.02,
                "entry_cutoff_minutes": 180,
            },
        )
        db_session = AsyncMock()

        with patch.object(
            runtime,
            "_ensure_ref_symbols",
            new=AsyncMock(
                return_value={
                    "TCS": SimpleNamespace(symbol_id=7, symbol="TCS"),
                    "INFY": SimpleNamespace(symbol_id=8, symbol="INFY"),
                }
            ),
        ):
            with patch.object(runtime, "_resolve_instrument_token_map", return_value={"TCS": 12345, "INFY": 67890}):
                result = asyncio.run(runtime._fetch_queue_from_watchlist(db_session, plan))

        assert result["queue_size"] == 2
        assert result["symbols"] == ["INFY", "TCS"]
        assert result["rows"][0]["watch_state"] == "WATCH"
        assert result["rows"][0]["instrument_token"] == 12345

    @patch(
        "nse_momentum_lab.services.paper.runtime.get_paper_session_summary", new_callable=AsyncMock
    )
    @patch("nse_momentum_lab.services.paper.runtime.list_session_signals", new_callable=AsyncMock)
    @patch("nse_momentum_lab.services.paper.live_watchlist.check_intraday_trigger")
    def test_process_live_ticks_uses_signal_entry_cutoff_minutes(
        self,
        mock_check_trigger: MagicMock,
        mock_list_signals: AsyncMock,
        mock_summary: AsyncMock,
    ) -> None:
        runtime = PaperRuntimeScaffold()
        sessionmaker = _sessionmaker_mock()
        mock_summary.return_value = {"session": {"session_id": "paper-live", "trade_date": date(2026, 3, 27)}}
        mock_list_signals.return_value = [
            {
                "signal_id": 1,
                "symbol_id": 101,
                "state": "ARCHIVED",
                "planned_entry_date": None,
                "asof_date": date(2026, 3, 26),
                "metadata_json": {
                    "symbol": "TCS",
                    "watch_state": "WATCH",
                    "instrument_token": 12345,
                    "prev_close": 100.0,
                    "threshold": 0.001,
                    "direction": "long",
                    "entry_cutoff_minutes": 180,
                },
            }
        ]
        mock_check_trigger.return_value = {
            "triggered": False,
            "trigger_price": None,
            "state": "WATCH",
            "reason": "waiting for high >= 100.10",
        }

        asyncio.run(
            runtime.process_live_ticks(
                sessionmaker,
                "paper-live",
                [{"instrument_token": 12345, "last_price": 100.2}],
                observe_only=False,
            )
        )

        assert mock_check_trigger.call_args.kwargs["entry_cutoff_minutes"] == 180

    @patch(
        "nse_momentum_lab.services.paper.runtime.get_paper_session_summary", new_callable=AsyncMock
    )
    @patch("nse_momentum_lab.services.paper.runtime.list_session_signals", new_callable=AsyncMock)
    @patch("nse_momentum_lab.services.paper.runtime.PaperTrader")
    def test_execute_live_cycle_uses_session_trade_date_for_entry_date(
        self,
        mock_trader_cls: MagicMock,
        mock_list_signals: AsyncMock,
        mock_summary: AsyncMock,
    ) -> None:
        runtime = PaperRuntimeScaffold()
        sessionmaker = _sessionmaker_mock()
        trade_date = date(2026, 3, 23)
        mock_summary.side_effect = [
            {"session": {"session_id": "paper-1", "risk_config": {}, "trade_date": trade_date}},
            {"session": {"session_id": "paper-1", "risk_config": {}, "trade_date": trade_date}},
        ]
        mock_list_signals.return_value = [
            {
                "signal_id": 1,
                "symbol_id": 101,
                "state": "NEW",
                "planned_entry_date": "2026-03-20",
                "asof_date": "2026-03-20",
            }
        ]
        trader = MagicMock()
        trader.process_signals = AsyncMock(return_value=[{"signal_id": 1}])
        mock_trader_cls.return_value = trader
        kite_client = MagicMock()

        with patch.object(runtime, "_load_live_prices", new_callable=AsyncMock) as mock_prices:
            mock_prices.return_value = {101: {trade_date: {"close_adj": 101.0}}}
            result = asyncio.run(
                runtime.execute_live_cycle(sessionmaker, "paper-1", kite_client=kite_client)
            )

        processed_signals = trader.process_signals.await_args.args[0]
        assert processed_signals[0]["planned_entry_date"] == trade_date
        assert result["processed_signals"] == 1
