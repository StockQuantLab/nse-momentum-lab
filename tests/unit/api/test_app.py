from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from nse_momentum_lab.api.app import create_app


class TestAPIApp:
    def setup_method(self) -> None:
        self.app = create_app()
        self.client = TestClient(self.app)

    def test_health(self) -> None:
        response = self.client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_ingestion_status(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_job = MagicMock()
        mock_job.job_name = "test_job"
        mock_job.asof_date = date(2024, 1, 1)
        mock_job.status = "SUCCEEDED"
        mock_job.started_at = None
        mock_job.duration_ms = 1000
        mock_result.scalars.return_value.all.return_value = [mock_job]
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/ingestion/status")
        assert response.status_code == 200
        data = response.json()
        assert "jobs" in data

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_symbols(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_sym = MagicMock()
        mock_sym.symbol_id = 1
        mock_sym.symbol = "TEST"
        mock_sym.series = "EQ"
        mock_sym.status = "ACTIVE"
        mock_result.scalars.return_value.all.return_value = [mock_sym]
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/symbols")
        assert response.status_code == 200
        data = response.json()
        assert "symbols" in data
        assert len(data["symbols"]) == 1

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_scan_runs(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_run = MagicMock()
        mock_run.scan_run_id = 1
        mock_run.asof_date = date(2024, 1, 1)
        mock_run.status = "SUCCEEDED"
        mock_run.dataset_hash = "abc123"
        mock_run.started_at = None
        mock_result.scalars.return_value.all.return_value = [mock_run]
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/scans/runs")
        assert response.status_code == 200
        data = response.json()
        assert "runs" in data

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_scan_results(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_res = MagicMock()
        mock_res.symbol_id = 1
        mock_res.asof_date = date(2024, 1, 1)
        mock_res.passed = True
        mock_res.score = 0.8
        mock_res.reason_json = {}
        mock_result.scalars.return_value.all.return_value = [mock_res]
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/scans/results")
        assert response.status_code == 200
        data = response.json()
        assert "results" in data

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_experiments(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_exp = MagicMock()
        mock_exp.exp_hash = "exp123"
        mock_exp.strategy_name = "test"
        mock_exp.status = "SUCCEEDED"
        mock_exp.started_at = None
        mock_result.scalars.return_value.all.return_value = [mock_exp]
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/experiments")
        assert response.status_code == 200
        data = response.json()
        assert "experiments" in data

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_experiment_detail(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_exp = MagicMock()
        mock_exp.exp_hash = "exp123"
        mock_exp.strategy_name = "test"
        mock_exp.strategy_hash = "strat123"
        mock_exp.dataset_hash = "data123"
        mock_exp.params_json = {}
        mock_exp.status = "SUCCEEDED"
        mock_exp.started_at = None
        mock_exp.finished_at = None
        mock_result.scalar_one_or_none.return_value = mock_exp
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/experiments/exp123")
        assert response.status_code == 200
        data = response.json()
        assert data["exp_hash"] == "exp123"

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_experiment_artifacts(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        exp_result = MagicMock()
        exp = MagicMock()
        exp.exp_run_id = 42
        exp_result.scalar_one_or_none.return_value = exp

        artifacts_result = MagicMock()
        artifact = MagicMock()
        artifact.artifact_name = "summary.json"
        artifact.uri = "s3://artifacts/experiments/exp123/summary.json"
        artifact.sha256 = "abc123"
        artifacts_result.scalars.return_value.all.return_value = [artifact]

        mock_session.execute = AsyncMock(side_effect=[exp_result, artifacts_result])

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/experiments/exp123/artifacts")
        assert response.status_code == 200
        data = response.json()
        assert data["exp_hash"] == "exp123"
        assert len(data["artifacts"]) == 1
        assert data["artifacts"][0]["artifact_name"] == "summary.json"

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_paper_positions(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_pos = MagicMock()
        mock_pos.position_id = 1
        mock_pos.symbol_id = 100
        mock_pos.opened_at = None
        mock_pos.avg_entry = 100.0
        mock_pos.qty = 100
        mock_pos.pnl = 500.0
        mock_pos.state = "ENTERED"
        mock_result.scalars.return_value.all.return_value = [mock_pos]
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/paper/positions")
        assert response.status_code == 200
        data = response.json()
        assert "positions" in data

    @patch("nse_momentum_lab.api.app.list_paper_sessions", new_callable=AsyncMock)
    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_paper_sessions(self, mock_sm: MagicMock, mock_list: AsyncMock) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_list.return_value = [
            {
                "session_id": "paper-1",
                "strategy_name": "thresholdbreakout",
                "mode": "replay",
                "status": "PLANNING",
            }
        ]

        response = self.client.get("/api/paper/sessions")
        assert response.status_code == 200
        data = response.json()
        assert len(data["sessions"]) == 1
        assert data["sessions"][0]["session_id"] == "paper-1"

    @patch("nse_momentum_lab.api.app.get_paper_session_summary", new_callable=AsyncMock)
    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_paper_session_detail(self, mock_sm: MagicMock, mock_summary: AsyncMock) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_summary.return_value = {
            "session": {"session_id": "paper-1", "status": "ACTIVE"},
            "counts": {"signals": 3, "open_signals": 1, "open_positions": 1, "orders": 2, "fills": 2},
        }

        response = self.client.get("/api/paper/sessions/paper-1")
        assert response.status_code == 200
        data = response.json()
        assert data["session"]["session_id"] == "paper-1"
        assert data["counts"]["signals"] == 3

    @patch("nse_momentum_lab.api.app.get_paper_feed_state", new_callable=AsyncMock)
    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_paper_feed_state(self, mock_sm: MagicMock, mock_feed: AsyncMock) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_feed.return_value = MagicMock(
            session_id="paper-1",
            source="kite",
            mode="full",
            status="READY",
            is_stale=False,
            subscription_count=3,
            heartbeat_at=None,
            last_quote_at=None,
            last_tick_at=None,
            last_bar_at=None,
            metadata_json={"feed_mode": "full"},
            updated_at=None,
        )

        response = self.client.get("/api/paper/feed-state/paper-1")
        assert response.status_code == 200
        data = response.json()
        assert data["feed_state"]["session_id"] == "paper-1"
        assert data["feed_state"]["source"] == "kite"

    @patch("nse_momentum_lab.api.app.list_paper_orders", new_callable=AsyncMock)
    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_paper_session_orders(self, mock_sm: MagicMock, mock_orders: AsyncMock) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_orders.return_value = [
            {
                "order_id": 11,
                "session_id": "paper-1",
                "signal_id": 101,
                "side": "BUY",
                "qty": 100,
                "order_type": "MARKET",
                "status": "COMPLETE",
            }
        ]

        response = self.client.get("/api/paper/sessions/paper-1/orders")
        assert response.status_code == 200
        data = response.json()
        assert len(data["orders"]) == 1
        assert data["orders"][0]["order_id"] == 11

    @patch("nse_momentum_lab.api.app.list_paper_fills", new_callable=AsyncMock)
    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_paper_session_fills(self, mock_sm: MagicMock, mock_fills: AsyncMock) -> None:
        mock_session = AsyncMock()
        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context
        mock_fills.return_value = [
            {
                "fill_id": 21,
                "session_id": "paper-1",
                "order_id": 11,
                "fill_time": "2026-03-21T09:20:00+00:00",
                "fill_price": 101.5,
                "qty": 100,
            }
        ]

        response = self.client.get("/api/paper/sessions/paper-1/fills")
        assert response.status_code == 200
        data = response.json()
        assert len(data["fills"]) == 1
        assert data["fills"][0]["fill_id"] == 21

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_dashboard_summary(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 10

        async def mock_execute(q):
            return mock_result

        mock_session.execute = mock_execute

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/dashboard/summary")
        assert response.status_code == 200
        data = response.json()
        assert "scan_runs" in data
        assert "backtest_runs" in data
        assert "open_positions" in data

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_recent_alerts(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_alert = MagicMock()
        mock_alert.job_name = "failed_job"
        mock_alert.asof_date = date(2024, 1, 1)
        mock_alert.error_json = {}
        mock_result.scalars.return_value.all.return_value = [mock_alert]
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/alerts/recent")
        assert response.status_code == 200
        data = response.json()
        assert "alerts" in data

    def test_pipeline_run_invalid_date(self) -> None:
        response = self.client.post(
            "/api/pipeline/run",
            json={"date": "invalid-date"},
        )
        assert response.status_code == 400

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_pipeline_run_success(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_job = MagicMock()
        mock_job.job_run_id = 123
        mock_result.scalar_one_or_none.return_value = mock_job
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.post(
            "/api/pipeline/run",
            json={"date": "2024-01-15"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "triggered"

    @patch("nse_momentum_lab.db.market_db.get_market_db")
    def test_analytics_coverage_empty_symbols(self, mock_get_market_db: MagicMock) -> None:
        mock_db = MagicMock()
        mock_db.get_available_symbols.return_value = []
        mock_get_market_db.return_value = mock_db

        response = self.client.get("/api/analytics/coverage?series=EQ")
        assert response.status_code == 200
        assert response.json()["symbols"] == []

    @patch("nse_momentum_lab.db.market_db.get_market_db")
    def test_analytics_returns(self, mock_get_market_db: MagicMock) -> None:
        mock_db = MagicMock()
        mock_db.get_available_symbols.return_value = []
        mock_get_market_db.return_value = mock_db

        response = self.client.get("/api/analytics/returns")
        assert response.status_code == 200
        assert response.json()["symbols"] == []

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_scan_summary(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute = AsyncMock(return_value=mock_result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/scans/summary")
        assert response.status_code == 200

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_dataset_manifests(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()

        count_result = MagicMock()
        count_result.scalar.return_value = 1

        data_result = MagicMock()
        manifest = MagicMock()
        manifest.dataset_id = 1
        manifest.dataset_kind = "duckdb_market_daily"
        manifest.dataset_hash = "abcd1234"
        manifest.code_hash = "code1"
        manifest.params_hash = "default"
        manifest.source_uri = "s3://market-data/parquet/daily/*/*.parquet"
        manifest.row_count = 1000
        manifest.min_trading_date = date(2024, 1, 1)
        manifest.max_trading_date = date(2024, 1, 31)
        manifest.metadata_json = {"dataset_hash": "abcd1234"}
        manifest.created_at = None
        data_result.scalars.return_value.all.return_value = [manifest]

        mock_session.execute = AsyncMock(side_effect=[count_result, data_result])

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get("/api/datasets/manifests?dataset_kind=duckdb_market_daily")
        assert response.status_code == 200
        data = response.json()
        assert "manifests" in data
        assert data["pagination"]["total"] == 1
        assert data["manifests"][0]["dataset_hash"] == "abcd1234"

    @patch("nse_momentum_lab.api.app.get_sessionmaker")
    def test_dataset_manifest_latest(self, mock_sm: MagicMock) -> None:
        mock_session = AsyncMock()
        result = MagicMock()
        manifest = MagicMock()
        manifest.dataset_id = 2
        manifest.dataset_kind = "duckdb_market_daily"
        manifest.dataset_hash = "latesthash"
        manifest.code_hash = "code1"
        manifest.params_hash = "default"
        manifest.source_uri = "s3://market-data/parquet/daily/*/*.parquet"
        manifest.row_count = 500
        manifest.min_trading_date = date(2024, 2, 1)
        manifest.max_trading_date = date(2024, 2, 29)
        manifest.metadata_json = {"dataset_hash": "latesthash"}
        manifest.created_at = None
        result.scalar_one_or_none.return_value = manifest
        mock_session.execute = AsyncMock(return_value=result)

        mock_context = MagicMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_context.__aexit__ = AsyncMock()
        mock_sm.return_value.return_value = mock_context

        response = self.client.get(
            "/api/datasets/manifests/latest?dataset_kind=duckdb_market_daily"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["manifest"]["dataset_hash"] == "latesthash"
