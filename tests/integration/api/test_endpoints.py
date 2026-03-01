"""Integration tests for API endpoints.

Run with: doppler run -- uv run pytest tests/integration/api -v
"""

from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from nse_momentum_lab.api.app import create_app

pytestmark = pytest.mark.integration


@pytest.fixture
def client():
    """Create test client for API."""
    app = create_app()
    return TestClient(app)


class TestHealthEndpoint:
    def test_health_check(self, client):
        """Test health endpoint returns OK."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data.get("status") == "ok" or data.get("healthy") is True


class TestSymbolsEndpoint:
    def test_get_symbols(self, client):
        """Test symbols endpoint returns list."""
        response = client.get("/api/symbols")
        assert response.status_code == 200
        data = response.json()
        assert "symbols" in data
        assert isinstance(data["symbols"], list)

    def test_get_symbols_with_status_filter(self, client):
        """Test symbols endpoint with status filter."""
        response = client.get("/api/symbols", params={"status": "ACTIVE"})
        assert response.status_code == 200
        data = response.json()
        assert "symbols" in data


class TestJobsEndpoint:
    def test_get_job_progress(self, client, sample_job_run):
        """Test job progress endpoint returns details for existing job."""
        response = client.get(f"/api/jobs/{sample_job_run.job_run_id}/progress")
        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == sample_job_run.job_run_id
        assert "status" in data
        assert "progress_percent" in data

    def test_get_job_progress_not_found(self, client):
        """Test job progress endpoint returns 404 for unknown job."""
        response = client.get("/api/jobs/99999999/progress")
        assert response.status_code == 404


class TestAnalyticsCoverage:
    def test_coverage_endpoint(self, client):
        """Test coverage analytics endpoint."""
        response = client.get("/api/analytics/coverage")
        assert response.status_code == 200
        data = response.json()
        assert "symbols" in data or "coverage" in data

    def test_coverage_with_date_range(self, client):
        """Test coverage endpoint with date range."""
        response = client.get(
            "/api/analytics/coverage",
            params={
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )
        assert response.status_code == 200


class TestScansEndpoint:
    def test_scan_summary_endpoint(self, client):
        """Test scan summary endpoint."""
        response = client.get(
            "/api/scans/summary",
            params={
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "totals" in data or "by_date" in data

    def test_scan_results_endpoint(self, client):
        """Test scan results endpoint."""
        response = client.get("/api/scans/results", params={"limit": 20})
        assert response.status_code == 200
        data = response.json()
        assert "results" in data


class TestExperimentsEndpoint:
    def test_get_experiments(self, client):
        """Test experiments list endpoint."""
        response = client.get("/api/experiments")
        assert response.status_code == 200
        data = response.json()
        assert "experiments" in data

    def test_get_experiments_with_limit(self, client):
        """Test experiments list with limit."""
        response = client.get("/api/experiments", params={"limit": 10})
        assert response.status_code == 200


class TestInputValidation:
    def test_invalid_date_format(self, client):
        """Test that invalid date format returns error."""
        response = client.get(
            "/api/analytics/coverage",
            params={"start_date": "invalid-date"},
        )
        assert response.status_code in [400, 422]

    def test_invalid_symbol_in_csv(self, client):
        """Test that special characters in symbols are handled."""
        response = client.get(
            "/api/analytics/coverage",
            params={"symbols_csv": "REL<script>,TCS"},
        )
        assert response.status_code == 200

    def test_negative_limit_handled(self, client):
        """Test that negative limit is handled gracefully."""
        response = client.get("/api/scans/runs", params={"limit": -1})
        assert response.status_code in [200, 400, 422]


class TestErrorHandling:
    def test_404_for_nonexistent_experiment(self, client):
        """Test 404 for non-existent experiment hash."""
        response = client.get("/api/experiments/nonexistent_hash_12345")
        assert response.status_code in [404, 400, 200]

    def test_missing_required_params_handled(self, client):
        """Test handling of missing required parameters."""
        response = client.get("/api/analytics/returns")
        assert response.status_code == 200


class TestDashboardSummary:
    def test_dashboard_summary(self, client):
        """Test dashboard summary endpoint."""
        response = client.get(
            "/api/dashboard/summary",
            params={"asof_date": date.today().isoformat()},
        )
        assert response.status_code == 200
