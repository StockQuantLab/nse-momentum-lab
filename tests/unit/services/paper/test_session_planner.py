from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_plan_blocked_skips_bootstrap():
    """BLOCKED verdict skips session bootstrap."""
    with patch(
        "nse_momentum_lab.services.paper.session_planner.check_readiness",
        new_callable=AsyncMock,
        return_value={"verdict": "BLOCKED", "coverage_ready": False, "reasons": ["data_coverage_gap"]},
    ) as mock_ready, patch(
        "nse_momentum_lab.services.paper.session_planner.bootstrap_session",
        new_callable=AsyncMock,
    ) as mock_bootstrap:
        from nse_momentum_lab.services.paper.session_planner import (
            SessionPlan,
            plan_sessions,
        )
        config = SessionPlan(
            trade_date=date(2026, 4, 1),
            strategy_variants=[("thresholdbreakout", 0.04)],
        )
        manifest = await plan_sessions(config)
        assert manifest.verdict == "BLOCKED"
        assert len(manifest.sessions) == 0
        mock_bootstrap.assert_not_called()


@pytest.mark.asyncio
async def test_plan_ready_bootstraps_all_variants():
    """READY verdict bootstraps a session for each variant."""
    with patch(
        "nse_momentum_lab.services.paper.session_planner.check_readiness",
        new_callable=AsyncMock,
        return_value={"verdict": "READY", "coverage_ready": True, "reasons": []},
    ) as mock_ready, patch(
        "nse_momentum_lab.services.paper.session_planner.bootstrap_session",
        new_callable=AsyncMock,
        return_value=type("SE", (), {"strategy": "thresholdbreakout", "threshold": 0.04, "session_id": "sess-1", "status": "PLANNED"})(),
    ) as mock_bootstrap:
        from nse_momentum_lab.services.paper.session_planner import (
            SessionPlan,
            plan_sessions,
        )
        config = SessionPlan(
            trade_date=date(2026, 4, 1),
            strategy_variants=[
                ("thresholdbreakout", 0.04),
                ("thresholdbreakout", 0.02),
                ("thresholdbreakdown", 0.04),
            ],
        )
        manifest = await plan_sessions(config)
        assert manifest.verdict == "READY"
        assert mock_bootstrap.call_count == 3
        assert len(manifest.sessions) == 3


def test_manifest_to_dict():
    """PlanManifest serializes to dict correctly."""
    from nse_momentum_lab.services.paper.session_planner import (
        PlanManifest,
        SessionEntry,
    )
    manifest = PlanManifest(
        trade_date="2026-04-01",
        verdict="READY",
        coverage_ready=True,
        sessions=[
            SessionEntry(strategy="thresholdbreakout", threshold=0.04, session_id="sess-1"),
        ],
    )
    d = manifest.to_dict()
    assert d["trade_date"] == "2026-04-01"
    assert d["verdict"] == "READY"
    assert len(d["sessions"]) == 1
    assert d["sessions"][0]["session_id"] == "sess-1"


@pytest.mark.asyncio
async def test_plan_raises_on_bootstrap_failure():
    """plan_sessions raises SystemExit when any bootstrap has no session_id."""
    with patch(
        "nse_momentum_lab.services.paper.session_planner.check_readiness",
        new_callable=AsyncMock,
        return_value={"verdict": "READY", "coverage_ready": True, "reasons": []},
    ), patch(
        "nse_momentum_lab.services.paper.session_planner.bootstrap_session",
        new_callable=AsyncMock,
        return_value=type("SE", (), {"strategy": "thresholdbreakout", "threshold": 0.04, "session_id": None, "status": "UNKNOWN"})(),
    ):
        from nse_momentum_lab.services.paper.session_planner import (
            SessionPlan,
            plan_sessions,
        )
        config = SessionPlan(
            trade_date=date(2026, 4, 1),
            strategy_variants=[("thresholdbreakout", 0.04)],
        )
        with pytest.raises(SystemExit, match="Session bootstrap failed"):
            await plan_sessions(config)
