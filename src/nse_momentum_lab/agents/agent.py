"""NSE Momentum Lab Agent - Research Assistant using phidata.

This module provides an LLM-powered research assistant that can:
- Query scan results and backtest experiments
- Explain why stocks passed/failed scans
- Compare strategy variants
- Provide portfolio insights

Usage:
    # CLI mode
    doppler run -- uv run nse-agent -q "Show today's momentum stocks"

    # Dashboard
    doppler run -- uv run nseml-dashboard
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from phi.agent import Agent
from phi.memory.db import PostgresMemoryDb
from phi.model.anthropic import Claude
from phi.storage.agent.postgres import PostgresAgentStorage

from nse_momentum_lab.agents.tools import pipeline_tools
from nse_momentum_lab.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AGENT_NAME = "nse_momentum_researcher"
AGENT_DESCRIPTION = """You are a quant research assistant for NSE momentum trading strategies.
You have access to:
- Pipeline status and job tracking
- Scan results (4% breakout + 2LYNCH candidates)
- Backtest experiments and metrics
- Database statistics

You help users understand their trading data without computing any trades yourself.
All analysis is based on deterministic database queries."""


def get_model() -> Claude:
    """Create the LLM model instance."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError(
            "ANTHROPIC_API_KEY environment variable is required for agent functionality"
        )
    return Claude(
        id="claude-sonnet-4-5-20250929",
        api_key=api_key,
        temperature=0.7,
        max_tokens=2048,
    )


def create_agent(
    user_id: str = "default",
    session_id: str | None = None,
) -> Agent:
    """Create a configured research agent with tools and memory."""

    settings = get_settings()

    storage = PostgresAgentStorage(
        table_name="agent_sessions",
        db_url=settings.database_url,
    )

    memory_db = PostgresMemoryDb(
        table_name="agent_memory",
        db_url=settings.database_url,
    )

    tools = [
        pipeline_tools.get_pipeline_status,
        pipeline_tools.get_database_stats,
        pipeline_tools.get_scan_results,
        pipeline_tools.run_ingestion_sync,
        pipeline_tools.run_adjustment_sync,
        pipeline_tools.run_scan_sync,
        pipeline_tools.run_rollup_sync,
    ]

    agent = Agent(
        name=AGENT_NAME,
        model=get_model(),
        description=AGENT_DESCRIPTION,
        tools=tools,
        storage=storage,
        memory=memory_db,
        session_id=session_id,
        user_id=user_id,
        show_tool_calls=True,
        markdown=True,
        read_chat_history=True,
        num_history_messages=10,
        debug_mode=False,
    )

    return agent


def create_simple_agent() -> Agent:
    """Create a simple agent without persistence (for CLI use)."""

    tools = [
        pipeline_tools.get_pipeline_status,
        pipeline_tools.get_database_stats,
        pipeline_tools.get_scan_results,
    ]

    agent = Agent(
        name=AGENT_NAME,
        model=get_model(),
        description=AGENT_DESCRIPTION,
        tools=tools,
        markdown=True,
        show_tool_calls=False,
    )

    return agent


async def chat(message: str, user_id: str = "default", session_id: str | None = None) -> str:
    """Send a message to the agent and get the response."""
    agent = create_agent(user_id=user_id, session_id=session_id)
    response = await agent.arun(message)
    return response.content


def chat_sync(message: str, user_id: str = "default", session_id: str | None = None) -> str:
    """Synchronous wrapper for chat function."""
    return asyncio.run(chat(message, user_id, session_id))


async def get_recent_experiments(limit: int = 5) -> dict[str, Any]:
    """Get recent experiments for the dashboard."""
    from sqlalchemy import select

    from nse_momentum_lab.db import get_sessionmaker
    from nse_momentum_lab.db.models import ExpMetric, ExpRun

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await session.execute(
            select(ExpRun).order_by(ExpRun.started_at.desc()).limit(limit)
        )
        experiments = result.scalars().all()

        output = []
        for exp in experiments:
            metrics_result = await session.execute(
                select(ExpMetric).where(ExpMetric.exp_run_id == exp.exp_run_id)
            )
            metrics = {}
            for m in metrics_result.scalars().all():
                if m.metric_name.startswith("sharpe"):
                    metrics["sharpe"] = float(m.metric_value) if m.metric_value else 0
                elif m.metric_name.startswith("total_return"):
                    metrics["return"] = float(m.metric_value) if m.metric_value else 0

            output.append(
                {
                    "exp_hash": exp.exp_hash,
                    "strategy_name": exp.strategy_name,
                    "status": exp.status,
                    "started_at": exp.started_at.isoformat() if exp.started_at else None,
                    "metrics": metrics,
                }
            )

        return {"experiments": output}


async def get_daily_summary(asof_date: date | None = None) -> dict[str, Any]:
    """Get daily summary for a specific date."""
    from sqlalchemy import func, select

    from nse_momentum_lab.db import get_sessionmaker
    from nse_momentum_lab.db.models import JobRun, RptScanDaily
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    asof_date = asof_date or date.today()
    sessionmaker = get_sessionmaker()

    # Count open positions from DuckDB paper store.
    paper_db = PaperDB("data/paper.duckdb")
    try:
        open_positions = len(
            paper_db.execute("SELECT * FROM paper_positions WHERE state = 'OPEN'") or []
        )
    finally:
        paper_db.close()

    async with sessionmaker() as session:
        scan_result = await session.execute(
            select(func.count(RptScanDaily.scan_def_id)).where(RptScanDaily.asof_date == asof_date)
        )
        scan_count = scan_result.scalar() or 0

        job_result = await session.execute(
            select(JobRun).where(JobRun.asof_date == asof_date).order_by(JobRun.started_at.desc())
        )
        jobs = job_result.scalars().all()

        return {
            "date": asof_date.isoformat(),
            "scan_runs": scan_count,
            "open_positions": open_positions,
            "recent_jobs": [
                {
                    "job_name": j.job_name,
                    "status": j.status,
                    "started_at": j.started_at.isoformat() if j.started_at else None,
                }
                for j in jobs[:5]
            ],
        }


async def get_scan_candidates(scan_run_id: int | None = None, limit: int = 10) -> dict[str, Any]:
    """Get recent scan candidates."""
    from sqlalchemy import select

    from nse_momentum_lab.db import get_sessionmaker
    from nse_momentum_lab.db.models import RefSymbol, ScanResult, ScanRun

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        if scan_run_id:
            run_result = await session.execute(
                select(ScanRun).where(ScanRun.scan_run_id == scan_run_id)
            )
            run = run_result.scalar_one_or_none()
        else:
            run_result = await session.execute(
                select(ScanRun).order_by(ScanRun.started_at.desc()).limit(1)
            )
            run = run_result.scalar_one_or_none()

        if not run:
            return {"candidates": [], "message": "No scan runs found"}

        results = await session.execute(
            select(ScanResult, RefSymbol)
            .join(RefSymbol, ScanResult.symbol_id == RefSymbol.symbol_id)
            .where(ScanResult.scan_run_id == run.scan_run_id)
            .where(ScanResult.passed)
            .order_by(ScanResult.score.desc())
            .limit(limit)
        )

        candidates = []
        for sr, sym in results.all():
            candidates.append(
                {
                    "symbol": sym.symbol,
                    "score": float(sr.score) if sr.score else None,
                    "reason_json": sr.reason_json,
                }
            )

        return {
            "scan_run_id": run.scan_run_id,
            "asof_date": run.asof_date.isoformat() if run.asof_date else None,
            "candidates": candidates,
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="NSE Momentum Research Agent")
    parser.add_argument("-q", "--query", type=str, help="Query to send to the agent")
    parser.add_argument("--session", type=str, default=None, help="Session ID")
    parser.add_argument("--user", type=str, default="cli", help="User ID")
    args = parser.parse_args()

    if args.query:
        response = chat_sync(args.query, user_id=args.user, session_id=args.session)
        print("\n" + "=" * 60)
        print("AGENT RESPONSE")
        print("=" * 60)
        print(response)
        print("=" * 60)
    else:
        print('Usage: nse-agent -q "Your question here"')
        print('Example: nse-agent -q "Show today\'s momentum stocks"')


if __name__ == "__main__":
    main()
