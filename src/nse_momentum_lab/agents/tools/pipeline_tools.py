"""Pipeline tools for the nse-momentum-lab AI agent.

These tools allow the agent to:
- Check pipeline status
- Run ingestion
- Run adjustment
- Run scans
- Generate rollups
- Get database statistics
- Query market data from DuckDB
- Analyze backtest results from PostgreSQL
"""

from __future__ import annotations

import subprocess
from datetime import datetime
from typing import Any

from sqlalchemy import func, select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.db.models import BtTrade, ExpRun, JobRun, RefSymbol, ScanRun


async def get_market_data_stats(symbol: str, start_date: str, end_date: str) -> dict[str, Any]:
    """
    Get market data statistics from DuckDB for a symbol.

    Args:
        symbol: Stock symbol (e.g., "RELIANCE")
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Dictionary with market data statistics
    """
    db = get_market_db()

    try:
        # Get basic OHLCV stats
        df = db.con.execute(
            """
            SELECT
                COUNT(*) as total_days,
                MIN(close) as min_close,
                MAX(close) as max_close,
                AVG(close) as avg_close,
                AVG(volume) as avg_volume
            FROM v_daily
            WHERE symbol = ? AND date >= ? AND date <= ?
            """,
            [symbol, start_date, end_date],
        ).pl()

        if df.is_empty():
            return {
                "status": "error",
                "message": f"No data found for {symbol} between {start_date} and {end_date}",
            }

        row = df.row(0)

        # Get feature stats if available
        feature_stats = {}
        try:
            feat_df = db.con.execute(
                """
                SELECT
                    AVG(atr_20) as avg_atr,
                    MIN(atr_20) as min_atr,
                    MAX(atr_20) as max_atr,
                    AVG(rs_252) as avg_rs
                FROM feat_daily
                WHERE symbol = ? AND trading_date >= ? AND trading_date <= ?
                """,
                [symbol, start_date, end_date],
            ).pl()

            if not feat_df.is_empty():
                feat_row = feat_df.row(0)
                feature_stats = {
                    "avg_atr": float(feat_row["avg_atr"]) if feat_row["avg_atr"] else None,
                    "min_atr": float(feat_row["min_atr"]) if feat_row["min_atr"] else None,
                    "max_atr": float(feat_row["max_atr"]) if feat_row["max_atr"] else None,
                    "avg_rs": float(feat_row["avg_rs"]) if feat_row["avg_rs"] else None,
                }
        except Exception:
            pass  # Features not built yet

        return {
            "status": "success",
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "total_days": int(row["total_days"]),
            "min_close": float(row["min_close"]) if row["min_close"] else None,
            "max_close": float(row["max_close"]) if row["max_close"] else None,
            "avg_close": float(row["avg_close"]) if row["avg_close"] else None,
            "avg_volume": float(row["avg_volume"]) if row["avg_volume"] else None,
            **feature_stats,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error querying market data: {e!s}",
        }


async def get_backtest_summary(exp_run_id: int) -> dict[str, Any]:
    """
    Get backtest summary from PostgreSQL.

    Args:
        exp_run_id: Experiment run ID

    Returns:
        Dictionary with backtest summary statistics
    """
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # Get experiment info
        exp_result = await session.execute(select(ExpRun).filter(ExpRun.exp_run_id == exp_run_id))
        exp = exp_result.scalar_one_or_none()

        if not exp:
            return {
                "status": "error",
                "message": f"Experiment {exp_run_id} not found",
            }

        # Get trade statistics
        result = await session.execute(
            select(
                func.count(BtTrade.trade_id).label("total_trades"),
                func.sum(func.case((BtTrade.pnl > 0, 1), else_=0)).label("wins"),
                func.sum(func.case((BtTrade.pnl <= 0, 1), else_=0)).label("losses"),
                func.avg(BtTrade.pnl_r).label("avg_r"),
                func.max(BtTrade.pnl_r).label("best_r"),
                func.min(BtTrade.pnl_r).label("worst_r"),
                func.sum(BtTrade.pnl).label("total_pnl"),
            ).filter(BtTrade.exp_run_id == exp_run_id)
        )
        row = result.one()

        return {
            "status": "success",
            "exp_run_id": exp_run_id,
            "strategy_name": exp.strategy_name,
            "total_trades": int(row.total_trades) if row.total_trades else 0,
            "wins": int(row.wins) if row.wins else 0,
            "losses": int(row.losses) if row.losses else 0,
            "win_rate": (row.wins / row.total_trades) if row.total_trades else 0.0,
            "avg_r": float(row.avg_r) if row.avg_r else 0.0,
            "best_r": float(row.best_r) if row.best_r else 0.0,
            "worst_r": float(row.worst_r) if row.worst_r else 0.0,
            "total_pnl": float(row.total_pnl) if row.total_pnl else 0.0,
        }


async def get_pipeline_status(limit: int = 10) -> dict[str, Any]:
    """
    Get recent pipeline job runs.

    Args:
        limit: Number of recent jobs to return

    Returns:
        Dictionary with job run information
    """
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await session.execute(
            select(JobRun).order_by(JobRun.started_at.desc()).limit(limit)
        )
        jobs = result.scalars().all()

        return {
            "status": "success",
            "count": len(jobs),
            "jobs": [
                {
                    "job_name": j.job_name,
                    "asof_date": j.asof_date.isoformat() if j.asof_date else None,
                    "status": j.status,
                    "started_at": j.started_at.isoformat() if j.started_at else None,
                    "duration_ms": j.duration_ms,
                }
                for j in jobs
            ],
        }


async def get_database_stats() -> dict[str, Any]:
    """
    Get database statistics.

    Returns:
        Dictionary with table row counts and metadata
    """
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # Get table counts
        tables = {
            "ref_symbol": RefSymbol,
            # Add more tables as needed
        }

        stats = {}
        for name, model in tables.items():
            result = await session.execute(select(func.count()).select_from(model))
            stats[name] = result.scalar() or 0

        return {"status": "success", "stats": stats, "timestamp": datetime.now().isoformat()}


async def get_scan_results(scan_run_id: int | None = None, limit: int = 10) -> dict[str, Any]:
    """
    Get scan results.

    Args:
        scan_run_id: Optional scan run ID to filter
        limit: Maximum number of results to return

    Returns:
        Dictionary with scan results
    """
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        query = select(ScanRun).order_by(ScanRun.started_at.desc()).limit(limit)
        if scan_run_id:
            query = query.where(ScanRun.scan_run_id == scan_run_id)

        result = await session.execute(query)
        runs = result.scalars().all()

        return {
            "status": "success",
            "count": len(runs),
            "runs": [
                {
                    "scan_run_id": r.scan_run_id,
                    "asof_date": r.asof_date.isoformat() if r.asof_date else None,
                    "status": r.status,
                    "dataset_hash": r.dataset_hash,
                }
                for r in runs
            ],
        }


def run_ingestion_sync(trading_date: str) -> dict[str, Any]:
    """
    Run ingestion for a specific trading date (synchronous wrapper).

    Args:
        trading_date: Date in YYYY-MM-DD format

    Returns:
        Dictionary with execution result
    """
    try:
        # Validate date format
        datetime.strptime(trading_date, "%Y-%m-%d")

        # Run the ingestion worker
        cmd = ["uv", "run", "python", "-m", "nse_momentum_lab.services.ingest.worker", trading_date]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )

        if result.returncode == 0:
            return {
                "status": "success",
                "trading_date": trading_date,
                "message": f"Ingestion completed for {trading_date}",
                "stdout": result.stdout[-500:] if result.stdout else "",  # Last 500 chars
            }
        else:
            return {
                "status": "failed",
                "trading_date": trading_date,
                "error": result.stderr[-500:] if result.stderr else "Unknown error",
            }

    except ValueError as e:
        return {
            "status": "failed",
            "trading_date": trading_date,
            "error": f"Invalid date format: {e}",
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "failed",
            "trading_date": trading_date,
            "error": "Ingestion timed out after 5 minutes",
        }
    except Exception as e:
        return {"status": "failed", "trading_date": trading_date, "error": str(e)}


def run_adjustment_sync() -> dict[str, Any]:
    """
    Run corporate action adjustment (synchronous wrapper).

    Returns:
        Dictionary with execution result
    """
    try:
        cmd = ["uv", "run", "python", "-m", "nse_momentum_lab.services.adjust.worker"]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout
        )

        if result.returncode == 0:
            return {
                "status": "success",
                "message": "Adjustment completed successfully",
                "stdout": result.stdout[-500:] if result.stdout else "",
            }
        else:
            return {
                "status": "failed",
                "error": result.stderr[-500:] if result.stderr else "Unknown error",
            }

    except subprocess.TimeoutExpired:
        return {"status": "failed", "error": "Adjustment timed out after 10 minutes"}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


def run_scan_sync(trading_date: str) -> dict[str, Any]:
    """
    Run scan for a specific trading date (synchronous wrapper).

    Args:
        trading_date: Date in YYYY-MM-DD format

    Returns:
        Dictionary with execution result
    """
    try:
        # Validate date format
        datetime.strptime(trading_date, "%Y-%m-%d")

        cmd = ["uv", "run", "python", "-m", "nse_momentum_lab.services.scan.worker", trading_date]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )

        if result.returncode == 0:
            return {
                "status": "success",
                "trading_date": trading_date,
                "message": f"Scan completed for {trading_date}",
                "stdout": result.stdout[-500:] if result.stdout else "",
            }
        else:
            return {
                "status": "failed",
                "trading_date": trading_date,
                "error": result.stderr[-500:] if result.stderr else "Unknown error",
            }

    except ValueError as e:
        return {
            "status": "failed",
            "trading_date": trading_date,
            "error": f"Invalid date format: {e}",
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "failed",
            "trading_date": trading_date,
            "error": "Scan timed out after 5 minutes",
        }
    except Exception as e:
        return {"status": "failed", "trading_date": trading_date, "error": str(e)}


def run_rollup_sync(trading_date: str) -> dict[str, Any]:
    """
    Run daily rollup for a specific trading date (synchronous wrapper).

    Args:
        trading_date: Date in YYYY-MM-DD format

    Returns:
        Dictionary with execution result
    """
    try:
        # Validate date format
        parsed_date = datetime.strptime(trading_date, "%Y-%m-%d").date()

        cmd = [
            "uv",
            "run",
            "python",
            "-c",
            f"import asyncio; from datetime import date; from nse_momentum_lab.services.rollup.worker import run_daily_rollup; asyncio.run(run_daily_rollup(date({parsed_date.year},{parsed_date.month},{parsed_date.day})))",
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
        )

        if result.returncode == 0:
            return {
                "status": "success",
                "trading_date": trading_date,
                "message": f"Rollup completed for {trading_date}",
                "stdout": result.stdout[-500:] if result.stdout else "",
            }
        else:
            return {
                "status": "failed",
                "trading_date": trading_date,
                "error": result.stderr[-500:] if result.stderr else "Unknown error",
            }

    except ValueError as e:
        return {
            "status": "failed",
            "trading_date": trading_date,
            "error": f"Invalid date format: {e}",
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "failed",
            "trading_date": trading_date,
            "error": "Rollup timed out after 5 minutes",
        }
    except Exception as e:
        return {"status": "failed", "trading_date": trading_date, "error": str(e)}
