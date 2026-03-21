from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, timedelta
from typing import Any

import polars as pl
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import case, func, select

from nse_momentum_lab.api.validation import (
    ValidationError,
    validate_series,
    validate_status,
    validate_symbols_csv,
)
from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import (
    BtTrade,
    DatasetManifest,
    ExpArtifact,
    ExpMetric,
    ExpRun,
    JobRun,
    PaperPosition,
    RefSymbol,
    RptBtDaily,
    RptScanDaily,
    ScanResult,
    ScanRun,
)
from nse_momentum_lab.db.paper import (
    get_paper_feed_state,
    get_paper_session_summary,
    list_paper_fills,
    list_paper_order_events,
    list_paper_orders,
    list_paper_session_signals,
    list_paper_sessions,
)

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if os.getenv("SCHEDULER_ENABLED", "false").lower() == "true":
            from nse_momentum_lab.api.scheduler import start_scheduler

            start_scheduler()
        else:
            logger.info("Scheduler disabled - set SCHEDULER_ENABLED=true to enable")

        yield

        from nse_momentum_lab.api.scheduler import stop_scheduler

        stop_scheduler()

    app = FastAPI(title="nse-momentum-lab API", lifespan=lifespan)

    # CORS middleware - protect state-changing endpoints from CSRF
    allowed_origins = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:8501").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["X-API-Key", "Content-Type"],
    )

    @app.exception_handler(ValidationError)
    async def validation_error_handler(_request, exc: ValidationError):
        raise HTTPException(status_code=400, detail={"error": exc.message, "field": exc.field})

    def _percentile(sorted_values: list[float], p: float) -> float | None:
        """Compute percentile on pre-sorted values with linear interpolation."""
        if not sorted_values:
            return None
        if p <= 0:
            return float(sorted_values[0])
        if p >= 100:
            return float(sorted_values[-1])

        n = len(sorted_values)
        pos = (n - 1) * (p / 100.0)
        lo = int(pos)
        hi = min(lo + 1, n - 1)
        if hi == lo:
            return float(sorted_values[lo])
        w = pos - lo
        return float(sorted_values[lo] * (1 - w) + sorted_values[hi] * w)

    def _manifest_to_dict(m: DatasetManifest) -> dict[str, Any]:
        return {
            "dataset_id": m.dataset_id,
            "dataset_kind": m.dataset_kind,
            "dataset_hash": m.dataset_hash,
            "code_hash": m.code_hash,
            "params_hash": m.params_hash,
            "source_uri": m.source_uri,
            "row_count": m.row_count,
            "min_trading_date": m.min_trading_date.isoformat() if m.min_trading_date else None,
            "max_trading_date": m.max_trading_date.isoformat() if m.max_trading_date else None,
            "metadata_json": m.metadata_json,
            "created_at": m.created_at.isoformat() if m.created_at else None,
        }

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/ingestion/status")
    async def ingestion_status(asof_date: date | None = None) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            query = select(JobRun).order_by(JobRun.started_at.desc()).limit(10)
            if asof_date:
                query = query.where(JobRun.asof_date == asof_date)
            result = await session.execute(query)
            jobs = result.scalars().all()
            return {
                "jobs": [
                    {
                        "job_name": j.job_name,
                        "asof_date": j.asof_date.isoformat() if j.asof_date else None,
                        "status": j.status,
                        "started_at": j.started_at.isoformat() if j.started_at else None,
                        "duration_ms": j.duration_ms,
                    }
                    for j in jobs
                ]
            }

    @app.get("/api/symbols")
    async def symbols(status: str | None = None) -> dict[str, Any]:
        validated_status = validate_status(status)
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            query = select(RefSymbol)
            if validated_status:
                query = query.where(RefSymbol.status == validated_status)
            result = await session.execute(query)
            syms = result.scalars().all()
            return {
                "symbols": [
                    {
                        "symbol_id": s.symbol_id,
                        "symbol": s.symbol,
                        "series": s.series,
                        "status": s.status,
                    }
                    for s in syms
                ]
            }

    @app.get("/api/analytics/coverage")
    async def analytics_coverage(
        symbols_csv: str | None = None,
        series: str = "EQ",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Dataset coverage using DuckDB + Parquet (see ADR-009)."""
        from nse_momentum_lab.db.market_db import get_market_db

        symbols_filter = validate_symbols_csv(symbols_csv)

        db = get_market_db()
        all_symbols = db.get_available_symbols()

        if symbols_filter:
            symbols = [s for s in all_symbols if s in symbols_filter]
        else:
            symbols = all_symbols

        if not symbols:
            return {
                "range": {
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "global_trading_days": 0,
                },
                "symbols": [],
            }

        s_date = start_date.isoformat() if start_date else "2015-01-01"
        # Default end date: today + 1 year to capture recent data
        default_end = (date.today() + timedelta(days=365)).isoformat()
        e_date = end_date.isoformat() if end_date else default_end

        # Use parameterized query to prevent SQL injection
        placeholders = ",".join("?" for _ in symbols)
        rows = db.con.execute(
            f"""SELECT symbol, MIN(date) as min_date, MAX(date) as max_date, COUNT(*) as days
               FROM v_daily WHERE symbol IN ({placeholders})
               AND date >= ? AND date <= ?
               GROUP BY symbol ORDER BY symbol""",
            [*symbols, s_date, e_date],
        ).fetchall()

        out = []
        for row in rows:
            out.append(
                {
                    "symbol": row[0],
                    "min_date": str(row[1]) if row[1] else None,
                    "max_date": str(row[2]) if row[2] else None,
                    "days": row[3],
                }
            )

        return {
            "range": {
                "start_date": s_date,
                "end_date": e_date,
                "global_trading_days": len({r[1] for r in rows if r[1]}),
            },
            "symbols": out,
        }

    @app.get("/api/analytics/returns")
    async def analytics_returns(
        symbols_csv: str | None = None,
        series: str = "EQ",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Return distribution stats using DuckDB + Parquet (see ADR-009)."""
        import polars as pl

        from nse_momentum_lab.db.market_db import get_market_db

        symbols_filter = validate_symbols_csv(symbols_csv)

        db = get_market_db()
        all_symbols = db.get_available_symbols()

        # Configurable symbol limit for analytics (default 100 for performance)
        max_symbols = int(os.getenv("ANALYTICS_MAX_SYMBOLS", "100"))

        if symbols_filter:
            symbols = [s for s in all_symbols if s in symbols_filter]
        else:
            symbols = all_symbols[:max_symbols]

        if not symbols:
            return {"symbols": []}

        s_date = start_date.isoformat() if start_date else "2015-01-01"
        # Default end date: today + 1 year to capture recent data
        default_end = (date.today() + timedelta(days=365)).isoformat()
        e_date = end_date.isoformat() if end_date else default_end

        df = db.query_daily_multi(symbols, s_date, e_date)

        if df.is_empty():
            return {"symbols": []}

        out = []
        for symbol in symbols:
            sym_df = df.filter(pl.col("symbol") == symbol)
            if sym_df.is_empty():
                continue

            closes = sym_df.sort("date")["close"].to_list()
            rets = []
            for i in range(1, len(closes)):
                prev = closes[i - 1]
                if prev and prev != 0:
                    rets.append((closes[i] - prev) / prev)

            if not rets:
                continue

            rets_sorted = sorted(rets)
            out.append(
                {
                    "symbol": symbol,
                    "days": len(closes),
                    "ret_obs": len(rets),
                    "ge_4p": sum(1 for r in rets if r >= 0.04),
                    "ge_2p": sum(1 for r in rets if r >= 0.02),
                    "max_ret": max(rets) if rets else None,
                    "min_ret": min(rets) if rets else None,
                    "p95_ret": rets_sorted[int(len(rets) * 0.95)] if rets else None,
                    "p99_ret": rets_sorted[int(len(rets) * 0.99)] if rets else None,
                }
            )

        out.sort(key=lambda x: x["symbol"])
        return {"symbols": out}

    @app.get("/api/scans/runs")
    async def scan_runs(
        asof_date: date | None = None,
        limit: int = Query(20, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            count_query = select(func.count(ScanRun.scan_run_id))
            if asof_date:
                count_query = count_query.where(ScanRun.asof_date == asof_date)
            total_result = await session.execute(count_query)
            total = total_result.scalar() or 0

            query = select(ScanRun).order_by(ScanRun.started_at.desc()).offset(offset).limit(limit)
            if asof_date:
                query = query.where(ScanRun.asof_date == asof_date)
            result = await session.execute(query)
            runs = result.scalars().all()
            return {
                "runs": [
                    {
                        "scan_run_id": r.scan_run_id,
                        "asof_date": r.asof_date.isoformat() if r.asof_date else None,
                        "status": r.status,
                        "dataset_hash": r.dataset_hash,
                        "started_at": r.started_at.isoformat() if r.started_at else None,
                    }
                    for r in runs
                ],
                "pagination": {
                    "offset": offset,
                    "limit": limit,
                    "total": total,
                },
            }

    @app.get("/api/scans/results")
    async def scan_results(
        scan_run_id: int | None = None,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            base_filters = []
            if scan_run_id:
                base_filters.append(ScanResult.scan_run_id == scan_run_id)

            count_query = select(func.count(ScanResult.scan_run_id))
            if base_filters:
                count_query = count_query.where(*base_filters)
            total_result = await session.execute(count_query)
            total = total_result.scalar() or 0

            query = (
                select(ScanResult, RefSymbol)
                .join(RefSymbol, ScanResult.symbol_id == RefSymbol.symbol_id)
                .order_by(ScanResult.score.desc().nullslast())
                .offset(offset)
                .limit(limit)
            )
            if base_filters:
                query = query.where(*base_filters)
            result = await session.execute(query)
            return {
                "results": [
                    {
                        "symbol": s.symbol,
                        "symbol_id": r.symbol_id,
                        "asof_date": r.asof_date.isoformat() if r.asof_date else None,
                        "passed": r.passed,
                        "score": float(r.score) if r.score else None,
                        "reason_json": r.reason_json,
                    }
                    for r, s in result.all()
                ],
                "pagination": {
                    "offset": offset,
                    "limit": limit,
                    "total": total,
                },
            }

    @app.get("/api/scans/summary")
    async def scans_summary(
        start_date: date | None = None,
        end_date: date | None = None,
        symbols_csv: str | None = None,
        series: str = "EQ",
    ) -> dict[str, Any]:
        """Summarize historical scan outcomes over a date range.

        This answers: "Over the whole sample history, did anything pass?" without
        pretending a single as-of date is representative.
        """

        symbols_filter = validate_symbols_csv(symbols_csv)
        validated_series = validate_series(series)
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            sym_query = select(RefSymbol.symbol_id).where(RefSymbol.series == validated_series)
            if symbols_filter:
                sym_query = sym_query.where(RefSymbol.symbol.in_(symbols_filter))
            symbol_ids = [r[0] for r in (await session.execute(sym_query)).all()]
            if not symbol_ids:
                return {
                    "range": {"start_date": None, "end_date": None},
                    "totals": {},
                    "by_date": [],
                }

            filters = [ScanResult.symbol_id.in_(symbol_ids)]
            if start_date:
                filters.append(ScanResult.asof_date >= start_date)
            if end_date:
                filters.append(ScanResult.asof_date <= end_date)

            totals_row = (
                await session.execute(
                    select(
                        func.count().label("rows"),
                        func.sum(case((ScanResult.passed.is_(True), 1), else_=0)).label("passed"),
                        func.count(func.distinct(ScanResult.asof_date)).label("dates"),
                    ).where(*filters)
                )
            ).one()

            by_date_rows = (
                await session.execute(
                    select(
                        ScanResult.asof_date.label("asof_date"),
                        func.count().label("scanned"),
                        func.sum(case((ScanResult.passed.is_(True), 1), else_=0)).label("passed"),
                    )
                    .where(*filters)
                    .group_by(ScanResult.asof_date)
                    .order_by(ScanResult.asof_date.desc())
                    .limit(500)
                )
            ).all()

            return {
                "range": {
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                },
                "totals": {
                    "rows": int(totals_row.rows or 0),
                    "passed": int(totals_row.passed or 0),
                    "dates": int(totals_row.dates or 0),
                },
                "by_date": [
                    {
                        "asof_date": r.asof_date.isoformat() if r.asof_date else None,
                        "scanned": int(r.scanned or 0),
                        "passed": int(r.passed or 0),
                    }
                    for r in by_date_rows
                ],
            }

    @app.get("/api/datasets/manifests")
    async def dataset_manifests(
        dataset_kind: str | None = None,
        limit: int = Query(50, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            count_q = select(func.count(DatasetManifest.dataset_id))
            query = select(DatasetManifest)
            if dataset_kind:
                count_q = count_q.where(DatasetManifest.dataset_kind == dataset_kind)
                query = query.where(DatasetManifest.dataset_kind == dataset_kind)

            total_result = await session.execute(count_q)
            total = total_result.scalar() or 0
            result = await session.execute(
                query.order_by(DatasetManifest.created_at.desc()).offset(offset).limit(limit)
            )
            manifests = result.scalars().all()
            return {
                "manifests": [_manifest_to_dict(m) for m in manifests],
                "pagination": {
                    "offset": offset,
                    "limit": limit,
                    "total": int(total),
                },
            }

    @app.get("/api/datasets/manifests/latest")
    async def latest_dataset_manifest(dataset_kind: str) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(DatasetManifest)
                .where(DatasetManifest.dataset_kind == dataset_kind)
                .order_by(DatasetManifest.created_at.desc())
                .limit(1)
            )
            manifest = result.scalar_one_or_none()
            if manifest is None:
                return {"manifest": None}
            return {"manifest": _manifest_to_dict(manifest)}

    @app.get("/api/experiments")
    async def experiments(
        strategy_hash: str | None = None,
        dataset_hash: str | None = None,
        limit: int = Query(20, ge=1, le=500),
    ) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            query = select(ExpRun).order_by(ExpRun.started_at.desc()).limit(limit)
            if strategy_hash:
                query = query.where(ExpRun.strategy_hash == strategy_hash)
            if dataset_hash:
                query = query.where(ExpRun.dataset_hash == dataset_hash)
            result = await session.execute(query)
            experiments = result.scalars().all()
            return {
                "experiments": [
                    {
                        "exp_hash": e.exp_hash,
                        "strategy_name": e.strategy_name,
                        "status": e.status,
                        "started_at": e.started_at.isoformat() if e.started_at else None,
                    }
                    for e in experiments
                ]
            }

    @app.get("/api/experiments/{exp_hash}")
    async def experiment_detail(exp_hash: str) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(select(ExpRun).where(ExpRun.exp_hash == exp_hash))
            exp = result.scalar_one_or_none()
            if not exp:
                raise HTTPException(status_code=404, detail="Experiment not found")

            # Get metrics
            metrics_result = await session.execute(
                select(ExpMetric).where(ExpMetric.exp_run_id == exp.exp_run_id)
            )
            metrics = {}
            for row in metrics_result.all():
                m = row[0]  # ExpMetric object
                metrics[m.metric_name] = float(m.metric_value) if m.metric_value else None

            # Get trades with symbol names
            trades_result = await session.execute(
                select(BtTrade, RefSymbol)
                .join(RefSymbol, BtTrade.symbol_id == RefSymbol.symbol_id)
                .where(BtTrade.exp_run_id == exp.exp_run_id)
                .order_by(BtTrade.entry_date.desc())
            )

            trades = []
            for row in trades_result.all():
                t, s = row[0], row[1]
                trades.append(
                    {
                        "symbol": s.symbol,
                        "entry_date": t.entry_date.isoformat() if t.entry_date else None,
                        "entry_price": float(t.entry_price),
                        "entry_mode": t.entry_mode,
                        "exit_date": t.exit_date.isoformat() if t.exit_date else None,
                        "exit_price": float(t.exit_price) if t.exit_price else None,
                        "pnl": float(t.pnl) if t.pnl else None,
                        "pnl_r": float(t.pnl_r) if t.pnl_r else None,
                        "exit_reason": t.exit_reason,
                        "mfe_r": float(t.mfe_r) if t.mfe_r else None,
                        "mae_r": float(t.mae_r) if t.mae_r else None,
                        "fees": float(t.fees) if t.fees else 0,
                        "slippage_bps": float(t.slippage_bps) if t.slippage_bps else 0,
                    }
                )

            return {
                "exp_hash": exp.exp_hash,
                "strategy_name": exp.strategy_name,
                "strategy_hash": exp.strategy_hash,
                "dataset_hash": exp.dataset_hash,
                "params": exp.params_json,
                "status": exp.status,
                "started_at": exp.started_at.isoformat() if exp.started_at else None,
                "finished_at": exp.finished_at.isoformat() if exp.finished_at else None,
                "metrics": metrics,
                "trades": trades,
                "trade_count": len(trades),
            }

    @app.get("/api/experiments/{exp_hash}/artifacts")
    async def experiment_artifacts(exp_hash: str) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            exp_result = await session.execute(select(ExpRun).where(ExpRun.exp_hash == exp_hash))
            exp = exp_result.scalar_one_or_none()
            if not exp:
                raise HTTPException(status_code=404, detail="Experiment not found")

            artifacts_result = await session.execute(
                select(ExpArtifact)
                .where(ExpArtifact.exp_run_id == exp.exp_run_id)
                .order_by(ExpArtifact.artifact_name.asc())
            )
            artifacts = artifacts_result.scalars().all()
            return {
                "exp_hash": exp_hash,
                "artifacts": [
                    {
                        "artifact_name": a.artifact_name,
                        "uri": a.uri,
                        "sha256": a.sha256,
                    }
                    for a in artifacts
                ],
            }

    @app.get("/api/experiments/{exp_hash}/trades")
    async def get_experiment_trades_paginated(
        exp_hash: str,
        limit: int = Query(50, ge=1, le=2000),
        offset: int = Query(0, ge=0),
        exit_reason: str | None = None,
    ) -> dict[str, Any]:
        """Get paginated trade list with optional filtering by exit reason."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            # Get experiment
            exp_result = await session.execute(select(ExpRun).where(ExpRun.exp_hash == exp_hash))
            exp = exp_result.scalar_one_or_none()
            if not exp:
                raise HTTPException(status_code=404, detail="Experiment not found")

            # Build query
            query = (
                select(BtTrade, RefSymbol)
                .join(RefSymbol, BtTrade.symbol_id == RefSymbol.symbol_id)
                .where(BtTrade.exp_run_id == exp.exp_run_id)
            )

            # Optional exit reason filter
            if exit_reason:
                query = query.where(BtTrade.exit_reason == exit_reason)

            # Order by entry date descending
            query = query.order_by(BtTrade.entry_date.desc())

            # Get total count
            count_subquery = query.subquery()
            count_result = await session.execute(select(func.count()).select_from(count_subquery))
            total = count_result.scalar()

            # Apply pagination
            query = query.limit(limit).offset(offset)
            trades_result = await session.execute(query)

            trades = []
            for row in trades_result.all():
                t, s = row[0], row[1]
                trades.append(
                    {
                        "trade_id": t.trade_id,
                        "symbol": s.symbol,
                        "entry_date": t.entry_date.isoformat() if t.entry_date else None,
                        "exit_date": t.exit_date.isoformat() if t.exit_date else None,
                        "entry_price": float(t.entry_price),
                        "exit_price": float(t.exit_price) if t.exit_price else None,
                        "pnl": float(t.pnl) if t.pnl else None,
                        "pnl_r": float(t.pnl_r) if t.pnl_r else None,
                        "exit_reason": t.exit_reason,
                        "qty": t.qty,
                    }
                )

            return {
                "trades": trades,
                "total": total,
                "limit": limit,
                "offset": offset,
                "exp_hash": exp_hash,
            }

    @app.post("/api/experiments/compare")
    async def compare_experiments(request: dict[str, Any]) -> dict[str, Any]:
        """Compare multiple experiments side-by-side."""
        exp_hashes = request.get("exp_hashes", [])
        if len(exp_hashes) < 2:
            raise HTTPException(status_code=400, detail="Need at least 2 experiments to compare")

        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            # Get experiments
            result = await session.execute(select(ExpRun).where(ExpRun.exp_hash.in_(exp_hashes)))
            experiments = result.scalars().all()

            if not experiments:
                raise HTTPException(status_code=404, detail="No experiments found")

            # Batch fetch all metrics for all experiments at once (avoid N+1)
            exp_run_ids = [exp.exp_run_id for exp in experiments]
            metrics_result = await session.execute(
                select(ExpMetric).where(ExpMetric.exp_run_id.in_(exp_run_ids))
            )

            # Group metrics by exp_run_id
            metrics_by_exp: dict[int, dict[str, float]] = {}
            for m in metrics_result.scalars().all():
                if m.exp_run_id not in metrics_by_exp:
                    metrics_by_exp[m.exp_run_id] = {}
                metrics_by_exp[m.exp_run_id][m.metric_name] = (
                    float(m.metric_value) if m.metric_value else None
                )

            # Build comparison using pre-fetched metrics
            comparison = []
            for exp in experiments:
                comparison.append(
                    {
                        "exp_hash": exp.exp_hash,
                        "strategy_name": exp.strategy_name,
                        "started_at": exp.started_at.isoformat() if exp.started_at else None,
                        "status": exp.status,
                        "metrics": metrics_by_exp.get(exp.exp_run_id, {}),
                    }
                )

            return {"experiments": comparison}

    @app.get("/api/jobs/{job_id}/progress")
    async def get_job_progress(job_id: int) -> dict[str, Any]:
        """Get real-time job progress for dashboard."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(select(JobRun).where(JobRun.job_run_id == job_id))
            job = result.scalar_one_or_none()

            if not job:
                raise HTTPException(status_code=404, detail="Job not found")

            # Calculate progress from metrics_json if available
            progress = 0
            current_step = "Unknown"
            if job.metrics_json:
                progress = job.metrics_json.get("progress_percent", 0)
                current_step = job.metrics_json.get("current_step", "Unknown")

            return {
                "job_id": job.job_run_id,
                "job_name": job.job_name,
                "status": job.status,
                "progress_percent": progress,
                "current_step": current_step,
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
                "duration_ms": job.duration_ms,
                "error": job.error_json if job.status == "FAILED" else None,
            }

    @app.get("/api/experiments/{exp_hash}/export")
    async def export_experiment_trades(exp_hash: str, format: str = "csv") -> Response:
        """Export trade data in CSV or Excel format."""
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            # Get experiment
            exp_result = await session.execute(select(ExpRun).where(ExpRun.exp_hash == exp_hash))
            exp = exp_result.scalar_one_or_none()
            if not exp:
                raise HTTPException(status_code=404, detail="Experiment not found")

            # Get all trades with symbols
            trades_result = await session.execute(
                select(BtTrade, RefSymbol)
                .join(RefSymbol, BtTrade.symbol_id == RefSymbol.symbol_id)
                .where(BtTrade.exp_run_id == exp.exp_run_id)
                .order_by(BtTrade.entry_date)
            )

            # Build dataframe
            trades_data = []
            for row in trades_result.all():
                t, s = row[0], row[1]
                trades_data.append(
                    {
                        "Symbol": s.symbol,
                        "Entry Date": t.entry_date.isoformat() if t.entry_date else None,
                        "Exit Date": t.exit_date.isoformat() if t.exit_date else None,
                        "Entry Price": t.entry_price,
                        "Exit Price": t.exit_price if t.exit_price else None,
                        "P&L (₹)": t.pnl if t.pnl else 0,
                        "P&L (R)": t.pnl_r if t.pnl_r else None,
                        "Exit Reason": t.exit_reason,
                        "Quantity": t.qty,
                    }
                )

            df = pl.DataFrame(trades_data)

            # Export based on format
            if format.lower() == "csv":
                output = df.write_csv()
                media_type = "text/csv"
                filename = f"trades_{exp_hash}.csv"
            else:
                raise HTTPException(status_code=400, detail=f"Invalid format '{format}'. Use 'csv'")

            return Response(
                content=output,
                media_type=media_type,
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

    @app.post("/api/pipeline/run")
    async def trigger_pipeline(request: dict[str, Any]) -> dict[str, Any]:
        """Trigger daily pipeline execution."""
        from datetime import date as dt

        trading_date_str = request.get("date", dt.today().isoformat())
        skip_ingest = request.get("skip_ingest", False)

        # Validate date format
        try:
            if isinstance(trading_date_str, str):
                trading_date = dt.fromisoformat(trading_date_str)
            else:
                trading_date = trading_date_str
        except ValueError as e:
            raise HTTPException(
                status_code=400, detail="Invalid date format. Use YYYY-MM-DD"
            ) from e

        # Validate date range - disallow dates in the future
        today = dt.today()
        if trading_date > today:
            raise HTTPException(
                status_code=400,
                detail=f"Date {trading_date.isoformat()} is in the future",
            )

        # Validate reasonable lower bound (NSE NIFTY started in 1996, being conservative)
        min_allowed_date = dt(2000, 1, 1)
        if trading_date < min_allowed_date:
            raise HTTPException(
                status_code=400,
                detail=f"Date {trading_date.isoformat()} is before {min_allowed_date.isoformat()}",
            )

        # Warn about weekends (NSE closed)
        if trading_date.weekday() >= 5:  # Saturday=5, Sunday=6
            logger.warning(f"Pipeline requested for weekend date {trading_date} (NSE closed)")

        # Run pipeline in background task
        async def run_pipeline_bg():
            try:
                from nse_momentum_lab.cli.pipeline import run_daily_pipeline

                await run_daily_pipeline(trading_date, skip_ingest=skip_ingest, track_job=True)
            except Exception as e:
                logger.error(f"Pipeline failed: {e}")

        # Spawn background task (don't await)
        asyncio.create_task(run_pipeline_bg())  # noqa: RUF006

        # Get job ID to return to caller
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(JobRun)
                .where(JobRun.job_name == "daily_pipeline")
                .where(JobRun.asof_date == trading_date)
                .order_by(JobRun.started_at.desc())
                .limit(1)
            )
            job = result.scalar_one_or_none()

            return {
                "status": "triggered",
                "job_id": job.job_run_id if job else None,
                "trading_date": trading_date.isoformat()
                if hasattr(trading_date, "isoformat")
                else str(trading_date),
            }

    @app.get("/api/paper/positions")
    async def paper_positions(
        open_only: bool = True, session_id: str | None = None
    ) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            query = select(PaperPosition)
            if open_only:
                query = query.where(PaperPosition.closed_at.is_(None))
            if session_id:
                query = query.where(PaperPosition.session_id == session_id)
            result = await session.execute(query)
            positions = result.scalars().all()
            return {
                "positions": [
                    {
                        "position_id": p.position_id,
                        "session_id": p.session_id,
                        "symbol_id": p.symbol_id,
                        "opened_at": p.opened_at.isoformat() if p.opened_at else None,
                        "closed_at": p.closed_at.isoformat() if p.closed_at else None,
                        "avg_entry": p.avg_entry,
                        "avg_exit": p.avg_exit,
                        "qty": p.qty,
                        "pnl": p.pnl,
                        "state": p.state,
                    }
                    for p in positions
                ]
            }

    @app.get("/api/paper/sessions")
    async def paper_sessions(
        status: str | None = None, limit: int = Query(20, ge=1, le=200)
    ) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            rows = await list_paper_sessions(session, status=status, limit=limit)
            return {"sessions": rows}

    @app.get("/api/paper/sessions/{session_id}")
    async def paper_session(session_id: str) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            summary = await get_paper_session_summary(session, session_id)
            if summary is None:
                raise HTTPException(status_code=404, detail="Paper session not found")
            return summary

    @app.get("/api/paper/feed-state/{session_id}")
    async def paper_feed_state(session_id: str) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            row = await get_paper_feed_state(session, session_id)
            if row is None:
                raise HTTPException(status_code=404, detail="Paper feed state not found")
            return {
                "feed_state": {
                    "session_id": row.session_id,
                    "source": row.source,
                    "mode": row.mode,
                    "status": row.status,
                    "is_stale": row.is_stale,
                    "subscription_count": row.subscription_count,
                    "heartbeat_at": row.heartbeat_at.isoformat() if row.heartbeat_at else None,
                    "last_quote_at": row.last_quote_at.isoformat() if row.last_quote_at else None,
                    "last_tick_at": row.last_tick_at.isoformat() if row.last_tick_at else None,
                    "last_bar_at": row.last_bar_at.isoformat() if row.last_bar_at else None,
                    "metadata_json": row.metadata_json or {},
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
            }

    @app.get("/api/paper/sessions/{session_id}/signals")
    async def paper_session_signals(session_id: str) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            rows = await list_paper_session_signals(session, session_id)
            return {"signals": rows}

    @app.get("/api/paper/sessions/{session_id}/orders")
    async def paper_session_orders(session_id: str) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            rows = await list_paper_orders(session, session_id)
            return {"orders": rows}

    @app.get("/api/paper/sessions/{session_id}/fills")
    async def paper_session_fills(session_id: str) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            rows = await list_paper_fills(session, session_id)
            return {"fills": rows}

    @app.get("/api/paper/sessions/{session_id}/order-events")
    async def paper_order_events(
        session_id: str, limit: int = Query(100, ge=1, le=500)
    ) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            rows = await list_paper_order_events(session, session_id, limit=limit)
            return {"events": rows}

    @app.get("/api/alerts/recent")
    async def recent_alerts(limit: int = Query(20, ge=1, le=500)) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            result = await session.execute(
                select(JobRun)
                .where(JobRun.status == "FAILED")
                .order_by(JobRun.started_at.desc())
                .limit(limit)
            )
            alerts = result.scalars().all()
            return {
                "alerts": [
                    {
                        "job_name": j.job_name,
                        "asof_date": j.asof_date.isoformat() if j.asof_date else None,
                        "error": j.error_json,
                    }
                    for j in alerts
                ]
            }

    @app.get("/api/dashboard/summary")
    async def dashboard_summary(asof_date: date | None = None) -> dict[str, Any]:
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            scan_query = select(func.count(RptScanDaily.scan_def_id))
            if asof_date:
                scan_query = scan_query.where(RptScanDaily.asof_date == asof_date)
            scan_result = await session.execute(scan_query)
            scan_count = scan_result.scalar() or 0

            bt_query = select(func.count(RptBtDaily.strategy_name))
            if asof_date:
                bt_query = bt_query.where(RptBtDaily.asof_date == asof_date)
            bt_result = await session.execute(bt_query)
            bt_count = bt_result.scalar() or 0

            pos_query = select(func.count(PaperPosition.position_id)).where(
                PaperPosition.closed_at.is_(None)
            )
            pos_result = await session.execute(pos_query)
            open_positions = pos_result.scalar() or 0

            session_query = select(func.count(PaperPosition.position_id))
            session_query = session_query.where(PaperPosition.closed_at.is_(None))
            session_result = await session.execute(session_query)
            open_paper_positions = session_result.scalar() or 0

            return {
                "date": asof_date.isoformat() if asof_date else None,
                "scan_runs": scan_count,
                "backtest_runs": bt_count,
                "open_positions": open_positions,
                "open_paper_positions": open_paper_positions,
            }

    return app


app = create_app()
