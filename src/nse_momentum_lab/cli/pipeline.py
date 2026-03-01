"""CLI for running the daily pipeline."""

import asyncio
import hashlib
import logging
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import click

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import JobRun
from nse_momentum_lab.services.ingest.worker import IngestionWorker
from nse_momentum_lab.services.rollup.worker import run_daily_rollup
from nse_momentum_lab.services.scan.worker import ScanWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _generate_idempotency_key(job_name: str, trading_date: date, skip_ingest: bool = False) -> str:
    content = f"{job_name}:{trading_date.isoformat()}:{skip_ingest}"
    return hashlib.sha256(content.encode()).hexdigest()[:32]


async def _get_existing_job(idempotency_key: str) -> JobRun | None:
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await session.execute(
            select(JobRun).where(JobRun.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()


async def _create_pipeline_job(trading_date: date, skip_ingest: bool = False) -> tuple[int, bool]:
    """Create a job in JobRun table and return (job_id, is_new).

    Returns:
        tuple: (job_run_id, is_new) where is_new is False if job already existed
    """
    idempotency_key = _generate_idempotency_key("daily_pipeline", trading_date, skip_ingest)

    existing_job = await _get_existing_job(idempotency_key)
    if existing_job:
        if existing_job.status == "RUNNING":
            logger.info(f"Job {existing_job.job_run_id} already running for {trading_date}")
            return existing_job.job_run_id, False
        elif existing_job.status == "COMPLETED":
            logger.info(f"Job {existing_job.job_run_id} already completed for {trading_date}")
            return existing_job.job_run_id, False
        elif existing_job.status == "FAILED":
            logger.info(
                f"Job {existing_job.job_run_id} previously failed for {trading_date}, creating new"
            )

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        async with session.begin():
            job = JobRun(
                job_name="daily_pipeline",
                asof_date=trading_date,
                idempotency_key=idempotency_key,
                status="RUNNING",
                started_at=datetime.now(UTC),
            )
            session.add(job)
            await session.flush()
            return job.job_run_id, True


async def _update_job_progress(
    job_id: int,
    step: str,
    progress: int | None = None,
    status: str = "RUNNING",
    error: dict[str, Any] | None = None,
) -> bool:
    """Update job progress in database. Returns True if successful."""
    sessionmaker = get_sessionmaker()
    try:
        async with sessionmaker() as session:
            async with session.begin():
                job = await session.get(JobRun, job_id)
                if job is None:
                    logger.error(f"Job {job_id} not found for progress update")
                    return False

                job.metrics_json = {
                    "current_step": step,
                    "progress_percent": progress,
                }

                if status:
                    job.status = status

                if status == "COMPLETED":
                    job.finished_at = datetime.now(UTC)
                    if job.started_at:
                        duration_ms = int((job.finished_at - job.started_at).total_seconds() * 1000)
                        job.duration_ms = duration_ms

                if error:
                    job.error_json = error

                return True
    except Exception as e:
        logger.error(f"Failed to update job progress: {e}")
        return False


class PipelineResult:
    def __init__(self, trading_date: date):
        self.trading_date = trading_date
        self.stages: dict[str, dict[str, Any]] = {}
        self.overall_status = "PENDING"

    def add_stage(self, stage_name: str, status: str, details: dict[str, Any] | None = None):
        self.stages[stage_name] = {"status": status, "details": details or {}}

    def set_success(self):
        self.overall_status = "SUCCESS"

    def set_failed(self, failed_stage: str):
        self.overall_status = f"FAILED_AT_{failed_stage.upper()}"

    def print_summary(self):
        print(f"\n{'=' * 70}")
        print(f"Pipeline Summary for {self.trading_date}")
        print(f"{'=' * 70}")

        for stage_name, result in self.stages.items():
            status_icon = "✅" if result["status"] == "SUCCESS" else "❌"
            print(f"{status_icon} {stage_name}: {result['status']}")
            if result.get("details"):
                for key, value in result["details"].items():
                    print(f"   {key}: {value}")

        print(f"\nOverall Status: {self.overall_status}")
        print(f"{'=' * 70}\n")


async def run_daily_pipeline(
    trading_date: date, skip_ingest: bool = False, track_job: bool = True
) -> PipelineResult:
    """Run full daily pipeline with optional job tracking."""
    result = PipelineResult(trading_date)

    job_id = None
    is_new_job = True
    if track_job:
        try:
            job_id, is_new_job = await _create_pipeline_job(trading_date, skip_ingest)
            if is_new_job:
                print(f"✅ Created job #{job_id} for tracking pipeline execution")
            else:
                print(f"ℹ️  Found existing job #{job_id} for {trading_date}")
                existing_job = await _get_existing_job(
                    _generate_idempotency_key("daily_pipeline", trading_date, skip_ingest)
                )
                if existing_job and existing_job.status == "RUNNING":
                    print("⚠️  Job already running, skipping execution")
                    result.add_stage("pipeline", "SKIPPED", {"reason": "Job already running"})
                    result.overall_status = "SKIPPED"
                    return result
                elif existing_job and existing_job.status == "COMPLETED":
                    print("⚠️  Job already completed, skipping execution")
                    result.add_stage("pipeline", "SKIPPED", {"reason": "Job already completed"})
                    result.overall_status = "SKIPPED"
                    return result
        except Exception as e:
            print(f"⚠️  Failed to create tracking job: {e}")

    print(f"\n{'=' * 70}")
    print(f"Running Pipeline for {trading_date.isoformat()}")
    print(f"{'=' * 70}\n")

    if not skip_ingest:
        print("Stage 1/4: Ingestion...")
        if job_id:
            await _update_job_progress(job_id, "Ingesting data...", 10)

        try:
            worker = IngestionWorker()
            ingest_result = await worker.run(trading_date)
            await worker.close()

            if ingest_result.status == "SUCCESS":
                result.add_stage(
                    "ingestion",
                    "SUCCESS",
                    {
                        "rows_processed": ingest_result.rows_processed,
                        "rows_quarantined": ingest_result.rows_quarantined,
                    },
                )
                print(f"✅ Ingestion complete ({ingest_result.rows_processed} rows)")
            else:
                result.add_stage(
                    "ingestion", ingest_result.status, {"issues": ingest_result.issues}
                )
                result.set_failed("ingestion")
                result.print_summary()
                return result
        except Exception as e:
            result.add_stage("ingestion", "FAILED", {"error": str(e)})
            result.set_failed("ingestion")
            result.print_summary()
            return result
    else:
        print("⏭️  Skipping Ingestion (--skip-ingest)")
        result.add_stage("ingestion", "SKIPPED", {})

    print("\nStage 2/4: Adjustment...")
    if job_id:
        await _update_job_progress(job_id, "Adjusting prices...", 30)

    # Adjustment is now handled in DuckDB - skip this stage
    print("⏭️  Adjustment (now handled by DuckDB/Parquet)")
    result.add_stage("adjustment", "SKIPPED", {"note": "Using DuckDB/Parquet data"})

    print("\nStage 3/4: Scan...")
    if job_id:
        await _update_job_progress(job_id, "Running momentum scan...", 60)

    try:
        scan_worker = ScanWorker()
        scan_result = await scan_worker.run(trading_date)
        result.add_stage(
            "scan",
            "SUCCESS",
            {
                "scan_run_id": scan_result.scan_run_id,
                "candidates_found": scan_result.candidates_found,
                "total_universe": scan_result.total_universe,
            },
        )
        print(f"✅ Scan complete ({scan_result.candidates_found} candidates)")
    except Exception as e:
        result.add_stage("scan", "FAILED", {"error": str(e)})
        result.set_failed("scan")
        result.print_summary()
        return result

    print("\nStage 4/4: Rollup...")
    if job_id:
        await _update_job_progress(job_id, "Generating daily rollups...", 90)

    try:
        await run_daily_rollup(trading_date)
        result.add_stage("rollup", "SUCCESS")
        print("✅ Rollup complete")
    except Exception as e:
        result.add_stage("rollup", "FAILED", {"error": str(e)})
        if job_id:
            await _update_job_progress(
                job_id,
                f"Failed: {e!s}",
                progress=None,
                status="FAILED",
                error={"error": str(e), "type": type(e).__name__},
            )
        result.set_failed("rollup")
        result.print_summary()
        return result

    # Mark job as completed
    if job_id:
        await _update_job_progress(job_id, "Complete", 100, status="COMPLETED")

    result.set_success()
    result.print_summary()
    return result


@click.command()
@click.option(
    "--date",
    "-d",
    type=str,
    default=None,
    help="Trading date (YYYY-MM-DD). Defaults to today.",
)
@click.option(
    "--yesterday",
    "-y",
    is_flag=True,
    help="Use yesterday's date.",
)
@click.option(
    "--skip-ingest",
    is_flag=True,
    help="Skip ingestion stage (use existing data).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would run without executing.",
)
def main(
    date: str | None,
    yesterday: bool,
    skip_ingest: bool,
    dry_run: bool,
):
    """Run the complete daily pipeline (ingest → adjust → scan → rollup)."""
    from datetime import date as dt

    if yesterday:
        trading_date = dt.today() - timedelta(days=1)
    elif date:
        trading_date = dt.fromisoformat(date)
    else:
        trading_date = dt.today()

    if trading_date > dt.today():
        click.echo(f"❌ Error: Date {trading_date} is in the future", err=True)
        sys.exit(1)

    if trading_date.weekday() >= 5:
        click.echo(f"⚠️  Warning: {trading_date} is a weekend (NSE closed)")

    if dry_run:
        click.echo(f"� dry-run: Would run pipeline for {trading_date.isoformat()}")
        click.echo(f"   - Skip ingest: {skip_ingest}")
        return

    result = asyncio.run(run_daily_pipeline(trading_date, skip_ingest=skip_ingest))

    if result.overall_status != "SUCCESS":
        sys.exit(1)

    click.echo("\n✅ Pipeline completed successfully!")
    click.echo("\nView results at: http://localhost:8501")


if __name__ == "__main__":
    main()
