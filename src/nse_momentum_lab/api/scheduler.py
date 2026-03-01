"""Scheduler for automated daily pipeline execution."""

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from nse_momentum_lab.cli.pipeline import run_daily_pipeline

logger = logging.getLogger(__name__)

# Create scheduler
scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")


async def scheduled_pipeline():
    """Run pipeline if today is a trading day."""
    from datetime import date as dt

    today = dt.today()

    # Check if trading day (skip weekends)
    if today.weekday() >= 5:  # Sat or Sun
        logger.info(f"Skipping {today} - weekend (NSE closed)")
        return

    # Run pipeline
    logger.info(f"Running scheduled pipeline for {today}")
    try:
        await run_daily_pipeline(today, skip_ingest=False, track_job=True)
    except Exception as e:
        logger.error(f"Scheduled pipeline failed: {e}")


def start_scheduler():
    """Start the daily pipeline scheduler."""
    # Schedule at 6:30 PM IST every weekday
    scheduler.add_job(
        scheduled_pipeline,
        trigger=CronTrigger(hour=18, minute=30, day_of_week="mon-fri"),
        id="daily-pipeline",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started - daily pipeline at 6:30 PM IST")


def stop_scheduler():
    """Stop the scheduler gracefully, allowing running jobs to complete."""
    try:
        if not scheduler.running:
            logger.info("Scheduler was not running")
            return
        logger.info("Stopping scheduler - waiting for running jobs to complete...")
        scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")
    except Exception as e:
        logger.error(f"Error stopping scheduler: {e}")
