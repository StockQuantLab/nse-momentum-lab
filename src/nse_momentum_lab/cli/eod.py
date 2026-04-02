"""CLI entrypoint for nseml-eod — one-shot EOD pipeline runner.

Thin wrapper around existing pipeline.run_daily_pipeline().
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, timedelta

from nse_momentum_lab.cli.pipeline import run_daily_pipeline

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nseml-eod",
        description="Run the EOD data pipeline for a given date (ingest -> features -> scan -> rollup).",
    )
    parser.add_argument("--date", "-d", type=str, help="Trading date YYYY-MM-DD (default: today)")
    parser.add_argument("--yesterday", "-y", action="store_true", help="Use yesterday's date")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip ingestion stage")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run")
    return parser


def _resolve_date(args: argparse.Namespace) -> date:
    if args.yesterday:
        return date.today() - timedelta(days=1)
    if args.date:
        return date.fromisoformat(args.date)
    return date.today()


async def _run(args: argparse.Namespace) -> None:
    trading_date = _resolve_date(args)

    if args.dry_run:
        logger.info(
            "[DRY-RUN] Would run EOD pipeline for %s (skip_ingest=%s)",
            trading_date,
            args.skip_ingest,
        )
        return

    logger.info("Starting EOD pipeline for %s", trading_date)
    result = await run_daily_pipeline(
        trading_date=trading_date,
        skip_ingest=args.skip_ingest,
        track_job=True,
    )

    # Use real PipelineResult API: overall_status is a string
    if "FAILED" in result.overall_status:
        logger.error("EOD pipeline FAILED: %s", result.overall_status)
        sys.exit(1)
    elif result.overall_status == "SKIPPED":
        logger.info("EOD pipeline SKIPPED (already completed)")
    else:
        logger.info("EOD pipeline completed: %s", result.overall_status)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
