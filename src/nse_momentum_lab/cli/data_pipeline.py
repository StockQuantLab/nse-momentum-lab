"""CLI for incremental data pipeline operations."""

import asyncio
import logging
import sys
from datetime import date as date_type
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent.parent))

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.services.data_lake import (
    DataLayer,
    DatasetKind,
    IncrementalRefreshPlanner,
    MinIOPublisher,
    PartitionKey,
    PartitionManifestManager,
    PartitionStatus,
)
from nse_momentum_lab.services.ingest.pipeline import IngestionPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Incremental data pipeline commands."""
    pass


@cli.command()
@click.option("--date", "-d", type=str, default=None, help="Date to ingest (YYYY-MM-DD)")
@click.option("--source", "-s", type=click.Path(exists=True), multiple=True, help="Source files")
@click.option("--dry-run", is_flag=True, help="Show plan without executing")
def ingest(date: str | None, source: tuple, dry_run: bool):
    """Ingest new data into the pipeline."""
    trading_date = date_type.fromisoformat(date) if date else date_type.today()

    if dry_run:
        click.echo(f"Would ingest data for {trading_date}")
        for src in source:
            click.echo(f"  - {src}")
        return

    async def _run():
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            pipeline = IngestionPipeline(session)
            result = await pipeline.ingest_daily_data(
                source_files=[Path(s) for s in source],
                trading_date=trading_date,
            )
            click.echo(f"Ingestion: {result.status}")
            click.echo(f"  Rows processed: {result.rows_processed}")
            click.echo(f"  Partitions created: {result.partitions_created}")

    asyncio.run(_run())


@cli.command()
@click.option("--dataset", "-d", type=str, required=True, help="Dataset kind (daily, 5min, events)")
@click.option("--layer", "-l", type=str, default="silver", help="Data layer (bronze, silver, gold)")
def discover_partitions(dataset: str, layer: str):
    """Discover and list partitions for a dataset."""

    async def _run():
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            manager = PartitionManifestManager(session)

            try:
                dataset_kind = DatasetKind(dataset)
                data_layer = DataLayer(layer)
            except ValueError:
                click.echo("Invalid dataset or layer", err=True)
                return

            partitions = await manager.get_partitions_by_status(
                status=PartitionStatus.READY,
                dataset_kind=dataset_kind,
                data_layer=data_layer,
            )

            click.echo(f"Found {len(partitions)} partitions:")
            for p in partitions:
                click.echo(f"  - {p.partition_key}: {p.row_count} rows")

    asyncio.run(_run())


@cli.command()
@click.option("--start", "-s", type=str, required=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", "-e", type=str, required=True, help="End date (YYYY-MM-DD)")
@click.option("--dry-run", is_flag=True, help="Show plan without executing")
def plan_refresh(start: str, end: str, dry_run: bool):
    """Plan incremental refresh for a date range."""
    start_date = date_type.fromisoformat(start)
    end_date = date_type.fromisoformat(end)

    async def _run():
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            planner = IncrementalRefreshPlanner(session)
            plan = await planner.plan_daily_data_update((start_date, end_date))

            click.echo(f"Refresh plan for {start_date} to {end_date}:")
            click.echo(f"  Total partitions: {plan.get_total_partitions()}")

            for feature_set, refresh_plan in plan.refresh_plans.items():
                click.echo(f"\n  {feature_set}:")
                click.echo(f"    Partitions to update: {len(refresh_plan.partitions_to_update)}")
                click.echo(f"    Partitions to build: {len(refresh_plan.partitions_to_build)}")

    asyncio.run(_run())


@cli.command()
@click.option("--dataset", "-d", type=str, required=True, help="Dataset kind")
@click.option("--year", "-y", type=int, required=True, help="Year to mark stale")
@click.option("--symbol", "-s", type=str, default=None, help="Optional symbol filter")
def mark_stale(dataset: str, year: int, symbol: str | None):
    """Mark partitions as stale (for testing or repair)."""

    async def _run():
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            manager = PartitionManifestManager(session)

            try:
                dataset_kind = DatasetKind(dataset)
            except ValueError:
                click.echo(f"Invalid dataset: {dataset}", err=True)
                return

            from nse_momentum_lab.services.data_lake import RefreshScope

            scope = RefreshScope(year_filter=[year])
            if symbol:
                scope.symbol_filter = [symbol]

            marked = await manager.mark_partitions_stale(dataset_kind, scope)
            click.echo(f"Marked {len(marked)} partitions as stale")

    asyncio.run(_run())


@cli.command()
@click.option("--bucket-check", is_flag=True, help="Check MinIO bucket exists")
def minio(bucket_check: bool):
    """MinIO operations."""

    async def _check():
        publisher = MinIOPublisher()
        if bucket_check:
            exists = await publisher.ensure_bucket_exists()
            click.echo(f"Bucket exists: {exists}")

    asyncio.run(_check())


@cli.command()
@click.option("--feature-set", "-f", type=str, required=True, help="Feature set name")
def stale_partitions(feature_set: str):
    """List stale partitions for a feature set."""

    async def _run():
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            planner = IncrementalRefreshPlanner(session)
            partitions = await planner.get_stale_partitions(feature_set)

            click.echo(f"Found {len(partitions)} stale partitions for {feature_set}:")
            for p in partitions:
                click.echo(f"  - {p.partition_key}")

    asyncio.run(_run())


@cli.command()
def list_datasets():
    """List all datasets and their status."""

    async def _run():
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            manager = PartitionManifestManager(session)

            for kind in DatasetKind:
                for status in PartitionStatus:
                    partitions = await manager.get_partitions_by_status(
                        status=status,
                        dataset_kind=kind,
                    )
                    if partitions:
                        click.echo(f"{kind.value}/{status.value}: {len(partitions)} partitions")

    asyncio.run(_run())


@cli.command()
@click.option("--symbol", "-s", type=str, required=True, help="Symbol to repair")
@click.option("--year", "-y", type=int, required=True, help="Year to repair")
@click.option("--source", type=click.Path(exists=True), required=True, help="Source data path")
def repair(symbol: str, year: int, source: str):
    """Repair a specific partition."""

    async def _run():
        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session:
            pipeline = IngestionPipeline(session)
            result = await pipeline.repair_partition(
                dataset_kind=DatasetKind.DAILY,
                partition_key=PartitionKey(symbol=symbol, year=year),
                source_path=Path(source),
            )
            click.echo(f"Repair: {result.status}")
            if result.issues:
                for issue in result.issues:
                    click.echo(f"  Issue: {issue}")

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
