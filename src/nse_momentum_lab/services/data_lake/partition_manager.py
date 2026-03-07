"""
Partition manifest manager for the incremental data pipeline.

This module provides the PartitionManifestManager service which:
- Discovers partitions in the data lake (local or MinIO)
- Computes partition checksums (SHA256)
- Tracks partition metadata in Postgres
- Publishes partition manifests to MinIO/Postgres
- Queries for stale/affected partitions

Partitions are the unit of incremental rebuild in the platform.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.config import get_settings
from nse_momentum_lab.db.models import (
    DatasetManifest,
    PartitionManifest,
)

logger = logging.getLogger(__name__)


class DataLayer(StrEnum):
    """Medallion architecture layers."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class PartitionStatus(StrEnum):
    """Partition lifecycle states."""

    READY = "READY"
    FAILED = "FAILED"
    STALE = "STALE"
    SUPERSEDED = "SUPERSEDED"


class DatasetKind(StrEnum):
    """Dataset kinds for partitioning."""

    DAILY = "daily"
    FIVE_MIN = "5min"
    EVENTS = "events"
    FEAT_DAILY_CORE = "feat_daily_core"
    FEAT_INTRADAY_CORE = "feat_intraday_core"
    FEAT_EVENT_CORE = "feat_event_core"
    FEAT_STRATEGY_DERIVED = "feat_strategy_derived"


@dataclass(frozen=True)
class PartitionKey:
    """Identifies a specific partition within a dataset."""

    symbol: str | None = None
    year: int | None = None
    month: int | None = None
    event_type: str | None = None

    def to_string(self) -> str:
        """Convert partition key to string format for storage."""
        parts = []
        if self.symbol:
            parts.append(f"symbol={self.symbol}")
        if self.year is not None:
            parts.append(f"year={self.year}")
        if self.month is not None:
            parts.append(f"month={self.month:02d}")
        if self.event_type:
            parts.append(f"type={self.event_type}")
        return "/".join(parts) if parts else "default"

    @classmethod
    def from_string(cls, key_str: str) -> PartitionKey:
        """Parse partition key string back to PartitionKey object."""
        kwargs = {}
        for part in key_str.split("/"):
            if "=" in part:
                k, v = part.split("=", 1)
                if k == "symbol":
                    kwargs["symbol"] = v
                elif k == "year":
                    kwargs["year"] = int(v)
                elif k == "month":
                    kwargs["month"] = int(v)
                elif k == "type":
                    kwargs["event_type"] = v
        return cls(**kwargs)


@dataclass
class PartitionInfo:
    """Metadata about a discovered or tracked partition."""

    dataset_kind: DatasetKind
    partition_key: PartitionKey
    data_layer: DataLayer
    object_uri: str
    row_count: int = 0
    min_trading_date: date | None = None
    max_trading_date: date | None = None
    size_bytes: int | None = None
    partition_hash: str | None = None
    status: PartitionStatus = PartitionStatus.READY
    metadata: dict = field(default_factory=dict)


@dataclass
class RefreshScope:
    """Defines the scope of an incremental refresh operation."""

    partition_keys: list[PartitionKey] = field(default_factory=list)
    symbol_filter: list[str] | None = None
    year_filter: list[int] | None = None
    date_range: tuple[date, date] | None = None

    def affects_partition(self, partition_key: PartitionKey) -> bool:
        """Check if a partition key falls within this refresh scope."""
        if not self.partition_keys:
            # Using filters instead
            if self.symbol_filter and partition_key.symbol not in self.symbol_filter:
                return False
            if self.year_filter is not None and partition_key.year is not None:
                if partition_key.year not in self.year_filter:
                    return False
            return True
        # Using explicit partition keys
        return partition_key in self.partition_keys


class PartitionManifestManager:
    """
    Manages partition manifests for incremental data pipeline.

    Responsibilities:
    - Discover partitions in local filesystem or MinIO
    - Compute SHA256 checksums for partition verification
    - Track partition lineage in Postgres
    - Query for affected partitions on data updates
    - Mark partitions as stale when upstream data changes
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the partition manager.

        Args:
            session: Postgres async session for manifest persistence
        """
        self._session = session
        self._settings = get_settings()

    async def register_partition(
        self,
        info: PartitionInfo,
        dataset_id: int,
        code_hash: str | None = None,
    ) -> PartitionManifest:
        """
        Register or update a partition in the manifest.

        Args:
            info: Partition metadata
            dataset_id: Parent dataset ID
            code_hash: Optional code hash for reproducibility

        Returns:
            Created or updated PartitionManifest record
        """
        partition_key_str = info.partition_key.to_string()

        # Check if partition exists
        stmt = select(PartitionManifest).where(
            PartitionManifest.dataset_id == dataset_id,
            PartitionManifest.partition_key == partition_key_str,
        )
        result = await self._session.execute(stmt)
        existing = result.scalar_one_or_none()

        # Compute hash if not provided
        if info.partition_hash is None:
            info.partition_hash = await self._compute_partition_hash(info.object_uri)

        if existing:
            # Update existing partition
            existing.partition_hash = info.partition_hash
            existing.object_uri = info.object_uri
            existing.row_count = info.row_count
            existing.min_trading_date = info.min_trading_date
            existing.max_trading_date = info.max_trading_date
            existing.size_bytes = info.size_bytes
            existing.status = info.status.value
            existing.metadata_json = info.metadata
            if code_hash:
                existing.code_hash = code_hash
            logger.debug(f"Updated partition manifest: {partition_key_str}")
            return existing
        else:
            # Create new partition
            partition = PartitionManifest(
                dataset_id=dataset_id,
                partition_key=partition_key_str,
                data_layer=info.data_layer.value,
                dataset_kind=info.dataset_kind.value,
                partition_hash=info.partition_hash,
                object_uri=info.object_uri,
                row_count=info.row_count,
                min_trading_date=info.min_trading_date,
                max_trading_date=info.max_trading_date,
                size_bytes=info.size_bytes,
                status=info.status.value,
                code_hash=code_hash,
                metadata_json=info.metadata,
            )
            self._session.add(partition)
            await self._session.flush()
            logger.info(f"Registered new partition: {partition_key_str}")
            return partition

    async def mark_partitions_stale(
        self,
        dataset_kind: DatasetKind,
        refresh_scope: RefreshScope | None = None,
    ) -> list[PartitionManifest]:
        """
        Mark partitions as stale based on refresh scope.

        Args:
            dataset_kind: Kind of dataset to mark stale
            refresh_scope: Optional scope filter (marks all if None)

        Returns:
            List of marked partitions
        """
        stmt = select(PartitionManifest).where(
            PartitionManifest.dataset_kind == dataset_kind.value,
            PartitionManifest.status == PartitionStatus.READY.value,
        )

        partitions = (await self._session.execute(stmt)).scalars().all()

        marked = []
        for partition in partitions:
            partition_key = PartitionKey.from_string(partition.partition_key)
            if refresh_scope is None or refresh_scope.affects_partition(partition_key):
                partition.status = PartitionStatus.STALE.value
                marked.append(partition)

        if marked:
            logger.info(f"Marked {len(marked)} partitions as stale for {dataset_kind.value}")

        return marked

    async def get_partitions_by_status(
        self,
        status: PartitionStatus,
        dataset_kind: DatasetKind | None = None,
        data_layer: DataLayer | None = None,
    ) -> list[PartitionManifest]:
        """
        Query partitions by status.

        Args:
            status: Status to filter by
            dataset_kind: Optional dataset kind filter
            data_layer: Optional data layer filter

        Returns:
            List of matching partitions
        """
        stmt = select(PartitionManifest).where(
            PartitionManifest.status == status.value,
        )

        if dataset_kind:
            stmt = stmt.where(PartitionManifest.dataset_kind == dataset_kind.value)
        if data_layer:
            stmt = stmt.where(PartitionManifest.data_layer == data_layer.value)

        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_partitions_for_refresh(
        self,
        dataset_kind: DatasetKind,
        date_range: tuple[date, date],
    ) -> list[PartitionManifest]:
        """
        Get partitions that overlap with a date range.

        Used for incremental refresh planning when new data arrives.

        Args:
            dataset_kind: Dataset kind to query
            date_range: (start_date, end_date) to find overlapping partitions

        Returns:
            List of partitions that overlap the date range
        """
        start_date, end_date = date_range

        stmt = (
            select(PartitionManifest)
            .where(
                PartitionManifest.dataset_kind == dataset_kind.value,
                PartitionManifest.status == PartitionStatus.READY.value,
            )
            .where(
                # Overlap condition: partition max >= start AND partition min <= end
                (PartitionManifest.max_trading_date >= start_date)
                & (PartitionManifest.min_trading_date <= end_date),
            )
        )

        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def discover_local_partitions(
        self,
        base_path: Path,
        dataset_kind: DatasetKind,
        data_layer: DataLayer,
    ) -> list[PartitionInfo]:
        """
        Discover partitions in local filesystem.

        Expected structure:
            data_layer/dataset_kind/symbol=SYMBOL/year=YYYY/part-*.parquet
            data_layer/dataset_kind/symbol=SYMBOL/year=YYYY/month=MM/part-*.parquet

        Args:
            base_path: Base path to search from
            dataset_kind: Kind of dataset
            data_layer: Data layer (bronze/silver/gold)

        Returns:
            List of discovered partition info
        """
        partitions = []

        # Build search path
        search_path = base_path / data_layer.value / dataset_kind.value

        if not search_path.exists():
            logger.warning(f"Partition path does not exist: {search_path}")
            return partitions

        # Discover symbol=SYMBOL/year=YYYY structure
        for symbol_dir in search_path.glob("symbol=*"):
            symbol = symbol_dir.name.split("=", 1)[1] if "=" in symbol_dir.name else None

            for year_dir in symbol_dir.glob("year=*"):
                year = int(year_dir.name.split("=", 1)[1]) if "=" in year_dir.name else None

                # Check for monthly partitions
                month_dirs = list(year_dir.glob("month=*"))
                if month_dirs:
                    for month_dir in month_dirs:
                        month = (
                            int(month_dir.name.split("=", 1)[1]) if "=" in month_dir.name else None
                        )
                        partition_info = await self._scan_partition_directory(
                            month_dir,
                            dataset_kind,
                            data_layer,
                            PartitionKey(symbol=symbol, year=year, month=month),
                        )
                        if partition_info:
                            partitions.append(partition_info)
                else:
                    # Yearly partition
                    partition_info = await self._scan_partition_directory(
                        year_dir, dataset_kind, data_layer, PartitionKey(symbol=symbol, year=year)
                    )
                    if partition_info:
                        partitions.append(partition_info)

        logger.info(f"Discovered {len(partitions)} partitions in {search_path}")
        return partitions

    async def _scan_partition_directory(
        self,
        dir_path: Path,
        dataset_kind: DatasetKind,
        data_layer: DataLayer,
        partition_key: PartitionKey,
    ) -> PartitionInfo | None:
        """Scan a partition directory and compute metadata."""
        parquet_files = list(dir_path.glob("*.parquet"))
        if not parquet_files:
            return None

        # Compute total size and find representative file
        total_size = sum(f.stat().st_size for f in parquet_files)
        object_uri = str(dir_path)

        # For now, row count and date range require reading Parquet metadata
        # This will be implemented with Polars lazy frame scanning

        return PartitionInfo(
            dataset_kind=dataset_kind,
            partition_key=partition_key,
            data_layer=data_layer,
            object_uri=object_uri,
            size_bytes=total_size,
        )

    async def _compute_partition_hash(self, object_uri: str) -> str:
        """
        Compute SHA256 hash for partition verification.

        For local files, hashes the concatenated file metadata.
        For MinIO objects, uses the object's ETag or computes from metadata.

        Args:
            object_uri: URI to the partition data

        Returns:
            SHA256 hash hex string
        """
        # For now, use a simple hash of the URI path + mtime
        # In production, this would compute actual content hash
        path = Path(object_uri)
        if path.exists():
            mtime = path.stat().st_mtime_ns
            content = f"{object_uri}:{mtime}".encode()
            return hashlib.sha256(content).hexdigest()
        return hashlib.sha256(object_uri.encode()).hexdigest()

    async def get_or_create_dataset(
        self,
        dataset_kind: str,
        dataset_hash: str,
        code_hash: str | None = None,
        source_uri: str | None = None,
    ) -> DatasetManifest:
        """
        Get existing dataset or create new one.

        Args:
            dataset_kind: Dataset kind identifier
            dataset_hash: Hash of dataset contents
            code_hash: Optional code hash
            source_uri: Optional source URI

        Returns:
            DatasetManifest record
        """
        stmt = select(DatasetManifest).where(
            DatasetManifest.dataset_kind == dataset_kind,
            DatasetManifest.dataset_hash == dataset_hash,
        )
        result = await self._session.execute(stmt)
        dataset = result.scalar_one_or_none()

        if dataset is None:
            dataset = DatasetManifest(
                dataset_kind=dataset_kind,
                dataset_hash=dataset_hash,
                code_hash=code_hash,
                source_uri=source_uri,
                metadata_json={},
            )
            self._session.add(dataset)
            await self._session.flush()
            logger.info(f"Created new dataset manifest: {dataset_kind}")

        return dataset
