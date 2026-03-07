"""
Incremental refresh planner for minimal data rebuilds.

This module provides the IncrementalRefreshPlanner which determines
which partitions need rebuilding when new data arrives.

Key concepts:
- Lookback windows: rolling features need historical overlap
- Dependency cascading: upstream changes mark downstream stale
- Partition affinity: minimize cross-partition dependencies
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import StrEnum

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.db.models import (
    IncrementalRefreshState,
    PartitionManifest,
)
from nse_momentum_lab.services.data_lake.partition_manager import (
    DatasetKind,
    PartitionKey,
    PartitionManifestManager,
    PartitionStatus,
)

logger = logging.getLogger(__name__)


class JobKind(StrEnum):
    """Materialization job types for incremental pipeline."""

    RAW_INGEST_DAILY = "raw_ingest_daily"
    RAW_INGEST_5MIN = "raw_ingest_5min"
    RAW_INGEST_EVENTS = "raw_ingest_events"
    SILVER_VALIDATE = "silver_validate"
    GOLD_MATERIALIZE_FEATURES = "gold_materialize_features"
    GOLD_REFRESH_STRATEGY_VIEWS = "gold_refresh_strategy_views"
    RESEARCH_RERUN_IMPACTED = "research_rerun_impacted"


@dataclass
class FeatureSetConfig:
    """Configuration for a feature set's refresh requirements."""

    name: str
    version: str
    required_lookback_days: int  # Days of history needed before first valid output
    input_datasets: list[DatasetKind]
    output_partition_grain: str  # "yearly", "monthly", "daily"
    rolling_dependency: bool = False  # True if needs trailing window


# Standard feature set configurations
FEATURE_SET_CONFIGS: dict[str, FeatureSetConfig] = {
    "feat_daily_core": FeatureSetConfig(
        name="feat_daily_core",
        version="1.0",
        required_lookback_days=252,  # For 252-day returns
        input_datasets=[DatasetKind.DAILY],
        output_partition_grain="yearly",
        rolling_dependency=True,
    ),
    "feat_intraday_core": FeatureSetConfig(
        name="feat_intraday_core",
        version="1.0",
        required_lookback_days=1,  # Only needs current day
        input_datasets=[DatasetKind.DAILY, DatasetKind.FIVE_MIN],
        output_partition_grain="monthly",
        rolling_dependency=False,
    ),
    "feat_event_core": FeatureSetConfig(
        name="feat_event_core",
        version="1.0",
        required_lookback_days=30,  # For post-event drift windows
        input_datasets=[DatasetKind.EVENTS, DatasetKind.DAILY],
        output_partition_grain="monthly",
        rolling_dependency=False,
    ),
    "feat_2lynch_derived": FeatureSetConfig(
        name="feat_2lynch_derived",
        version="1.0",
        required_lookback_days=30,  # For young breakout counter
        input_datasets=[DatasetKind.FEAT_DAILY_CORE],
        output_partition_grain="yearly",
        rolling_dependency=True,
    ),
}


@dataclass
class RefreshPlan:
    """A plan for incremental refresh operations."""

    feature_set: str
    partitions_to_build: list[PartitionKey] = field(default_factory=list)
    partitions_to_update: list[PartitionKey] = field(default_factory=list)
    partitions_to_skip: list[PartitionKey] = field(default_factory=list)
    affected_date_range: tuple[date, date] | None = None
    lookback_partitions: list[PartitionKey] = field(default_factory=list)
    estimated_partitions_total: int = 0

    def total_partitions(self) -> int:
        return len(self.partitions_to_build) + len(self.partitions_to_update)


@dataclass
class MaterializationPlan:
    """Complete materialization plan for a data update."""

    new_data_range: tuple[date, date]
    refresh_plans: dict[str, RefreshPlan] = field(default_factory=dict)
    upstream_partitions_marked_stale: int = 0
    downstream_partitions_to_refresh: int = 0

    def add_refresh_plan(self, plan: RefreshPlan) -> None:
        """Add a feature set refresh plan."""
        self.refresh_plans[plan.feature_set] = plan

    def get_total_partitions(self) -> int:
        """Get total partitions across all refresh plans."""
        return sum(p.total_partitions() for p in self.refresh_plans.values())


class IncrementalRefreshPlanner:
    """
    Plans minimal incremental refresh operations when new data arrives.

    When new data is added (e.g., 2026-01-01 through 2026-01-31):
    1. Identifies which silver partitions are new/updated
    2. Determines which gold feature partitions depend on that data
    3. Calculates lookback windows for rolling features
    4. Produces a minimal rebuild plan

    Example:
        New daily data for 2026-01:
        - Rebuild feat_daily_core for 2025 (overlap for 252-day window) + 2026
        - Rebuild feat_intraday_core for 2026-01
        - feat_event_core: no rebuild (no new events)
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the refresh planner.

        Args:
            session: Postgres async session
        """
        self._session = session
        self._partition_manager = PartitionManifestManager(session)

    async def plan_daily_data_update(
        self,
        new_data_range: tuple[date, date],
    ) -> MaterializationPlan:
        """
        Plan refresh for new daily OHLCV data.

        Args:
            new_data_range: (start_date, end_date) of new data

        Returns:
            MaterializationPlan with minimal rebuild scope
        """
        plan = MaterializationPlan(new_data_range=new_data_range)

        # Mark new silver daily partitions as ready (this happens during ingest)
        # Plan downstream gold feature rebuilds

        # feat_daily_core needs lookback
        plan.add_refresh_plan(
            await self._plan_feature_refresh(
                "feat_daily_core",
                new_data_range,
            )
        )

        # feat_intraday_core needs 5min data too
        plan.add_refresh_plan(
            await self._plan_feature_refresh(
                "feat_intraday_core",
                new_data_range,
            )
        )

        # feat_2lynch_derived depends on feat_daily_core
        plan.add_refresh_plan(
            await self._plan_feature_refresh(
                "feat_2lynch_derived",
                new_data_range,
            )
        )

        logger.info(
            f"Planned refresh for {new_data_range}: {plan.get_total_partitions()} partitions total"
        )

        return plan

    async def plan_feature_refresh(
        self,
        feature_set_name: str,
        new_data_range: tuple[date, date],
    ) -> RefreshPlan:
        """
        Plan refresh for a specific feature set.

        Args:
            feature_set_name: Name of feature set
            new_data_range: Range of new upstream data

        Returns:
            RefreshPlan for the feature set
        """
        config = FEATURE_SET_CONFIGS.get(feature_set_name)
        if not config:
            logger.warning(f"No config found for feature set: {feature_set_name}")
            return RefreshPlan(feature_set=feature_set_name)

        plan = RefreshPlan(
            feature_set=feature_set_name,
            affected_date_range=new_data_range,
        )

        # Calculate affected years including lookback
        start_date, end_date = new_data_range
        lookback_start = start_date - timedelta(days=config.required_lookback_days)

        # Determine partition keys to rebuild
        if config.output_partition_grain == "yearly":
            years_to_rebuild = self._years_in_range(lookback_start, end_date)
            for year in years_to_rebuild:
                plan.partitions_to_update.append(PartitionKey(year=year))
        elif config.output_partition_grain == "monthly":
            months = self._months_in_range(lookback_start, end_date)
            plan.partitions_to_update.extend(months)

        plan.estimated_partitions_total = plan.total_partitions()

        return plan

    async def mark_upstream_stale(
        self,
        affected_partitions: list[PartitionManifest],
        downstream_feature_set: str,
    ) -> list[IncrementalRefreshState]:
        """
        Mark downstream feature sets as needing refresh.

        Creates IncrementalRefreshState records linking upstream changes
        to downstream refresh requirements.

        Args:
            affected_partitions: Partitions that changed
            downstream_feature_set: Feature set to mark stale

        Returns:
            Created refresh state records
        """
        config = FEATURE_SET_CONFIGS.get(downstream_feature_set)
        if not config:
            logger.warning(f"Unknown feature set: {downstream_feature_set}")
            return []

        states = []
        for partition in affected_partitions:
            # Check if state already exists
            existing = await self._get_refresh_state(
                partition.partition_id,
                downstream_feature_set,
            )
            if existing:
                existing.needs_refresh = True
                states.append(existing)
            else:
                state = IncrementalRefreshState(
                    upstream_partition_id=partition.partition_id,
                    downstream_feature_set=downstream_feature_set,
                    downstream_lookback_days=config.required_lookback_days,
                    needs_refresh=True,
                )
                self._session.add(state)
                states.append(state)

        await self._session.flush()
        logger.info(f"Marked {len(states)} refresh states for {downstream_feature_set}")
        return states

    async def get_stale_partitions(
        self,
        feature_set_name: str,
    ) -> list[PartitionManifest]:
        """
        Get all partitions for a feature set that need refresh.

        Args:
            feature_set_name: Feature set to check

        Returns:
            List of stale partitions
        """
        config = FEATURE_SET_CONFIGS.get(feature_set_name)
        if not config:
            return []

        # Find all refresh states for this feature set
        stmt = select(IncrementalRefreshState).where(
            IncrementalRefreshState.downstream_feature_set == feature_set_name,
            IncrementalRefreshState.needs_refresh,
        )
        result = await self._session.execute(stmt)
        refresh_states = result.scalars().all()

        if not refresh_states:
            return []

        # Get unique partition IDs
        partition_ids = {s.upstream_partition_id for s in refresh_states}

        # Fetch partitions
        stmt = select(PartitionManifest).where(
            PartitionManifest.partition_id.in_(partition_ids),
            PartitionManifest.dataset_kind == feature_set_name,
            PartitionManifest.status == PartitionStatus.STALE.value,
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def clear_refresh_state(
        self,
        partition_id: int,
        feature_set_name: str,
    ) -> None:
        """
        Clear refresh state after successful rebuild.

        Args:
            partition_id: Partition that was rebuilt
            feature_set_name: Feature set that was refreshed
        """
        stmt = select(IncrementalRefreshState).where(
            IncrementalRefreshState.upstream_partition_id == partition_id,
            IncrementalRefreshState.downstream_feature_set == feature_set_name,
        )
        result = await self._session.execute(stmt)
        state = result.scalar_one_or_none()

        if state:
            state.needs_refresh = False
            state.last_refreshed_at = func.now()
            await self._session.flush()
            logger.debug(f"Cleared refresh state for partition {partition_id}")

    def _years_in_range(self, start: date, end: date) -> list[int]:
        """Get list of years in date range."""
        years = []
        for year in range(start.year, end.year + 1):
            years.append(year)
        return years

    def _months_in_range(self, start: date, end: date) -> list[PartitionKey]:
        """Get list of (year, month) partition keys in date range."""
        months = []
        current = date(start.year, start.month, 1)
        end_month = date(end.year, end.month, 1)

        while current <= end_month:
            months.append(PartitionKey(year=current.year, month=current.month))
            # Advance to next month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

        return months

    async def _get_refresh_state(
        self,
        upstream_partition_id: int,
        downstream_feature_set: str,
    ) -> IncrementalRefreshState | None:
        """Get existing refresh state if any."""
        stmt = select(IncrementalRefreshState).where(
            IncrementalRefreshState.upstream_partition_id == upstream_partition_id,
            IncrementalRefreshState.downstream_feature_set == downstream_feature_set,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
