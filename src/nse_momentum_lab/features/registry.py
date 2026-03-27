"""
Feature Registry for NSE Momentum Lab.

The registry is the source of truth for:
- Feature set metadata (name, version, description)
- Input dependencies (datasets, other feature sets)
- Build logic (SQL or Python function)
- Incremental refresh policies
- Output schema validation

Feature sets are organized in layers:
1. Core features (strategy-agnostic): feat_daily_core, feat_intraday_core
2. Event features (for episodic strategies): feat_event_core
3. Strategy-derived features: feat_2lynch_derived, feat_threshold_breakout_derived, etc.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

logger = logging.getLogger(__name__)


class FeatureGranularity(Enum):
    """Time granularity of feature data."""

    DAILY = "daily"
    FIVE_MIN = "5min"
    EVENT = "event"


class IncrementalPolicy(Enum):
    """How incremental refresh works for this feature set."""

    NONE = "none"  # Full rebuild only
    APPEND = "append"  # New partitions append, old never touched
    ROLLING_WINDOW = "rolling_window"  # Rebuild affected windows + overlap
    DEPENDENCY_CASCADE = "dependency_cascade"  # Rebuild only if dependencies change


@dataclass(frozen=True)
class FeatureDependency:
    """A dependency on another feature set or raw dataset."""

    name: str  # e.g., "v_daily", "feat_daily_core"
    is_dataset: bool = False  # True if raw view like v_daily, False if feature set
    required_lookback_days: int = 0  # Days of history needed for warmup


@dataclass(frozen=True)
class FeatureDefinition:
    """Metadata and build contract for a feature set."""

    # Identity
    name: str
    version: str
    description: str

    # Characterization
    granularity: FeatureGranularity
    layer: str  # "core", "event", "derived"

    # Dependencies
    input_datasets: list[str] = field(default_factory=list)  # e.g., ["v_daily", "v_5min"]
    feature_dependencies: list[FeatureDependency] = field(default_factory=list)

    # Build contract
    required_lookback_days: int = 252  # Trading days needed for rolling windows
    build_sql: str | None = None  # SQL for CREATE TABLE AS SELECT
    build_function: Callable[[DuckDBPyConnection, date, date], None] | None = None

    # Incremental refresh
    incremental_policy: IncrementalPolicy = IncrementalPolicy.ROLLING_WINDOW
    partition_grain: str = "year"  # "year", "month", or "none"

    # Output schema (for validation)
    output_columns: list[str] = field(default_factory=list)

    def get_all_dependencies(self) -> list[str]:
        """Return all dependency names including datasets and feature sets."""
        deps = list(self.input_datasets)
        deps.extend(fd.name for fd in self.feature_dependencies)
        return deps

    def get_total_lookback(self) -> int:
        """Return the maximum lookback required across all dependencies."""
        lookback = self.required_lookback_days
        for fd in self.feature_dependencies:
            lookback = max(lookback, fd.required_lookback_days)
        return lookback


@dataclass
class FeatureSetState:
    """Runtime state of a materialized feature set."""

    table_name: str
    dataset_hash: str
    query_version: str
    row_count: int
    min_date: date | None
    max_date: date | None
    updated_at: str | None
    status: str = "ready"  # ready, stale, failed


class FeatureRegistry:
    """
    Central registry of all feature sets.

    The registry is responsible for:
    - Registering feature definitions
    - Resolving build order based on dependencies
    - Checking materialization state
    - Planning incremental refreshes
    """

    def __init__(self) -> None:
        self._features: dict[str, FeatureDefinition] = {}

    def register(self, definition: FeatureDefinition) -> None:
        """Register a feature set definition."""
        key = definition.name.lower()
        if key in self._features:
            logger.warning("Feature %s already registered, overwriting", definition.name)
        self._features[key] = definition
        logger.info("Registered feature set: %s v%s", definition.name, definition.version)

    def get(self, name: str) -> FeatureDefinition | None:
        """Get a feature definition by name."""
        return self._features.get(name.lower())

    def require(self, name: str) -> FeatureDefinition:
        """Get a feature definition, raising KeyError if not found."""
        feat = self.get(name)
        if feat is None:
            available = ", ".join(sorted(self._features.keys()))
            raise KeyError(f"Feature '{name}' not found. Available: {available}")
        return feat

    def list_all(self) -> list[FeatureDefinition]:
        """Return all registered feature definitions."""
        return list(self._features.values())

    def list_by_layer(self, layer: str) -> list[FeatureDefinition]:
        """Return feature definitions for a specific layer."""
        return [f for f in self._features.values() if f.layer == layer]

    def list_by_granularity(self, granularity: FeatureGranularity) -> list[FeatureDefinition]:
        """Return feature definitions for a specific granularity."""
        return [f for f in self._features.values() if f.granularity == granularity]

    def resolve_build_order(
        self, feature_names: list[str] | None = None
    ) -> list[FeatureDefinition]:
        """
        Resolve build order based on dependencies.

        Uses topological sort to ensure dependencies are built before dependents.
        """
        if feature_names is None:
            to_build = list(self._features.values())
        else:
            to_build = [self.require(name) for name in feature_names]

        built: set[str] = set()
        order: list[FeatureDefinition] = []
        remaining = list(to_build)

        max_iterations = len(to_build) * 2  # Safety limit
        iteration = 0

        while remaining and iteration < max_iterations:
            iteration += 1
            ready = [
                f
                for f in remaining
                if all(
                    dep.is_dataset or dep.name.lower() in built for dep in f.feature_dependencies
                )
            ]

            if not ready:
                # Circular dependency or missing dependency
                logger.error(
                    "Cannot resolve build order. Remaining: %s", [f.name for f in remaining]
                )
                raise RuntimeError("Circular or missing feature dependency detected")

            for feat in ready:
                order.append(feat)
                built.add(feat.name.lower())
                remaining.remove(feat)

        if remaining:
            raise RuntimeError(f"Failed to resolve build order for: {[f.name for f in remaining]}")

        return order

    def get_incremental_refresh_plan(
        self,
        feature_name: str,
        current_state: FeatureSetState | None,
        new_data_start: date,
        new_data_end: date,
    ) -> dict[str, Any]:
        """
        Plan an incremental refresh for a feature set.

        Returns a plan with:
        - rebuild_type: "full", "incremental_append", "incremental_window"
        - affected_date_ranges: list of (start, end) tuples to rebuild
        - affected_partitions: list of partition keys to rebuild
        - cascade_features: list of dependent features that also need refresh
        """
        feat = self.require(feature_name)
        plan: dict[str, Any] = {
            "feature_name": feature_name,
            "rebuild_type": "full",
            "affected_date_ranges": [],
            "affected_partitions": [],
            "cascade_features": [],
        }

        # If no state exists, full rebuild
        if current_state is None:
            return plan

        # Check if query version changed
        if current_state.query_version != feat.version:
            return plan

        # Check dataset hash (would be passed externally)
        # For now, assume dataset hash check happens elsewhere

        # Plan incremental based on policy
        if feat.incremental_policy == IncrementalPolicy.NONE:
            return plan

        if feat.incremental_policy == IncrementalPolicy.APPEND:
            # Only rebuild new data
            if current_state.max_date and new_data_start > current_state.max_date:
                plan["rebuild_type"] = "incremental_append"
                plan["affected_date_ranges"] = [(current_state.max_date, new_data_end)]
            else:
                # Gap in data or no previous state, full rebuild
                return plan

        elif feat.incremental_policy == IncrementalPolicy.ROLLING_WINDOW:
            # Rebuild new data + lookback window
            lookback_date = _subtract_trading_days(new_data_start, feat.get_total_lookback())
            if current_state.max_date and new_data_start > current_state.max_date:
                plan["rebuild_type"] = "incremental_window"
                plan["affected_date_ranges"] = [(lookback_date, new_data_end)]
            else:
                return plan

        elif feat.incremental_policy == IncrementalPolicy.DEPENDENCY_CASCADE:
            # Rebuild only if dependencies changed
            # This would check dependency states
            return plan

        # Find cascading features that depend on this one
        for other in self._features.values():
            if any(dep.name == feature_name for dep in other.feature_dependencies):
                plan["cascade_features"].append(other.name)

        return plan


def _subtract_trading_days(d: date, days: int) -> date:
    """Approximate subtraction of trading days (5/7 of calendar days)."""
    from datetime import timedelta

    # Rough approximation: 1 trading day ≈ 1.4 calendar days
    calendar_days = int(days * 1.4)
    # Add buffer for weekends/holidays
    return d - timedelta(days=calendar_days + 20)


# Global feature registry instance
_global_registry: FeatureRegistry | None = None


def get_feature_registry() -> FeatureRegistry:
    """Get or create the global feature registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = FeatureRegistry()
        _register_core_features(_global_registry)
    return _global_registry


def _register_core_features(registry: FeatureRegistry) -> None:
    """Register core feature sets.

    Actual feature definitions are in their respective modules.
    This function is called during registry initialization.
    """
    # Import feature builders to trigger their self-registration
    from nse_momentum_lab.features.daily_core import register_feat_daily_core
    from nse_momentum_lab.features.event_core import register_feat_event_core
    from nse_momentum_lab.features.intraday_core import register_feat_intraday_core
    from nse_momentum_lab.features.strategy_derived import register_2lynch_derived

    register_feat_daily_core(registry)
    register_feat_intraday_core(registry)
    register_feat_event_core(registry)
    register_2lynch_derived(registry)
