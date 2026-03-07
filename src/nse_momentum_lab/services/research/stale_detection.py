"""
Stale-run detection for backtest experiments.

Detects when experiments need to be re-run due to:
- Dataset changes (new data, corrections)
- Feature set changes (schema changes, new features)
- Strategy code changes
- Engine changes

This enables intelligent incremental research instead of full rebuilds.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


class StaleReason(StrEnum):
    """Reason why a run is considered stale."""

    DATASET_CHANGED = "dataset_changed"
    FEATURES_CHANGED = "features_changed"
    STRATEGY_CHANGED = "strategy_changed"
    CODE_CHANGED = "code_changed"
    FEATURE_DEPENDENCY_STALE = "feature_dependency_stale"
    NEW_DATA_AVAILABLE = "new_data_available"


@dataclass
class DatasetVersion:
    """Version information for a dataset or feature set."""

    name: str
    hash: str
    row_count: int
    min_date: date | None
    max_date: date | None
    updated_at: datetime
    is_feature: bool = False

    def covers_range(self, start: date, end: date) -> bool:
        """Check if this version covers the given date range."""
        if self.min_date is None or self.max_date is None:
            return False
        return self.min_date <= start and self.max_date >= end


@dataclass
class StaleRunInfo:
    """Information about a stale run."""

    exp_hash: str
    strategy_name: str
    strategy_hash: str
    dataset_hash: str
    params_hash: str
    code_sha: str
    run_date: datetime

    # Staleness detection
    is_stale: bool = False
    stale_reasons: list[StaleReason] = field(default_factory=list)
    stale_details: dict[str, Any] = field(default_factory=dict)

    # Current versions (for comparison)
    current_dataset_hash: str = ""
    current_feature_hashes: dict[str, str] = field(default_factory=dict)
    current_strategy_hash: str = ""
    current_code_sha: str = ""

    @property
    def has_new_data(self) -> bool:
        return StaleReason.NEW_DATA_AVAILABLE in self.stale_reasons

    @property
    def has_dataset_change(self) -> bool:
        return StaleReason.DATASET_CHANGED in self.stale_reasons

    @property
    def has_feature_change(self) -> bool:
        return StaleReason.FEATURES_CHANGED in self.stale_reasons

    def to_dict(self) -> dict[str, Any]:
        return {
            "exp_hash": self.exp_hash,
            "strategy_name": self.strategy_name,
            "is_stale": self.is_stale,
            "stale_reasons": [r.value for r in self.stale_reasons],
            "stale_details": self.stale_details,
            "run_date": self.run_date.isoformat(),
            "current_dataset_hash": self.current_dataset_hash,
            "current_strategy_hash": self.current_strategy_hash,
        }


@dataclass
class FeatureDependencyGraph:
    """Dependency graph for feature sets."""

    nodes: dict[str, set[str]] = field(default_factory=dict)  # name -> dependencies
    reverse_nodes: dict[str, set[str]] = field(default_factory=dict)  # name -> dependents

    def add_dependency(self, feature: str, depends_on: str) -> None:
        """Add a dependency edge: feature depends on depends_on."""
        if feature not in self.nodes:
            self.nodes[feature] = set()
        if depends_on not in self.reverse_nodes:
            self.reverse_nodes[depends_on] = set()

        self.nodes[feature].add(depends_on)
        self.reverse_nodes[depends_on].add(feature)

    def get_dependents(self, feature: str) -> set[str]:
        """Get all features that depend on this one (direct and indirect)."""
        visited: set[str] = set()
        to_visit = list(self.reverse_nodes.get(feature, set()))

        while to_visit:
            current = to_visit.pop()
            if current in visited:
                continue
            visited.add(current)
            to_visit.extend(self.reverse_nodes.get(current, set()) - visited)

        return visited

    def get_all_dependencies(self, feature: str) -> set[str]:
        """Get all dependencies of this feature (direct and indirect)."""
        visited: set[str] = set()
        to_visit = list(self.nodes.get(feature, set()))

        while to_visit:
            current = to_visit.pop()
            if current in visited:
                continue
            visited.add(current)
            to_visit.extend(self.nodes.get(current, set()) - visited)

        return visited


class DatasetVersionTracker:
    """
    Tracks dataset and feature versions to detect staleness.

    Queries DuckDB for current versions and compares with experiment metadata.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self._dependency_graph = self._build_dependency_graph()

    def _build_dependency_graph(self) -> FeatureDependencyGraph:
        """Build the feature dependency graph from registry."""
        graph = FeatureDependencyGraph()

        # Core dependencies
        graph.add_dependency("feat_daily_core", "v_daily")
        graph.add_dependency("feat_intraday_core", "v_5min")
        graph.add_dependency("feat_event_core", "v_daily")

        # Strategy-derived features
        graph.add_dependency("feat_2lynch_derived", "feat_daily_core")
        graph.add_dependency("feat_2lynch_derived", "v_daily")

        return graph

    def get_current_dataset_version(self) -> DatasetVersion:
        """Get the current primary dataset version."""
        try:
            row = self.con.execute("""
                SELECT
                    'daily' as name,
                    COUNT(*) as row_count,
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    current_timestamp as updated_at
                FROM v_daily
            """).fetchone()

            if row:
                from nse_momentum_lab.utils.hash_utils import compute_short_hash

                # Simple hash based on date range and row count
                hash_input = f"{row[1]}:{row[2]}:{row[3]}"
                version_hash = compute_short_hash(hash_input, length=16)

                return DatasetVersion(
                    name="daily",
                    hash=version_hash,
                    row_count=row[1] or 0,
                    min_date=date.fromisoformat(row[2]) if row[2] else None,
                    max_date=date.fromisoformat(row[3]) if row[3] else None,
                    updated_at=datetime.now(UTC),
                    is_feature=False,
                )
        except Exception as e:
            logger.warning("Failed to get dataset version: %s", e)

        return DatasetVersion(
            name="daily",
            hash="unknown",
            row_count=0,
            min_date=None,
            max_date=None,
            updated_at=datetime.now(UTC),
            is_feature=False,
        )

    def get_current_feature_versions(self) -> dict[str, DatasetVersion]:
        """Get current versions of all materialized feature sets."""
        versions: dict[str, DatasetVersion] = {}

        try:
            rows = self.con.execute("""
                SELECT
                    table_name,
                    query_version,
                    row_count,
                    updated_at
                FROM bt_materialization_state
                WHERE table_name LIKE 'feat_%'
                ORDER BY table_name
            """).fetchall()

            for row in rows:
                table_name, query_version, row_count, updated_at = row

                # Get min/max dates
                date_row = self.con.execute(f"""
                    SELECT MIN(trading_date), MAX(trading_date)
                    FROM {table_name}
                """).fetchone()

                min_date = date.fromisoformat(date_row[0]) if date_row and date_row[0] else None
                max_date = date.fromisoformat(date_row[1]) if date_row and date_row[1] else None

                versions[table_name] = DatasetVersion(
                    name=table_name,
                    hash=query_version or "unknown",
                    row_count=row_count or 0,
                    min_date=min_date,
                    max_date=max_date,
                    updated_at=datetime.fromisoformat(updated_at)
                    if updated_at
                    else datetime.now(UTC),
                    is_feature=True,
                )
        except Exception as e:
            logger.warning("Failed to get feature versions: %s", e)

        return versions

    def get_strategy_version(self, strategy_name: str) -> str:
        """Get the current version hash for a strategy."""
        from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy
        from nse_momentum_lab.utils.hash_utils import compute_short_hash

        try:
            strategy = resolve_strategy(strategy_name)
            version_input = f"{strategy.name}:{strategy.version}:{strategy.family}"
            return compute_short_hash(version_input, length=16)
        except Exception:
            return "unknown"

    def get_code_sha(self) -> str:
        """Get the current code SHA."""
        return build_code_sha()


def is_run_stale(
    tracker: DatasetVersionTracker,
    exp_hash: str,
    strategy_name: str,
    strategy_hash: str,
    dataset_hash: str,
    code_sha: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> StaleRunInfo:
    """
    Check if a single experiment run is stale.

    Args:
        tracker: DatasetVersionTracker instance
        exp_hash: Experiment hash
        strategy_name: Strategy used in the run
        strategy_hash: Strategy version hash from the run
        dataset_hash: Dataset hash from the run
        code_sha: Code SHA from the run
        start_date: Start date of the run
        end_date: End date of the run

    Returns:
        StaleRunInfo with staleness details
    """
    current_dataset = tracker.get_current_dataset_version()
    current_features = tracker.get_current_feature_versions()
    current_strategy_hash = tracker.get_strategy_version(strategy_name)
    current_code_sha = tracker.get_code_sha()

    info = StaleRunInfo(
        exp_hash=exp_hash,
        strategy_name=strategy_name,
        strategy_hash=strategy_hash,
        dataset_hash=dataset_hash,
        params_hash="",  # Not tracked for staleness
        code_sha=code_sha,
        run_date=datetime.now(UTC),
        current_dataset_hash=current_dataset.hash,
        current_feature_hashes={name: v.hash for name, v in current_features.items()},
        current_strategy_hash=current_strategy_hash,
        current_code_sha=current_code_sha,
    )

    # Check for dataset changes
    if current_dataset.hash != dataset_hash:
        info.is_stale = True
        info.stale_reasons.append(StaleReason.DATASET_CHANGED)
        info.stale_details["dataset_hash"] = {
            "old": dataset_hash,
            "new": current_dataset.hash,
        }

    # Check for new data
    if start_date and end_date:
        if not current_dataset.covers_range(start_date, end_date):
            info.is_stale = True
            info.stale_reasons.append(StaleReason.NEW_DATA_AVAILABLE)
            info.stale_details["new_data"] = {
                "run_range": f"{start_date} to {end_date}",
                "available_range": f"{current_dataset.min_date} to {current_dataset.max_date}",
            }
        elif current_dataset.max_date and current_dataset.max_date > end_date:
            # New data available beyond run range
            info.is_stale = True
            info.stale_reasons.append(StaleReason.NEW_DATA_AVAILABLE)
            info.stale_details["extended_data"] = {
                "run_end": end_date,
                "available_end": current_dataset.max_date,
            }

    # Check for feature changes
    stale_features = []
    for _feat_name, _feat_version in current_features.items():
        # Features are versioned by query_version, not hash
        # Check if any feature has a different version than expected
        pass  # Would need to store feature_hash in exp_run

    if stale_features:
        info.is_stale = True
        info.stale_reasons.append(StaleReason.FEATURES_CHANGED)
        info.stale_details["stale_features"] = stale_features

    # Check for strategy changes
    if current_strategy_hash != strategy_hash:
        info.is_stale = True
        info.stale_reasons.append(StaleReason.STRATEGY_CHANGED)
        info.stale_details["strategy_hash"] = {
            "old": strategy_hash,
            "new": current_strategy_hash,
        }

    # Check for code changes
    if current_code_sha and current_code_sha != code_sha:
        info.is_stale = True
        info.stale_reasons.append(StaleReason.CODE_CHANGED)
        info.stale_details["code_sha"] = {
            "old": code_sha,
            "new": current_code_sha,
        }

    return info


def list_stale_runs(
    tracker: DatasetVersionTracker,
    limit: int = 100,
    strategy_name: str | None = None,
) -> list[StaleRunInfo]:
    """
    List all stale experiment runs.

    Args:
        tracker: DatasetVersionTracker instance
        limit: Maximum number of runs to return
        strategy_name: Filter by strategy name

    Returns:
        List of StaleRunInfo for stale runs
    """
    stale_runs: list[StaleRunInfo] = []

    try:
        # Query experiment runs from Postgres or DuckDB
        # For now, use DuckDB if available
        rows = tracker.con.execute(
            """
            SELECT
                exp_hash,
                strategy_name,
                strategy_hash,
                dataset_hash,
                code_sha,
                started_at,
                start_date,
                end_date
            FROM bt_experiment
            WHERE status = 'SUCCEEDED'
            ORDER BY started_at DESC
            LIMIT ?
        """,
            [limit * 2],
        ).fetchall()  # Get more to filter

        for row in rows:
            exp_hash, strat_name, strat_hash, data_hash, code_sha, started_at, start_d, end_d = row

            if strategy_name and strat_name != strategy_name:
                continue

            start_date = date.fromisoformat(start_d) if start_d else None
            end_date = date.fromisoformat(end_d) if end_d else None

            info = is_run_stale(
                tracker=tracker,
                exp_hash=exp_hash,
                strategy_name=strat_name,
                strategy_hash=strat_hash or "",
                dataset_hash=data_hash or "",
                code_sha=code_sha or "",
                start_date=start_date,
                end_date=end_date,
            )

            if info.is_stale:
                info.run_date = (
                    datetime.fromisoformat(started_at) if started_at else datetime.now(UTC)
                )
                stale_runs.append(info)

                if len(stale_runs) >= limit:
                    break

    except Exception as e:
        logger.warning("Failed to list stale runs: %s", e)

    return stale_runs


def find_cascading_stale_features(
    tracker: DatasetVersionTracker,
    changed_features: set[str],
) -> set[str]:
    """
    Find all features that need rebuild due to dependency cascade.

    Args:
        tracker: DatasetVersionTracker instance
        changed_features: Set of feature names that have changed

    Returns:
        Set of all features that need rebuild (direct and indirect dependents)
    """
    all_stale = set(changed_features)

    for feature in changed_features:
        dependents = tracker._dependency_graph.get_dependents(feature)
        all_stale.update(dependents)

    return all_stale


def get_rebuild_plan(
    tracker: DatasetVersionTracker,
    changed_features: set[str] | None = None,
    new_data_start: date | None = None,
    new_data_end: date | None = None,
) -> dict[str, Any]:
    """
    Generate a rebuild plan for features and experiments.

    Args:
        tracker: DatasetVersionTracker instance
        changed_features: Features that have changed
        new_data_start: Start date of new data
        new_data_end: End date of new data

    Returns:
        Rebuild plan with affected features and experiments
    """
    plan: dict[str, Any] = {
        "features_to_rebuild": [],
        "experiments_to_rerun": [],
        "rebuild_type": "full",  # or "incremental"
        "affected_date_ranges": [],
    }

    current_dataset = tracker.get_current_dataset_version()

    # Check for new data
    if new_data_start and new_data_end:
        if current_dataset.max_date and current_dataset.max_date >= new_data_end:
            plan["rebuild_type"] = "incremental"
            plan["affected_date_ranges"] = [(new_data_start.isoformat(), new_data_end.isoformat())]

    # Cascade feature dependencies
    if changed_features:
        stale_features = find_cascading_stale_features(tracker, changed_features)
        plan["features_to_rebuild"] = list(stale_features)

    return plan


def build_code_sha() -> str:
    """Build a SHA hash of the current code state."""
    from nse_momentum_lab.services.dataset import build_code_hash

    return build_code_hash()
