"""
Incremental Feature Materializer for NSE Momentum Lab.

The materializer is responsible for:
- Building feature sets in dependency order
- Planning incremental refreshes based on new data
- Managing materialization state tracking
- Providing progress feedback during builds
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING

from nse_momentum_lab.features.progress import (
    FeatureBuildProgressReporter,
    configure_duckdb_for_feature_build,
)
from nse_momentum_lab.features.registry import (
    FeatureDefinition,
    FeatureRegistry,
    FeatureSetState,
    get_feature_registry,
)

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

logger = logging.getLogger(__name__)


@dataclass
class MaterializationPlan:
    """A plan for building one or more feature sets."""

    feature_sets: list[str]
    rebuild_type: str = "auto"  # auto, full, incremental
    force: bool = False
    stop_on_error: bool = True
    cascade: bool = True  # Build dependents when a feature changes

    # Computed fields
    build_order: list[FeatureDefinition] = field(default_factory=list)
    affected_date_ranges: list[tuple[date, date]] = field(default_factory=list)

    def __post_init__(self):
        if not self.build_order:
            registry = get_feature_registry()
            self.build_order = registry.resolve_build_order(self.feature_sets)


@dataclass
class MaterializationResult:
    """Result of a feature materialization run."""

    feature_name: str
    status: str  # success, skipped, failed
    row_count: int = 0
    duration_seconds: float = 0.0
    error_message: str | None = None
    rebuild_type: str = "full"


@dataclass
class MaterializationSummary:
    """Summary of a materialization run."""

    total_features: int
    successful: int
    skipped: int
    failed: int
    total_duration_seconds: float
    results: list[MaterializationResult]

    @property
    def has_failures(self) -> bool:
        return self.failed > 0


class IncrementalFeatureMaterializer:
    """
    Materializes feature sets with incremental refresh support.

    The materializer:
    1. Resolves build order based on dependencies
    2. Checks current materialization state
    3. Plans incremental vs full rebuild
    4. Executes builds in order
    5. Updates state tracking
    """

    def __init__(self, registry: FeatureRegistry | None = None):
        self.registry = registry or get_feature_registry()
        self._results: list[MaterializationResult] = []

    def build_all(
        self,
        con: DuckDBPyConnection,
        force: bool = False,
        stop_on_error: bool = False,
    ) -> MaterializationSummary:
        """Build all registered feature sets."""
        feature_names = [f.name for f in self.registry.list_all()]
        return self.build_many(
            con,
            feature_names,
            force=force,
            stop_on_error=stop_on_error,
        )

    def build_many(
        self,
        con: DuckDBPyConnection,
        feature_names: list[str],
        force: bool = False,
        stop_on_error: bool = False,
        cascade: bool = True,
    ) -> MaterializationSummary:
        """Build multiple feature sets in dependency order."""
        plan = MaterializationPlan(
            feature_sets=feature_names,
            force=force,
            stop_on_error=stop_on_error,
            cascade=cascade,
        )
        return self.execute_plan(con, plan)

    def build_one(
        self,
        con: DuckDBPyConnection,
        feature_name: str,
        force: bool = False,
    ) -> MaterializationResult:
        """Build a single feature set."""
        plan = MaterializationPlan(
            feature_sets=[feature_name],
            force=force,
            stop_on_error=True,
            cascade=False,
        )
        summary = self.execute_plan(con, plan)
        return (
            summary.results[0]
            if summary.results
            else MaterializationResult(
                feature_name=feature_name,
                status="failed",
                error_message="No result returned",
            )
        )

    def execute_plan(
        self,
        con: DuckDBPyConnection,
        plan: MaterializationPlan,
        progress: FeatureBuildProgressReporter | None = None,
    ) -> MaterializationSummary:
        """Execute a materialization plan."""
        start_time = datetime.now()
        self._results.clear()
        progress = progress or FeatureBuildProgressReporter()
        logger.info("Starting materialization plan: %d feature sets", len(plan.build_order))
        progress.emit(
            stage="start",
            message=f"Starting materialization plan with {len(plan.build_order)} feature sets",
            status="running",
            progress_pct=0.0,
            step=0,
            step_total=len(plan.build_order),
            pending_features=len(plan.build_order),
        )
        source_summary = self._summarize_sources(con)
        progress.emit(
            stage="source_summary",
            message=f"Source summary: {source_summary}",
            status="running",
            progress_pct=0.0,
            step=0,
            step_total=len(plan.build_order),
            pending_features=len(plan.build_order),
        )
        configure_duckdb_for_feature_build(con)

        for i, feat_def in enumerate(plan.build_order, 1):
            logger.info("[%d/%d] Building %s...", i, len(plan.build_order), feat_def.name)
            progress.emit(
                stage="feature_start",
                message=f"Building {feat_def.name}",
                status="running",
                progress_pct=((i - 1) / max(len(plan.build_order), 1)) * 100.0,
                step=i,
                step_total=len(plan.build_order),
                pending_features=len(plan.build_order) - i + 1,
                feature_name=feat_def.name,
            )

            result = self._build_feature(con, feat_def, plan.force, progress=progress)
            self._results.append(result)
            progress.emit(
                stage=f"feature_{result.status}",
                message=(
                    f"{feat_def.name} {result.status}"
                    + (f" ({result.row_count:,} rows)" if result.row_count else "")
                ),
                status=result.status,
                progress_pct=(i / max(len(plan.build_order), 1)) * 100.0,
                step=i,
                step_total=len(plan.build_order),
                pending_features=len(plan.build_order) - i,
                feature_name=feat_def.name,
                row_count=result.row_count,
                duration_seconds=result.duration_seconds,
                error_message=result.error_message,
            )

            if result.status == "failed" and plan.stop_on_error:
                logger.error("Stopping due to error in %s: %s", feat_def.name, result.error_message)
                break

            if result.status == "success" and plan.cascade:
                # Check if any dependent features need rebuild
                dependents = self._find_dependents(feat_def.name)
                if dependents:
                    logger.info("Cascade: %d dependent features may need rebuild", len(dependents))
                    # Add them to the plan if not already included
                    for dep in dependents:
                        if dep not in plan.feature_sets:
                            plan.feature_sets.append(dep)
                            # Re-resolve build order
                            plan.build_order = self.registry.resolve_build_order(plan.feature_sets)

        duration = (datetime.now() - start_time).total_seconds()
        progress.emit(
            stage="complete",
            message=(
                f"Materialization plan complete: {sum(1 for r in self._results if r.status == 'success')} "
                f"success, {sum(1 for r in self._results if r.status == 'skipped')} skipped, "
                f"{sum(1 for r in self._results if r.status == 'failed')} failed"
            ),
            status="failed" if any(r.status == "failed" for r in self._results) else "success",
            progress_pct=100.0,
            step=len(self._results),
            step_total=len(plan.build_order),
            pending_features=0,
        )

        return MaterializationSummary(
            total_features=len(self._results),
            successful=sum(1 for r in self._results if r.status == "success"),
            skipped=sum(1 for r in self._results if r.status == "skipped"),
            failed=sum(1 for r in self._results if r.status == "failed"),
            total_duration_seconds=duration,
            results=self._results,
        )

    def _build_feature(
        self,
        con: DuckDBPyConnection,
        feat_def: FeatureDefinition,
        force: bool,
        progress: FeatureBuildProgressReporter | None = None,
    ) -> MaterializationResult:
        """Build a single feature set."""
        import time

        start_time = time.time()

        try:
            # Import the builder function
            if feat_def.name == "feat_daily_core":
                from nse_momentum_lab.features.daily_core import build_feat_daily_core

                row_count = build_feat_daily_core(con, force=force)
                rebuild_type = "full" if force else "cached"
            elif feat_def.name == "feat_intraday_core":
                from nse_momentum_lab.features.intraday_core import build_feat_intraday_core

                row_count = build_feat_intraday_core(con, force=force, progress=progress)
                rebuild_type = "full" if force else "cached"
            elif feat_def.name == "feat_event_core":
                from nse_momentum_lab.features.event_core import build_feat_event_core

                row_count = build_feat_event_core(con, force=force)
                rebuild_type = "full" if force else "cached"
            elif feat_def.name == "feat_2lynch_derived":
                from nse_momentum_lab.features.strategy_derived import build_2lynch_derived

                row_count = build_2lynch_derived(con, force=force)
                rebuild_type = "full" if force else "cached"
            else:
                # Try to use a generic builder via SQL
                if feat_def.build_sql:
                    row_count = self._build_from_sql(con, feat_def, force)
                    rebuild_type = "full" if force else "cached"
                else:
                    return MaterializationResult(
                        feature_name=feat_def.name,
                        status="failed",
                        error_message=f"No builder found for {feat_def.name}",
                    )

            duration = time.time() - start_time

            # Check if it was actually built or cached
            if not force and row_count > 0:
                # Determine if cached by checking if we got a "is up-to-date" log
                # For now, assume not cached if we got here
                status = "success"
            else:
                status = "success"

            return MaterializationResult(
                feature_name=feat_def.name,
                status=status,
                row_count=row_count,
                duration_seconds=duration,
                rebuild_type=rebuild_type,
            )

        except Exception as e:
            duration = time.time() - start_time
            logger.exception("Failed to build %s", feat_def.name)
            return MaterializationResult(
                feature_name=feat_def.name,
                status="failed",
                duration_seconds=duration,
                error_message=str(e),
            )

    def _summarize_sources(self, con: DuckDBPyConnection) -> str:
        """Summarize active source views for human-readable progress messages."""
        parts: list[str] = []
        for view_name, label in (("v_daily", "daily"), ("v_5min", "5min")):
            try:
                row = con.execute(
                    f"""
                    SELECT
                        COUNT(*)::BIGINT AS rows,
                        COUNT(DISTINCT symbol)::BIGINT AS symbols
                    FROM {view_name}
                    """
                ).fetchone()
                if row:
                    parts.append(
                        f"{label}={int(row[1]) if row[1] is not None else 0:,} symbols/"
                        f"{int(row[0]) if row[0] is not None else 0:,} rows"
                    )
            except Exception:
                continue
        return ", ".join(parts) if parts else "sources unavailable"

    def _build_from_sql(
        self,
        con: DuckDBPyConnection,
        feat_def: FeatureDefinition,
        force: bool,
    ) -> int:
        """Build a feature from its SQL definition."""
        if not feat_def.build_sql:
            raise ValueError(f"No build SQL for {feat_def.name}")

        # Check if already built
        if not force:
            try:
                row = con.execute(
                    f"SELECT table_name, query_version, row_count FROM bt_materialization_state "
                    f"WHERE table_name = '{feat_def.name}'"
                ).fetchone()
                if row:
                    _table_name, query_version, row_count = row
                    if query_version == feat_def.version:
                        logger.info("%s is up-to-date (%d rows).", feat_def.name, row_count)
                        return int(row_count)
            except Exception:
                pass

        # Drop and rebuild
        logger.info("Building %s from SQL...", feat_def.name)
        con.execute(f"DROP TABLE IF EXISTS {feat_def.name}")
        con.execute(feat_def.build_sql)

        # Create index
        con.execute(
            f"CREATE INDEX idx_{feat_def.name}_symbol_date ON {feat_def.name}(symbol, trading_date)"
        )

        row = con.execute(f"SELECT COUNT(*) FROM {feat_def.name}").fetchone()
        n = int(row[0]) if row and row[0] is not None else 0

        # Update state
        con.execute(
            """
            INSERT OR REPLACE INTO bt_materialization_state
            (table_name, dataset_hash, query_version, row_count, updated_at)
            VALUES (?, ?, ?, ?, current_timestamp)
        """,
            [feat_def.name, "auto", feat_def.version, n],
        )

        return n

    def _find_dependents(self, feature_name: str) -> list[str]:
        """Find all feature sets that depend on this one."""
        dependents: list[str] = []
        for feat in self.registry.list_all():
            for dep in feat.feature_dependencies:
                if dep.name == feature_name:
                    dependents.append(feat.name)
                    break
        return dependents

    def get_feature_state(
        self, con: DuckDBPyConnection, feature_name: str
    ) -> FeatureSetState | None:
        """Get the current materialization state of a feature set."""
        try:
            row = con.execute(f"""
                SELECT table_name, dataset_hash, query_version, row_count, updated_at
                FROM bt_materialization_state
                WHERE table_name = '{feature_name}'
            """).fetchone()
            if not row:
                return None

            # Get min/max dates
            date_row = con.execute(f"""
                SELECT MIN(trading_date), MAX(trading_date)
                FROM {feature_name}
            """).fetchone()

            return FeatureSetState(
                table_name=row[0],
                dataset_hash=row[1],
                query_version=row[2],
                row_count=int(row[3]) if row[3] else 0,
                min_date=date.fromisoformat(date_row[0]) if date_row and date_row[0] else None,
                max_date=date.fromisoformat(date_row[1]) if date_row and date_row[1] else None,
                updated_at=str(row[4]) if row[4] else None,
            )
        except Exception:
            return None

    def list_materialized(self, con: DuckDBPyConnection) -> list[FeatureSetState]:
        """List all materialized feature sets with their state."""
        states: list[FeatureSetState] = []
        try:
            rows = con.execute("""
                SELECT table_name, dataset_hash, query_version, row_count, updated_at
                FROM bt_materialization_state
                WHERE table_name LIKE 'feat_%'
                ORDER BY table_name
            """).fetchall()

            for row in rows:
                # Get min/max dates
                try:
                    date_row = con.execute(f"""
                        SELECT MIN(trading_date), MAX(trading_date)
                        FROM {row[0]}
                    """).fetchone()
                    min_date = date.fromisoformat(date_row[0]) if date_row and date_row[0] else None
                    max_date = date.fromisoformat(date_row[1]) if date_row and date_row[1] else None
                except Exception:
                    min_date = None
                    max_date = None

                states.append(
                    FeatureSetState(
                        table_name=row[0],
                        dataset_hash=row[1],
                        query_version=row[2],
                        row_count=int(row[3]) if row[3] else 0,
                        min_date=min_date,
                        max_date=max_date,
                        updated_at=str(row[4]) if row[4] else None,
                    )
                )
        except Exception:
            pass

        return states
