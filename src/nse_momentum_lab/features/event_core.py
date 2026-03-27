"""
Event Core Features for NSE Momentum Lab.

feat_event_core contains features for episodic pivot and event-driven strategies:

- Event type (earnings, corporate actions, news, etc.)
- Event timestamp and date
- Event freshness (days since event)
- Earnings date and gap context
- Post-event drift window markers
- Event surprise placeholder fields (for future vendor enrichment)

This is a placeholder module for future event data ingestion.
When event data becomes available, this module will be expanded.
"""

from __future__ import annotations

import logging
from datetime import date

from nse_momentum_lab.features.registry import (
    FeatureDefinition,
    FeatureDependency,
    FeatureGranularity,
    IncrementalPolicy,
)

logger = logging.getLogger(__name__)

# Version for feat_event_core - bump when SQL logic changes
FEAT_EVENT_CORE_VERSION = "feat_event_core_v1_2026_03_06"


# SQL for building feat_event_core (placeholder - creates empty table structure)
FEAT_EVENT_CORE_SQL = """
CREATE TABLE feat_event_core AS
WITH daily_dates AS (
    SELECT DISTINCT symbol, date AS trading_date
    FROM v_daily
    WHERE date IS NOT NULL
)
SELECT
    symbol,
    trading_date,
    -- Event placeholders (NULL until event data is ingested)
    NULL::VARCHAR AS event_type,
    NULL::TIMESTAMP AS event_timestamp,
    NULL::INTEGER AS days_since_event,
    NULL::DATE AS earnings_date,
    NULL::DOUBLE AS earnings_gap_pct,
    NULL::INTEGER AS post_event_day,
    NULL::DOUBLE AS event_surprise_pct,
    NULL::VARCHAR AS event_sentiment,
    NULL::BOOLEAN AS is_earnings_event,
    NULL::BOOLEAN AS is_corporate_action,
    NULL::BOOLEAN AS has_event_today
FROM daily_dates
"""


def _sql_date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _event_core_sql_for_source(source_view: str) -> str:
    return FEAT_EVENT_CORE_SQL.replace("CREATE TABLE feat_event_core AS\n", "").replace(
        "FROM v_daily", f"FROM {source_view}"
    )


def _build_feat_event_core_incremental(con, *, since_date: date, dataset_hash: str) -> int:
    source_view = "_feat_event_core_src"
    delta_table = "_feat_event_core_delta"
    table_exists = True

    try:
        con.execute("SELECT 1 FROM feat_event_core LIMIT 1").fetchone()
    except Exception:
        table_exists = False

    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {source_view} AS "
        f"SELECT * FROM v_daily WHERE date >= {_sql_date_literal(since_date)}"
    )
    try:
        con.execute(f"DROP TABLE IF EXISTS {delta_table}")
        con.execute(f"CREATE TEMP TABLE {delta_table} AS {_event_core_sql_for_source(source_view)}")
        if table_exists:
            con.execute("DELETE FROM feat_event_core WHERE trading_date >= ?", [since_date])
            con.execute(
                f"""
                INSERT INTO feat_event_core
                SELECT *
                FROM {delta_table}
                WHERE trading_date >= ?
                """,
                [since_date],
            )
        else:
            con.execute(
                f"""
                CREATE TABLE feat_event_core AS
                SELECT *
                FROM {delta_table}
                WHERE trading_date >= {_sql_date_literal(since_date)}
                """
            )
    finally:
        con.execute(f"DROP VIEW IF EXISTS {source_view}")
        con.execute(f"DROP TABLE IF EXISTS {delta_table}")

    if not table_exists:
        con.execute(
            "CREATE INDEX idx_feat_event_core_symbol_date ON feat_event_core(symbol, trading_date)"
        )

    row = con.execute("SELECT COUNT(*) FROM feat_event_core").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0
    con.execute(
        """
        INSERT OR REPLACE INTO bt_materialization_state
        (table_name, dataset_hash, query_version, row_count, updated_at)
        VALUES (?, ?, ?, ?, current_timestamp)
    """,
        ["feat_event_core", dataset_hash, FEAT_EVENT_CORE_VERSION, n],
    )
    logger.info("feat_event_core incrementally refreshed from %s: %d rows", since_date, n)
    return n


def build_feat_event_core(
    con,  # DuckDBPyConnection
    force: bool = False,
    dataset_hash: str | None = None,
    since_date: date | None = None,
) -> int:
    """
    Build the feat_event_core materialized table.

    This is currently a placeholder that creates an empty structure
    for future event data ingestion.

    Args:
        con: DuckDB connection
        force: Force rebuild even if up-to-date
        dataset_hash: Hash of input dataset for incremental detection

    Returns:
        Number of rows in the built table
    """
    # Check if v_daily exists before trying to hash the source snapshot.
    try:
        con.execute("SELECT 1 FROM v_daily LIMIT 1").fetchone()
    except Exception:
        logger.warning("v_daily view not available, skipping feat_event_core")
        return 0

    if dataset_hash is None:
        snapshot_row = con.execute("""
            SELECT
                COUNT(*)::BIGINT AS rows,
                COUNT(DISTINCT symbol)::BIGINT AS symbols,
                MIN(date)::VARCHAR AS min_date,
                MAX(date)::VARCHAR AS max_date
            FROM v_daily
        """).fetchone()
        import hashlib
        import json

        snapshot = {
            "rows": int(snapshot_row[0]) if snapshot_row and snapshot_row[0] else 0,
            "symbols": int(snapshot_row[1]) if snapshot_row and snapshot_row[1] else 0,
            "min_date": snapshot_row[2] if snapshot_row else None,
            "max_date": snapshot_row[3] if snapshot_row else None,
        }
        dataset_hash = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()[
            :16
        ]

    if since_date is not None and not force:
        return _build_feat_event_core_incremental(
            con, since_date=since_date, dataset_hash=dataset_hash
        )

    # Check if already built
    if not force:
        try:
            row = con.execute(
                "SELECT table_name, dataset_hash, query_version, row_count FROM bt_materialization_state "
                "WHERE table_name = 'feat_event_core'"
            ).fetchone()
            if row:
                _table_name, current_dataset_hash, query_version, row_count = row
                if (
                    query_version == FEAT_EVENT_CORE_VERSION
                    and current_dataset_hash == dataset_hash
                ):
                    logger.info("feat_event_core is up-to-date (%d rows).", row_count)
                    return int(row_count)
        except Exception:
            pass  # Table doesn't exist yet

    # Drop and rebuild
    logger.info("Building feat_event_core materialized table (placeholder)...")
    con.execute("DROP TABLE IF EXISTS feat_event_core")
    con.execute(FEAT_EVENT_CORE_SQL)

    # Create index for common queries
    con.execute(
        "CREATE INDEX idx_feat_event_core_symbol_date ON feat_event_core(symbol, trading_date)"
    )

    row = con.execute("SELECT COUNT(*) FROM feat_event_core").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0

    # Update materialization state
    con.execute(
        """
        INSERT OR REPLACE INTO bt_materialization_state
        (table_name, dataset_hash, query_version, row_count, updated_at)
        VALUES (?, ?, ?, ?, current_timestamp)
    """,
        ["feat_event_core", dataset_hash, FEAT_EVENT_CORE_VERSION, n],
    )

    logger.info("feat_event_core built: %d rows (placeholder structure)", n)
    return n


def register_feat_event_core(registry) -> None:
    """Register feat_event_core with the feature registry."""

    registry.register(
        FeatureDefinition(
            name="feat_event_core",
            version=FEAT_EVENT_CORE_VERSION,
            description="Event features for episodic strategies: event_type, event_timestamp, earnings context, post-event markers. Currently a placeholder.",
            granularity=FeatureGranularity.EVENT,
            layer="event",
            input_datasets=["v_daily"],
            feature_dependencies=[
                FeatureDependency(name="v_daily", is_dataset=True, required_lookback_days=1),
            ],
            required_lookback_days=1,
            build_sql=FEAT_EVENT_CORE_SQL,
            incremental_policy=IncrementalPolicy.APPEND,
            partition_grain="year",
            output_columns=[
                "symbol",
                "trading_date",
                "event_type",
                "event_timestamp",
                "days_since_event",
                "earnings_date",
                "earnings_gap_pct",
                "post_event_day",
                "event_surprise_pct",
                "event_sentiment",
                "is_earnings_event",
                "is_corporate_action",
                "has_event_today",
            ],
        )
    )
