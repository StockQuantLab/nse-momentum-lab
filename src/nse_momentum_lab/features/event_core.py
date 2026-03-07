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
    NULL::BOOLEAN EXISTS AS has_event_today
FROM daily_dates
"""


def build_feat_event_core(
    con,  # DuckDBPyConnection
    force: bool = False,
    dataset_hash: str | None = None,
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
    # Check if already built
    if not force:
        try:
            row = con.execute(
                "SELECT table_name, query_version, row_count FROM bt_materialization_state "
                "WHERE table_name = 'feat_event_core'"
            ).fetchone()
            if row:
                _table_name, query_version, row_count = row
                if query_version == FEAT_EVENT_CORE_VERSION:
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
    if dataset_hash is None:
        # Generate a simple hash from v_daily
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
            "rows": int(snapshot_row[0]) if snapshot_row[0] else 0,
            "symbols": int(snapshot_row[1]) if snapshot_row[1] else 0,
            "min_date": snapshot_row[2],
            "max_date": snapshot_row[3],
        }
        dataset_hash = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()[
            :16
        ]

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
