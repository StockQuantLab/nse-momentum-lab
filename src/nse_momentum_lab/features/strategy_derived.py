"""
Strategy-Derived Features for NSE Momentum Lab.

This module contains strategy-specific derived features built on top of
core feature sets. These features do NOT pollute the core tables.

Strategy families:
- threshold_breakout: Configurable threshold breakout counters
- threshold_breakdown: Configurable threshold breakdown counters
- episodic_pivot: Gap-based pivot qualifiers
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

from nse_momentum_lab.features.registry import (
    FeatureDefinition,
    FeatureDependency,
    FeatureGranularity,
    IncrementalPolicy,
)

logger = logging.getLogger(__name__)

# Version for 2LYNCH derived features - bump when SQL logic changes
FEAT_2LYNCH_DERIVED_VERSION = "feat_2lynch_derived_v1_2026_03_06"


# SQL for building feat_2lynch_derived
# This creates a view-like table with 2LYNCH-specific features on top of feat_daily_core
FEAT_2LYNCH_DERIVED_SQL = """
CREATE TABLE feat_2lynch_derived AS
WITH base AS (
    SELECT
        symbol,
        trading_date,
        ret_1d,
        ret_5d,
        atr_20,
        range_pct,
        close_pos_in_range,
        ma_20,
        ma_65,
        ma_7,
        ma_65_sma,
        rs_252,
        vol_dryup_ratio,
        atr_compress_ratio,
        range_percentile_252 AS range_percentile,
        breakout_4pct_up_30d AS prior_breakouts_30d,
        breakout_4pct_up_90d AS prior_breakouts_90d,
        breakdown_4pct_down_90d AS prior_breakdowns_90d,
         r2_65,
        open,
        close
    FROM feat_daily_core
),
with_lag AS (
    SELECT
        symbol,
        trading_date,
        ret_1d,
        ret_5d,
        atr_20,
        range_pct,
        close_pos_in_range,
        ma_20,
        ma_65,
        ma_7,
        ma_65_sma,
        rs_252,
        vol_dryup_ratio,
        atr_compress_ratio,
        range_percentile,
        prior_breakouts_30d,
        prior_breakouts_90d,
        prior_breakdowns_90d,
        r2_65,
        open,
        close,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_close,
        LAG(range_pct * close, 1) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_range,
        LAG(open, 1) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_open,
        (LAG(close, 1) OVER (PARTITION BY symbol ORDER BY trading_date)
         - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY trading_date))
        / NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY trading_date), 0) AS ret_1d_lag1,
        (LAG(close, 2) OVER (PARTITION BY symbol ORDER BY trading_date)
         - LAG(close, 3) OVER (PARTITION BY symbol ORDER BY trading_date))
        / NULLIF(LAG(close, 3) OVER (PARTITION BY symbol ORDER BY trading_date), 0) AS ret_1d_lag2
    FROM base
),
with_filters AS (
    SELECT
        symbol,
        trading_date,
        ret_1d,
        ret_5d,
        atr_20,
        range_pct,
        close_pos_in_range,
        ma_20,
        ma_65,
        ma_7,
        ma_65_sma,
        rs_252,
        vol_dryup_ratio,
        atr_compress_ratio,
        range_percentile,
        prior_breakouts_30d,
        prior_breakouts_90d,
        prior_breakdowns_90d,
        r2_65,
        open,
        close,
        prev_close,
        prev_range,
        prev_open,
        ret_1d_lag1,
        ret_1d_lag2,
        -- 2LYNCH Filters (as boolean flags)
        (close_pos_in_range >= 0.70) AS filter_h,
        (prev_range < (atr_20 * 0.5) OR prev_close < prev_open) AS filter_n,
        (COALESCE(prior_breakouts_30d, 0) <= 2) AS filter_y,
        (vol_dryup_ratio < 1.3) AS filter_c,
        (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER)
         + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
        (ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0) AS filter_2,
        -- Combined filter score
        (CAST(close_pos_in_range >= 0.70 AS INTEGER) +
         CAST(prev_range < (atr_20 * 0.5) OR prev_close < prev_open AS INTEGER) +
         CAST(COALESCE(prior_breakouts_30d, 0) <= 2 AS INTEGER) +
         CAST(vol_dryup_ratio < 1.3 AS INTEGER) +
         CAST((CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER)
              + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS INTEGER) +
         CAST(ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0 AS INTEGER)
        ) AS filters_passed
    FROM with_lag
    WHERE close IS NOT NULL
)
SELECT
    symbol,
    trading_date,
    ret_1d,
    ret_5d,
    atr_20,
    range_pct,
    close_pos_in_range,
    ma_20,
    ma_65,
    ma_7,
    ma_65_sma,
    rs_252,
    vol_dryup_ratio,
    atr_compress_ratio,
    range_percentile,
    prior_breakouts_30d,
    prior_breakouts_90d,
    prior_breakdowns_90d,
    r2_65,
    open,
    close,
    filter_h,
    filter_n,
    filter_y,
    filter_c,
    filter_l,
    filter_2,
    filters_passed
FROM with_filters
"""


def _sql_date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _derived_sql_for_source(source_view: str) -> str:
    return FEAT_2LYNCH_DERIVED_SQL.replace("CREATE TABLE feat_2lynch_derived AS\n", "").replace(
        "FROM feat_daily_core", f"FROM {source_view}"
    )


def _build_2lynch_derived_incremental(con, *, since_date: date, dataset_hash: str) -> int:
    source_view = "_feat_2lynch_derived_src"
    delta_table = "_feat_2lynch_derived_delta"
    rebuild_start = since_date - timedelta(days=14)
    table_exists = True

    try:
        con.execute("SELECT 1 FROM feat_2lynch_derived LIMIT 1").fetchone()
    except Exception:
        table_exists = False

    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {source_view} AS "
        f"SELECT * FROM feat_daily_core WHERE trading_date >= {_sql_date_literal(rebuild_start)}"
    )
    try:
        con.execute(f"DROP TABLE IF EXISTS {delta_table}")
        con.execute(f"CREATE TEMP TABLE {delta_table} AS {_derived_sql_for_source(source_view)}")
        if table_exists:
            con.execute("DELETE FROM feat_2lynch_derived WHERE trading_date >= ?", [since_date])
            con.execute(
                f"""
                INSERT INTO feat_2lynch_derived
                SELECT *
                FROM {delta_table}
                WHERE trading_date >= ?
                """,
                [since_date],
            )
        else:
            con.execute(
                f"""
                CREATE TABLE feat_2lynch_derived AS
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
                "CREATE INDEX idx_feat_2lynch_symbol_date ON feat_2lynch_derived(symbol, trading_date)"
            )
            con.execute(
                "CREATE INDEX idx_feat_2lynch_close_pos_in_range ON feat_2lynch_derived(close_pos_in_range)"
            )
            con.execute(
                "CREATE INDEX idx_feat_2lynch_vol_dryup_ratio ON feat_2lynch_derived(vol_dryup_ratio)"
            )

    row = con.execute("SELECT COUNT(*) FROM feat_2lynch_derived").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0
    con.execute(
        """
        INSERT OR REPLACE INTO bt_materialization_state
        (table_name, dataset_hash, query_version, row_count, updated_at)
        VALUES (?, ?, ?, ?, current_timestamp)
    """,
        ["feat_2lynch_derived", dataset_hash, FEAT_2LYNCH_DERIVED_VERSION, n],
    )
    logger.info("feat_2lynch_derived incrementally refreshed from %s: %d rows", since_date, n)
    return n


def build_2lynch_derived(
    con,  # DuckDBPyConnection
    force: bool = False,
    dataset_hash: str | None = None,
    since_date: date | None = None,
) -> int:
    """
    Build the feat_2lynch_derived materialized table.

    This table contains 2LYNCH-specific filter flags and is built
    on top of feat_daily_core.

    Args:
        con: DuckDB connection
        force: Force rebuild even if up-to-date
        dataset_hash: Hash of input dataset for incremental detection

    Returns:
        Number of rows in the built table
    """
    # Check if feat_daily_core exists
    try:
        con.execute("SELECT 1 FROM feat_daily_core LIMIT 1").fetchone()
    except Exception:
        logger.warning("feat_daily_core not available, cannot build feat_2lynch_derived")
        return 0

    if dataset_hash is None:
        core_row = con.execute(
            "SELECT dataset_hash FROM bt_materialization_state WHERE table_name = 'feat_daily_core'"
        ).fetchone()
        dataset_hash = core_row[0] if core_row and core_row[0] else None

    if since_date is not None and not force:
        return _build_2lynch_derived_incremental(
            con, since_date=since_date, dataset_hash=dataset_hash or "unknown"
        )

    # Check if already built
    if not force:
        try:
            row = con.execute(
                "SELECT table_name, dataset_hash, query_version, row_count FROM bt_materialization_state "
                "WHERE table_name = 'feat_2lynch_derived'"
            ).fetchone()
            if row:
                _table_name, current_dataset_hash, query_version, row_count = row
                if (
                    query_version == FEAT_2LYNCH_DERIVED_VERSION
                    and current_dataset_hash == dataset_hash
                    and dataset_hash is not None
                ):
                    logger.info("feat_2lynch_derived is up-to-date (%d rows).", row_count)
                    return int(row_count)
        except Exception:
            pass  # Table doesn't exist yet

    # Drop and rebuild
    logger.info("Building feat_2lynch_derived materialized table...")
    con.execute("DROP TABLE IF EXISTS feat_2lynch_derived")
    con.execute(FEAT_2LYNCH_DERIVED_SQL)

    # Create indexes for common candidate queries
    con.execute(
        "CREATE INDEX idx_feat_2lynch_symbol_date ON feat_2lynch_derived(symbol, trading_date)"
    )
    con.execute(
        "CREATE INDEX idx_feat_2lynch_close_pos_in_range ON feat_2lynch_derived(close_pos_in_range)"
    )
    con.execute(
        "CREATE INDEX idx_feat_2lynch_vol_dryup_ratio ON feat_2lynch_derived(vol_dryup_ratio)"
    )

    row = con.execute("SELECT COUNT(*) FROM feat_2lynch_derived").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0

    # Update materialization state
    if dataset_hash is None:
        dataset_hash = "unknown"

    con.execute(
        """
        INSERT OR REPLACE INTO bt_materialization_state
        (table_name, dataset_hash, query_version, row_count, updated_at)
        VALUES (?, ?, ?, ?, current_timestamp)
    """,
        ["feat_2lynch_derived", dataset_hash, FEAT_2LYNCH_DERIVED_VERSION, n],
    )

    logger.info("feat_2lynch_derived built: %d rows", n)
    return n


def register_2lynch_derived(registry) -> None:
    """Register feat_2lynch_derived with the feature registry."""

    registry.register(
        FeatureDefinition(
            name="feat_2lynch_derived",
            version=FEAT_2LYNCH_DERIVED_VERSION,
            description="2LYNCH strategy-specific derived features: filter flags (H, N, Y, C, L, 2), young breakout counters. Built on feat_daily_core.",
            granularity=FeatureGranularity.DAILY,
            layer="derived",
            input_datasets=[],
            feature_dependencies=[
                FeatureDependency(
                    name="feat_daily_core", is_dataset=False, required_lookback_days=252
                ),
            ],
            required_lookback_days=252,
            build_sql=FEAT_2LYNCH_DERIVED_SQL,
            incremental_policy=IncrementalPolicy.DEPENDENCY_CASCADE,
            partition_grain="year",
            output_columns=[
                "symbol",
                "trading_date",
                "ret_1d",
                "ret_5d",
                "atr_20",
                "range_pct",
                "close_pos_in_range",
                "ma_20",
                "ma_65",
                "ma_7",
                "ma_65_sma",
                "rs_252",
                "vol_dryup_ratio",
                "atr_compress_ratio",
                "range_percentile",
                "prior_breakouts_30d",
                "prior_breakouts_90d",
                "prior_breakdowns_90d",
                "r2_65",
                "filter_h",
                "filter_n",
                "filter_y",
                "filter_c",
                "filter_l",
                "filter_2",
                "filters_passed",
            ],
        )
    )


# ----------------------------------------------------------------------
# Backward compatibility: Create feat_daily as a view over core + derived
# ----------------------------------------------------------------------

FEAT_DAILY_VIEW_SQL = """
CREATE OR REPLACE VIEW feat_daily AS
SELECT
    symbol,
    trading_date AS date,
    ret_1d,
    ret_5d,
    atr_20,
    range_pct,
    close_pos_in_range,
    ma_20,
    ma_65,
    ma_7,
    ma_65_sma,
    rs_252,
    vol_20,
    dollar_vol_20,
    r2_65,
    atr_compress_ratio,
    range_percentile_252 AS range_percentile,
    vol_dryup_ratio,
    breakout_4pct_up_30d AS prior_breakouts_30d,
    breakout_4pct_up_90d AS prior_breakouts_90d,
    breakdown_4pct_down_90d AS prior_breakdowns_90d,
    open,
    close
FROM feat_daily_core
"""


def create_legacy_feat_daily_view(con) -> None:
    """
    Create a backward-compatible feat_daily view over feat_daily_core.

    This ensures existing queries continue working while we migrate
    to the new modular feature store.
    """
    logger.info("Creating backward-compatible feat_daily view...")
    for statement in ("DROP VIEW IF EXISTS feat_daily", "DROP TABLE IF EXISTS feat_daily"):
        try:
            con.execute(statement)
        except Exception:
            pass
    con.execute(FEAT_DAILY_VIEW_SQL)
