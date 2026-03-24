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
        LAG(high, 1) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_high,
        LAG(low, 1) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_low,
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
        prev_high,
        prev_low,
        prev_open,
        ret_1d_lag1,
        ret_1d_lag2,
        -- 2LYNCH Filters (as boolean flags)
        (close_pos_in_range >= 0.70) AS filter_h,
        ((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close < prev_open) AS filter_n,
        (COALESCE(prior_breakouts_30d, 0) <= 2) AS filter_y,
        (vol_dryup_ratio < 1.3) AS filter_c,
        (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER)
         + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
        (ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0) AS filter_2,
        -- Combined filter score
        (CAST(close_pos_in_range >= 0.70 AS INTEGER) +
         CAST((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close < prev_open AS INTEGER) +
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


def build_2lynch_derived(
    con,  # DuckDBPyConnection
    force: bool = False,
    dataset_hash: str | None = None,
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

    # Check if already built
    if not force:
        try:
            row = con.execute(
                "SELECT table_name, query_version, row_count FROM bt_materialization_state "
                "WHERE table_name = 'feat_2lynch_derived'"
            ).fetchone()
            if row:
                _table_name, query_version, row_count = row
                if query_version == FEAT_2LYNCH_DERIVED_VERSION:
                    logger.info("feat_2lynch_derived is up-to-date (%d rows).", row_count)
                    return int(row_count)
        except Exception:
            pass  # Table doesn't exist yet

    # Drop and rebuild
    logger.info("Building feat_2lynch_derived materialized table...")
    con.execute("DROP TABLE IF EXISTS feat_2lynch_derived")
    con.execute(FEAT_2LYNCH_DERIVED_SQL)

    # Create index for common queries
    con.execute(
        "CREATE INDEX idx_feat_2lynch_symbol_date ON feat_2lynch_derived(symbol, trading_date)"
    )

    row = con.execute("SELECT COUNT(*) FROM feat_2lynch_derived").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0

    # Update materialization state
    if dataset_hash is None:
        # Use feat_daily_core's dataset hash
        core_row = con.execute(
            "SELECT dataset_hash FROM bt_materialization_state WHERE table_name = 'feat_daily_core'"
        ).fetchone()
        if core_row:
            dataset_hash = core_row[0]
        else:
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
    range_percentile AS range_percentile,
    vol_dryup_ratio,
    prior_breakouts_30d,
    prior_breakouts_90d,
    prior_breakdowns_90d,
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
    con.execute(FEAT_DAILY_VIEW_SQL)
