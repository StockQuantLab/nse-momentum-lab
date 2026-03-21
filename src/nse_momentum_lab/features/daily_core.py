"""
Daily Core Features for NSE Momentum Lab.

feat_daily_core contains strategy-agnostic daily features shared across
most momentum strategies:

- Returns: ret_1d, ret_2d, ret_5d, ret_10d, ret_20d, ret_63d, ret_252d
- Volatility: atr_14, atr_20, true_range
- Trend: ma_10, ma_20, ma_50, ma_65, ma_200, ma_7 (for TI65), ma_65_sma
- Liquidity: vol_20, dollar_vol_20
- Gap features: gap_open_vs_prev_close, gap_high_vs_prev_close, gap_low_vs_prev_close
- Candle structure: range_pct, close_pos_in_range, body_ratio, wick ratios
- Position in range: range_percentile_63, range_percentile_252
- Basic breakout counters: breakout_4pct_up_90d, breakdown_4pct_down_90d

These features are universal and do not include strategy-specific filters.
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

# Version for feat_daily_core - bump when SQL logic changes
FEAT_DAILY_CORE_VERSION = "feat_daily_core_v1_2026_03_06"


# SQL for building feat_daily_core
FEAT_DAILY_CORE_SQL = """
CREATE TABLE feat_daily_core AS
WITH base AS (
    SELECT
        symbol,
        date AS trading_date,
        close,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS close_1d,
        LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date) AS close_2d,
        LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) AS close_5d,
        LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date) AS close_10d,
        LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date) AS close_20d,
        LAG(close, 50) OVER (PARTITION BY symbol ORDER BY date) AS close_50d,
        LAG(close, 63) OVER (PARTITION BY symbol ORDER BY date) AS close_63d,
        LAG(close, 65) OVER (PARTITION BY symbol ORDER BY date) AS close_65d,
        LAG(close, 200) OVER (PARTITION BY symbol ORDER BY date) AS close_200d,
        LAG(close, 252) OVER (PARTITION BY symbol ORDER BY date) AS close_252d,
        high,
        low,
        open,
        volume,
        LAG(high, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_high,
        LAG(low, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_low,
        LAG(open, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_open,
        close * volume AS dollar_vol
    FROM v_daily
),
features AS (
    SELECT
        symbol,
        trading_date,
        -- Returns over multiple periods
        (close / NULLIF(close_1d, 0)) - 1 AS ret_1d,
        (close / NULLIF(close_2d, 0)) - 1 AS ret_2d,
        (close / NULLIF(close_5d, 0)) - 1 AS ret_5d,
        (close / NULLIF(close_10d, 0)) - 1 AS ret_10d,
        (close / NULLIF(close_20d, 0)) - 1 AS ret_20d,
        (close / NULLIF(close_63d, 0)) - 1 AS ret_63d,
        (close / NULLIF(close_252d, 0)) - 1 AS ret_252d,
        -- True range and range metrics
        (high - low) AS true_range,
        (high - low) / NULLIF(close, 0) AS range_pct,
        (close - low) / NULLIF(high - low, 0) AS close_pos_in_range,
        -- Gap features
        (open - NULLIF(close_1d, 0)) / NULLIF(close_1d, 0) AS gap_open_vs_prev_close,
        (high - NULLIF(close_1d, 0)) / NULLIF(close_1d, 0) AS gap_high_vs_prev_close,
        (low - NULLIF(close_1d, 0)) / NULLIF(close_1d, 0) AS gap_low_vs_prev_close,
        -- Candle structure
        CASE
            WHEN high - low > 0 THEN
                CASE
                    WHEN close >= open THEN (close - open) / (high - low)  -- Bullish body ratio
                    ELSE (open - close) / (high - low)  -- Bearish body ratio
                END
            ELSE NULL
        END AS body_ratio,
        CASE
            WHEN high - low > 0 THEN (high - GREATEST(open, close)) / NULLIF(high - low, 0)
            ELSE NULL
        END AS upper_wick_ratio,
        CASE
            WHEN high - low > 0 THEN (LEAST(open, close) - low) / NULLIF(high - low, 0)
            ELSE NULL
        END AS lower_wick_ratio,
        -- Moving averages (for reference in strategies)
        close_20d AS ma_20,
        close_50d AS ma_50,
        close_65d AS ma_65,
        close_200d AS ma_200,
        -- Relative strength
        (close / NULLIF(close_252d, 0)) - 1 AS rs_252,
        -- Liquidity
        volume,
        dollar_vol,
        open,
        close
    FROM base
    WHERE close IS NOT NULL
),
smoothed AS (
    SELECT
        symbol,
        trading_date,
        ret_1d,
        ret_2d,
        ret_5d,
        ret_10d,
        ret_20d,
        ret_63d,
        ret_252d,
        true_range,
        range_pct,
        close_pos_in_range,
        gap_open_vs_prev_close,
        gap_high_vs_prev_close,
        gap_low_vs_prev_close,
        body_ratio,
        upper_wick_ratio,
        lower_wick_ratio,
        ma_20,
        ma_50,
        ma_65,
        ma_200,
        rs_252,
        volume,
        dollar_vol,
        -- Smoothed volatility metrics
        AVG(true_range) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS 13 PRECEDING
        ) AS atr_14,
        AVG(true_range) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS 19 PRECEDING
        ) AS atr_20,
        -- TI65 components: true rolling averages
        AVG(close) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7,
        AVG(close) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 64 PRECEDING AND CURRENT ROW
        ) AS ma_65_sma,
        -- Liquidity smoothing
        AVG(volume) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS 19 PRECEDING
        ) AS vol_20,
        AVG(dollar_vol) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS 19 PRECEDING
        ) AS dollar_vol_20,
        open,
        close
    FROM features
),
with_rownum AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trading_date) AS rn
    FROM smoothed
),
advanced_features AS (
    SELECT
        symbol,
        trading_date,
        ret_1d,
        ret_2d,
        ret_5d,
        ret_10d,
        ret_20d,
        ret_63d,
        ret_252d,
        atr_14,
        atr_20,
        range_pct,
        close_pos_in_range,
        gap_open_vs_prev_close,
        gap_high_vs_prev_close,
        gap_low_vs_prev_close,
        body_ratio,
        upper_wick_ratio,
        lower_wick_ratio,
        ma_7,
        ma_20,
        ma_50,
        ma_65,
        ma_65_sma,
        ma_200,
        rs_252,
        vol_20,
        dollar_vol_20,
        -- Range percentiles (position in N-day range)
        (close - MIN(close) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 62 PRECEDING AND CURRENT ROW
        )) / NULLIF(
            MAX(close) OVER (
                PARTITION BY symbol ORDER BY trading_date
                ROWS BETWEEN 62 PRECEDING AND CURRENT ROW
            ) -
            MIN(close) OVER (
                PARTITION BY symbol ORDER BY trading_date
                ROWS BETWEEN 62 PRECEDING AND CURRENT ROW
            ), 0
        ) AS range_percentile_63,
        (close - MIN(close) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        )) / NULLIF(
            MAX(close) OVER (
                PARTITION BY symbol ORDER BY trading_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) -
            MIN(close) OVER (
                PARTITION BY symbol ORDER BY trading_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ), 0
        ) AS range_percentile_252,
        -- Breakout counters (parameter-free 4% threshold)
        SUM(CASE WHEN ret_1d >= 0.04 THEN 1 ELSE 0 END) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) AS breakout_4pct_up_30d,
        SUM(CASE WHEN ret_1d >= 0.04 THEN 1 ELSE 0 END) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 89 PRECEDING AND 1 PRECEDING
        ) AS breakout_4pct_up_90d,
        SUM(CASE WHEN ret_1d <= -0.04 THEN 1 ELSE 0 END) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 89 PRECEDING AND 1 PRECEDING
        ) AS breakdown_4pct_down_90d,
        -- Linear regression R-squared for trend strength
        REGR_R2(close, rn) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 64 PRECEDING AND CURRENT ROW
        ) AS r2_65,
        -- ATR compression (volatility squeeze detection)
        atr_20 / NULLIF(AVG(atr_20) OVER (
            PARTITION BY symbol ORDER BY trading_date
            ROWS BETWEEN 49 PRECEDING AND 1 PRECEDING
        ), 0) AS atr_compress_ratio,
        -- Volume dryup ratio
        volume / NULLIF(vol_20, 0) AS vol_dryup_ratio,
        open,
        close
    FROM with_rownum
)
SELECT
    symbol,
    trading_date,
    ret_1d,
    ret_2d,
    ret_5d,
    ret_10d,
    ret_20d,
    ret_63d,
    ret_252d,
    atr_14,
    atr_20,
    range_pct,
    close_pos_in_range,
    gap_open_vs_prev_close,
    gap_high_vs_prev_close,
    gap_low_vs_prev_close,
    body_ratio,
    upper_wick_ratio,
    lower_wick_ratio,
    ma_7,
    ma_20,
    ma_50,
    ma_65,
    ma_65_sma,
    ma_200,
    rs_252,
    vol_20,
    dollar_vol_20,
    range_percentile_63,
    range_percentile_252,
    breakout_4pct_up_30d,
    breakout_4pct_up_90d,
    breakdown_4pct_down_90d,
    r2_65,
    atr_compress_ratio,
    vol_dryup_ratio,
    open,
    close
FROM advanced_features
WHERE close IS NOT NULL
"""


def build_feat_daily_core(
    con,  # DuckDBPyConnection
    force: bool = False,
    dataset_hash: str | None = None,
) -> int:
    """
    Build the feat_daily_core materialized table.

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
                "WHERE table_name = 'feat_daily_core'"
            ).fetchone()
            if row:
                _table_name, query_version, row_count = row
                if query_version == FEAT_DAILY_CORE_VERSION:
                    logger.info("feat_daily_core is up-to-date (%d rows).", row_count)
                    return int(row_count)
        except Exception:
            pass  # Table doesn't exist yet

    # Drop and rebuild
    logger.info("Building feat_daily_core materialized table...")
    con.execute("DROP TABLE IF EXISTS feat_daily_core")
    con.execute(FEAT_DAILY_CORE_SQL)

    # Create index for common queries
    con.execute(
        "CREATE INDEX idx_feat_daily_core_symbol_date ON feat_daily_core(symbol, trading_date)"
    )

    row = con.execute("SELECT COUNT(*) FROM feat_daily_core").fetchone()
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
        ["feat_daily_core", dataset_hash, FEAT_DAILY_CORE_VERSION, n],
    )

    logger.info("feat_daily_core built: %d rows", n)
    return n


def register_feat_daily_core(registry) -> None:
    """Register feat_daily_core with the feature registry."""

    registry.register(
        FeatureDefinition(
            name="feat_daily_core",
            version=FEAT_DAILY_CORE_VERSION,
            description="Core daily features: returns, volatility, trend, liquidity, gaps, candle structure. Strategy-agnostic.",
            granularity=FeatureGranularity.DAILY,
            layer="core",
            input_datasets=["v_daily"],
            feature_dependencies=[
                FeatureDependency(name="v_daily", is_dataset=True, required_lookback_days=252),
            ],
            required_lookback_days=252,
            build_sql=FEAT_DAILY_CORE_SQL,
            incremental_policy=IncrementalPolicy.ROLLING_WINDOW,
            partition_grain="year",
            output_columns=[
                "symbol",
                "trading_date",
                "ret_1d",
                "ret_2d",
                "ret_5d",
                "ret_10d",
                "ret_20d",
                "ret_63d",
                "ret_252d",
                "atr_14",
                "atr_20",
                "range_pct",
                "close_pos_in_range",
                "gap_open_vs_prev_close",
                "gap_high_vs_prev_close",
                "gap_low_vs_prev_close",
                "body_ratio",
                "upper_wick_ratio",
                "lower_wick_ratio",
                "ma_7",
                "ma_20",
                "ma_50",
                "ma_65",
                "ma_65_sma",
                "ma_200",
                "rs_252",
                "vol_20",
                "dollar_vol_20",
                "range_percentile_63",
                "range_percentile_252",
                "breakout_4pct_up_30d",
                "breakout_4pct_up_90d",
                "breakdown_4pct_down_90d",
                "r2_65",
                "atr_compress_ratio",
                "vol_dryup_ratio",
                "open",
                "close",
            ],
        )
    )
