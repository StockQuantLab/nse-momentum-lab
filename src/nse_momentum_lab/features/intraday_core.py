"""
Intraday Core Features for NSE Momentum Lab.

feat_intraday_core contains strategy-agnostic intraday features for
strategies that use 5-minute entry timing:

- Opening range high/low (first 15/30/60 minutes)
- First trigger time (first time price crosses a threshold)
- First break of prior high/low
- Intraday volume percentile
- Intraday range expansion
- First-hour high/low
- Trigger-to-stop distance calculations
- Entry cutoff window markers

These features use v_5min and are designed for strategies that need
precise intraday entry timing (like FEE - Find and Enter Early).
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

# Version for feat_intraday_core - bump when SQL logic changes
FEAT_INTRADAY_CORE_VERSION = "feat_intraday_core_v1_2026_03_06"


# SQL for building feat_intraday_core
FEAT_INTRADAY_CORE_SQL = """
CREATE TABLE feat_intraday_core AS
WITH base AS (
    SELECT
        symbol,
        date AS trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        close * volume AS dollar_vol,
        -- Row number per day for intraday calculations
        ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY candle_time) AS rn_5min,
        -- Count of 5-min candles per day (for robustness checks)
        COUNT(*) OVER (PARTITION BY symbol, date) AS candles_per_day
    FROM v_5min
    WHERE volume IS NOT NULL
),
with_prior_day AS (
    SELECT
        b.*,
        -- Get prior day's high/low for breakout detection
        ld.high AS prior_day_high,
        ld.low AS prior_day_low,
        ld.close AS prior_day_close
    FROM base b
    LEFT JOIN (
        SELECT
            symbol,
            date,
            high,
            low,
            close,
            LEAD(date) OVER (PARTITION BY symbol ORDER BY date) AS next_date
        FROM v_daily
    ) ld ON b.symbol = ld.symbol AND b.trading_date = ld.next_date
),
opening_ranges AS (
    SELECT
        symbol,
        trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        dollar_vol,
        prior_day_high,
        prior_day_low,
        prior_day_close,
        rn_5min,
        candles_per_day,
        -- Opening range metrics
        MAX(high) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) FILTER (WHERE rn_5min <= 3) AS or_15min_high,  -- First 15 min (3 candles)
        MAX(high) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) FILTER (WHERE rn_5min <= 6) AS or_30min_high,  -- First 30 min (6 candles)
        MAX(high) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) FILTER (WHERE rn_5min <= 12) AS or_60min_high,  -- First 60 min (12 candles)
        MIN(low) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) FILTER (WHERE rn_5min <= 3) AS or_15min_low,
        MIN(low) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) FILTER (WHERE rn_5min <= 6) AS or_30min_low,
        MIN(low) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) FILTER (WHERE rn_5min <= 12) AS or_60min_low
    FROM with_prior_day
),
first_hour AS (
    SELECT
        symbol,
        trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        dollar_vol,
        prior_day_high,
        prior_day_low,
        prior_day_close,
        rn_5min,
        candles_per_day,
        or_15min_high,
        or_30min_high,
        or_60min_high,
        or_15min_low,
        or_30min_low,
        or_60min_low,
        -- First hour metrics
        MAX(high) OVER (
            PARTITION BY symbol, trading_date
        ) FILTER (WHERE rn_5min <= 12) AS first_hour_high,
        MIN(low) OVER (
            PARTITION BY symbol, trading_date
        ) FILTER (WHERE rn_5min <= 12) AS first_hour_low,
        AVG(volume) OVER (
            PARTITION BY symbol, trading_date
        ) FILTER (WHERE rn_5min <= 12) AS first_hour_avg_vol
    FROM opening_ranges
),
breakout_detection AS (
    SELECT
        symbol,
        trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        dollar_vol,
        prior_day_high,
        prior_day_low,
        prior_day_close,
        rn_5min,
        candles_per_day,
        or_15min_high,
        or_30min_high,
        or_60min_high,
        or_15min_low,
        or_30min_low,
        or_60min_low,
        first_hour_high,
        first_hour_low,
        first_hour_avg_vol,
        -- First breakout of prior high (time when it happened)
        CASE
            WHEN prior_day_high IS NOT NULL AND high > prior_day_high THEN
                MIN(candle_time) OVER (
                    PARTITION BY symbol, trading_date
                    ORDER BY candle_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) FILTER (WHERE high > prior_day_high)
            ELSE NULL
        END AS first_breakout_time,
        -- First breakdown of prior low
        CASE
            WHEN prior_day_low IS NOT NULL AND low < prior_day_low THEN
                MIN(candle_time) OVER (
                    PARTITION BY symbol, trading_date
                    ORDER BY candle_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) FILTER (WHERE low < prior_day_low)
            ELSE NULL
        END AS first_breakdown_time,
        -- Gap at open
        CASE
            WHEN prior_day_close IS NOT NULL THEN
                (open - prior_day_close) / NULLIF(prior_day_close, 0)
            ELSE NULL
        END AS gap_open_pct,
        -- Intraday high vs opening range
        CASE
            WHEN or_15min_high IS NOT NULL THEN
                (high - or_15min_high) / NULLIF(or_15min_high, 0)
            ELSE NULL
        END AS range_expansion_vs_15min,
        -- Intraday low vs opening range
        CASE
            WHEN or_15min_low IS NOT NULL THEN
                (low - or_15min_low) / NULLIF(or_15min_low, 0)
            ELSE NULL
        END AS range_contraction_vs_15min
    FROM first_hour
),
volume_metrics AS (
    SELECT
        *,
        -- Cumulative volume by time of day
        SUM(volume) OVER (
            PARTITION BY symbol, trading_date
            ORDER BY candle_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_volume,
        -- Daily total volume (for percentile calc)
        SUM(volume) OVER (PARTITION BY symbol, trading_date) AS daily_total_volume
    FROM breakout_detection
),
final AS (
    SELECT
        symbol,
        trading_date,
        candle_time,
        open,
        high,
        low,
        close,
        volume,
        dollar_vol,
        rn_5min,
        candles_per_day,
        -- Opening ranges
        or_15min_high,
        or_15min_low,
        or_30min_high,
        or_30min_low,
        or_60min_high,
        or_60min_low,
        -- First hour
        first_hour_high,
        first_hour_low,
        first_hour_avg_vol,
        -- Breakout/breakdown times
        first_breakout_time,
        first_breakdown_time,
        gap_open_pct,
        -- Range expansion
        range_expansion_vs_15min,
        range_contraction_vs_15min,
        -- Volume percentile (how much of day's volume has traded by this candle)
        CASE
            WHEN daily_total_volume > 0 THEN
                cumulative_volume / NULLIF(daily_total_volume, 0)
            ELSE NULL
        END AS volume_percentile,
        -- Entry window flags (for FEE strategy)
        CASE
            -- NSE opens at 09:15 IST
            -- First 30 min = 09:15 to 09:45 (candles 1-6)
            WHEN rn_5min <= 6 THEN 'first_30min'
            -- First 60 min = 09:15 to 10:15 (candles 1-12)
            WHEN rn_5min <= 12 THEN 'first_60min'
            -- First 90 min = 09:15 to 10:45 (candles 1-18)
            WHEN rn_5min <= 18 THEN 'first_90min'
            ELSE 'after_90min'
        END AS entry_window
    FROM volume_metrics
    WHERE close IS NOT NULL
)
SELECT * FROM final
"""


def build_feat_intraday_core(
    con,  # DuckDBPyConnection
    force: bool = False,
    dataset_hash: str | None = None,
) -> int:
    """
    Build the feat_intraday_core materialized table.

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
                "WHERE table_name = 'feat_intraday_core'"
            ).fetchone()
            if row:
                _table_name, query_version, row_count = row
                if query_version == FEAT_INTRADAY_CORE_VERSION:
                    logger.info("feat_intraday_core is up-to-date (%d rows).", row_count)
                    return int(row_count)
        except Exception:
            pass  # Table doesn't exist yet

    # Check if v_5min exists
    try:
        con.execute("SELECT 1 FROM v_5min LIMIT 1").fetchone()
    except Exception:
        logger.warning("v_5min view not available, skipping feat_intraday_core")
        return 0

    # Drop and rebuild
    logger.info("Building feat_intraday_core materialized table...")
    con.execute("DROP TABLE IF EXISTS feat_intraday_core")
    con.execute(FEAT_INTRADAY_CORE_SQL)

    # Create index for common queries
    con.execute(
        "CREATE INDEX idx_feat_intraday_core_symbol_date ON feat_intraday_core(symbol, trading_date)"
    )
    con.execute(
        "CREATE INDEX idx_feat_intraday_core_date_time ON feat_intraday_core(trading_date, candle_time)"
    )

    row = con.execute("SELECT COUNT(*) FROM feat_intraday_core").fetchone()
    n = int(row[0]) if row and row[0] is not None else 0

    # Update materialization state
    if dataset_hash is None:
        # Generate a simple hash from v_5min
        snapshot_row = con.execute("""
            SELECT
                COUNT(*)::BIGINT AS rows,
                COUNT(DISTINCT symbol)::BIGINT AS symbols,
                MIN(date)::VARCHAR AS min_date,
                MAX(date)::VARCHAR AS max_date
            FROM v_5min
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
        ["feat_intraday_core", dataset_hash, FEAT_INTRADAY_CORE_VERSION, n],
    )

    logger.info("feat_intraday_core built: %d rows", n)
    return n


def register_feat_intraday_core(registry) -> None:
    """Register feat_intraday_core with the feature registry."""

    registry.register(
        FeatureDefinition(
            name="feat_intraday_core",
            version=FEAT_INTRADAY_CORE_VERSION,
            description="Core intraday features: opening ranges, breakout/breakdown times, volume metrics, entry windows. For FEE strategies.",
            granularity=FeatureGranularity.FIVE_MIN,
            layer="core",
            input_datasets=["v_5min", "v_daily"],
            feature_dependencies=[
                FeatureDependency(name="v_5min", is_dataset=True, required_lookback_days=1),
                FeatureDependency(name="v_daily", is_dataset=True, required_lookback_days=1),
            ],
            required_lookback_days=1,
            build_sql=FEAT_INTRADAY_CORE_SQL,
            incremental_policy=IncrementalPolicy.ROLLING_WINDOW,
            partition_grain="year",
            output_columns=[
                "symbol",
                "trading_date",
                "candle_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "dollar_vol",
                "rn_5min",
                "candles_per_day",
                "or_15min_high",
                "or_15min_low",
                "or_30min_high",
                "or_30min_low",
                "or_60min_high",
                "or_60min_low",
                "first_hour_high",
                "first_hour_low",
                "first_hour_avg_vol",
                "first_breakout_time",
                "first_breakdown_time",
                "gap_open_pct",
                "range_expansion_vs_15min",
                "range_contraction_vs_15min",
                "volume_percentile",
                "entry_window",
            ],
        )
    )
