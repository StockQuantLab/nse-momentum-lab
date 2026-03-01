"""Filter Analysis for 2LYNCH Strategy.

Analyzes which of the 6 filters contribute most to performance:
- Filter H: Close position in range >= 70%
- Filter N: Narrow range day (OR close < open)
- Filter Y: Prior breakouts <= 2 in 90 days
- Filter C: Volume dryup ratio < 1.3
- Filter L: Long-term setup (above MA20 OR positive 5d momentum OR R² >= 70%)

This helps identify redundant or weak filters that can be removed or modified.
"""

import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    VectorBTEngine,
    VectorBTConfig,
)


def get_top_n_liquid_symbols(db: get_market_db, n: int = 100) -> list[str]:
    """Get top N symbols by average traded value."""
    query = f"""
    SELECT symbol, AVG(close * volume) as avg_value_traded
    FROM v_daily
    WHERE date BETWEEN DATE '2018-01-01' AND DATE '2024-12-31'
      AND close >= 10
    GROUP BY symbol
    ORDER BY avg_value_traded DESC
    LIMIT {n}
    """
    result = db.con.execute(query).fetchdf()
    return result["symbol"].to_list()


def analyze_filters(
    db: get_market_db,
    symbols: list[str],
    start_year: int = 2015,
    end_year: int = 2024,
) -> dict:
    """Analyze filter performance across all years."""

    symbols_list_str = "', '".join(symbols)

    # Build query for all years
    query = f"""
    WITH numbered_daily AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
        FROM v_daily
        WHERE date BETWEEN DATE '{start_year}-01-01' AND DATE '{end_year}-12-31'
          AND symbol IN ('{symbols_list_str}')
    ),
    with_lag AS (
        SELECT
            symbol,
            date as trading_date,
            open,
            high,
            low,
            close,
            volume,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
            (open - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) /
                NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS gap_pct,
            close * volume AS value_traded_inr
        FROM numbered_daily
        WHERE rn > 1
    ),
    gap_ups AS (
        SELECT *
        FROM with_lag
        WHERE gap_pct >= 0.04
          AND prev_close IS NOT NULL
          AND close >= 10
          AND value_traded_inr >= 3000000
          AND volume >= 50000
    ),
    with_features AS (
        SELECT
            g.*,
            f.close_pos_in_range,
            f.ma_20,
            f.ret_5d,
            f.atr_20,
            f.vol_dryup_ratio,
            f.atr_compress_ratio,
            f.range_percentile,
            f.prior_breakouts_90d,
            f.r2_65
        FROM gap_ups g
        LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
    )
    SELECT
        symbol,
        trading_date,
        open,
        high,
        low,
        close,
        gap_pct,
        close_pos_in_range,
        (close > ma_20) AS above_ma20,
        (ret_5d > 0) AS positive_momentum,
        atr_20,
        vol_dryup_ratio,
        atr_compress_ratio,
        range_percentile,
        prior_breakouts_90d,
        (close_pos_in_range >= 0.70) AS filter_h,
        ((high - low) / NULLIF(close, 0) < (atr_20 * 0.5) OR close < open) AS filter_n,
        (prior_breakouts_90d <= 2) AS filter_y,
        (vol_dryup_ratio < 1.3) AS filter_c,
        (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER) +
            CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
        -- Also capture the individual components of filter L
        (close > ma_20) AS l_above_ma20,
        (ret_5d > 0) AS l_positive_mom,
        (COALESCE(NULLIF(r2_65, 0), 0) >= 0.70) AS l_r2_high
    FROM with_features
    WHERE close_pos_in_range IS NOT NULL
    ORDER BY trading_date, symbol
    """

    df = db.con.execute(query).fetchdf()
    if df.empty:
        return {}

    df_pl = pl.from_pandas(df)

    # Fill nulls
    df_pl = df_pl.with_columns([
        pl.col("filter_h").fill_null(False),
        pl.col("filter_n").fill_null(False),
        pl.col("filter_y").fill_null(True),
        pl.col("filter_c").fill_null(False),
        pl.col("filter_l").fill_null(False),
    ])

    # Count filters passed
    df_pl = df_pl.with_columns([
        (pl.col("filter_h").cast(int) +
         pl.col("filter_n").cast(int) +
         pl.col("filter_y").cast(int) +
         pl.col("filter_c").cast(int) +
         pl.col("filter_l").cast(int)).alias("filters_passed")
    ])

    return df_pl


def run_filter_analysis():
    """Run comprehensive filter analysis."""

    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    print("\n" + "=" * 80)
    print("2LYNCH FILTER ANALYSIS")
    print("=" * 80)

    db = get_market_db()

    # Get top 100 liquid symbols
    print("\nFetching top 100 liquid symbols...")
    top_symbols = get_top_n_liquid_symbols(db, n=100)
    print(f"Selected {len(top_symbols)} symbols")

    # Build features if needed
    print("\nEnsuring feat_daily is built...")
    db.build_feat_daily_table()

    # Analyze filters
    print("\nAnalyzing filter performance...")
    df = analyze_filters(db, top_symbols, 2015, 2024)

    if df is None or df.height == 0:
        print("No data found")
        return

    print(f"\nTotal gap-ups: {len(df)}")

    # Filter pass rates
    print(f"\n{'=' * 80}")
    print(f"FILTER PASS RATES (on all gap-ups)")
    print(f"{'=' * 80}")

    filters = ["filter_h", "filter_n", "filter_y", "filter_c", "filter_l"]
    filter_names = {
        "filter_h": "H - Close position >= 70%",
        "filter_n": "N - Narrow range (OR close < open)",
        "filter_y": "Y - Prior breakouts <= 2",
        "filter_c": "C - Volume dryup < 1.3",
        "filter_l": "L - Long-term setup (2/3 passed)",
    }

    for f in filters:
        passed = df[f].sum()
        total = len(df)
        pct = (passed / total * 100) if total > 0 else 0
        print(f"  {filter_names[f]:<40} {passed:>6} / {total:<6} ({pct:>5.1f}%)")

    # Filter L breakdown
    print(f"\nFilter L Breakdown:")
    for col, name in [("l_above_ma20", "Above MA20"), ("l_positive_mom", "Positive 5d momentum"), ("l_r2_high", "R² >= 70%")]:
        passed = df[col].fill_null(False).sum()
        total = len(df)
        pct = (passed / total * 100) if total > 0 else 0
        print(f"  {name:<40} {passed:>6} / {total:<6} ({pct:>5.1f}%)")

    # Correlation analysis
    print(f"\n{'=' * 80}")
    print(f"FILTER CORRELATION ANALYSIS")
    print(f"{'=' * 80}")

    # Calculate filter correlations
    filter_cols = ["filter_h", "filter_n", "filter_y", "filter_c", "filter_l"]
    corr_matrix = {}
    for f1 in filter_cols:
        corr_matrix[f1] = {}
        for f2 in filter_cols:
            if f1 == f2:
                corr_matrix[f1][f2] = 1.0
            else:
                # Calculate overlap (both filters passed)
                both = df.filter(pl.col(f1) & pl.col(f2)).height
                f1_count = df.filter(pl.col(f1)).height
                overlap_pct = (both / f1_count * 100) if f1_count > 0 else 0
                corr_matrix[f1][f2] = overlap_pct

    print(f"\nFilter Overlap (when row filter passes, % of time column filter also passes):")
    print(f"{'':<10}", end="")
    for f in filter_cols:
        print(f"{f:>8}", end="")
    print()

    for f1 in filter_cols:
        print(f"{f1:<10}", end="")
        for f2 in filter_cols:
            print(f"{corr_matrix[f1][f2]:>7.1f}%", end="")
        print()

    # Performance by filter count
    print(f"\n{'=' * 80}")
    print(f"PERFORMANCE BY FILTER COUNT")
    print(f"{'=' * 80}")

    for n in range(6):
        subset = df.filter(pl.col("filters_passed") == n)
        count = len(subset)
        if count == 0:
            continue
        pct = count / len(df) * 100
        print(f"  {n}/6 filters passed: {count:>6} signals ({pct:>5.1f}%)")

    # Filter combination analysis
    print(f"\n{'=' * 80}")
    print(f"TOP FILTER COMBINATIONS")
    print(f"{'=' * 80}")

    # Count unique filter combinations
    df_combo = df.with_columns([
        pl.format("H:{} N:{} Y:{} C:{} L:{}",
                  pl.col("filter_h").cast(str).str.replace("True", "1").str.replace("False", "0"),
                  pl.col("filter_n").cast(str).str.replace("True", "1").str.replace("False", "0"),
                  pl.col("filter_y").cast(str).str.replace("True", "1").str.replace("False", "0"),
                  pl.col("filter_c").cast(str).str.replace("True", "1").str.replace("False", "0"),
                  pl.col("filter_l").cast(str).str.replace("True", "1").str.replace("False", "0"),
                  ).alias("combination")
    ])

    combo_counts = df_combo.group_by("combination").agg(
        pl.len().alias("count")
    ).sort("count", descending=True).head(20)

    print(f"\nTop 20 Filter Combinations:")
    print(f"{'Combination (H N Y C L)':<25} {'Count':>8} {'%':>6}")
    for row in combo_counts.iter_rows(named=True):
        pct = row["count"] / len(df) * 100
        print(f"  {row['combination']:<25} {row['count']:>8} {pct:>5.1f}%")

    # Individual filter effectiveness
    print(f"\n{'=' * 80}")
    print(f"INDIVIDUAL FILTER EFFECTIVENESS")
    print(f"{'=' * 80}")

    print(f"\nFor each filter, what happens when it PASSES vs FAILS?")

    for f in filter_cols:
        passed = df.filter(pl.col(f))
        failed = df.filter(~pl.col(f))

        if len(passed) > 0 and len(failed) > 0:
            # Average filters passed in each group
            avg_filters_passed = passed["filters_passed"].mean()
            avg_filters_failed = failed["filters_passed"].mean()

            print(f"\n  {filter_names[f]}:")
            print(f"    When PASSES: avg {avg_filters_passed:.2f} filters total ({len(passed)} signals)")
            print(f"    When FAILS:  avg {avg_filters_failed:.2f} filters total ({len(failed)} signals)")

            # Check what other filters tend to pass/fail with this one
            for other in filter_cols:
                if other == f:
                    continue
                pass_with_pass = passed.filter(pl.col(other)).height / len(passed) * 100 if len(passed) > 0 else 0
                pass_with_fail = failed.filter(pl.col(other)).height / len(failed) * 100 if len(failed) > 0 else 0
                print(f"      {other}: {pass_with_pass:.1f}% pass | {pass_with_fail:.1f}% pass")

    # Filter redundancy analysis
    print(f"\n{'=' * 80}")
    print(f"FILTER REDUNDANCY ANALYSIS")
    print(f"{'=' * 80}")

    print(f"\nFilters with HIGH overlap (>80%) may be redundant:")
    redundant_pairs = []
    for f1 in filter_cols:
        for f2 in filter_cols:
            if f1 >= f2:
                continue
            # Check双向 overlap
            overlap_1to2 = corr_matrix[f1][f2]
            overlap_2to1 = corr_matrix[f2][f1]

            if overlap_1to2 > 80 and overlap_2to1 > 80:
                redundant_pairs.append((f1, f2, overlap_1to2, overlap_2to1))

    if redundant_pairs:
        print(f"\n  Potentially Redundant Filter Pairs:")
        for f1, f2, o1, o2 in redundant_pairs:
            print(f"    {f1} <-> {f2}: {o1:.1f}% / {o2:.1f}% overlap")
    else:
        print(f"\n  No highly redundant filter pairs found (>80% overlap)")

    # Recommendations
    print(f"\n{'=' * 80}")
    print(f"RECOMMENDATIONS")
    print(f"{'=' * 80}")

    # Find most selective filter (lowest pass rate)
    pass_rates = {f: (df[f].sum() / len(df) * 100) for f in filter_cols}
    most_selective = min(pass_rates, key=pass_rates.get)
    least_selective = max(pass_rates, key=pass_rates.get)

    print(f"\n  Most Selective Filter: {filter_names[most_selective]} ({pass_rates[most_selective]:.1f}% pass rate)")
    print(f"  Least Selective Filter: {filter_names[least_selective]} ({pass_rates[least_selective]:.1f}% pass rate)")

    print(f"\n  Considerations:")
    print(f"    - Low pass rate filters are 'gatekeepers' - they define signal quality")
    print(f"    - High pass rate filters may need tightening or removal")
    print(f"    - Filter combinations matter more than individual filters")

    print(f"\n{'=' * 80}")
    print(f"FILTER ANALYSIS COMPLETE")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    run_filter_analysis()
