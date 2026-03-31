"""Compute PROPER R² using true linear regression.

This script:
1. Reads price data from Parquet files
2. Computes R² using proper linear regression for each 65-day window
3. Writes results back to the database

R² = 1 - (SS_res / SS_tot)
where:
- SS_res = Σ(y - ŷ)² (residual sum of squares)
- SS_tot = Σ(y - ȳ)² (total sum of squares)
- ŷ = mx + b (predicted value from linear regression)
"""

import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import numpy as np
import polars as pl
from tqdm import tqdm

from nse_momentum_lab.db.market_db import get_market_db


def compute_linear_rise2(prices: np.ndarray, window_size: int = 65) -> np.ndarray:
    """Compute proper linear regression R² for a price series.

    This implements the true statistical formula:
    R² = 1 - (SS_res / SS_tot)

    Args:
        prices: Array of price values
        window_size: Size of the rolling window (default 65 for 2LYNCH)

    Returns:
        Array of R² values (same length as prices)
    """
    n = len(prices)
    r2_values = np.zeros(n)

    # For each position in the series
    for i in range(window_size - 1, n):
        # Get window of prices
        window = prices[i - window_size + 1 : i + 1]

        # Remove NaN values
        valid_mask = ~np.isnan(window)
        if valid_mask.sum() < 10:  # Need at least 10 valid points
            r2_values[i] = 0.0
            continue

        y = window[valid_mask]
        x = np.arange(len(y))

        # Calculate means
        x_mean = np.mean(x)
        y_mean = np.mean(y)

        # Check for zero variance
        ss_tot = np.sum((y - y_mean) ** 2)
        if ss_tot == 0:
            r2_values[i] = 0.0
            continue

        # Calculate slope (m) and intercept (b) using least squares
        # m = Σ((x - x̄)(y - ȳ)) / Σ((x - x̄)²)
        numerator = np.sum((x - x_mean) * (y - y_mean))
        denominator = np.sum((x - x_mean) ** 2)

        if denominator == 0:
            r2_values[i] = 0.0
            continue

        m = numerator / denominator
        b = y_mean - m * x_mean

        # Calculate predicted values
        y_pred = m * x + b

        # Calculate residual sum of squares
        ss_res = np.sum((y - y_pred) ** 2)

        # Calculate R²
        r2 = 1 - (ss_res / ss_tot)

        # Clamp to [0, 1] (can be negative if model is worse than horizontal line)
        r2_values[i] = max(0.0, min(1.0, r2))

    return r2_values


def compute_rise2_for_symbol_polars(symbol: str, db, window_size: int = 65) -> pl.DataFrame:
    """Compute R² for a single symbol using Polars.

    Returns a DataFrame with trading_date and r2_65 columns.
    """
    # Load price data from v_daily view (which reads from Parquet)
    query = f"""
    SELECT date, close
    FROM v_daily
    WHERE symbol = '{symbol}'
    ORDER BY date ASC
    """

    result = db.con.execute(query).fetchall()

    if not result:
        return pl.DataFrame()

    # Convert to numpy for computation
    dates = [r[0] for r in result]
    closes = np.array([float(r[1]) if r[1] is not None else np.nan for r in result])

    # Compute R²
    r2_values = compute_linear_rise2(closes, window_size)

    # Return as Polars DataFrame
    return pl.DataFrame(
        {"symbol": [symbol] * len(dates), "trading_date": dates, "r2_65": r2_values}
    )


def main():
    """Compute proper R² for all symbols."""
    print("=" * 80)
    print("PROPER LINEAR REGRESSION R² COMPUTATION")
    print("Using Polars + NumPy for true statistical R²")
    print("=" * 80)

    db = get_market_db()

    # Get all symbols
    symbols_result = db.con.execute("""
        SELECT DISTINCT symbol
        FROM feat_daily
        ORDER BY symbol
    """).fetchall()

    symbols = [s[0] for s in symbols_result]

    print(f"\nProcessing {len(symbols)} symbols...")
    print("Window size: 65 days (2LYNCH standard)")
    print("Estimated time: ~15-20 minutes\n")

    # Process symbols and collect results
    all_results = []

    for symbol in tqdm(symbols, desc="Computing R²"):
        try:
            df = compute_rise2_for_symbol_polars(symbol, db)
            if not df.is_empty():
                all_results.append(df)
        except Exception as e:
            print(f"\nError processing {symbol}: {e}")

    # Combine all results
    print("\nCombining results...")
    combined_df = pl.concat(all_results)

    print(f"Total rows computed: {len(combined_df):,}")

    # Create a temporary table in DuckDB
    print("Updating database...")
    db.con.execute("CREATE OR REPLACE TEMP TABLE r2_updates AS SELECT * FROM combined_df")

    # Update feat_daily
    db.con.execute("""
        UPDATE feat_daily f
        SET r2_65 = r.r2_65
        FROM r2_updates r
        WHERE f.symbol = r.symbol
        AND f.trading_date = r.trading_date
    """)

    # Cleanup
    db.con.execute("DROP TABLE r2_updates")

    # Verify results
    result = db.con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE r2_65 > 0) as positive,
            COUNT(*) FILTER (WHERE r2_65 >= 0.5) as medium,
            COUNT(*) FILTER (WHERE r2_65 >= 0.7) as high,
            COUNT(*) FILTER (WHERE r2_65 >= 0.8) as very_high,
            COUNT(*) FILTER (WHERE r2_65 >= 0.9) as excellent,
            MIN(r2_65) as min_r2,
            MAX(r2_65) as max_r2,
            AVG(r2_65) as avg_r2
        FROM feat_daily
    """).fetchone()

    print(f"\n{'=' * 80}")
    print("RESULTS:")
    print(f"  Total rows: {result[0]:,}")
    print(f"  Positive R² (> 0): {result[1]:,} ({result[1] / result[0] * 100:.1f}%)")
    print(f"  Medium trend (>= 0.5): {result[2]:,}")
    print(f"  High trend (>= 0.7): {result[3]:,}")
    print(f"  Very high trend (>= 0.8): {result[4]:,}")
    print(f"  Excellent trend (>= 0.9): {result[5]:,}")
    print(f"  Min R²: {result[6]:.4f}")
    print(f"  Max R²: {result[7]:.4f}")
    print(f"  Average R²: {result[8]:.4f}")
    print(f"{'=' * 80}")

    # Check backtest period specifically
    backtest_result = db.con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE r2_65 >= 0.7) as high_trend,
            AVG(r2_65) as avg_r2
        FROM feat_daily
        WHERE trading_date >= '2020-01-01' AND trading_date <= '2024-12-31'
    """).fetchone()

    print("\nBACKTEST PERIOD (2020-2024):")
    print(f"  Total rows: {backtest_result[0]:,}")
    print(
        f"  High trend (>= 0.7): {backtest_result[1]:,} ({backtest_result[1] / backtest_result[0] * 100:.1f}%)"
    )
    print(f"  Average R²: {backtest_result[2]:.4f}")


if __name__ == "__main__":
    start_time = datetime.now()
    main()
    elapsed = datetime.now() - start_time
    print(f"\nTotal time: {elapsed}")
