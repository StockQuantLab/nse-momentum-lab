"""Batch compute R² for all symbols using efficient Python processing."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import numpy as np
from tqdm import tqdm

from nse_momentum_lab.db.market_db import get_market_db


def compute_r2_vectorized(prices: np.ndarray, window_size: int = 65) -> np.ndarray:
    """Compute R² for all points in a price series using vectorized operations.

    Args:
        prices: Array of price values
        window_size: Size of the rolling window

    Returns:
        Array of R² values (same length as prices)
    """
    n = len(prices)
    r2_values = np.zeros(n)

    for i in range(window_size - 1, n):
        # Get window
        window = prices[i - window_size + 1 : i + 1]

        # Remove NaN
        valid = ~np.isnan(window)
        if valid.sum() < 2:
            r2_values[i] = 0.0
            continue

        y = window[valid]
        x = np.arange(len(y))

        # Check variance
        if np.var(y) == 0:
            r2_values[i] = 0.0
            continue

        # Linear regression
        x_mean = np.mean(x)
        y_mean = np.mean(y)

        # Slope and intercept
        numerator = np.sum((x - x_mean) * (y - y_mean))
        denominator = np.sum((x - x_mean) ** 2)

        if denominator == 0:
            r2_values[i] = 0.0
            continue

        m = numerator / denominator
        b = y_mean - m * x_mean

        # R²
        y_pred = m * x + b
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y_mean) ** 2)

        if ss_tot == 0:
            r2_values[i] = 0.0
        else:
            r2 = 1 - (ss_res / ss_tot)
            r2_values[i] = max(0.0, min(1.0, r2))

    return r2_values


def main():
    """Batch compute R² for all symbols."""
    print("=" * 80)
    print("BATCH R² COMPUTATION")
    print("=" * 80)

    db = get_market_db()

    # Get all symbols
    symbols = db.con.execute("""
        SELECT DISTINCT symbol
        FROM feat_daily
        ORDER BY symbol
    """).fetchall()

    symbols = [s[0] for s in symbols]
    print(f"\nProcessing {len(symbols)} symbols...")

    # Process in batches
    batch_updates = []
    window_size = 65

    for symbol in tqdm(symbols, desc="Computing R²"):
        try:
            # Get price data
            result = db.con.execute(f"""
                SELECT date, close
                FROM v_daily
                WHERE symbol = '{symbol}'
                ORDER BY date ASC
            """).fetchall()

            if not result:
                continue

            dates = [r[0] for r in result]
            closes = np.array([float(r[1]) if r[1] else np.nan for r in result])

            # Compute R²
            r2_values = compute_r2_vectorized(closes, window_size)

            # Collect updates
            for date, r2_val in zip(dates, r2_values, strict=False):
                batch_updates.append((float(r2_val), symbol, date))

            # Batch update every 1000 rows
            if len(batch_updates) >= 10000:
                db.con.executemany(
                    "UPDATE feat_daily SET r2_65 = ? WHERE symbol = ? AND trading_date = ?",
                    batch_updates,
                )
                batch_updates = []

        except Exception as e:
            print(f"\nError processing {symbol}: {e}")

    # Final batch
    if batch_updates:
        db.con.executemany(
            "UPDATE feat_daily SET r2_65 = ? WHERE symbol = ? AND trading_date = ?", batch_updates
        )

    # Verify
    result = db.con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE r2_65 > 0) as positive,
            COUNT(*) FILTER (WHERE r2_65 >= 0.5) as medium,
            COUNT(*) FILTER (WHERE r2_65 >= 0.7) as high,
            AVG(r2_65) as avg_r2
        FROM feat_daily
    """).fetchone()

    print(f"\n{'=' * 80}")
    print("RESULTS:")
    print(f"  Total rows: {result[0]:,}")
    print(f"  Positive R² (> 0): {result[1]:,} ({result[1] / result[0] * 100:.1f}%)")
    print(f"  Medium trend (>= 0.5): {result[2]:,}")
    print(f"  High trend (>= 0.7): {result[3]:,}")
    print(f"  Average R²: {result[4]:.4f}")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
