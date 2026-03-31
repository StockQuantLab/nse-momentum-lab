"""Fix R² computation for all rows.

The previous compute_r2 script only updated rows where R² could be computed.
This script ensures ALL rows get proper R² values, using available data.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import numpy as np

from nse_momentum_lab.db.market_db import get_market_db


def compute_r2_for_window(y_values):
    """Compute R² for a window of price values.

    Uses linear regression to find the best fit line.
    R² = 1 - (SS_res / SS_tot)

    Returns 0.0 if insufficient data or if variance is zero.
    """
    n = len(y_values)

    if n < 2:
        return 0.0

    x = np.arange(n)
    y = np.array(y_values, dtype=float)

    # Remove NaN values
    valid_mask = ~np.isnan(y)
    if valid_mask.sum() < 2:
        return 0.0

    x = x[valid_mask]
    y = y[valid_mask]
    n = len(x)

    # Check for zero variance
    if np.var(y) == 0:
        return 0.0

    # Linear regression: y = mx + b
    # Using least squares
    x_mean = np.mean(x)
    y_mean = np.mean(y)

    # Compute slope (m) and intercept (b)
    numerator = np.sum((x - x_mean) * (y - y_mean))
    denominator = np.sum((x - x_mean) ** 2)

    if denominator == 0:
        return 0.0

    m = numerator / denominator
    b = y_mean - m * x_mean

    # Calculate R²
    y_pred = m * x + b
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)

    if ss_tot == 0:
        return 0.0

    r2 = 1 - (ss_res / ss_tot)

    # Clamp R² to [0, 1] range (can go negative if fit is worse than horizontal line)
    return max(0.0, min(1.0, r2))


def fix_r2_for_symbol(db, symbol: str) -> int:
    """Fix R² values for a single symbol.

    Returns the number of rows updated.
    """
    # Get all daily data for this symbol (use v_daily view)
    query = f"""
    SELECT date, close
    FROM v_daily
    WHERE symbol = '{symbol}'
    ORDER BY date ASC
    """

    result = db.con.execute(query).fetchall()

    if not result:
        return 0

    dates = [row[0] for row in result]  # row[0] is 'date' from v_daily
    closes = [float(row[1]) if row[1] is not None else np.nan for row in result]

    # Compute R² for each row
    r2_values = []
    window_size = 65

    for i in range(len(dates)):
        # Get window of closes ending at this index
        start_idx = max(0, i - window_size + 1)
        window = closes[start_idx : i + 1]

        r2 = compute_r2_for_window(window)
        r2_values.append(r2)

    # Update feat_daily in batches
    updates = 0
    batch_size = 1000

    for i in range(0, len(dates), batch_size):
        batch_end = min(i + batch_size, len(dates))
        batch_dates = dates[i:batch_end]
        batch_r2 = r2_values[i:batch_end]

        for trading_date, r2_val in zip(batch_dates, batch_r2, strict=False):
            db.con.execute(
                """
                UPDATE feat_daily
                SET r2_65 = ?
                WHERE symbol = ? AND trading_date = ?
            """,
                [float(r2_val), symbol, trading_date],
            )
            updates += 1

    return updates


def main():
    """Fix R² values for all symbols."""
    print("=" * 80)
    print("FIXING R² VALUES FOR feat_daily TABLE")
    print("=" * 80)

    db = get_market_db()

    # Get all symbols
    symbols_result = db.con.execute("""
        SELECT DISTINCT symbol
        FROM feat_daily
        ORDER BY symbol
    """).fetchall()

    symbols = [row[0] for row in symbols_result]

    print(f"\nProcessing {len(symbols)} symbols...")
    print("This will take approximately 10-15 minutes...\n")

    total_updates = 0

    for i, symbol in enumerate(symbols):
        try:
            updates = fix_r2_for_symbol(db, symbol)
            total_updates += updates

            if (i + 1) % 50 == 0:
                print(f"  [{i + 1}/{len(symbols)}] {symbol}: {updates} rows updated")
        except Exception as e:
            print(f"  ERROR processing {symbol}: {e}")

    print(f"\n{'=' * 80}")
    print(f"COMPLETE! Total rows updated: {total_updates:,}")
    print(f"{'=' * 80}")

    # Verify results
    print("\nVerifying results...")

    verification = db.con.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE r2_65 > 0) as positive,
            COUNT(*) FILTER (WHERE r2_65 >= 0.7) as high_trend,
            AVG(r2_65) as avg_r2
        FROM feat_daily
    """).fetchone()

    print(f"  Total rows: {verification[0]:,}")
    print(f"  Positive R²: {verification[1]:,} ({verification[1] / verification[0] * 100:.1f}%)")
    print(f"  High trend (R² >= 0.7): {verification[2]:,}")
    print(f"  Average R²: {verification[3]:.4f}")


if __name__ == "__main__":
    main()
