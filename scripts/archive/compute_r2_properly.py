"""Compute R² properly for feat_daily table.

This script computes R² (R-squared) for 65-day linear regression
for every row in feat_daily, replacing the placeholder 0.0 values.

This processes ~3.5M rows across 1,832 symbols.
Estimated time: 15-20 minutes
"""

import sys
from pathlib import Path

import numpy as np

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db


def compute_r2_for_all_symbols():
    """Compute R² for all symbols and update feat_daily table."""
    print("\n" + "=" * 80)
    print("COMPUTING R² VALUES FOR feat_daily TABLE")
    print("=" * 80)

    db = get_market_db()

    # Check current state
    print("\n[CHECKING CURRENT STATE]")
    total_rows = db.con.execute("SELECT COUNT(*) FROM feat_daily").fetchone()[0]
    r2_zero_count = db.con.execute(
        "SELECT COUNT(*) FROM feat_daily WHERE r2_65 = 0.0 OR r2_65 IS NULL"
    ).fetchone()[0]
    r2_nonzero_count = db.con.execute("SELECT COUNT(*) FROM feat_daily WHERE r2_65 > 0").fetchone()[
        0
    ]

    print(f"  Total rows: {total_rows:,}")
    print(f"  R² = 0.0 or NULL: {r2_zero_count:,}")
    print(f"  R² > 0: {r2_nonzero_count:,}")

    if r2_nonzero_count > 1000:
        print("\n  R² appears to already be computed!")
        response = input("  Re-compute anyway? (y/N): ").strip().lower()
        if response != "y":
            print("  Aborted.")
            return

    # Get all symbols
    print("\n[LOADING SYMBOLS]")
    symbols_result = db.con.execute(
        "SELECT DISTINCT symbol FROM v_daily ORDER BY symbol"
    ).fetchall()
    all_symbols = [row[0] for row in symbols_result]
    print(f"  Total symbols: {len(all_symbols)}")

    # Process in batches
    batch_size = 50
    total_symbols = len(all_symbols)

    # Add a temporary column for tracking progress
    try:
        db.con.execute("ALTER TABLE feat_daily ADD COLUMN r2_computed BOOLEAN DEFAULT FALSE")
    except Exception:
        print("  Column r2_computed already exists, continuing...")

    # Process each symbol
    print("\n[COMPUTING R² VALUES]")
    print(f"  Processing {total_symbols} symbols in batches of {batch_size}...")
    print("  Estimated time: 15-20 minutes", flush=True)

    completed_symbols = []
    failed_symbols = []

    for batch_idx in range(0, total_symbols, batch_size):
        batch_symbols = all_symbols[batch_idx : batch_idx + batch_size]
        batch_num = batch_idx // batch_size + 1
        total_batches = (total_symbols + batch_size - 1) // batch_size

        print(
            f"\n  Batch {batch_num}/{total_batches} ({len(batch_symbols)} symbols)...", flush=True
        )

        for symbol in batch_symbols:
            try:
                # Load daily data for this symbol
                query = f"""
                SELECT date, close
                FROM v_daily
                WHERE symbol = '{symbol}'
                ORDER BY date
                """
                df_result = db.con.execute(query).fetchall()

                if not df_result:
                    continue

                dates = [row[0] for row in df_result]
                closes = np.array([row[1] for row in df_result], dtype=np.float64)

                n = len(closes)
                if n < 65:
                    # Not enough data for R² computation
                    # Mark rows as computed with NaN
                    for date_val in dates:
                        db.con.execute(f"""
                            UPDATE feat_daily
                            SET r2_65 = NULL, r2_computed = TRUE
                            WHERE symbol = '{symbol}' AND trading_date = '{date_val}'
                        """)
                    completed_symbols.append(symbol)
                    continue

                # Compute R² using vectorized approach
                r2_values = compute_r2_vectorized(closes, period=65)

                # Update feat_daily table with computed R² values
                for j, date_val in enumerate(dates):
                    r2_val = r2_values[j]
                    if not np.isnan(r2_val):
                        db.con.execute(f"""
                            UPDATE feat_daily
                            SET r2_65 = {r2_val}, r2_computed = TRUE
                            WHERE symbol = '{symbol}' AND trading_date = '{date_val}'
                        """)
                    else:
                        # Set to NULL if not computable
                        db.con.execute(f"""
                            UPDATE feat_daily
                            SET r2_65 = NULL, r2_computed = TRUE
                            WHERE symbol = '{symbol}' AND trading_date = '{date_val}'
                        """)

                completed_symbols.append(symbol)

            except Exception as e:
                print(f"    ERROR: {symbol} - {e}")
                failed_symbols.append((symbol, str(e)))
                continue

        # Progress update every batch
        print(f"    Completed: {len(completed_symbols)}/{total_symbols}", flush=True)

    # Final verification
    print("\n[VERIFICATION]")
    r2_zero_count = db.con.execute(
        "SELECT COUNT(*) FROM feat_daily WHERE r2_65 = 0.0 OR r2_65 IS NULL"
    ).fetchone()[0]
    r2_nonzero_count = db.con.execute("SELECT COUNT(*) FROM feat_daily WHERE r2_65 > 0").fetchone()[
        0
    ]
    r2_null_count = db.con.execute(
        "SELECT COUNT(*) FROM feat_daily WHERE r2_65 IS NULL"
    ).fetchone()[0]

    print(f"  R² = 0.0: {r2_zero_count:,}")
    print(f"  R² IS NULL: {r2_null_count:,}")
    print(f"  R² > 0: {r2_nonzero_count:,}")
    print(
        f"  Computed: {db.con.execute('SELECT COUNT(*) FROM feat_daily WHERE r2_computed = TRUE').fetchone()[0]:,}"
    )

    # Show sample R² values
    print("\n[SAMPLE R² VALUES]")
    print("  Top 10 highest R² (most linear):")
    sample = db.con.execute("""
        SELECT symbol, trading_date, r2_65
        FROM feat_daily
        WHERE r2_65 IS NOT NULL AND r2_65 > 0
        ORDER BY r2_65 DESC
        LIMIT 10
    """).fetchall()

    for i, row in enumerate(sample):
        print(f"    {i + 1}. {row[0]} @ {row[1]}: R² = {row[2]:.4f}")

    print("\n  Bottom 10 R² (least linear / most choppy):")
    sample = db.con.execute("""
        SELECT symbol, trading_date, r2_65
        FROM feat_daily
        WHERE r2_65 IS NOT NULL AND r2_65 > 0
        ORDER BY r2_65 ASC
        LIMIT 10
    """).fetchall()

    for i, row in enumerate(sample):
        print(f"    {i + 1}. {row[0]} @ {row[1]}: R² = {row[2]:.4f}")

    print("\n  [FAILED SYMBOLS]")
    if failed_symbols:
        print(f"    Failed: {len(failed_symbols)}")
        for symbol, error in failed_symbols[:5]:
            print(f"      - {symbol}: {error}")
    else:
        print("    All symbols processed successfully!")

    # Drop temporary column
    print("\n[CLEANUP]")
    db.con.execute("ALTER TABLE feat_daily DROP COLUMN IF EXISTS r2_computed")

    print(f"\n{'=' * 80}")
    print("R² COMPUTATION COMPLETE")
    print(f"{'=' * 80}")
    print(f"\n  {len(completed_symbols)} symbols processed successfully")
    print(f"  {r2_nonzero_count:,} R² values computed")
    print("\n  Ready for proper 2LYNCH filtering!")
    print(f"{'=' * 80}\n")


def compute_r2_vectorized(closes: np.ndarray, period: int = 65) -> np.ndarray:
    """
    Compute R-squared for linear trend over given period.

    R² measures how well the data fits a linear trend.
    R² = 1 - (SS_res / SS_tot)
    Where:
    - SS_res = sum of squared residuals (distance from trend line)
    - SS_tot = total sum of squares (variance from mean)

    Values closer to 1.0 indicate strong linear trend.
    Values closer to 0.0 indicate no trend (random/choppy).
    """
    n = len(closes)
    r2 = np.full(n, np.nan, dtype=np.float64)

    if n < period:
        return r2

    # For each position, compute R² over the last `period` closes
    for i in range(period - 1, n):
        window = closes[i - period + 1 : i + 1]

        try:
            # Simple linear regression: y = mx + b
            x = np.arange(period, dtype=np.float64)

            # Calculate sums
            n_points = period
            sum_x = np.sum(x)
            sum_y = np.sum(window)
            sum_xy = np.sum(x * window)
            sum_x2 = np.sum(x**2)
            np.sum(window**2)

            # Calculate slope and intercept
            denominator = n_points * sum_x2 - sum_x**2
            if abs(denominator) < 1e-10:
                r2[i] = 0.0
                continue

            slope = (n_points * sum_xy - sum_x * sum_y) / denominator
            intercept = (sum_y - slope * sum_x) / n_points

            # Calculate R²
            y_mean = np.mean(window)
            ss_tot = np.sum((window - y_mean) ** 2)
            ss_res = np.sum((window - (slope * x + intercept)) ** 2)

            if abs(ss_tot) < 1e-10:
                r2[i] = 0.0
            else:
                r2[i] = 1.0 - (ss_res / ss_tot)

        except Exception:
            r2[i] = 0.0

    return r2


if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    compute_r2_for_all_symbols()
