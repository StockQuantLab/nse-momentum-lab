"""Rebuild feat_daily table with 2LYNCH filter features.

This script adds the new 2LYNCH features to the feat_daily table:
- r2_65: R-squared of 65-day linear trend
- atr_compress_ratio: Current ATR / 50-day avg ATR
- range_percentile: Price position in 252-day range
- vol_dryup_ratio: Recent volume / 20-day avg volume
- prior_breakouts_90d: Count of 4%+ gaps in last 90 days

Usage:
    python scripts/rebuild_features_with_2lynch.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db


def rebuild_features():
    """Rebuild feat_daily table with 2LYNCH features."""
    print("\n" + "=" * 80)
    print("REBUILDING feat_daily WITH 2LYNCH FILTERS")
    print("=" * 80)

    db = get_market_db()

    # Check current state
    print("\n[CHECKING CURRENT STATE]")
    if db._table_exists("feat_daily"):
        n = db.con.execute("SELECT COUNT(*) FROM feat_daily").fetchone()[0]
        print(f"  Current feat_daily: {n:,} rows")

        columns = db.con.execute("DESCRIBE feat_daily").fetchall()
        col_names = [c[0] for c in columns]
        has_2lynch = all(
            col in col_names
            for col in [
                "r2_65",
                "atr_compress_ratio",
                "range_percentile",
                "vol_dryup_ratio",
                "prior_breakouts_90d",
            ]
        )

        if has_2lynch:
            print("  Status: 2LYNCH features already exist")
            response = input("\n  Rebuild anyway? (y/N): ").strip().lower()
            if response != "y":
                print("  Aborted.")
                return
        else:
            print("  Status: Missing 2LYNCH features")
            print(f"  Current columns: {', '.join(col_names)}")
    else:
        print("  Status: feat_daily table does not exist")

    # Rebuild with force=True
    print("\n[REBUILDING]")
    print("  This will take several minutes...")
    print("  Processing 1,832 symbols × 10 years of data...", flush=True)

    try:
        n = db.build_feat_daily_table(force=True)
        print(f"\n  [SUCCESS] Built {n:,} rows with 2LYNCH features")

        # Verify new columns
        columns = db.con.execute("DESCRIBE feat_daily").fetchall()
        col_names = [c[0] for c in columns]
        has_2lynch = all(
            col in col_names
            for col in [
                "r2_65",
                "atr_compress_ratio",
                "range_percentile",
                "vol_dryup_ratio",
                "prior_breakouts_90d",
            ]
        )

        print("\n[VERIFICATION]")
        print(f"  Total columns: {len(col_names)}")
        print(f"  2LYNCH features present: {has_2lynch}")

        if has_2lynch:
            print("\n  [2LYNCH FEATURES]")
            for col in [
                "r2_65",
                "atr_compress_ratio",
                "range_percentile",
                "vol_dryup_ratio",
                "prior_breakouts_90d",
            ]:
                if col in col_names:
                    # Check sample data
                    sample = db.con.execute(
                        f"SELECT {col} FROM feat_daily WHERE {col} IS NOT NULL LIMIT 1"
                    ).fetchone()
                    print(f"    {col}: OK (sample: {sample[0] if sample else 'NULL'})")

        # Sample data
        print("\n[SAMPLE DATA]")
        sample_rows = db.con.execute("""
            SELECT symbol, trading_date, r2_65, atr_compress_ratio, range_percentile,
                   vol_dryup_ratio, prior_breakouts_90d
            FROM feat_daily
            WHERE r2_65 IS NOT NULL
            ORDER BY trading_date DESC
            LIMIT 5
        """).fetchall()

        for i, row in enumerate(sample_rows):
            print(f"  {i + 1}. {row[0]} @ {row[1]}")
            print(f"     R²: {row[2]:.3f} | ATR Ratio: {row[3]:.2f} | Range Pct: {row[4]:.2f}")
            print(f"     Vol Ratio: {row[5]:.2f} | Prior Breakouts: {row[6]}")

        print(f"\n{'=' * 80}")
        print("2LYNCH FEATURES SUCCESSFULLY ADDED")
        print(f"{'=' * 80}\n")

    except Exception as e:
        print(f"\n  [ERROR] {e}")
        import traceback

        traceback.print_exc()
        print(f"\n{'=' * 80}")
        print("REBUILD FAILED")
        print(f"{'=' * 80}\n")


if __name__ == "__main__":
    rebuild_features()
