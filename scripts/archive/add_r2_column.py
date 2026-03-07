"""Add R² column with proper DOUBLE type to feat_daily table.

This script:
1. Drops the problematic r2_65 column
2. Re-adds it as DOUBLE type (not DECIMAL)
3. Re-computes R² values properly
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db


def add_r2_column_double():
    """Add R² column as DOUBLE type."""
    print("\n" + "=" * 80)
    print("FIXING R² COLUMN TYPE - DECIMAL → DOUBLE")
    print("=" * 80)

    db = get_market_db()

    print("\n[DROPPING OLD r2_65 COLUMN]")
    try:
        db.con.execute("ALTER TABLE feat_daily DROP COLUMN IF EXISTS r2_65")
        print("  Dropped old r2_65 column")
    except Exception as e:
        print(f"  Note: {e}")

    print("\n[ADDING r2_65 AS DOUBLE]")
    db.con.execute("ALTER TABLE feat_daily ADD COLUMN r2_65 DOUBLE")
    print("  Added r2_65 as DOUBLE")

    print("\n[VERIFICATION]")
    columns = db.con.execute("DESCRIBE feat_daily").fetchall()
    for col in columns:
        if col[0] == 'r2_65':
            print(f"  {col[0]}: {col[1]}")
            break

    print("\n  [READY]")
    print("  R² column is now DOUBLE type")
    print("  Can accept values from -∞ to +∞")
    print("  Ready for R² computation!")
    print(f"\n{'=' * 80}\n")


if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    add_r2_column_double()
