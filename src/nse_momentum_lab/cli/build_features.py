#!/usr/bin/env python3
"""
Build feature materialized tables in DuckDB.

Usage:
    doppler run -- uv run nseml-build-features
    doppler run -- uv run nseml-build-features --force
"""

from __future__ import annotations

import argparse

from nse_momentum_lab.db.market_db import get_market_db


def main():
    parser = argparse.ArgumentParser(description="Build DuckDB feature tables")
    parser.add_argument("--force", action="store_true", help="Force rebuild")
    args = parser.parse_args()

    print("Building DuckDB feature tables...\n")
    db = get_market_db()
    db.build_feat_daily_table(force=args.force)

    print("\n" + "=" * 60)
    print("Status:")
    status = db.get_status()
    for key, value in status.items():
        if key == "tables":
            for t, cnt in value.items():
                print(f"  {t}: {cnt:,} rows")
        else:
            print(f"  {key}: {value}")
    print("=" * 60)


if __name__ == "__main__":
    main()
