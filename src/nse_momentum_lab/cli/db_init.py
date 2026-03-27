#!/usr/bin/env python3
"""
Initialize databases for NSE Momentum Lab.

- PostgreSQL: Creates tables via init SQL scripts
- DuckDB: Builds materialized tables from Parquet data

Usage:
    doppler run -- uv run nseml-db-init
    doppler run -- uv run nseml-db-init --duckdb-only
    doppler run -- uv run nseml-db-init --duckdb-only --force --allow-full-rebuild
    doppler run -- uv run nseml-db-init --postgres-only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def init_postgres() -> bool:
    """Initialize PostgreSQL tables via Docker init scripts."""
    import psycopg

    from nse_momentum_lab.config import get_settings

    settings = get_settings()
    db_url = str(settings.database_url)

    print("=" * 60)
    print("PostgreSQL Initialization")
    print("=" * 60)

    init_dir = PROJECT_ROOT / "db" / "init"
    sql_files = sorted(init_dir.glob("*.sql"))

    if not sql_files:
        print("[WARN] No SQL files found in db/init/")
        return False

    try:
        with psycopg.connect(db_url) as conn:
            for sql_file in sql_files:
                print(f"\nRunning: {sql_file.name}")
                sql = sql_file.read_text(encoding="utf-8")
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
                print(f"  [OK] {sql_file.name} executed successfully")

        print("\n[OK] PostgreSQL initialization complete")
        return True
    except Exception as e:
        print(f"\n[FAIL] PostgreSQL initialization failed: {e}")
        return False


def init_duckdb(force: bool = False) -> bool:
    """Initialize DuckDB materialized tables from Parquet data."""
    print("\n" + "=" * 60)
    print("DuckDB Initialization")
    print("=" * 60)

    from nse_momentum_lab.db.market_db import get_market_db

    try:
        db = get_market_db()
        status = db.get_status()

        print("\nParquet data:")
        print(
            "  [OK] 5-min files" if status.get("parquet_5min") else "  [WARN] 5-min files not found"
        )
        print(
            "  [OK] Daily files"
            if status.get("parquet_daily")
            else "  [FAIL] Daily files not found"
        )

        if not status.get("parquet_daily"):
            print("\n[FAIL] No daily Parquet data found. Run data conversion first.")
            print("  See: docs/IMPLEMENTATION_GUIDE_DUCKDB.md")
            return False

        print("\nBuilding feature tables...")
        n = db.build_feat_daily_table(force=force)
        print(f"  [OK] feat_daily: {n:,} rows")

        print("\n[OK] DuckDB initialization complete")
        return True
    except Exception as e:
        print(f"\n[FAIL] DuckDB initialization failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Initialize NSE Momentum Lab databases")
    parser.add_argument("--postgres-only", action="store_true", help="Only initialize PostgreSQL")
    parser.add_argument("--duckdb-only", action="store_true", help="Only initialize DuckDB")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild DuckDB tables (requires --allow-full-rebuild)",
    )
    parser.add_argument(
        "--allow-full-rebuild",
        action="store_true",
        help="Acknowledge a destructive full DuckDB rebuild when used with --force.",
    )
    args = parser.parse_args()

    if args.force and not args.allow_full_rebuild and not args.postgres_only:
        parser.error(
            "nseml-db-init --duckdb-only --force is destructive and expensive. "
            "Use nseml-db-init --duckdb-only without --force for normal rebuilds. "
            "If you intentionally want the full rebuild, add --allow-full-rebuild."
        )

    success = True

    if not args.duckdb_only:
        if not init_postgres():
            success = False

    if not args.postgres_only:
        if not init_duckdb(force=args.force):
            success = False

    print("\n" + "=" * 60)
    if success:
        print("[OK] Database initialization complete")
        print("\nNext steps:")
        print("  1. Run: doppler run -- uv run nseml-db-verify")
        print("  2. Start API: doppler run -- uv run nseml-api")
    else:
        print("[FAIL] Database initialization had errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
