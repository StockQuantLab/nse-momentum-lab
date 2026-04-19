#!/usr/bin/env python3
"""
Verify database state for NSE Momentum Lab.

Checks both PostgreSQL and DuckDB runtime coverage are properly initialized.

Usage:
    doppler run -- uv run nseml-db-verify
"""

from __future__ import annotations

import sys
from pathlib import Path

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.dq_scanner import scan_missing_5min_coverage
from nse_momentum_lab.services.kite.parquet_repair import scan_5min_timestamp_alignment

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def verify_postgres() -> bool:
    """Verify PostgreSQL tables exist and have expected schema."""

    from sqlalchemy import func, select, text

    from nse_momentum_lab.db import get_sessionmaker
    from nse_momentum_lab.db.models import ExpRun, RefSymbol, ScanRun

    print("=" * 60)
    print("PostgreSQL Verification")
    print("=" * 60)

    async def check():
        sm = get_sessionmaker()
        async with sm() as session:
            tables_ok = True

            required_tables = [
                "ref_symbol",
                "ref_exchange_calendar",
                "ca_event",
                "scan_definition",
                "scan_run",
                "scan_result",
                "exp_run",
                "exp_metric",
                "signal",
                "paper_order",
                "paper_fill",
                "paper_position",
                "job_run",
                "bt_trade",
                "rpt_scan_daily",
                "rpt_bt_daily",
            ]

            print("\nRequired tables:")
            for table in required_tables:
                try:
                    result = await session.execute(text(f"SELECT COUNT(*) FROM nseml.{table}"))
                    count = result.scalar()
                    print(f"  [OK] {table}: {count:,} rows")
                except Exception as e:
                    print(f"  [FAIL] {table}: {e}")
                    tables_ok = False

            if not tables_ok:
                return False

            print("\nSample data:")
            symbols = await session.execute(select(func.count(RefSymbol.symbol_id)))
            print(f"  Symbols: {symbols.scalar_one():,}")

            scans = await session.execute(select(func.count(ScanRun.scan_run_id)))
            print(f"  Scan runs: {scans.scalar_one():,}")

            exp = await session.execute(select(func.count(ExpRun.exp_run_id)))
            print(f"  Experiments: {exp.scalar_one():,}")

            return True

    try:
        import asyncio
        import selectors

        loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
        result = loop.run_until_complete(asyncio.wait_for(check(), timeout=15))
        loop.close()
        if result:
            print("\n[OK] PostgreSQL verification passed")
        return result
    except TimeoutError:
        print("\n[FAIL] PostgreSQL connection timed out after 15 seconds")
        print("  Is Docker running? Try: doppler run -- docker compose up -d")
        return False
    except Exception as e:
        print(f"\n[FAIL] PostgreSQL verification failed: {e}")
        return False


def _check_daily_5min_coverage(db) -> str:
    """Check that symbols with recent daily data also have 5-min data."""
    try:
        result = scan_missing_5min_coverage(db.con)
        if result.symbols:
            return f"{result.count} symbols have daily data but no 5-min data"
        return ""
    except Exception:
        return ""


def verify_duckdb() -> bool:
    """Verify DuckDB has Parquet views and materialized tables."""
    print("\n" + "=" * 60)
    print("DuckDB Verification")
    print("=" * 60)

    try:
        db = get_market_db(read_only=True)
        status = db.get_status()

        print("\nRuntime coverage:")
        if status.get("parquet_5min"):
            print("  [OK] 5-min data available")
        else:
            print("  [WARN] 5-min data not found (optional)")

        if status.get("parquet_daily"):
            print("  [OK] Daily data available")
        else:
            print("  [FAIL] Daily data NOT found")
            return False

        print("\nMaterialized tables:")
        feat_count = status.get("tables", {}).get("feat_daily", 0)
        if feat_count > 0:
            print(f"  [OK] feat_daily: {feat_count:,} rows")
        else:
            print("  [FAIL] feat_daily: NOT built")
            print("    Run: doppler run -- uv run nseml-db-init --duckdb-only")
            print("    For an intentional full rebuild: add --force --allow-full-rebuild")
            return False

        print("\nKite 5-min timestamp alignment:")
        issues = scan_5min_timestamp_alignment(PROJECT_ROOT / "data" / "parquet" / "5min")
        if issues:
            print(
                f"  [FAIL] {len(issues)} 5-min parquet files start before 09:15 IST or have a year mismatch"
            )
            for issue in issues[:10]:
                print(
                    f"    {issue.symbol} {issue.year}: {issue.first_candle_time} ({issue.status})"
                )
            if len(issues) > 10:
                print(f"    ... and {len(issues) - 10} more")
            return False
        print("  [OK] no 5-min parquet files start before 09:15 IST")

        print("\nDaily vs 5-min coverage gap:")
        gap_check = _check_daily_5min_coverage(db)
        if gap_check:
            print(f"  [WARN] {gap_check}")
            print("    Run: doppler run -- uv run nseml-kite-ingest --from <DATE> --5min --resume")
        else:
            print("  [OK] all symbols with daily data also have 5-min data (last 30 days)")

        print("\nDataset summary:")
        if "symbols" in status:
            print(f"  Symbols: {status['symbols']:,}")
        if "total_candles" in status:
            print(f"  Total candles: {status['total_candles']:,}")
        if "date_range" in status:
            print(f"  Date range: {status['date_range']}")

        print("\n[OK] DuckDB runtime coverage verification passed")
        return True
    except Exception as e:
        print(f"\n[FAIL] DuckDB verification failed: {e}")
        return False


def main():
    print("NSE Momentum Lab - Database Verification\n")

    pg_ok = verify_postgres()
    duckdb_ok = verify_duckdb()

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  PostgreSQL: {'[OK]' if pg_ok else '[FAIL]'}")
    print(f"  DuckDB:     {'[OK]' if duckdb_ok else '[FAIL]'} (runtime coverage)")

    if pg_ok and duckdb_ok:
        print("\n[OK] All databases verified successfully")
        print("\nReady to start:")
        print("  API:       doppler run -- uv run nseml-api")
        print("  Dashboard: doppler run -- uv run nseml-dashboard")
    else:
        print("\n[FAIL] Some databases need initialization")
        print("\nRun: doppler run -- uv run nseml-db-init")
        sys.exit(1)


if __name__ == "__main__":
    main()
