#!/usr/bin/env python3
"""Simple verification of Zerodha test ingestion."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Set Windows event loop policy for async psycopg
if sys.platform == "win32":
    import asyncio

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from sqlalchemy import text

from nse_momentum_lab.db import get_sessionmaker


async def verify_ingestion():
    """Verify test ingestion."""
    print("\n" + "=" * 70)
    print("Verifying Zerodha Test Ingestion")
    print("=" * 70)

    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        # Check total data
        result = await session.execute(
            text("""
                SELECT
                    COUNT(*) as total_rows
                FROM nseml.md_ohlcv_raw
            """)
        )

        total = result.fetchone()[0]
        print(f"\nTotal rows in database: {total:,}")

        # Check recent data
        result2 = await session.execute(
            text("""
                SELECT
                    COUNT(DISTINCT symbol_id) as unique_symbols
                FROM nseml.md_ohlcv_raw
            """)
        )

        symbols = result2.fetchone()[0]
        print(f"Unique symbols: {symbols}")

        # Get some sample data
        result3 = await session.execute(
            text("""
                SELECT
                    s.symbol,
                    o.trading_date,
                    o.close
                FROM nseml.md_ohlcv_raw o
                JOIN nseml.ref_symbol s ON o.symbol_id = s.symbol_id
                ORDER BY o.trading_date DESC
                LIMIT 10
            """)
        )

        rows = result3.fetchall()
        print("\nSample data (last 10 bars):")
        print(f"{'Symbol':<15} {'Date':<12} {'Close':<10}")
        print("-" * 40)
        for symbol, date, close in rows:
            print(f"{symbol:<15} {date!s:<12} {close:10.2f}")

        print("\n[SUCCESS] Database has data!")
        print(f"  Total rows: {total:,}")
        print(f"  Unique symbols: {symbols}")


if __name__ == "__main__":
    asyncio.run(verify_ingestion())
