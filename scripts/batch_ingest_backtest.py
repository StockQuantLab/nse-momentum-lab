#!/usr/bin/env python3
"""Batch ingest and backtest - process N stocks at a time."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.services.adjust.worker import AdjustmentWorker
from sqlalchemy import func, select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import MdOhlcvRaw, RefSymbol, ScanResult
from nse_momentum_lab.services.backtest.worker import BacktestWorker
from nse_momentum_lab.services.scan.worker import ScanWorker


async def get_ingested_symbols():
    """Get list of already ingested symbols."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await session.execute(
            select(RefSymbol.symbol).where(RefSymbol.status == "ACTIVE")
        )
        return sorted([r[0] for r in result.fetchall()])


async def get_trading_dates():
    """Get all trading dates from OHLCV data."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        result = await session.execute(
            select(MdOhlcvRaw.trading_date).distinct().order_by(MdOhlcvRaw.trading_date)
        )
        return [r[0] for r in result.fetchall()]


async def run_scan_for_date(trading_date):
    """Run scan for a single date."""
    worker = ScanWorker()
    result = await worker.run(trading_date)
    return result


async def run_backtest():
    """Run backtest on all scan results."""
    worker = BacktestWorker()
    result = await worker.run_backtest_all()
    return result


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Batch ingest and backtest")
    parser.add_argument("--batch-size", type=int, default=100, help="Number of stocks to process")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip ingestion, just scan/backtest")
    parser.add_argument("--scan-only", action="store_true", help="Only run scans, no backtest")
    args = parser.parse_args()

    print("=" * 70)
    print("BATCH INGEST AND BACKTEST")
    print("=" * 70)

    # Check current state
    symbols = await get_ingested_symbols()
    trading_dates = await get_trading_dates()

    print("\nCurrent state:")
    print(f"  Symbols: {len(symbols)}")
    print(f"  Trading dates: {len(trading_dates)}")
    if trading_dates:
        print(f"  Date range: {trading_dates[0]} to {trading_dates[-1]}")

    if not args.skip_ingest:
        print(f"\nTarget batch size: {args.batch_size} stocks")
        if len(symbols) >= args.batch_size:
            print(f"  Already have {len(symbols)} symbols, skipping ingest")
        else:
            print(f"  Please run ingest manually for first {args.batch_size} stocks:")
            print("  doppler run -- uv run python scripts/ingest_vendor_candles.py \\")
            print("    'data/zerodha-april-2015-to-march-2025/timeframe - daily' \\")
            print(f"    --timeframe day --vendor zerodha --limit {args.batch_size}")
            return

    # Run adjustments
    print("\n" + "=" * 70)
    print("STEP 1: Running price adjustments...")
    print("=" * 70)
    worker = AdjustmentWorker()
    results = await worker.run_all()
    print(f"Adjusted {len(results)} symbols")

    # Get scan results count
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        existing_scans = await session.execute(select(func.count(ScanResult.scan_result_id)))
        existing_scans.scalar_one() or 0

    # Run scans
    print("\n" + "=" * 70)
    print("STEP 2: Running momentum scans...")
    print("=" * 70)
    print(f"  Trading dates to scan: {len(trading_dates)}")

    scan_count = 0
    signal_count = 0

    for i, date in enumerate(trading_dates):
        if i % 50 == 0:
            print(f"  Progress: [{i}/{len(trading_dates)}] signals so far: {signal_count}")

        result = await run_scan_for_date(date)
        scan_count += 1
        signal_count += result.candidates_found

    print(f"\n  Scans completed: {scan_count}")
    print(f"  Total signals found: {signal_count}")

    # Run backtest
    if not args.scan_only:
        print("\n" + "=" * 70)
        print("STEP 3: Running backtests...")
        print("=" * 70)

        result = await run_backtest()

        print("\nBacktest Results:")
        print(f"  Trades executed: {result.get('trades_count', 0)}")
        print(f"  Total return: {result.get('total_return', 0):.2%}")
        print(f"  Win rate: {result.get('win_rate', 0):.2%}")

    print("\n" + "=" * 70)
    print("BATCH PROCESSING COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
