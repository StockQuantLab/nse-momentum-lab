#!/usr/bin/env python3
"""
Phase 1 End-to-End Test Script

This script tests the complete Phase 1 pipeline:
1. Adjustment - Create adjusted OHLCV from raw data
2. Scan - Run 4% + 2LYNCH scan
3. Backtest - Run backtest on scan results

Usage:
    python test_phase1_pipeline.py [YYYY-MM-DD]

Example:
    python test_phase1_pipeline.py 2025-03-28
"""

import asyncio
import io
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any

# Fix Windows encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.services.adjust.worker import AdjustmentWorker
from sqlalchemy import func, select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import (
    BtTrade,
    MdOhlcvAdj,
    MdOhlcvRaw,
    RefSymbol,
    ScanResult,
)
from nse_momentum_lab.services.backtest.registry import ExperimentRegistry
from nse_momentum_lab.services.scan.worker import ScanWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Colors:
    """ANSI color codes for terminal output."""

    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"


def print_header(text: str):
    """Print a formatted header."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text:^70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}\n")


def print_success(text: str):
    """Print success message."""
    print(f"{Colors.OKGREEN}✅ {text}{Colors.ENDC}")


def print_error(text: str):
    """Print error message."""
    print(f"{Colors.FAIL}❌ {text}{Colors.ENDC}")


def print_warning(text: str):
    """Print warning message."""
    print(f"{Colors.WARNING}⚠️  {text}{Colors.ENDC}")


def print_info(text: str):
    """Print info message."""
    print(f"{Colors.OKCYAN}ℹ️  {text}{Colors.ENDC}")


async def check_initial_state() -> dict[str, Any]:
    """Check initial database state."""
    print_header("Checking Initial Database State")

    sm = get_sessionmaker()
    async with sm() as session:
        # Count symbols
        symbols = await session.execute(select(func.count(RefSymbol.symbol_id)))
        symbol_count = symbols.scalar_one() or 0

        # Count raw OHLCV
        raw = await session.execute(select(func.count(MdOhlcvRaw.symbol_id)))
        raw_count = raw.scalar_one() or 0

        # Count adjusted OHLCV
        adj = await session.execute(select(func.count(MdOhlcvAdj.symbol_id)))
        adj_count = adj.scalar_one() or 0

        # Get date ranges
        raw_min = await session.execute(select(func.min(MdOhlcvRaw.trading_date)))
        raw_max = await session.execute(select(func.max(MdOhlcvRaw.trading_date)))
        raw_min_date = raw_min.scalar_one()
        raw_max_date = raw_max.scalar_one()

        print_info(f"Symbols: {symbol_count}")
        print_info(f"Raw OHLCV rows: {raw_count:,}")
        print_info(f"Raw date range: {raw_min_date} to {raw_max_date}")
        print_info(f"Adjusted OHLCV rows: {adj_count:,}")

        if symbol_count == 0:
            print_error("No symbols found. Please run ingestion first.")
            sys.exit(1)

        if raw_count == 0:
            print_error("No raw OHLCV data found. Please run ingestion first.")
            sys.exit(1)

        print_success(f"Found {symbol_count} symbols with {raw_count:,} raw OHLCV rows")

        return {
            "symbol_count": symbol_count,
            "raw_count": raw_count,
            "adj_count": adj_count,
            "raw_min_date": raw_min_date,
            "raw_max_date": raw_max_date,
        }


async def run_adjustment() -> dict[str, Any]:
    """Run adjustment step."""
    print_header("Step 1: Running Adjustment")

    worker = AdjustmentWorker()

    try:
        print_info("Starting adjustment worker...")
        results = await worker.run_all()

        # Count results
        success_count = len([r for r in results if not r.issues])
        fail_count = len([r for r in results if r.issues])

        print_success(f"Adjustment complete: {success_count} symbols processed")

        if fail_count > 0:
            print_warning(f"{fail_count} symbols had issues")

        # Check adjusted row count
        sm = get_sessionmaker()
        async with sm() as session:
            adj = await session.execute(select(func.count(MdOhlcvAdj.symbol_id)))
            adj_count = adj.scalar_one() or 0

            adj_min = await session.execute(select(func.min(MdOhlcvAdj.trading_date)))
            adj_max = await session.execute(select(func.max(MdOhlcvAdj.trading_date)))
            adj_min_date = adj_min.scalar_one()
            adj_max_date = adj_max.scalar_one()

            print_info(f"Adjusted OHLCV rows: {adj_count:,}")
            print_info(f"Adjusted date range: {adj_min_date} to {adj_max_date}")

        # AdjustmentWorker doesn't have close(), so we skip that

        return {
            "success_count": success_count,
            "fail_count": fail_count,
            "adj_count": adj_count,
            "adj_min_date": adj_min_date,
            "adj_max_date": adj_max_date,
        }

    except Exception as e:
        print_error(f"Adjustment failed: {e}")
        logger.exception("Adjustment failed")
        sys.exit(1)


async def run_scan(asof_date: date) -> dict[str, Any]:
    """Run scan step."""
    print_header(f"Step 2: Running Scan for {asof_date}")

    worker = ScanWorker()

    try:
        print_info(f"Starting scan worker for {asof_date}...")
        result = await worker.run(asof_date)

        print_success(
            f"Scan complete: {result.candidates_found}/{result.total_universe} candidates passed"
        )

        # Get scan results details
        sm = get_sessionmaker()
        async with sm() as session:
            # Get candidates with details
            scan_results = await session.execute(
                select(ScanResult, RefSymbol)
                .join(RefSymbol, ScanResult.symbol_id == RefSymbol.symbol_id)
                .where(ScanResult.scan_run_id == result.scan_run_id)
                .order_by(ScanResult.score.desc())
            )

            results = scan_results.all()

            print_info("\nTop candidates:")
            for i, (sr, rs) in enumerate(results[:10], 1):
                status = "✅" if sr.passed else "❌"
                print(f"  {status} {i}. {rs.symbol:10s} - Score: {sr.score:.2f}")

            if result.candidates_found == 0:
                print_warning("No candidates found. This is expected for small sample data.")

        return {
            "scan_run_id": result.scan_run_id,
            "candidates_found": result.candidates_found,
            "total_universe": result.total_universe,
            "status": result.status,
        }

    except Exception as e:
        print_error(f"Scan failed: {e}")
        logger.exception("Scan failed")
        sys.exit(1)


async def run_backtest(
    scan_run_id: int,
    asof_date: date,
    initial_state: dict[str, Any],
) -> dict[str, Any]:
    """Run backtest step."""
    print_header("Step 3: Running Backtest")

    # Load scan results
    sm = get_sessionmaker()
    async with sm() as session:
        # Get scan results
        scan_results = await session.execute(
            select(ScanResult, RefSymbol)
            .join(RefSymbol, ScanResult.symbol_id == RefSymbol.symbol_id)
            .where(ScanResult.scan_run_id == scan_run_id)
            .where(ScanResult.passed)
        )

        results = scan_results.all()

        if not results:
            print_warning("No scan candidates to backtest. This is expected for small sample data.")
            print_info("Creating synthetic signals for testing...")

            # Create synthetic signals for testing
            from nse_momentum_lab.db.models import MdOhlcvAdj

            # Get a few symbols with data
            symbols_with_data = await session.execute(
                select(MdOhlcvAdj.symbol_id, RefSymbol.symbol)
                .join(RefSymbol, MdOhlcvAdj.symbol_id == RefSymbol.symbol_id)
                .where(MdOhlcvAdj.trading_date == asof_date)
                .limit(3)
            )

            test_signals = []
            for symbol_id, symbol in symbols_with_data:
                # Get the price data
                price_data = await session.execute(
                    select(MdOhlcvAdj)
                    .where(MdOhlcvAdj.symbol_id == symbol_id)
                    .where(MdOhlcvAdj.trading_date <= asof_date)
                    .order_by(MdOhlcvAdj.trading_date.desc())
                    .limit(100)
                )

                prices = list(price_data.scalars().all())
                if prices:
                    latest = prices[0]
                    test_signals.append(
                        (
                            asof_date,
                            symbol_id,
                            symbol,
                            float(latest.close_adj) * 0.98,  # 2% stop
                            {"test": True},
                        )
                    )

            print_info(f"Created {len(test_signals)} synthetic signals for testing")

        else:
            test_signals = []
            for sr, rs in results:
                # Get price data to compute stop
                price_data = await session.execute(
                    select(MdOhlcvAdj)
                    .where(MdOhlcvAdj.symbol_id == sr.symbol_id)
                    .where(MdOhlcvAdj.trading_date == asof_date)
                )

                price = price_data.scalar_one_or_none()
                if price:
                    test_signals.append(
                        (
                            asof_date,
                            sr.symbol_id,
                            rs.symbol,
                            float(price.close_adj * 0.98),  # 2% stop
                            sr.reason_json,
                        )
                    )

        print_info(f"Total signals to backtest: {len(test_signals)}")

        # Load price data for backtest
        price_data: dict[int, dict[date, dict[str, float]]] = {}
        dollar_vol: dict[int, float] = {}

        for _, symbol_id, _, _, _ in test_signals:
            # Load price history
            prices = await session.execute(
                select(MdOhlcvAdj)
                .where(MdOhlcvAdj.symbol_id == symbol_id)
                .where(MdOhlcvAdj.trading_date >= asof_date)
                .order_by(MdOhlcvAdj.trading_date)
                .limit(10)
            )

            price_data[symbol_id] = {}
            for p in prices.scalars().all():
                price_data[symbol_id][p.trading_date] = {
                    "open_adj": float(p.open_adj),
                    "high_adj": float(p.high_adj),
                    "low_adj": float(p.low_adj),
                    "close_adj": float(p.close_adj),
                }

            # Get dollar volume
            feat = await session.execute(
                select(MdOhlcvAdj)
                .where(MdOhlcvAdj.symbol_id == symbol_id)
                .where(MdOhlcvAdj.trading_date == asof_date)
            )

            f = feat.scalar_one_or_none()
            if f and f.value_traded:
                dollar_vol[symbol_id] = float(f.value_traded)

    # Run backtest
    registry = ExperimentRegistry()

    try:
        print_info("Starting backtest...")

        result = await registry.register_and_run(
            strategy_name="4P_2LYNCH",
            params={
                "breakout_threshold": 0.04,
                "close_pos_threshold": 0.70,
            },
            signals=test_signals,
            price_data=price_data,
            dollar_vol=dollar_vol,
            code_sha="test",
            dataset_hash=f"test_{asof_date.isoformat()}",
        )

        print_success(f"Backtest complete: {result.exp_hash}")

        # Print metrics
        print_info("\nBacktest Metrics:")
        for key, value in result.metrics.items():
            print(f"  {key}: {value:.4f}")

        # Get trade details
        async with sm() as session:
            trades = await session.execute(
                select(BtTrade)
                .where(BtTrade.exp_run_id == result.exp_run_id)
                .order_by(BtTrade.entry_date.desc())
            )

            trade_list = list(trades.scalars().all())

            print_info("\nTrade Summary:")
            print(f"  Total trades: {len(trade_list)}")

            if trade_list:
                wins = sum(1 for t in trade_list if t.pnl and t.pnl > 0)
                losses = sum(1 for t in trade_list if t.pnl and t.pnl < 0)
                avg_pnl = sum(t.pnl or 0 for t in trade_list) / len(trade_list)

                print(f"  Wins: {wins}")
                print(f"  Losses: {losses}")
                print(f"  Avg P&L: {avg_pnl:.2f}")

                print_info("\nLast 5 trades:")
                for i, trade in enumerate(trade_list[:5], 1):
                    pnl_str = f"{trade.pnl:.2f}" if trade.pnl else "N/A"
                    print(
                        f"    {i}. {trade.entry_date} - P&L: {pnl_str} - Reason: {trade.exit_reason}"
                    )

        return {
            "exp_run_id": result.exp_run_id,
            "exp_hash": result.exp_hash,
            "status": result.status,
            "metrics": result.metrics,
            "trade_count": len(trade_list) if trade_list else 0,
        }

    except Exception as e:
        print_error(f"Backtest failed: {e}")
        logger.exception("Backtest failed")
        sys.exit(1)


async def main():
    """Main test pipeline."""
    print_header("Phase 1 End-to-End Test")

    # Parse date
    if len(sys.argv) > 1:
        try:
            test_date = date.fromisoformat(sys.argv[1])
        except ValueError:
            print_error(f"Invalid date format: {sys.argv[1]}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        test_date = date(2025, 3, 28)
        print_info(f"No date provided, using default: {test_date}")

    print_info(f"Test date: {test_date}")

    # Step 0: Check initial state
    initial_state = await check_initial_state()

    # Step 1: Run adjustment
    adjustment_result = await run_adjustment()

    # Step 2: Run scan
    scan_result = await run_scan(test_date)

    # Step 3: Run backtest
    backtest_result = await run_backtest(
        scan_result["scan_run_id"],
        test_date,
        initial_state,
    )

    # Final summary
    print_header("Test Summary")

    print_success("Phase 1 Pipeline Complete!")
    print("\n📊 Results:")
    print(f"  Symbols: {initial_state['symbol_count']}")
    print(f"  Raw OHLCV: {initial_state['raw_count']:,} rows")
    print(f"  Adjusted OHLCV: {adjustment_result['adj_count']:,} rows")
    print(f"  Scan date: {test_date}")
    print(f"  Scan candidates: {scan_result['candidates_found']}")
    print(f"  Experiment: {backtest_result['exp_hash']}")
    print(f"  Trades: {backtest_result['trade_count']}")

    print("\n📈 Backtest Metrics:")
    for key, value in backtest_result["metrics"].items():
        print(f"  {key}: {value:.4f}")

    print_success("\n✅ All Phase 1 components validated!")
    print_info("You can now proceed with full dataset ingestion.")


if __name__ == "__main__":
    # Windows async fix
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
