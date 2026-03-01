#!/usr/bin/env python3
"""
Comprehensive Phase 1 Analysis Script

This script provides detailed analysis of backtest results including:
- Trade-by-trade breakdown
- Performance metrics
- Win/loss analysis
- Drawdown analysis
- Strategy effectiveness for Indian markets
"""

import asyncio
import io
import sys
from pathlib import Path
from typing import Any

# Fix Windows encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import select

from nse_momentum_lab.db import get_sessionmaker
from nse_momentum_lab.db.models import (
    BtTrade,
    ExpMetric,
    ExpRun,
    RefSymbol,
)


class Colors:
    """ANSI color codes."""

    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"


def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text:^80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}\n")


def print_success(text: str):
    print(f"{Colors.OKGREEN}✅ {text}{Colors.ENDC}")


def print_error(text: str):
    print(f"{Colors.FAIL}❌ {text}{Colors.ENDC}")


def print_warning(text: str):
    print(f"{Colors.WARNING}⚠️  {text}{Colors.ENDC}")


def print_info(text: str):
    print(f"{Colors.OKCYAN}ℹ️  {text}{Colors.ENDC}")


def print_metric(label: str, value: Any, color: str = Colors.OKCYAN):
    print(f"{color}{label:30s}: {Colors.ENDC}{value}")


async def analyze_experiment() -> dict[str, Any]:
    """Analyze the latest experiment results."""
    print_header("Phase 1 Backtest Analysis - Indian Markets")

    sm = get_sessionmaker()
    async with sm() as session:
        # Get latest experiment
        result = await session.execute(select(ExpRun).order_by(ExpRun.started_at.desc()).limit(1))
        exp = result.scalar_one_or_none()

        if not exp:
            print_error("No experiments found. Please run the Phase 1 test first.")
            print_info("Run: python scripts/test_phase1_pipeline.py 2025-03-28")
            return {}

        print_info(f"Experiment: {exp.exp_hash}")
        print_info(f"Strategy: {exp.strategy_name}")
        print_info(f"Status: {exp.status}")

        # Get metrics
        metrics_result = await session.execute(
            select(ExpMetric).where(ExpMetric.exp_run_id == exp.exp_run_id)
        )
        metrics = {}
        for row in metrics_result.all():
            # Access by index since ExpMetric is a tuple
            m = row[0]  # ExpMetric object
            metrics[m.metric_name] = m.metric_value

        print_header("Backtest Performance Metrics")

        print_metric("Sharpe Ratio (Open)", f"{metrics.get('sharpe_open', 0):.4f}")
        print_metric("Sharpe Ratio (Close)", f"{metrics.get('sharpe_close', 0):.4f}")
        print_metric("Total Return (Open)", f"{metrics.get('total_return_open', 0) * 100:.2f}%")
        print_metric("Total Return (Close)", f"{metrics.get('total_return_close', 0) * 100:.2f}%")
        print_metric("Win Rate (Open)", f"{metrics.get('win_rate_open', 0) * 100:.2f}%")
        print_metric("Win Rate (Close)", f"{metrics.get('win_rate_close', 0) * 100:.2f}%")
        print_metric("Max Drawdown (Open)", f"{metrics.get('max_dd_open', 0) * 100:.2f}%")
        print_metric("Max Drawdown (Close)", f"{metrics.get('max_dd_close', 0) * 100:.2f}%")
        print_metric("Profit Factor (Open)", f"{metrics.get('profit_factor_open', 0):.2f}")
        print_metric("Profit Factor (Close)", f"{metrics.get('profit_factor_close', 0):.2f}")
        print_metric("Avg R-Multiple (Open)", f"{metrics.get('avg_r_open', 0):.2f}R")
        print_metric("Avg R-Multiple (Close)", f"{metrics.get('avg_r_close', 0):.2f}R")

        # Get trades
        trades_result = await session.execute(
            select(BtTrade, RefSymbol)
            .join(RefSymbol, BtTrade.symbol_id == RefSymbol.symbol_id)
            .where(BtTrade.exp_run_id == exp.exp_run_id)
            .order_by(BtTrade.entry_date.desc())
        )

        trades = []
        for row in trades_result.all():
            t, s = row[0], row[1]
            trades.append(
                {
                    "symbol": s.symbol,
                    "entry_date": t.entry_date,
                    "entry_price": float(t.entry_price),
                    "entry_mode": t.entry_mode,
                    "exit_date": t.exit_date,
                    "exit_price": float(t.exit_price) if t.exit_price else None,
                    "pnl": float(t.pnl) if t.pnl else None,
                    "pnl_r": float(t.pnl_r) if t.pnl_r else None,
                    "exit_reason": t.exit_reason,
                    "mfe_r": float(t.mfe_r) if t.mfe_r else None,
                    "mae_r": float(t.mae_r) if t.mae_r else None,
                    "fees": float(t.fees) if t.fees else 0,
                    "slippage_bps": float(t.slippage_bps) if t.slippage_bps else 0,
                }
            )

        print_header("Trade-by-Trade Analysis")

        if not trades:
            print_warning("No trades found in this experiment.")
            return {"exp": exp, "metrics": metrics, "trades": []}

        # Separate by entry mode
        open_trades = [t for t in trades if t["entry_mode"] == "open"]
        close_trades = [t for t in trades if t["entry_mode"] == "close"]

        print_info(
            f"Total Trades: {len(trades)} ({len(open_trades)} open, {len(close_trades)} close)"
        )

        print_header("Open Entry Mode Trades")
        if open_trades:
            print(
                f"{'Symbol':<12} {'Entry':<12} {'Exit':<12} {'P&L':>10} {'R':>6} {'MFE':>6} {'MAE':>6} {'Reason':<20}"
            )
            print("-" * 90)
            for t in open_trades:
                pnl_str = f"₹{t['pnl']:,.2f}" if t["pnl"] else "N/A"
                r_str = f"{t['pnl_r']:.2f}R" if t["pnl_r"] else "N/A"
                mfe_str = f"{t['mfe_r']:.2f}R" if t["mfe_r"] else "N/A"
                mae_str = f"{t['mae_r']:.2f}R" if t["mae_r"] else "N/A"
                reason = t["exit_reason"] or "N/A"
                print(
                    f"{t['symbol']:<12} {t['entry_date']!s:<12} {t['exit_date']!s:<12} {pnl_str:>10} {r_str:>6} {mfe_str:>6} {mae_str:>6} {reason:<20}"
                )

            # Statistics
            wins = [t for t in open_trades if t["pnl"] and t["pnl"] > 0]
            losses = [t for t in open_trades if t["pnl"] and t["pnl"] < 0]
            avg_win = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
            avg_loss = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
            total_pnl = sum(t["pnl"] for t in open_trades if t["pnl"])

            print("\n" + "-" * 90)
            print(f"{'Wins:':<20} {len(wins)} ({len(wins) / len(open_trades) * 100:.1f}%)")
            print(f"{'Losses:':<20} {len(losses)} ({len(losses) / len(open_trades) * 100:.1f}%)")
            print(f"{'Average Win:':<20} ₹{avg_win:,.2f}")
            print(f"{'Average Loss:':<20} ₹{avg_loss:,.2f}")
            print(f"{'Total P&L:':<20} ₹{total_pnl:,.2f}")
            print(f"{'Win/Loss Ratio:':<20} {abs(avg_win / avg_loss) if avg_loss != 0 else 0:.2f}")

        print_header("Close Entry Mode Trades")
        if close_trades:
            print(
                f"{'Symbol':<12} {'Entry':<12} {'Exit':<12} {'P&L':>10} {'R':>6} {'MFE':>6} {'MAE':>6} {'Reason':<20}"
            )
            print("-" * 90)
            for t in close_trades:
                pnl_str = f"₹{t['pnl']:,.2f}" if t["pnl"] else "N/A"
                r_str = f"{t['pnl_r']:.2f}R" if t["pnl_r"] else "N/A"
                mfe_str = f"{t['mfe_r']:.2f}R" if t["mfe_r"] else "N/A"
                mae_str = f"{t['mae_r']:.2f}R" if t["mae_r"] else "N/A"
                reason = t["exit_reason"] or "N/A"
                print(
                    f"{t['symbol']:<12} {t['entry_date']!s:<12} {t['exit_date']!s:<12} {pnl_str:>10} {r_str:>6} {mfe_str:>6} {mae_str:>6} {reason:<20}"
                )

            # Statistics
            wins = [t for t in close_trades if t["pnl"] and t["pnl"] > 0]
            losses = [t for t in close_trades if t["pnl"] and t["pnl"] < 0]
            avg_win = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
            avg_loss = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
            total_pnl = sum(t["pnl"] for t in close_trades if t["pnl"])

            print("\n" + "-" * 90)
            print(f"{'Wins:':<20} {len(wins)} ({len(wins) / len(close_trades) * 100:.1f}%)")
            print(f"{'Losses:':<20} {len(losses)} ({len(losses) / len(close_trades) * 100:.1f}%)")
            print(f"{'Average Win:':<20} ₹{avg_win:,.2f}")
            print(f"{'Average Loss:':<20} ₹{avg_loss:,.2f}")
            print(f"{'Total P&L:':<20} ₹{total_pnl:,.2f}")
            print(f"{'Win/Loss Ratio:':<20} {abs(avg_win / avg_loss) if avg_loss != 0 else 0:.2f}")

        # Exit reason analysis
        print_header("Exit Reason Analysis")

        exit_reasons = {}
        for t in trades:
            reason = t["exit_reason"] or "UNKNOWN"
            if reason not in exit_reasons:
                exit_reasons[reason] = {"count": 0, "pnl": 0}
            exit_reasons[reason]["count"] += 1
            if t["pnl"]:
                exit_reasons[reason]["pnl"] += t["pnl"]

        print(f"{'Exit Reason':<25} {'Count':>10} {'Avg P&L':>15} {'Total P&L':>15}")
        print("-" * 65)
        for reason, data in sorted(exit_reasons.items(), key=lambda x: x[1]["count"], reverse=True):
            avg_pnl = data["pnl"] / data["count"] if data["count"] > 0 else 0
            print(f"{reason:<25} {data['count']:>10} ₹{avg_pnl:>13,.2f} ₹{data['pnl']:>13,.2f}")

        # Strategy effectiveness analysis
        print_header("Strategy Effectiveness for Indian Markets")

        print_info("Key Observations:")
        print()

        # Check if we have meaningful results
        if len(trades) >= 10:
            win_rate = len([t for t in trades if t["pnl"] and t["pnl"] > 0]) / len(trades)
            avg_r = sum(t["pnl_r"] for t in trades if t["pnl_r"]) / len(trades) if trades else 0

            if win_rate >= 0.40 and avg_r > 0:
                print_success(
                    f"✓ Win rate of {win_rate * 100:.1f}% is acceptable for momentum strategy"
                )
                print_success(f"✓ Average R-multiple of {avg_r:.2f}R shows positive expectancy")
            elif win_rate < 0.40:
                print_warning(f"⚠ Win rate of {win_rate * 100:.1f}% is below ideal (target: 40%+)")
            else:
                print_warning(
                    f"⚠ Average R-multiple of {avg_r:.2f}R needs improvement (target: >0)"
                )

            # Check exit reasons
            time_stops = len([t for t in trades if t["exit_reason"] in ("TIME_STOP", "TIME_STOP_DAY3")])
            if time_stops > len(trades) * 0.5:
                print_info("ℹ Most trades hit time stops - consider extending holding period")
                print_info("  or adjusting stop loss strategy for Indian market volatility")

            # Check MFE/MAE
            avg_mfe = (
                sum(t["mfe_r"] for t in trades if t["mfe_r"])
                / len([t for t in trades if t["mfe_r"]])
                if trades
                else 0
            )
            avg_mae = (
                sum(t["mae_r"] for t in trades if t["mae_r"])
                / len([t for t in trades if t["mae_r"]])
                if trades
                else 0
            )

            print()
            print_metric("Avg MFE (Best Case)", f"{avg_mfe:.2f}R")
            print_metric("Avg MAE (Worst Case)", f"{avg_mae:.2f}R")

            if avg_mfe > abs(avg_mae) * 1.5:
                print_success("✓ Favorable Risk/Reward - trades show potential")
            else:
                print_warning("⚠ Risk/Reward ratio needs optimization")

        else:
            print_warning("⚠ Limited sample size (need 100+ trades for statistical significance)")
            print_info("  Current results are from synthetic test signals")
            print_info("  Run full dataset backtest for reliable analysis")

        print()
        print_header("Recommendations for Indian Markets")

        print("Based on the analysis:")
        print()
        print("1. **Volatility Adjustment**: Indian stocks can be more volatile")
        print("   - Consider wider stops (2.5-3 ATR instead of 2 ATR)")
        print("   - Extend time stops to 5-7 days for trend development")
        print()
        print("2. **Liquidity Filter**: Ensure minimum volume thresholds")
        print("   - Indian mid-caps may have slippage issues")
        print("   - Consider ₹50Cr+ daily volume filter")
        print()
        print("3. **Sector Analysis**: Some sectors outperform with momentum")
        print("   - IT, Pharma, Finance typically show strong trends")
        print("   - Avoid cyclical sectors during downturns")
        print()
        print("4. **Market Regime**: Consider Nifty/Vix trends")
        print("   - Momentum works best in bullish markets")
        print("   - Reduce exposure when VIX > 20")
        print()
        print("5. **Position Sizing**: Indian stocks can gap up/down 10-20%")
        print("   - Use 1-2% risk per trade")
        print("   - Limit sector concentration to 20%")

        return {"exp": exp, "metrics": metrics, "trades": trades}


async def main():
    """Main analysis function."""
    result = await analyze_experiment()

    if result:
        print_header("Analysis Complete")
        print()
        print_success("Open the dashboard to explore further:")
        print_info("  🌐 Dashboard: http://localhost:8501")
        print_info("  📊 Scans Page: View scan results and candidates")
        print_info("  🧪 Experiments Page: View backtest details")
        print()
        print_info("Dashboard pages available:")
        print("  1. Pipeline Status - Job runs and progress")
        print("  2. Scans - Momentum scan results")
        print("  3. Experiments - Backtest results")
        print("  4. Paper Ledger - Paper trading positions")
        print("  5. Daily Summary - Daily market summary")
        print()


if __name__ == "__main__":
    # Windows async fix
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
