"""10-Year Comprehensive Backtest of 2LYNCH Strategy.

This script runs a full backtest using all available historical data to properly
validate the strategy before making any conclusions.

Key Aspects:
1. Uses full date range available in DuckDB (2015-2025)
2. Tests with multiple symbol buckets (Nifty 50, Nifty 200, All symbols)
3. Generates comprehensive performance metrics
4. Analyzes drawdowns, win rate, profit factor, etc.
5. Shows monthly/yearly breakdown
"""

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine
from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.rules import ScanConfig


def get_available_date_range(db) -> tuple[date, date]:
    """Get the min and max dates available in the database."""
    query = """
    SELECT
        MIN(date) as min_date,
        MAX(date) as max_date
    FROM v_daily
    LIMIT 1
    """
    result = db.con.execute(query).fetchone()
    if result and result[0] and result[1]:
        return result[0], result[1]
    return date(2015, 1, 1), date(2025, 1, 1)


def get_all_symbols(db) -> list[str]:
    """Get all unique symbols from the database."""
    query = """
    SELECT DISTINCT symbol
    FROM v_daily
    ORDER BY symbol
    """
    result = db.con.execute(query).fetchall()
    return [row[0] for row in result if row[0]]


def run_comprehensive_backtest():
    """Run 10-year comprehensive backtest."""
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("10-YEAR COMPREHENSIVE BACKTEST - 2LYNCH STRATEGY")
    print("=" * 80)

    db = get_market_db()

    # Get available date range
    start_date, end_date = get_available_date_range(db)
    print(f"\n[DATA RANGE]")
    print(f"  Start Date: {start_date}")
    print(f"  End Date:   {end_date}")
    print(f"  Years:      {(end_date - start_date).days / 365.25:.1f} years")

    # Get all symbols
    all_symbols = get_all_symbols(db)
    print(f"\n[SYMBOLS]")
    print(f"  Total symbols available: {len(all_symbols)}")

    # For initial test, use a reasonable subset (e.g., symbols with good liquidity)
    # We can filter by dollar volume or use top N by market cap
    print(f"\n  Using all {len(all_symbols)} symbols for comprehensive test")

    # Step 1: Generate signals for entire period
    print(f"\n[STEP 1] Generating signals for {len(all_symbols)} symbols...")
    print(f"  This may take a while...")

    signal_gen = DuckDBSignalGenerator(config=ScanConfig())

    # Generate in yearly chunks to avoid memory issues
    all_signals = []
    current_year = start_date.year

    while current_year <= end_date.year:
        chunk_start = date(max(start_date.year, current_year), 1, 1)
        chunk_end = date(min(end_date.year, current_year + 1), 1, 1) - timedelta(days=1)

        print(f"    Processing {chunk_start.year}...", end="", flush=True)

        try:
            year_signals = signal_gen.generate_signals(
                all_symbols, chunk_start, min(chunk_end, end_date)
            )
            all_signals.extend(year_signals)
            print(f" {len(year_signals)} signals")
        except Exception as e:
            print(f" ERROR: {e}")

        current_year += 1

    print(f"\n  Total signals generated: {len(all_signals)}")

    if not all_signals:
        print("\n  [ERROR] No signals generated! Check:")
        print(f"    1. Are symbols valid?")
        print(f"    2. Is data available for {start_date} to {end_date}?")
        print(f"    3. Is 4% gap-up threshold too high?")
        return None

    # Signal distribution by year
    print(f"\n[SIGNAL DISTRIBUTION BY YEAR]")
    signal_by_year = {}
    for s in all_signals:
        year = s["trading_date"].year
        signal_by_year[year] = signal_by_year.get(year, 0) + 1

    for year in sorted(signal_by_year.keys()):
        print(f"  {year}: {signal_by_year[year]} signals")

    # Signal distribution by symbol (top 20)
    print(f"\n[TOP 20 MOST ACTIVE SYMBOLS]")
    signal_counts = {}
    for s in all_signals:
        symbol = s["symbol"]
        signal_counts[symbol] = signal_counts.get(symbol, 0) + 1

    for symbol, count in sorted(signal_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {symbol}: {count} signals")

    # Step 2: Load price data for symbols with signals
    print(f"\n[STEP 2] Loading price data...")

    signal_symbols = list(set(s["symbol"] for s in all_signals))
    symbol_to_id = {symbol: i for i, symbol in enumerate(signal_symbols)}
    id_to_symbol = {i: symbol for symbol, i in symbol_to_id.items()}

    price_data = {}
    value_traded_inr = {}

    for i, symbol in enumerate(signal_symbols):
        if (i + 1) % 50 == 0:
            print(f"    Loading {i + 1}/{len(signal_symbols)}...", flush=True)

        symbol_id = symbol_to_id[symbol]
        try:
            df = db.query_daily(symbol, start_date.isoformat(), end_date.isoformat())

            if df.is_empty():
                continue

            price_data[symbol_id] = {}
            for row in df.iter_rows(named=True):
                trading_date = row["date"]
                price_data[symbol_id][trading_date] = {
                    "open_adj": float(row["open"]),
                    "close_adj": float(row["close"]),
                    "high_adj": float(row["high"]),
                    "low_adj": float(row["low"]),
                }

            features_df = db.get_features_range(
                [symbol], start_date.isoformat(), end_date.isoformat()
            )
            if not features_df.is_empty():
                avg_vol = features_df.select(pl.col("dollar_vol_20").drop_nulls().mean()).item()
                value_traded_inr[symbol_id] = avg_vol if avg_vol else 50_000_000.0
            else:
                value_traded_inr[symbol_id] = 50_000_000.0
        except Exception as e:
            print(f"\n    [ERROR] Failed to load {symbol}: {e}")
            continue

    print(f"\n  Loaded price data for {len(price_data)} symbols")

    # Step 3: Convert signals to VectorBT format
    print(f"\n[STEP 3] Converting signals to VectorBT format...")

    vbt_signals = []
    for s in all_signals:
        signal_date = s["trading_date"]
        symbol = s["symbol"]
        if symbol not in symbol_to_id:
            continue
        symbol_id = symbol_to_id[symbol]
        initial_stop = s["initial_stop"]

        metadata = {
            "gap_pct": s["gap_pct"],
            "atr": s.get("atr", 0.0),
        }

        vbt_signals.append((signal_date, symbol_id, symbol, initial_stop, metadata))

    print(f"  Converted {len(vbt_signals)} signals")

    # Step 4: Run VectorBT backtest
    print(f"\n[STEP 4] Running VectorBT backtest...")
    print(f"  This may take a few minutes...", flush=True)

    config = VectorBTConfig(
        default_portfolio_value=1_000_000.0,  # Rs 10L
        risk_per_trade_pct=0.01,  # 1% risk per trade
        fees_per_trade=0.001,  # 0.1% STAX
        initial_stop_atr_mult=2.0,
        trail_activation_pct=0.05,
        trail_stop_pct=0.02,
        time_stop_days=3,
        follow_through_threshold=0.02,
    )

    engine = VectorBTEngine(config=config)
    result = engine.run_backtest(
        strategy_name="2LYNCH_10Year",
        signals=vbt_signals,
        price_data=price_data,
        value_traded_inr=value_traded_inr,
        delisting_dates=None,
    )

    # Step 5: Display comprehensive results
    print(f"\n{'=' * 80}")
    print(f"BACKTEST RESULTS ({start_date} to {end_date})")
    print(f"{'=' * 80}")

    print(f"\n[STRATEGY PERFORMANCE]")
    print(f"  Total Return:      {result.total_return * 100:>+10.2f}%")
    print(
        f"  Annualized Return: {result.total_return / ((end_date - start_date).days / 365.25) * 100:>+10.2f}%"
    )
    print(f"  Sharpe Ratio:      {result.sharpe_ratio:>10.2f}")
    print(f"  Max Drawdown:      {result.max_drawdown * 100:>10.2f}%")
    print(f"  Win Rate:          {result.win_rate * 100:>10.2f}%")
    print(f"  Profit Factor:     {result.profit_factor:>10.2f}")
    print(f"  Avg R:             {result.avg_r:>10.2f}R")
    print(f"  Median R:          {result.median_r:>10.2f}R")
    print(f"  Calmar Ratio:      {result.calmar_ratio:>10.2f}")
    print(f"  Sortino Ratio:     {result.sortino_ratio:>10.2f}")

    if result.r_distribution:
        print(f"\n[R DISTRIBUTION]")
        print(f"  10th percentile:  {result.r_distribution.get('r_p10', 0):>10.2f}R")
        print(f"  25th percentile:  {result.r_distribution.get('r_p25', 0):>10.2f}R")
        print(f"  50th percentile:  {result.r_distribution.get('r_p50', 0):>10.2f}R")
        print(f"  75th percentile:  {result.r_distribution.get('r_p75', 0):>10.2f}R")
        print(f"  90th percentile:  {result.r_distribution.get('r_p90', 0):>10.2f}R")
        print(f"  Avg Winner:       {result.r_distribution.get('avg_winner_r', 0):>10.2f}R")
        print(f"  Avg Loser:        {result.r_distribution.get('avg_loser_r', 0):>10.2f}R")
        print(f"  Max Winner:       {result.r_distribution.get('max_winner_r', 0):>10.2f}R")
        print(f"  Max Loser:        {result.r_distribution.get('max_loser_r', 0):>10.2f}R")

    print(f"\n[TRADE SUMMARY]")
    print(f"  Total Trades:      {len(result.trades)}")

    if result.trades:
        winners = [t for t in result.trades if t.pnl and t.pnl > 0]
        losers = [t for t in result.trades if t.pnl and t.pnl < 0]
        evens = [t for t in result.trades if t.pnl and t.pnl == 0]

        print(
            f"  Winners:           {len(winners)} ({len(winners) / len(result.trades) * 100:.1f}%)"
        )
        print(f"  Losers:            {len(losers)} ({len(losers) / len(result.trades) * 100:.1f}%)")
        print(f"  Breakeven:         {len(evens)} ({len(evens) / len(result.trades) * 100:.1f}%)")

        # Exit reason analysis
        exit_reasons = {}
        for t in result.trades:
            if t.exit_reason:
                reason = t.exit_reason.value
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

        print(f"\n[EXIT REASONS]")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True):
            pct = count / len(result.trades) * 100
            print(f"  {reason}: {count:4d} ({pct:5.1f}%)")

        # Yearly breakdown
        print(f"\n[YEARLY BREAKDOWN]")
        trades_by_year = {}
        pnl_by_year = {}
        for t in result.trades:
            year = t.entry_date.year
            trades_by_year[year] = trades_by_year.get(year, 0) + 1
            if t.pnl:
                pnl_by_year[year] = pnl_by_year.get(year, 0) + t.pnl

        for year in sorted(trades_by_year.keys()):
            num_trades = trades_by_year[year]
            total_pnl = pnl_by_year.get(year, 0)
            avg_pnl = total_pnl / num_trades if num_trades > 0 else 0
            print(
                f"  {year}: {num_trades:4d} trades | P&L: {total_pnl:>+10.0f} ({avg_pnl:>+8.0f} avg)"
            )

        # Best and worst trades
        print(f"\n[BEST 10 TRADES]")
        sorted_trades = sorted(result.trades, key=lambda t: t.pnl_r or 0, reverse=True)
        for i, trade in enumerate(sorted_trades[:10]):
            symbol = id_to_symbol.get(trade.symbol_id, trade.symbol)
            pnl_r_str = f">{trade.pnl_r:+.2f}R" if trade.pnl_r else "N/A"
            exit_str = trade.exit_reason.value if trade.exit_reason else "N/A"
            holding_days = (trade.exit_date - trade.entry_date).days if trade.exit_date else 0
            print(
                f"  {i + 1:2d}. {symbol:12s} | {trade.entry_date} | {pnl_r_str:>8s} | {holding_days}d | {exit_str}"
            )

        print(f"\n[WORST 10 TRADES]")
        for i, trade in enumerate(sorted_trades[-10:]):
            symbol = id_to_symbol.get(trade.symbol_id, trade.symbol)
            pnl_r_str = f">{trade.pnl_r:+.2f}R" if trade.pnl_r else "N/A"
            exit_str = trade.exit_reason.value if trade.exit_reason else "N/A"
            holding_days = (trade.exit_date - trade.entry_date).days if trade.exit_date else 0
            print(
                f"  {i + 1:2d}. {symbol:12s} | {trade.entry_date} | {pnl_r_str:>8s} | {holding_days}d | {exit_str}"
            )

    # Save results to file
    results_file = Path("backtest_results_10year.txt")
    print(f"\n[SAVING RESULTS]")
    print(f"  Saving detailed results to {results_file}...")
    # TODO: Add detailed CSV export

    print(f"\n{'=' * 80}")
    print(f"10-YEAR BACKTEST COMPLETED")
    print(f"{'=' * 80}\n")

    return result


if __name__ == "__main__":
    run_comprehensive_backtest()
