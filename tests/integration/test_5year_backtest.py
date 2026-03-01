"""5-Year Practical Backtest with Focused Symbol Set.

This runs a realistic backtest that:
1. Uses liquid stocks (Nifty 200 + quality mid-caps)
2. Covers 5 years including different market regimes
3. Completes in reasonable time
4. Provides actionable insights
"""

import sys
from datetime import date, timedelta

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine
from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.rules import ScanConfig


def run_5year_backtest():
    """Run 5-year practical backtest with liquid stocks."""
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("5-YEAR PRACTICAL BACKTEST - 2LYNCH STRATEGY")
    print("=" * 80)

    db = get_market_db()

    # Use last 5 years of data
    end_date = date(2024, 12, 31)
    start_date = date(2020, 1, 1)

    print(f"\n[CONFIGURATION]")
    print(f"  Period: {start_date} to {end_date}")
    print(f"  Years:  {(end_date - start_date).days / 365.25:.1f} years")

    # Get liquid stocks (filter by dollar volume)
    print(f"\n[SYMBOL SELECTION]")
    print(f"  Selecting liquid stocks...")

    # Get symbols with good liquidity (use average dollar volume)
    query = """
    SELECT symbol, AVG(dollar_vol_20) as avg_vol
    FROM feat_daily
    WHERE trading_date >= '2020-01-01'
    GROUP BY symbol
    HAVING AVG(dollar_vol_20) > 10000000  -- > ₹1 Cr daily volume
    ORDER BY avg_vol DESC
    LIMIT 500
    """

    result = db.con.execute(query).fetchall()
    symbols = [row[0] for row in result if row[0]]

    print(f"  Selected {len(symbols)} liquid stocks")
    print(f"  Liquidity threshold: > ₹1 Cr daily avg volume")

    # Step 1: Generate signals
    print(f"\n[STEP 1] Generating signals...")

    signal_gen = DuckDBSignalGenerator(config=ScanConfig())

    # Process in yearly chunks for better progress tracking
    all_signals = []
    for year in range(start_date.year, end_date.year + 1):
        chunk_start = date(year, 1, 1)
        chunk_end = date(year, 12, 31)

        print(f"  Processing {year}...", end="", flush=True)

        try:
            year_signals = signal_gen.generate_signals(symbols, chunk_start, chunk_end)
            all_signals.extend(year_signals)
            print(f" {len(year_signals)} signals")
        except Exception as e:
            print(f" ERROR: {e}")

    print(f"\n  Total signals: {len(all_signals)}")

    if not all_signals:
        print("\n  [ERROR] No signals generated!")
        return None

    # Signal distribution
    print(f"\n[SIGNAL DISTRIBUTION BY YEAR]")
    signal_by_year = {}
    for s in all_signals:
        year = s["trading_date"].year
        signal_by_year[year] = signal_by_year.get(year, 0) + 1

    for year in sorted(signal_by_year.keys()):
        print(f"  {year}: {signal_by_year[year]:4d} signals")

    print(f"\n[TOP 20 ACTIVE STOCKS]")
    signal_counts = {}
    for s in all_signals:
        symbol = s["symbol"]
        signal_counts[symbol] = signal_counts.get(symbol, 0) + 1

    for symbol, count in sorted(signal_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
        print(f"  {symbol}: {count} signals")

    # Step 2: Load price data
    print(f"\n[STEP 2] Loading price data...")

    signal_symbols = list(set(s["symbol"] for s in all_signals))
    symbol_to_id = {symbol: i for i, symbol in enumerate(signal_symbols)}
    id_to_symbol = {i: symbol for symbol, i in symbol_to_id.items()}

    price_data = {}
    value_traded_inr = {}

    for i, symbol in enumerate(signal_symbols):
        if (i + 1) % 50 == 0 or i == 0:
            print(f"  Loading {i + 1}/{len(signal_symbols)}...", flush=True)

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
            continue

    print(f"\n  Loaded {len(price_data)} symbols")

    # Step 3: Convert signals
    print(f"\n[STEP 3] Converting signals...")

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

    # Step 4: Run backtest
    print(f"\n[STEP 4] Running VectorBT backtest...", flush=True)

    config = VectorBTConfig(
        default_portfolio_value=1_000_000.0,
        risk_per_trade_pct=0.01,
        fees_per_trade=0.001,
        initial_stop_atr_mult=2.0,
        trail_activation_pct=0.05,
        trail_stop_pct=0.02,
        time_stop_days=3,
        follow_through_threshold=0.02,
    )

    engine = VectorBTEngine(config=config)
    result = engine.run_backtest(
        strategy_name="2LYNCH_5Year",
        signals=vbt_signals,
        price_data=price_data,
        value_traded_inr=value_traded_inr,
        delisting_dates=None,
    )

    # Step 5: Display results
    print(f"\n{'=' * 80}")
    print(f"5-YEAR BACKTEST RESULTS ({start_date} to {end_date})")
    print(f"{'=' * 80}")

    print(f"\n[OVERALL PERFORMANCE]")
    print(f"  Total Return:       {result.total_return * 100:>+10.2f}%")
    print(f"  Annualized Return:  {result.total_return / 5 * 100:>+10.2f}%")
    print(f"  Sharpe Ratio:       {result.sharpe_ratio:>10.2f}")
    print(f"  Max Drawdown:       {result.max_drawdown * 100:>10.2f}%")
    print(f"  Win Rate:           {result.win_rate * 100:>10.2f}%")
    print(f"  Profit Factor:      {result.profit_factor:>10.2f}")
    print(f"  Avg R:              {result.avg_r:>10.2f}R")
    print(f"  Median R:           {result.median_r:>10.2f}R")
    print(f"  Calmar Ratio:       {result.calmar_ratio:>10.2f}")

    if result.r_distribution:
        print(f"\n[R DISTRIBUTION]")
        print(f"  10th percentile:  {result.r_distribution.get('r_p10', 0):>10.2f}R")
        print(f"  25th percentile:  {result.r_distribution.get('r_p25', 0):>10.2f}R")
        print(f"  50th percentile:  {result.r_distribution.get('r_p50', 0):>10.2f}R")
        print(f"  75th percentile:  {result.r_distribution.get('r_p75', 0):>10.2f}R")
        print(f"  90th percentile:  {result.r_distribution.get('r_p90', 0):>10.2f}R")
        print(f"  Avg Winner:       {result.r_distribution.get('avg_winner_r', 0):>10.2f}R")
        print(f"  Avg Loser:        {result.r_distribution.get('avg_loser_r', 0):>10.2f}R")

    print(f"\n[TRADE SUMMARY]")
    print(f"  Total Trades:       {len(result.trades)}")

    if result.trades:
        winners = [t for t in result.trades if t.pnl and t.pnl > 0]
        losers = [t for t in result.trades if t.pnl and t.pnl < 0]

        print(
            f"  Winners:           {len(winners)} ({len(winners) / len(result.trades) * 100:.1f}%)"
        )
        print(f"  Losers:            {len(losers)} ({len(losers) / len(result.trades) * 100:.1f}%)")

        # Exit reasons
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
        win_rate_by_year = {}

        for t in result.trades:
            year = t.entry_date.year
            trades_by_year[year] = trades_by_year.get(year, 0) + 1
            if t.pnl:
                pnl_by_year[year] = pnl_by_year.get(year, 0) + t.pnl
            if t.pnl and t.pnl > 0:
                win_rate_by_year[year] = win_rate_by_year.get(year, 0) + 1

        for year in sorted(trades_by_year.keys()):
            num_trades = trades_by_year[year]
            total_pnl = pnl_by_year.get(year, 0)
            wins = win_rate_by_year.get(year, 0)
            win_rate = wins / num_trades * 100 if num_trades > 0 else 0
            avg_pnl = total_pnl / num_trades if num_trades > 0 else 0
            print(
                f"  {year}: {num_trades:4d} trades | P&L: {total_pnl:>+9.0f} | Win Rate: {win_rate:5.1f}% | Avg: {avg_pnl:>+7.0f}"
            )

        # Best trades
        print(f"\n[BEST 10 TRADES]")
        sorted_trades = sorted(result.trades, key=lambda t: t.pnl_r or 0, reverse=True)
        for i, trade in enumerate(sorted_trades[:10]):
            symbol = id_to_symbol.get(trade.symbol_id, trade.symbol)
            pnl_r_str = f">{trade.pnl_r:+.2f}R" if trade.pnl_r else "N/A"
            exit_str = trade.exit_reason.value if trade.exit_reason else "N/A"
            print(f"  {i + 1:2d}. {symbol:12s} | {trade.entry_date} | {pnl_r_str:>8s} | {exit_str}")

    print(f"\n{'=' * 80}")
    print(f"5-YEAR BACKTEST COMPLETED")
    print(f"{'=' * 80}\n")

    return result


if __name__ == "__main__":
    run_5year_backtest()
