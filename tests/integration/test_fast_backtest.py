"""Fast 5-Year Backtest - Optimized for Quick Results.

Optimizations:
1. Use top 100 most liquid stocks (instead of 500)
2. Process all years in single query (not yearly chunks)
3. Skip symbols that have no signals early
"""

import sys
from datetime import date

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine
from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.rules import ScanConfig


def run_fast_backtest():
    """Run optimized 5-year backtest."""
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("FAST 5-YEAR BACKTEST - 2LYNCH STRATEGY")
    print("=" * 80)

    db = get_market_db()

    end_date = date(2024, 12, 31)
    start_date = date(2020, 1, 1)

    print(f"\n[CONFIGURATION]")
    print(f"  Period: {start_date} to {end_date} (5 years)")

    # Use top 100 most liquid stocks only
    print(f"\n[SYMBOL SELECTION]")
    query = """
    SELECT symbol, AVG(dollar_vol_20) as avg_vol
    FROM feat_daily
    WHERE trading_date >= '2020-01-01'
    GROUP BY symbol
    ORDER BY avg_vol DESC
    LIMIT 100
    """
    result = db.con.execute(query).fetchall()
    symbols = [row[0] for row in result if row[0]]

    print(f"  Selected top {len(symbols)} liquid stocks")
    print(f"  Symbols: {', '.join(symbols[:20])}...")

    # Single call to generate all signals
    print(f"\n[STEP 1] Generating signals...", flush=True)

    signal_gen = DuckDBSignalGenerator(config=ScanConfig())
    all_signals = signal_gen.generate_signals(symbols, start_date, end_date)

    print(f"  Generated {len(all_signals)} signals")

    if not all_signals:
        print("\n  [ERROR] No signals generated!")
        return None

    # Quick stats
    print(f"\n[SIGNAL STATS]")
    signal_by_year = {}
    for s in all_signals:
        year = s["trading_date"].year
        signal_by_year[year] = signal_by_year.get(year, 0) + 1

    for year in sorted(signal_by_year.keys()):
        print(f"  {year}: {signal_by_year[year]:4d} signals")

    # Load only symbols that have signals
    print(f"\n[STEP 2] Loading price data...", flush=True)

    signal_symbols = list(set(s["symbol"] for s in all_signals))
    symbol_to_id = {symbol: i for i, symbol in enumerate(signal_symbols)}
    id_to_symbol = {i: symbol for symbol, i in symbol_to_id.items()}

    price_data = {}
    value_traded_inr = {}

    for i, symbol in enumerate(signal_symbols):
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
        except Exception:
            continue

    print(f"  Loaded {len(price_data)} symbols")

    # Convert signals
    print(f"\n[STEP 3] Preparing backtest...", flush=True)

    vbt_signals = []
    for s in all_signals:
        if s["symbol"] not in symbol_to_id:
            continue
        vbt_signals.append(
            (
                s["trading_date"],
                symbol_to_id[s["symbol"]],
                s["symbol"],
                s["initial_stop"],
                {"gap_pct": s["gap_pct"], "atr": s.get("atr", 0.0)},
            )
        )

    print(f"  Converted {len(vbt_signals)} signals")

    # Run backtest
    print(f"\n[STEP 4] Running backtest...", flush=True)

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
        strategy_name="2LYNCH_Fast",
        signals=vbt_signals,
        price_data=price_data,
        value_traded_inr=value_traded_inr,
        delisting_dates=None,
    )

    # Results
    print(f"\n{'=' * 80}")
    print(f"RESULTS ({start_date} - {end_date})")
    print(f"{'=' * 80}")

    print(f"\n[PERFORMANCE]")
    print(f"  Total Return:       {result.total_return * 100:>+10.2f}%")
    print(f"  Annual Return:      {result.total_return / 5 * 100:>+10.2f}%")
    print(f"  Sharpe Ratio:       {result.sharpe_ratio:>10.2f}")
    print(f"  Max Drawdown:       {result.max_drawdown * 100:>10.2f}%")
    print(f"  Win Rate:           {result.win_rate * 100:>10.2f}%")
    print(f"  Profit Factor:      {result.profit_factor:>10.2f}")
    print(f"  Avg R:              {result.avg_r:>10.2f}R")
    print(f"  Median R:           {result.median_r:>10.2f}R")

    if result.r_distribution:
        print(f"\n[R METRICS]")
        print(f"  Avg Winner:         {result.r_distribution.get('avg_winner_r', 0):>10.2f}R")
        print(f"  Avg Loser:          {result.r_distribution.get('avg_loser_r', 0):>10.2f}R")
        print(f"  Max Winner:         {result.r_distribution.get('max_winner_r', 0):>10.2f}R")
        print(f"  Max Loser:          {result.r_distribution.get('max_loser_r', 0):>10.2f}R")

    print(f"\n[TRADES]")
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
                exit_reasons[t.exit_reason.value] = exit_reasons.get(t.exit_reason.value, 0) + 1

        print(f"\n[EXIT REASONS]")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"  {reason}: {count:4d} ({count / len(result.trades) * 100:5.1f}%)")

        # Yearly breakdown
        print(f"\n[YEARLY BREAKDOWN]")
        trades_by_year = {}
        pnl_by_year = {}
        wins_by_year = {}

        for t in result.trades:
            year = t.entry_date.year
            trades_by_year[year] = trades_by_year.get(year, 0) + 1
            if t.pnl:
                pnl_by_year[year] = pnl_by_year.get(year, 0) + t.pnl
            if t.pnl and t.pnl > 0:
                wins_by_year[year] = wins_by_year.get(year, 0) + 1

        for year in sorted(trades_by_year.keys()):
            n = trades_by_year[year]
            pnl = pnl_by_year.get(year, 0)
            wins = wins_by_year.get(year, 0)
            win_rate = wins / n * 100 if n > 0 else 0
            print(
                f"  {year}: {n:4d} trades | P&L: {pnl:>+8.0f} | Win%: {win_rate:5.1f}% | Avg: {pnl / n:>+7.0f}"
            )

    print(f"\n{'=' * 80}\n")

    return result


if __name__ == "__main__":
    run_fast_backtest()
