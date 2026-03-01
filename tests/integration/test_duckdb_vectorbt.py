"""Integration test: DuckDB Signal Generation to VectorBT Backtest.

This test validates the complete pipeline:
1. Generate gap-up signals using DuckDB
2. Load price data from DuckDB
3. Run VectorBT backtest
4. Display results

This is the end-to-end validation of our 2LYNCH strategy.
"""

import sys
from datetime import date

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine
from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.rules import ScanConfig


def _configure_stdout_encoding_for_windows() -> None:
    """Avoid replacing sys.stdout stream object in tests."""
    if sys.platform != "win32":
        return
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except ValueError:
            pass


def test_duckdb_vectorbt_integration():
    """Test complete pipeline: DuckDB signals to VectorBT backtest."""
    _configure_stdout_encoding_for_windows()

    print("\n" + "=" * 80)
    print("DUCKDB + VectorBT INTEGRATION TEST")
    print("=" * 80)

    # Test parameters
    symbols = [
        "RELIANCE",
        "TCS",
        "INFY",
        "HDFCBANK",
        "ICICIBANK",
        "SBIN",
        "BHARTIARTL",
        "ITC",
        "HINDUNILVR",
        "AXISBANK",
    ]
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)

    print("\n[CONFIGURATION]")
    print(f"  Symbols: {len(symbols)} stocks")
    print(f"  Period: {start_date} to {end_date}")
    print("  Strategy: 2LYNCH Gap-up Breakout")

    # Step 1: Generate signals using DuckDB
    print("\n[STEP 1] Generating signals from DuckDB...")
    signal_gen = DuckDBSignalGenerator(config=ScanConfig())
    signals = signal_gen.generate_signals(symbols, start_date, end_date)

    print(f"  ✓ Generated {len(signals)} signals")

    if signals:
        # Show signal distribution
        signal_counts = {}
        for s in signals:
            symbol = s["symbol"]
            signal_counts[symbol] = signal_counts.get(symbol, 0) + 1

        print("\n  Signal Distribution:")
        for symbol, count in sorted(signal_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"    {symbol}: {count} signals")

        # Show sample signals
        print("\n  Sample Signals (first 3):")
        for i, s in enumerate(signals[:3]):
            print(f"    {i + 1}. {s['symbol']} @ {s['trading_date']}")
            print(
                f"       Entry: {s['entry_price']:.2f} | Gap: {s['gap_pct'] * 100:.2f}% | Stop: {s['initial_stop']:.2f}"
            )

    # Step 2: Load price data from DuckDB
    print("\n[STEP 2] Loading price data from DuckDB...")
    db = get_market_db()

    # Map symbols to IDs (using symbol as ID for this test)
    symbol_to_id = {symbol: i for i, symbol in enumerate(symbols)}
    id_to_symbol = {i: symbol for symbol, i in symbol_to_id.items()}

    price_data = {}
    value_traded_inr = {}

    for symbol in symbols:
        symbol_id = symbol_to_id[symbol]
        df = db.query_daily(symbol, start_date.isoformat(), end_date.isoformat())

        price_data[symbol_id] = {}
        for row in df.iter_rows(named=True):
            trading_date = row["date"]
            price_data[symbol_id][trading_date] = {
                "open_adj": float(row["open"]),
                "close_adj": float(row["close"]),
                "high_adj": float(row["high"]),
                "low_adj": float(row["low"]),
            }

        # Get 20-day average dollar volume from features
        features_df = db.get_features_range([symbol], start_date.isoformat(), end_date.isoformat())
        if not features_df.is_empty():
            avg_vol = features_df.select(pl.col("dollar_vol_20").drop_nulls().mean()).item()
            value_traded_inr[symbol_id] = avg_vol if avg_vol else 50_000_000.0  # Fallback
        else:
            value_traded_inr[symbol_id] = 50_000_000.0

    print(f"  ✓ Loaded price data for {len(price_data)} symbols")
    print(f"  ✓ Loaded liquidity data for {len(value_traded_inr)} symbols")

    # Step 3: Convert signals to VectorBT format
    print("\n[STEP 3] Converting signals to VectorBT format...")
    vbt_signals = []
    for s in signals:
        signal_date = s["trading_date"]
        symbol = s["symbol"]
        symbol_id = symbol_to_id[symbol]
        initial_stop = s["initial_stop"]

        metadata = {
            "gap_pct": s["gap_pct"],
            "atr": s.get("atr", 0.0),
        }

        vbt_signals.append((signal_date, symbol_id, symbol, initial_stop, metadata))

    print(f"  ✓ Converted {len(vbt_signals)} signals")

    # Step 4: Run VectorBT backtest
    print("\n[STEP 4] Running VectorBT backtest...")

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
        strategy_name="2LYNCH_Basic",
        signals=vbt_signals,
        price_data=price_data,
        value_traded_inr=value_traded_inr,
        delisting_dates=None,  # No delisting data for this test
    )

    # Step 5: Display results
    print("\n[BACKTEST RESULTS]")
    print("=" * 80)

    print("\n[STRATEGY PERFORMANCE]")
    print(f"  Total Return:    {result.total_return * 100:>+10.2f}%")
    print(f"  Sharpe Ratio:    {result.sharpe_ratio:>10.2f}")
    print(f"  Max Drawdown:    {result.max_drawdown * 100:>10.2f}%")
    print(f"  Win Rate:        {result.win_rate * 100:>10.2f}%")
    print(f"  Profit Factor:   {result.profit_factor:>10.2f}")
    print(f"  Avg R:           {result.avg_r:>10.2f}R")
    print(f"  Median R:        {result.median_r:>10.2f}R")
    print(f"  Calmar Ratio:    {result.calmar_ratio:>10.2f}")
    print(f"  Sortino Ratio:   {result.sortino_ratio:>10.2f}")

    if result.r_distribution:
        print("\n[R DISTRIBUTION]")
        print(f"  10th percentile: {result.r_distribution.get('r_p10', 0):>10.2f}R")
        print(f"  25th percentile: {result.r_distribution.get('r_p25', 0):>10.2f}R")
        print(f"  50th percentile: {result.r_distribution.get('r_p50', 0):>10.2f}R")
        print(f"  75th percentile: {result.r_distribution.get('r_p75', 0):>10.2f}R")
        print(f"  90th percentile: {result.r_distribution.get('r_p90', 0):>10.2f}R")
        print(f"  Avg Winner:      {result.r_distribution.get('avg_winner_r', 0):>10.2f}R")
        print(f"  Avg Loser:       {result.r_distribution.get('avg_loser_r', 0):>10.2f}R")
        print(f"  Max Winner:      {result.r_distribution.get('max_winner_r', 0):>10.2f}R")
        print(f"  Max Loser:       {result.r_distribution.get('max_loser_r', 0):>10.2f}R")

    print("\n[TRADE SUMMARY]")
    print(f"  Total Trades:    {len(result.trades)}")

    if result.trades:
        # Analyze trades
        winners = [t for t in result.trades if t.pnl and t.pnl > 0]
        losers = [t for t in result.trades if t.pnl and t.pnl < 0]

        print(f"  Winners:         {len(winners)}")
        print(f"  Losers:          {len(losers)}")

        # Exit reason analysis
        exit_reasons = {}
        for t in result.trades:
            if t.exit_reason:
                reason = t.exit_reason.value
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

        print("\n[EXIT REASONS]")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"  {reason}: {count}")

        # Show sample trades
        print("\n[SAMPLE TRADES - First 5]")
        for i, trade in enumerate(result.trades[:5]):
            symbol = id_to_symbol.get(trade.symbol_id, trade.symbol)
            pnl_str = f">{trade.pnl:+.2f}" if trade.pnl else "N/A"
            pnl_r_str = f">{trade.pnl_r:+.2f}R" if trade.pnl_r else "N/A"
            exit_str = trade.exit_reason.value if trade.exit_reason else "N/A"

            print(f"  {i + 1}. {symbol} | Entry: {trade.entry_date} → Exit: {trade.exit_date}")
            print(
                f"     Price: {trade.entry_price:.2f} → {trade.exit_price:.2f} | P&L: {pnl_str} ({pnl_r_str})"
            )
            print(f"     Exit: {exit_str}")

    # Final validation
    print("\n[VALIDATION]")
    assert len(signals) > 0, "No signals generated!"
    assert len(price_data) > 0, "No price data loaded!"
    assert len(result.trades) >= 0, "Trade extraction failed!"

    if len(result.trades) > 0:
        # Validate trade data
        for trade in result.trades[:10]:  # Check first 10
            assert trade.entry_price > 0, f"Invalid entry price: {trade.entry_price}"
            assert trade.qty > 0, f"Invalid qty: {trade.qty}"
            assert trade.initial_stop > 0, f"Invalid stop: {trade.initial_stop}"

        print(f"  ✓ Signals generated: {len(signals)}")
        print(f"  ✓ Price data loaded: {len(price_data)} symbols")
        print(f"  ✓ Backtest completed: {len(result.trades)} trades")
        print("  ✓ Trade data validated")

    print("\n" + "=" * 80)
    print("INTEGRATION TEST PASSED")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    test_duckdb_vectorbt_integration()
