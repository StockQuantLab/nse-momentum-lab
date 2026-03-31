"""Test signal generation with DuckDB for 10 stocks."""

from datetime import date

from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.rules import ScanConfig


def main():
    print("Testing DuckDB Signal Generation for 2LYNCH Strategy")
    print("=" * 60)

    # Get first 10 symbols
    from nse_momentum_lab.db.market_db import get_market_db

    db = get_market_db()
    symbols = db.get_available_symbols()[:10]

    print(f"\nTesting with symbols: {symbols}")
    print("Date range: 2024-01-01 to 2024-12-31")

    # Configure signal generator
    config = ScanConfig(
        breakout_threshold=0.04,  # 4% gap-up
        close_pos_threshold=0.70,  # Close in top 30% of range
    )

    generator = DuckDBSignalGenerator(config)

    # Generate signals
    print("\nGenerating signals...")
    signals = generator.generate_signals(
        symbols=symbols,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
    )

    print(f"\nFound {len(signals)} signals:")
    for signal in signals[:20]:  # Show first 20
        print(
            f"  {signal['symbol']}: {signal['trading_date']} | "
            f"Gap: {signal['gap_pct']:.2%} | "
            f"Entry: {signal['entry_price']:.2f} | "
            f"Stop: {signal['initial_stop']:.2f}"
        )

    # Show statistics
    if signals:
        by_symbol = {}
        for s in signals:
            by_symbol[s["symbol"]] = by_symbol.get(s["symbol"], 0) + 1

        print("\nSignals by symbol:")
        for symbol, count in sorted(by_symbol.items(), key=lambda x: x[1], reverse=True):
            print(f"  {symbol}: {count} signals")

    print("\n" + "=" * 60)
    print("SUCCESS: DuckDB signal generation is working!")
    print("\nNext steps:")
    print("  1. Add more 2LYNCH filters (NR, R², ATR compression, etc.)")
    print("  2. Integrate with VectorBT backtest engine")
    print("  3. Store signals in PostgreSQL")
    print("  4. Run full backtest")


if __name__ == "__main__":
    main()
