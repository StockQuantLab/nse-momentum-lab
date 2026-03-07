"""Run simple backtest using DuckDB data."""

from datetime import date

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine


def main():
    print("Running Backtest with DuckDB + Parquet Data")
    print("=" * 60)

    # Get first 10 symbols
    db = get_market_db()
    symbols = db.get_available_symbols()[:10]
    print(f"\nSymbols: {symbols}")

    # Configure backtest
    config = VectorBTConfig(
        breakout_threshold=0.04,  # 4% gap-up
        risk_per_trade_pct=0.01,  # 1% risk per trade
        initial_stop_atr_mult=2.0,
        time_stop_days=3,
    )

    # Initialize engine
    engine = VectorBTEngine(config)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)

    # Load data from DuckDB (10-100x faster!)
    print(f"\nLoading data for {start_date} to {end_date}...")
    market_data = engine.load_market_data_from_duckdb(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )

    # Load features from DuckDB
    features = engine.load_features_from_duckdb(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )

    print("\nData loaded successfully!")
    print(f"  Market data: {sum(len(v) for v in market_data.values()):,} candles")
    print(f"  Features: {sum(len(v) for v in features.values()):,} feature-records")

    # TODO: Implement signal generation and backtest execution
    # This requires integrating with the scan/strategy logic
    print("\n" + "=" * 60)
    print("Data loading successful!")
    print("Next: Integrate with scan rules and run VectorBT backtest")
    print("\nNote: Full backtest execution requires:")
    print("  1. Signal generation (gap-up detection)")
    print("  2. Entry/exit logic")
    print("  3. VectorBT portfolio simulation")
    print("\nFor now, we've validated the DuckDB data pipeline!")


if __name__ == "__main__":
    main()
