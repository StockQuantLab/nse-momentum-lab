"""Test backtest with DuckDB for 10 stocks."""

from datetime import date

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTEngine


def main():
    print("Testing DuckDB + Parquet Backtest")
    print("=" * 60)

    # Get first 10 symbols
    db = get_market_db()
    symbols = db.get_available_symbols()[:10]
    print(f"\nTesting with symbols: {symbols}")

    # Load market data from DuckDB
    print("\nLoading market data from DuckDB...")
    engine = VectorBTEngine()
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)

    market_data = engine.load_market_data_from_duckdb(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )

    print(f"\nLoaded data for {len(market_data)} symbols:")
    for symbol, data in market_data.items():
        print(f"  {symbol}: {len(data)} trading days")

    # Load features from DuckDB
    print("\nLoading features from DuckDB...")
    features = engine.load_features_from_duckdb(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )

    print(f"\nLoaded features for {len(features)} symbols:")
    for symbol, data in features.items():
        print(f"  {symbol}: {len(data)} days with features")

    # Sample feature data
    if features:
        first_symbol = list(features.keys())[0]
        first_date = list(features[first_symbol].keys())[0]
        print(f"\nSample features for {first_symbol} on {first_date}:")
        for key, value in features[first_symbol][first_date].items():
            if value is not None:
                print(f"  {key}: {value:.4f}")

    print("\n" + "=" * 60)
    print("SUCCESS: DuckDB + Parquet integration is working!")
    print("Next step: Run full backtest with VectorBT engine")


if __name__ == "__main__":
    main()
