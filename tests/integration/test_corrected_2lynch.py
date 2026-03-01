"""Test corrected 2LYNCH implementation.

Key fixes:
1. Narrow range filter now correctly rejects wide-range days
2. Uses config.min_filters_pass for threshold
3. Adjustable R² threshold (lower for approximate R² values)
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    VectorBTConfig,
    VectorBTEngine,
)
from nse_momentum_lab.services.scan.duckdb_signal_generator_complete import (
    CompleteDuckDBSignalGenerator,
)
from nse_momentum_lab.services.scan.rules import ScanConfig


def run_corrected_backtest():
    """Run backtest with corrected filters."""
    print("\n" + "=" * 80)
    print("CORRECTED 2LYNCH BACKTEST")
    print("=" * 80)

    db = get_market_db()

    # Get top 100 liquid stocks
    symbols_query = """
    SELECT symbol, AVG(dollar_vol_20) as avg_vol
    FROM feat_daily
    WHERE trading_date >= '2020-01-01'
    GROUP BY symbol
    ORDER BY avg_vol DESC
    LIMIT 100
    """
    result = db.con.execute(symbols_query).fetchall()
    symbols = [row[0] for row in result if row[0]]

    start_date = date(2020, 1, 1)
    end_date = date(2024, 12, 31)

    print(f"\n[CONFIGURATION]")
    print(f"  Period: {start_date} to {end_date} (5 years)")
    print(f"  Symbols: {len(symbols)} top liquid stocks")

    # Test different R² thresholds (since our R² values are approximate)
    configs = [
        (
            "R² >= 0.90 (strict)",
            ScanConfig(
                close_pos_threshold=0.70,
                min_r2_l=0.90,
                min_filters_pass=4,
            ),
        ),
        (
            "R² >= 0.85",
            ScanConfig(
                close_pos_threshold=0.70,
                min_r2_l=0.85,
                min_filters_pass=4,
            ),
        ),
        (
            "R² >= 0.80",
            ScanConfig(
                close_pos_threshold=0.70,
                min_r2_l=0.80,
                min_filters_pass=4,
            ),
        ),
    ]

    results = {}

    for config_name, config in configs:
        print(f"\n{'=' * 80}")
        print(f"CONFIG: {config_name}")
        print(f"{'=' * 80}")

        signal_gen = CompleteDuckDBSignalGenerator(config=config)
        signals = signal_gen.generate_signals(symbols, start_date, end_date)

        print(f"  Signals generated: {len(signals)}")

        if len(signals) == 0:
            print("  No signals - skipping")
            results[config_name] = None
            continue

        # Load price data
        signal_symbols = list(set(s["symbol"] for s in signals))
        symbol_to_id = {symbol: i for i, symbol in enumerate(signal_symbols)}

        price_data = {}
        value_traded_inr = {}

        for symbol in signal_symbols:
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

        # Convert signals
        vbt_signals = []
        for s in signals:
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

        # Run backtest
        vbt_config = VectorBTConfig(
            default_portfolio_value=1_000_000.0,
            risk_per_trade_pct=0.01,
            fees_per_trade=0.001,
            initial_stop_atr_mult=2.0,
            trail_activation_pct=0.05,
            trail_stop_pct=0.02,
            time_stop_days=3,
            follow_through_threshold=0.02,
        )

        engine = VectorBTEngine(config=vbt_config)
        result = engine.run_backtest(
            strategy_name=f"2LYNCH_{config_name}",
            signals=vbt_signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
            delisting_dates=None,
        )

        results[config_name] = result

        print(f"    Trades: {len(result.trades)}")
        print(f"    Return: {result.total_return * 100:+.2f}%")
        print(f"    Win Rate: {result.win_rate * 100:.1f}%")
        print(f"    Sharpe: {result.sharpe_ratio:.2f}")

    # Compare results
    print(f"\n{'=' * 80}")
    print(f"COMPARISON")
    print(f"{'=' * 80}")

    print(
        f"\n{'Config':<25} {'Signals':>10} {'Trades':>10} {'Return %':>12} {'Win Rate %':>12} {'Sharpe':>10}"
    )
    print(f"{'-' * 80}")

    for config_name in configs:
        name = config_name[0]
        result = results.get(name)
        if result:
            signal_count = next(
                (
                    len(s.generate_signals(symbols, start_date, end_date))
                    for n, s in [
                        (name, CompleteDuckDBSignalGenerator(config=c))
                        for c_name, c in configs
                        if c_name == name
                    ]
                ),
                0,
            )
            print(
                f"{name:<25} {signal_count:>10} {len(result.trades):>10} "
                f"{result.total_return * 100:>11.2f}% {result.win_rate * 100:>11.1f}% "
                f"{result.sharpe_ratio:>9.2f}"
            )

    print(f"\n{'=' * 80}")
    print(f"TEST COMPLETE")
    print(f"{'=' * 80}\n")

    return results


if __name__ == "__main__":
    run_corrected_backtest()
