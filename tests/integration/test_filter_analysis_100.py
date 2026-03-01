"""Quick 2LYNCH filter analysis with 100 stocks to show patterns."""

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine
from nse_momentum_lab.services.scan.duckdb_signal_generator_complete import (
    CompleteDuckDBSignalGenerator,
)
from nse_momentum_lab.services.scan.rules import ScanConfig


def quick_filter_analysis():
    """Quick filter analysis with 100 stocks."""
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("QUICK 2LYNCH FILTER ANALYSIS - 100 Stocks")
    print("=" * 80)

    db = get_market_db()

    # Get top 100 liquid stocks
    symbols_query = """
    SELECT symbol, AVG(dollar_vol_20) as avg_vol
    FROM feat_daily
    WHERE trading_date >= '2014-01-01'
    GROUP BY symbol
    ORDER BY avg_vol DESC
    LIMIT 100
    """
    result = db.con.execute(symbols_query).fetchall()
    symbols = [row[0] for row in result if row[0]]

    start_date = date(2014, 1, 1)
    end_date = date(2024, 12, 31)

    print(f"\nPeriod: {start_date} to {end_date} (10 years)")
    print(f"Symbols: {len(symbols)}")

    # Test key filter combinations
    configs = [
        ("Baseline: Close>=50%, 3/6", 0.50, 3),
        ("Moderate: Close>=60%, 3/6", 0.60, 3),
        ("Conservative: Close>=70%, 4/6", 0.70, 4),
        ("Strict: Close>=70%, 5/6", 0.70, 5),
        ("Liberal: Close>=50%, 4/6", 0.50, 4),
    ]

    results = []

    for config_name, close_thresh, min_filters in configs:
        print(f"\n{'=' * 80}")
        print(f"Testing: {config_name}")
        print(f"{'=' * 80}")

        config = ScanConfig(close_pos_threshold=close_thresh, min_filters_pass=min_filters)
        signal_gen = CompleteDuckDBSignalGenerator(config=config)

        signals = signal_gen.generate_signals(symbols, start_date, end_date)
        print(f"Signals: {len(signals)} ({len(signals) / 10:.1f}/year)")

        if not signals:
            continue

        # Quick backtest
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
                    price_data[symbol_id][row["date"]] = {
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
            except Exception:
                continue

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
            strategy_name=config_name,
            signals=vbt_signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
            delisting_dates=None,
        )

        # Calculate weak FT %
        exit_reasons = {}
        for t in result.trades:
            if t.exit_reason:
                exit_reasons[t.exit_reason.value] = exit_reasons.get(t.exit_reason.value, 0) + 1
        weak_ft_pct = exit_reasons.get("WEAK_FOLLOW_THROUGH", 0) / len(result.trades) * 100

        results.append(
            {
                "name": config_name,
                "signals": len(signals),
                "trades": len(result.trades),
                "return": result.total_return * 100,
                "win_rate": result.win_rate * 100,
                "sharpe": result.sharpe_ratio,
                "avg_r": result.avg_r,
                "weak_ft": weak_ft_pct,
            }
        )

        print(f"Trades: {len(result.trades)}")
        print(f"Return: {result.total_return * 100:+.2f}%")
        print(f"Win Rate: {result.win_rate * 100:.1f}%")
        print(f"Sharpe: {result.sharpe_ratio:.2f}")
        print(f"Weak FT: {weak_ft_pct:.1f}%")

    # Summary table
    print(f"\n{'=' * 80}")
    print(f"SUMMARY")
    print(f"{'=' * 80}")

    print(
        f"\n{'Config':<30} {'Sig/Yr':>8} {'Trades':>8} {'Return%':>10} {'Win%':>8} {'Sharpe':>8} {'WeakFT%':>8}"
    )
    print(f"{'-' * 80}")

    for r in results:
        sig_per_year = r["signals"] / 10
        print(
            f"{r['name']:<30} {sig_per_year:>7.1f} {r['trades']:>8} {r['return']:>9.2f}% {r['win_rate']:>7.1f}% {r['sharpe']:>7.2f} {r['weak_ft']:>7.1f}%"
        )

    print(f"\n{'=' * 80}")
    print(f"ANALYSIS")
    print(f"{'=' * 80}")

    if results:
        print(f"\nUsing: VectorBT for backtesting")
        print(f"Engine: Portfolio simulation with realistic slippage & fees")
        print(f"Fees: 0.1% per trade (STAX)")
        print(f"Risk: 1% per trade")
        print(f"Stop: 2x ATR initial, 5% trail activation, 2% trail")

        # Find best balance
        best = max(results, key=lambda x: x["return"] * x["win_rate"] / 100)
        print(f"\nBest balanced config: {best['name']}")
        print(f"  {best['return']:+.2f}% return, {best['win_rate']:.1f}% win rate")

    print(f"\n{'=' * 80}\n")


if __name__ == "__main__":
    quick_filter_analysis()
