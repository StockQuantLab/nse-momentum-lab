"""Comprehensive 2LYNCH filter analysis with 500 stocks.

Tests:
1. Different filter combinations (which specific filters work best?)
2. 500 stocks instead of 100
3. 10-year backtest period
"""

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


def analyze_filter_combinations():
    """Analyze which specific 2LYNCH filter combinations work best."""
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("COMPREHENSIVE 2LYNCH FILTER ANALYSIS")
    print("500 Stocks, 10 Years, VectorBT Backtesting")
    print("=" * 80)

    db = get_market_db()

    # Get top 500 liquid stocks
    symbols_query = """
    SELECT symbol, AVG(dollar_vol_20) as avg_vol
    FROM feat_daily
    WHERE trading_date >= '2014-01-01'
    GROUP BY symbol
    ORDER BY avg_vol DESC
    LIMIT 500
    """
    result = db.con.execute(symbols_query).fetchall()
    symbols = [row[0] for row in result if row[0]]

    start_date = date(2014, 1, 1)
    end_date = date(2024, 12, 31)

    print(f"\n[CONFIGURATION]")
    print(f"  Period: {start_date} to {end_date} (10 years)")
    print(f"  Symbols: {len(symbols)} top liquid stocks")
    print(f"  Backtesting Engine: VectorBT")

    # Test different close position thresholds
    close_thresholds = [0.50, 0.60, 0.70, 0.80]
    min_filters_options = [3, 4, 5]

    results = []

    for close_thresh in close_thresholds:
        for min_filters in min_filters_options:
            config_name = f"Close>={close_thresh:.0%}, {min_filters}/6 filters"

            print(f"\n{'=' * 80}")
            print(f"CONFIG: {config_name}")
            print(f"{'=' * 80}")

            config = ScanConfig(
                close_pos_threshold=close_thresh,
                min_filters_pass=min_filters,
            )

            signal_gen = CompleteDuckDBSignalGenerator(config=config)
            signals = signal_gen.generate_signals(symbols, start_date, end_date)

            print(f"  Signals generated: {len(signals)}")

            if len(signals) == 0:
                print(f"  No signals - skipping")
                continue

            # Analyze signal quality
            print(f"\n  Signal Quality Metrics:")
            avg_gap = sum(s["gap_pct"] for s in signals) / len(signals)
            print(f"    Avg gap: {avg_gap:.2%}")

            gap_distribution = [4, 5, 6, 7, 8, 10, 15]
            for threshold in gap_distribution:
                count = sum(1 for s in signals if s["gap_pct"] >= threshold / 100)
                print(f"    Gap >={threshold}%: {count} ({count / len(signals) * 100:.1f}%)")

            # Load price data
            signal_symbols = list(set(s["symbol"] for s in signals))
            symbol_to_id = {symbol: i for i, symbol in enumerate(signal_symbols)}

            price_data = {}
            value_traded_inr = {}

            print(f"\n  Loading price data for {len(signal_symbols)} symbols...")
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
                        avg_vol = features_df.select(
                            pl.col("dollar_vol_20").drop_nulls().mean()
                        ).item()
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

            results.append(
                {
                    "config": config_name,
                    "close_thresh": close_thresh,
                    "min_filters": min_filters,
                    "signals": len(signals),
                    "trades": len(result.trades),
                    "return": result.total_return * 100,
                    "win_rate": result.win_rate * 100,
                    "sharpe": result.sharpe_ratio,
                    "avg_r": result.avg_r,
                    "weak_ft_pct": 0,
                }
            )

            # Calculate exit reason percentages
            exit_reasons = {}
            for t in result.trades:
                if t.exit_reason:
                    reason = t.exit_reason.value
                    exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

            weak_ft_pct = (
                exit_reasons.get("WEAK_FOLLOW_THROUGH", 0) / len(result.trades) * 100
                if result.trades
                else 0
            )
            results[-1]["weak_ft_pct"] = weak_ft_pct

            print(f"\n  Backtest Results:")
            print(f"    Trades: {len(result.trades)}")
            print(f"    Return: {result.total_return * 100:+.2f}%")
            print(f"    Win Rate: {result.win_rate * 100:.1f}%")
            print(f"    Sharpe: {result.sharpe_ratio:.2f}")
            print(f"    Avg R: {result.avg_r:.2f}R")
            print(f"    Signals/Year: {len(signals) / 10:.1f}")

            print(f"\n  Exit Reasons:")
            for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
                pct = count / len(result.trades) * 100 if result.trades else 0
                print(f"    {reason}: {count} ({pct:.1f}%)")

    # Summary comparison
    print(f"\n{'=' * 80}")
    print(f"SUMMARY: ALL CONFIGURATIONS")
    print(f"{'=' * 80}")

    print(
        f"\n{'Config':<35} {'Signals':>10} {'Trades':>10} {'Return %':>12} {'Win %':>10} {'Sharpe':>8} {'Weak FT%':>10} {'R':>8}"
    )
    print(f"{'-' * 80}")

    for r in results:
        print(
            f"{r['config']:<35} {r['signals']:>10} {r['trades']:>10} "
            f"{r['return']:>11.2f}% {r['win_rate']:>9.1f}% "
            f"{r['sharpe']:>7.2f} {r['weak_ft_pct']:>9.1f}% {r['avg_r']:>7.2f}"
        )

    # Find best configurations by different metrics
    print(f"\n{'=' * 80}")
    print(f"BEST CONFIGURATIONS BY METRIC")
    print(f"{'=' * 80}")

    if results:
        best_return = max(results, key=lambda x: x["return"])
        best_winrate = max(results, key=lambda x: x["win_rate"])
        best_sharpe = max(results, key=lambda x: x["sharpe"])
        best_quality = min(results, key=lambda x: x["weak_ft_pct"])
        best_balance = max(results, key=lambda x: x["return"] * x["win_rate"] / 100)

        print(f"\n  Best Return:      {best_return['config']} ({best_return['return']:+.2f}%)")
        print(f"  Best Win Rate:    {best_winrate['config']} ({best_winrate['win_rate']:.1f}%)")
        print(f"  Best Sharpe:      {best_sharpe['config']} ({best_sharpe['sharpe']:.2f})")
        print(
            f"  Best Quality:     {best_quality['config']} ({best_quality['weak_ft_pct']:.1f}% weak FT)"
        )
        print(
            f"  Best Balance:     {best_balance['config']} ({best_balance['return']:+.2f}%, {best_balance['win_rate']:.1f}% win)"
        )

    # Analyze filter effectiveness
    print(f"\n{'=' * 80}")
    print(f"FILTER THRESHOLD ANALYSIS")
    print(f"{'=' * 80}")

    # Group by close threshold
    for close_thresh in close_thresholds:
        close_results = [r for r in results if r["close_thresh"] == close_thresh]
        if close_results:
            avg_return = sum(r["return"] for r in close_results) / len(close_results)
            avg_winrate = sum(r["win_rate"] for r in close_results) / len(close_results)
            avg_weak_ft = sum(r["weak_ft_pct"] for r in close_results) / len(close_results)
            print(f"\n  Close Position >= {close_thresh:.0%}:")
            print(f"    Avg Return: {avg_return:+.2f}%")
            print(f"    Avg Win Rate: {avg_winrate:.1f}%")
            print(f"    Avg Weak FT: {avg_weak_ft:.1f}%")

    # Group by min filters
    print(f"\n  By Minimum Filters Required:")
    for min_filters in min_filters_options:
        filter_results = [r for r in results if r["min_filters"] == min_filters]
        if filter_results:
            avg_return = sum(r["return"] for r in filter_results) / len(filter_results)
            avg_winrate = sum(r["win_rate"] for r in filter_results) / len(filter_results)
            avg_signals = sum(r["signals"] for r in filter_results) / len(filter_results)
            print(
                f"    {min_filters}/6 filters: {avg_signals:.0f} signals/yr, {avg_return:+.2f}% return, {avg_winrate:.1f}% win"
            )

    print(f"\n{'=' * 80}")
    print(f"TEST COMPLETE")
    print(f"{'=' * 80}\n")

    return results


if __name__ == "__main__":
    analyze_filter_combinations()
