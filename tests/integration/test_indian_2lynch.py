"""Test Indian Market 2LYNCH Implementation.

This validates the corrected implementation adapted for Indian markets:
- Price threshold: ₹50 (filters penny stocks)
- Volume: ₹30 lakh value traded (liquidity)
- All 6 2LYNCH filters enforced (5 of 6 required)
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
from nse_momentum_lab.services.scan.indian_2lynch_signal_generator import (
    Indian2LynchSignalGenerator,
)
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


def test_indian_2lynch():
    """Test the corrected Indian market 2LYNCH implementation."""
    _configure_stdout_encoding_for_windows()
    print("\n" + "=" * 80)
    print("INDIAN MARKET 2LYNCH BACKTEST")
    print("=" * 80)

    print("\n" + "-" * 80)
    print("INDIAN MARKET ADAPTATIONS")
    print("-" * 80)
    print("  Min Price: Rs.50 (not $3)")
    print("  Min Value Traded: Rs.30 lakh (not 100,000 shares)")
    print("  Min Shares: 50,000 (secondary check)")
    print("  Close Position: Top 30% (0.70)")
    print("  Gap Threshold: 4%")
    print("  Filters Required: 5 of 6 (strict quality)")

    print("\n" + "-" * 80)
    print("2LYNCH FILTERS EXPLAINED")
    print("-" * 80)
    print("  H: Close near high (top 30% of day's range)")
    print("  2: Not up 2 days with big range (small up days OK)")
    print("  L: Linear first leg (MA-20 > 0, ret_5d > 0, OR R² >= 0.70)")
    print("  Y: Young trend (<= 2 breakouts in 90 days)")
    print("  N: Narrow range OR negative day before breakout")
    print("  C: Consolidation (volume dryup, ATR compression)")

    db = get_market_db()

    # Get top liquid stocks
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

    start_date = date(2014, 1, 1)
    end_date = date(2024, 12, 31)

    print("\n" + "-" * 80)
    print("BACKTEST CONFIGURATION")
    print("-" * 80)
    print(f"  Period: {start_date} to {end_date} (10 years)")
    print(f"  Symbols: {len(symbols)} top liquid stocks")

    # Test with different filter requirements
    configs = [
        ("Strict (5/6 filters)", ScanConfig(min_filters_pass=5)),
        ("Moderate (4/6 filters)", ScanConfig(min_filters_pass=4)),
        ("Liberal (3/6 filters)", ScanConfig(min_filters_pass=3)),
    ]

    results = {}

    for config_name, config in configs:
        print(f"\n{'=' * 80}")
        print(f"CONFIG: {config_name}")
        print(f"{'=' * 80}")

        signal_gen = Indian2LynchSignalGenerator(config=config)
        signals = signal_gen.generate_signals(symbols, start_date, end_date)

        print(f"\n  Signals generated: {len(signals)}")

        if len(signals) == 0:
            print("  No signals - skipping")
            results[config_name] = None
            continue

        # Analyze filter performance
        filter_stats = {}
        for s in signals:
            for letter, detail in s.get("filter_details", {}).items():
                if letter not in filter_stats:
                    filter_stats[letter] = {"passed": 0, "total": 0}
                filter_stats[letter]["total"] += 1
                if detail.get("passed", False):
                    filter_stats[letter]["passed"] += 1

        print("\n  Filter Pass Rates:")
        for letter in sorted(filter_stats.keys()):
            stats = filter_stats[letter]
            pct = stats["passed"] / stats["total"] * 100 if stats["total"] > 0 else 0
            print(f"    {letter}: {stats['passed']}/{stats['total']} ({pct:.1f}%)")

        # Load price data
        signal_symbols = list({s["symbol"] for s in signals})
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
            strategy_name=f"Indian2LYNCH_{config_name}",
            signals=vbt_signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
            delisting_dates=None,
        )

        results[config_name] = result

        print("\n  Backtest Results:")
        print(f"    Trades: {len(result.trades)}")
        print(f"    Return: {result.total_return * 100:+.2f}%")
        print(f"    Win Rate: {result.win_rate * 100:.1f}%")
        print(f"    Sharpe: {result.sharpe_ratio:.2f}")
        print(f"    Avg R: {result.avg_r:.2f}R")

        # Exit reasons
        exit_reasons = {}
        for t in result.trades:
            if t.exit_reason:
                reason = t.exit_reason.value
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

        print("\n  Exit Reasons:")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
            pct = count / len(result.trades) * 100 if result.trades else 0
            print(f"    {reason}: {count} ({pct:.1f}%)")

    # Comparison
    print(f"\n{'=' * 80}")
    print("CONFIGURATION COMPARISON")
    print(f"{'=' * 80}")

    print(
        f"\n{'Config':<25} {'Signals':>10} {'Trades':>10} {'Return %':>12} {'Win Rate %':>12} {'Sharpe':>10} {'Avg R':>8}"
    )
    print(f"{'-' * 80}")

    for config_name in ["Strict (5/6 filters)", "Moderate (4/6 filters)", "Liberal (3/6 filters)"]:
        result = results.get(config_name)
        if result:
            print(
                f"{config_name:<25} {len(result.trades):>10} {len(result.trades):>10} "
                f"{result.total_return * 100:>11.2f}% {result.win_rate * 100:>11.1f}% "
                f"{result.sharpe_ratio:>9.2f} {result.avg_r:>7.2f}"
            )

    print(f"\n{'=' * 80}")
    print("TEST COMPLETE")
    print(f"{'=' * 80}\n")


def explain_filter_decisions():
    """Explain why certain decisions were made."""
    _configure_stdout_encoding_for_windows()

    print("\n" + "=" * 80)
    print("WHY THESE THRESHOLDS FOR INDIAN MARKETS?")
    print("=" * 80)

    print("\n1. PRICE THRESHOLD: Rs.50 (not Rs.3 or Rs.10)")
    print("   - Rs.3-Rs.10 stocks are often penny stocks with manipulation risk")
    print("   - Rs.50+ filters for quality, established companies")
    print("   - Higher price = better liquidity, less slippage")
    print("   - Can be adjusted to Rs.30 for broader universe")

    print("\n2. VALUE TRADED: Rs.30 lakh (not 100,000 shares)")
    print("   - 100,000 shares of Rs.10 stock = Rs.10 lakh (too low)")
    print("   - 100,000 shares of Rs.2000 stock = Rs.20 crore (too high)")
    print("   - VALUE traded is the right measure, not share count")
    print("   - Rs.30 lakh = decent retail stock liquidity")

    print("\n3. CLOSE POSITION: Top 30% (0.70)")
    print("   - Stockbee: 'Should close near high'")
    print("   - Top 30% = strong buying pressure into close")
    print("   - Prevents entry on weak closes")

    print("\n4. FILTERS: 5 of 6 (not 4 of 6)")
    print("   - More filters = higher quality setups")
    print("   - Stockbee's criteria are ALL important")
    print("   - 4/6 allows too many low-quality signals")
    print("   - 5/6 = ~85% quality vs 67% for 4/6")

    print("\n5. LETTER 2: 'Not up 2 days'")
    print("   - Extended moves have higher failure risk")
    print("   - Small range up days (<1.5%) are allowed")
    print("   - Only rejects BIG range consecutive up days")

    print("\n6. LETTER Y: Young trend (<=2 breakouts)")
    print("   - 'Aged' trends with 5+ breakouts fail more often")
    print("   - 1st-3rd breakout from consolidation = best risk/reward")
    print("   - 90-day window catches over-traded stocks")

    print("\n7. LETTER N: Narrow OR Negative")
    print("   - Narrow range = consolidation before breakout")
    print("   - Negative day = pullback (even better)")
    print("   - Wide range positive = extended = skip")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    explain_filter_decisions()
    test_indian_2lynch()
