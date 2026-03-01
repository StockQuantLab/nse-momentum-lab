"""Compare old (incomplete) vs new (complete) 2LYNCH implementation.

This test validates that the complete ADR-007 compliant implementation
performs better than our current incomplete version.
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine
from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.duckdb_signal_generator_complete import (
    CompleteDuckDBSignalGenerator,
)
from nse_momentum_lab.services.scan.rules import ScanConfig


def compare_implementations():
    """Compare old vs new 2LYNCH implementation."""
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("2LYNCH IMPLEMENTATION COMPARISON")
    print("Old (Incomplete) vs New (ADR-007 Compliant)")
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

    start_date = date(2014, 1, 1)
    end_date = date(2024, 12, 31)

    print(f"\n[CONFIGURATION]")
    print(f"  Period: {start_date} to {end_date} (10 years)")
    print(f"  Symbols: {len(symbols)} top liquid stocks")

    results = {}

    # Test 1: Old implementation (incomplete)
    print(f"\n{'=' * 80}")
    print(f"TEST 1: OLD IMPLEMENTATION (Incomplete 2LYNCH)")
    print(f"{'=' * 80}")

    config_old = ScanConfig(
        close_pos_threshold=0.50,  # Wrong threshold
        min_filters_pass=3,
    )

    signal_gen_old = DuckDBSignalGenerator(config=config_old)
    signals_old = signal_gen_old.generate_signals(symbols, start_date, end_date)

    print(f"  Signals generated: {len(signals_old)}")
    print(f"  Issues:")
    print(f"    - R² set to 0.0 (not computed)")
    print(f"    - No 'not up 2 days' filter")
    print(f"    - No narrow range filter")
    print(f"    - Wrong close position threshold (0.50 vs 0.70)")

    # Test 2: New implementation (complete)
    print(f"\n{'=' * 80}")
    print(f"TEST 2: NEW IMPLEMENTATION (ADR-007 Compliant)")
    print(f"{'=' * 80}")

    config_new = ScanConfig(
        close_pos_threshold=0.70,  # ADR-007 compliant
        min_filters_pass=3,
    )

    signal_gen_new = CompleteDuckDBSignalGenerator(config=config_new)
    signals_new = signal_gen_new.generate_signals(symbols, start_date, end_date)

    print(f"  Signals generated: {len(signals_new)}")
    print(f"  Improvements:")
    print(f"    - ✓ 'Not up 2 days' filter added")
    print(f"    - ✓ Narrow range filter added")
    print(f"    - ✓ Close position threshold = 0.70 (ADR-007)")
    print(f"    - ✓ Volume > 100,000 filter")
    print(f"    - ✓ Price > ₹3 filter")
    print(f"    - ⚠ R² still 0.0 (needs rebuild)")

    # Run backtests for comparison
    print(f"\n{'=' * 80}")
    print(f"RUNNING BACKTESTS...")
    print(f"{'=' * 80}")

    for name, signals in [("OLD", signals_old), ("NEW", signals_new)]:
        if not signals:
            print(f"\n  [{name}] No signals - skipping")
            results[name] = None
            continue

        print(f"\n  [{name}] Processing {len(signals)} signals...")

        # Load price data
        signal_symbols = list(set(s["symbol"] for s in signals))
        symbol_to_id = {symbol: i for i, symbol in enumerate(signal_symbols)}
        id_to_symbol = {i: symbol for symbol, i in symbol_to_id.items()}

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
            strategy_name=f"2LYNCH_{name}",
            signals=vbt_signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
            delisting_dates=None,
        )

        results[name] = result

        print(f"    Trades: {len(result.trades)}")
        print(f"    Return: {result.total_return * 100:+.2f}%")
        print(f"    Win Rate: {result.win_rate * 100:.1f}%")
        print(f"    Sharpe: {result.sharpe_ratio:.2f}")

    # Compare results
    if results.get("OLD") and results.get("NEW"):
        print(f"\n{'=' * 80}")
        print(f"COMPARISON RESULTS")
        print(f"{'=' * 80}")

        old = results["OLD"]
        new = results["NEW"]

        print(f"\n{'METRIC':<25} {'OLD':>12} {'NEW':>12} {'CHANGE':>12}")
        print(f"{'-' * 80}")

        metrics = [
            ("Signals", len(signals_old), len(signals_new), len(signals_new) - len(signals_old)),
            ("Trades", len(old.trades), len(new.trades), len(new.trades) - len(old.trades)),
            (
                "Return %",
                old.total_return * 100,
                new.total_return * 100,
                new.total_return * 100 - old.total_return * 100,
            ),
            (
                "Win Rate %",
                old.win_rate * 100,
                new.win_rate * 100,
                new.win_rate * 100 - old.win_rate * 100,
            ),
            ("Sharpe", old.sharpe_ratio, new.sharpe_ratio, new.sharpe_ratio - old.sharpe_ratio),
            ("Avg R", old.avg_r, new.avg_r, new.avg_r - old.avg_r),
        ]

        for name, old_val, new_val, change in metrics:
            if isinstance(old_val, float) and isinstance(new_val, float):
                print(f"{name:<25} {old_val:>12.2f} {new_val:>12.2f} {change:>+12.2f}")
            else:
                print(f"{name:<25} {old_val:>12} {new_val:>12} {change:>+12}")

        # Exit reasons comparison
        print(f"\n{'EXIT REASONS':<25} {'OLD':>12} {'NEW':>12}")
        print(f"{'-' * 80}")

        old_exits = {}
        new_exits = {}

        for t in old.trades:
            if t.exit_reason:
                reason = t.exit_reason.value
                old_exits[reason] = old_exits.get(reason, 0) + 1

        for t in new.trades:
            if t.exit_reason:
                reason = t.exit_reason.value
                new_exits[reason] = new_exits.get(reason, 0) + 1

        all_reasons = set(list(old_exits.keys()) + list(new_exits.keys()))
        for reason in sorted(all_reasons):
            old_count = old_exits.get(reason, 0)
            new_count = new_exits.get(reason, 0)
            old_pct = old_count / len(old.trades) * 100 if old.trades else 0
            new_pct = new_count / len(new.trades) * 100 if new.trades else 0
            print(f"{reason:<25} {old_pct:>11.1f}% {new_pct:>11.1f}%")

    print(f"\n{'=' * 80}")
    print(f"TEST COMPLETE")
    print(f"{'=' * 80}\n")

    return results


if __name__ == "__main__":
    compare_implementations()
