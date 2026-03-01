"""Compare 2LYNCH filtered vs unfiltered backtest results.

This test runs two backtests:
1. Unfiltered: Original gap-up signals (baseline)
2. Filtered: 2LYNCH filters applied

Then compares the results to show filter effectiveness.
"""

import sys
from datetime import date

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    VectorBTConfig,
    VectorBTEngine,
)
from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.rules import ScanConfig


class FilterConfig:
    """Configuration for filter testing."""

    ENABLE_ALL_FILTERS = True


def run_comparison_backtest():
    """Run filtered vs unfiltered comparison."""
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("2LYNCH FILTERS vs UNFILTERED COMPARISON")
    print("=" * 80)

    # Test parameters
    symbols_query = """
    SELECT symbol, AVG(dollar_vol_20) as avg_vol
    FROM feat_daily
    WHERE trading_date >= '2020-01-01'
    GROUP BY symbol
    ORDER BY avg_vol DESC
    LIMIT 100
    """

    db = get_market_db()
    result = db.con.execute(symbols_query).fetchall()
    symbols = [row[0] for row in result if row[0]]

    start_date = date(2020, 1, 1)
    end_date = date(2024, 12, 31)

    print(f"\n[CONFIGURATION]")
    print(f"  Period: {start_date} to {end_date}")
    print(f"  Symbols: {len(symbols)} top liquid stocks")

    # Test 1: Unfiltered (baseline)
    print(f"\n{'=' * 80}")
    print(f"TEST 1: UNFILTERED (BASELINE)")
    print(f"{'=' * 80}")

    config_baseline = ScanConfig()
    config_baseline.min_filters_pass = 0  # Disable all filters

    signal_gen_baseline = DuckDBSignalGenerator(config=config_baseline)
    signals_baseline = signal_gen_baseline.generate_signals(symbols, start_date, end_date)

    print(f"\n  Signals generated: {len(signals_baseline)}")

    # Test 2: Filtered (2LYNCH)
    print(f"\n{'=' * 80}")
    print(f"TEST 2: 2LYNCH FILTERED")
    print(f"{'=' * 80}")

    config_filtered = ScanConfig()
    config_filtered.min_filters_pass = 4  # Require 4/6 filters to pass

    signal_gen_filtered = DuckDBSignalGenerator(config=config_filtered)
    signals_filtered = signal_gen_filtered.generate_signals(symbols, start_date, end_date)

    print(f"\n  Signals generated: {len(signals_filtered)}")
    print(
        f"  Filtered out: {len(signals_baseline) - len(signals_filtered)} signals ({(len(signals_baseline) - len(signals_filtered)) / len(signals_baseline) * 100:.1f}%)"
    )

    # Run backtests
    print(f"\n{'=' * 80}")
    print(f"RUNNING BACKTESTS...")
    print(f"{'=' * 80}")

    results = {}

    for name, signals, config in [
        ("Baseline", signals_baseline, config_baseline),
        ("2LYNCH", signals_filtered, config_filtered),
    ]:
        if not signals:
            print(f"\n  [{name}] No signals - skipping")
            results[name] = None
            continue

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
        print(f"\n  [{name}] {len(result.trades)} trades")

    # Compare results
    print(f"\n{'=' * 80}")
    print(f"COMPARISON RESULTS")
    print(f"{'=' * 80}")

    if results.get("Baseline") and results.get("2LYNCH"):
        baseline = results["Baseline"]
        filtered = results["2LYNCH"]

        print(f"\n{'METRIC':<25} {'BASELINE':>15} {'2LYNCH':>15} {'CHANGE':>15}")
        print(f"{'-' * 80}")

        metrics = [
            ("Total Return (%)", lambda r: r.total_return * 100),
            ("Annual Return (%)", lambda r: r.total_return / 5 * 100),
            ("Sharpe Ratio", lambda r: r.sharpe_ratio),
            ("Max Drawdown (%)", lambda r: r.max_drawdown * 100),
            ("Win Rate (%)", lambda r: r.win_rate * 100),
            ("Avg R", lambda r: r.avg_r),
            ("Median R", lambda r: r.median_r),
            ("Profit Factor", lambda r: r.profit_factor),
        ]

        for name, metric_fn in metrics:
            base_val = metric_fn(baseline)
            filt_val = metric_fn(filtered)
            change = filt_val - base_val
            change_pct = (change / abs(base_val) * 100) if base_val != 0 else 0

            if "Return" in name or "Drawdown" in name or "Rate" in name:
                print(f"{name:<25} {base_val:>15.2f} {filt_val:>15.2f} {change:>+15.2f}")
            else:
                print(f"{name:<25} {base_val:>15.2f} {filt_val:>15.2f} {change:>+15.2f}")

        # Trade comparison
        print(f"\n{'TRADE ANALYSIS':<25} {'BASELINE':>15} {'2LYNCH':>15} {'CHANGE':>15}")
        print(f"{'-' * 80}")

        print(
            f"{'Total Trades':<25} {len(baseline.trades):>15} {len(filtered.trades):>15} {len(filtered.trades) - len(baseline.trades):>+15d}"
        )

        if baseline.trades and filtered.trades:
            base_winners = len([t for t in baseline.trades if t.pnl and t.pnl > 0])
            filt_winners = len([t for t in filtered.trades if t.pnl and t.pnl > 0])

            print(
                f"{'Winners':<25} {base_winners:>15} {filt_winners:>15} {filt_winners - base_winners:>+15d}"
            )

            base_losers = len([t for t in baseline.trades if t.pnl and t.pnl < 0])
            filt_losers = len([t for t in filtered.trades if t.pnl and t.pnl < 0])

            print(
                f"{'Losers':<25} {base_losers:>15} {filt_losers:>15} {filt_losers - base_losers:>+15d}"
            )

            # Exit reasons comparison
            print(f"\n{'EXIT REASON':<25} {'BASELINE':>15} {'2LYNCH':>15}")
            print(f"{'-' * 80}")

            base_exits = {}
            filt_exits = {}

            for t in baseline.trades:
                if t.exit_reason:
                    reason = t.exit_reason.value
                    base_exits[reason] = base_exits.get(reason, 0) + 1

            for t in filtered.trades:
                if t.exit_reason:
                    reason = t.exit_reason.value
                    filt_exits[reason] = filt_exits.get(reason, 0) + 1

            all_reasons = set(list(base_exits.keys()) + list(filt_exits.keys()))
            for reason in sorted(all_reasons):
                base_count = base_exits.get(reason, 0)
                filt_count = filt_exits.get(reason, 0)
                base_pct = base_count / len(baseline.trades) * 100 if baseline.trades else 0
                filt_pct = filt_count / len(filtered.trades) * 100 if filtered.trades else 0
                print(f"{reason:<25} {base_pct:>14.1f}% {filt_pct:>14.1f}%")

    print(f"\n{'=' * 80}\n")

    return results


if __name__ == "__main__":
    run_comparison_backtest()
