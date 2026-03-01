"""Test 2LYNCH filters with tuned thresholds.

This test uses more lenient filter thresholds to find the sweet spot
between signal quality and quantity.
"""

import sys
from datetime import date

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine
from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.rules import ScanConfig


def run_tuned_filter_test():
    """Test with lenient filter thresholds."""
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("2LYNCH FILTERS - TUNED THRESHOLDS")
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
    print(f"  Period: {start_date} to {end_date}")
    print(f"  Symbols: {len(symbols)} top liquid stocks")

    # Test different filter configurations
    configs = {
        "Baseline": {
            "min_filters_pass": 0,
            "close_pos_threshold": 0.0,
        },
        "Conservative": {
            "min_filters_pass": 5,  # Require 5/6 filters
            "close_pos_threshold": 0.70,
            "min_r2_l": 0.3,
            "max_atr_compress_ratio": 1.2,
            "min_range_percentile": 0.6,
            "max_vol_dryup_ratio": 1.5,
            "max_prior_breakouts": 3,
        },
        "Moderate": {
            "min_filters_pass": 3,  # Require 3/6 filters
            "close_pos_threshold": 0.50,
            "min_r2_l": 0.1,
            "max_atr_compress_ratio": 1.5,
            "min_range_percentile": 0.4,
            "max_vol_dryup_ratio": 2.0,
            "max_prior_breakouts": 5,
        },
        "Liberal": {
            "min_filters_pass": 2,  # Require 2/6 filters
            "close_pos_threshold": 0.30,
            "min_r2_l": 0.0,
            "max_atr_compress_ratio": 2.0,
            "min_range_percentile": 0.2,
            "max_vol_dryup_ratio": 3.0,
            "max_prior_breakouts": 10,
        },
    }

    results = {}

    for config_name, params in configs.items():
        print(f"\n{'=' * 80}")
        print(f"CONFIG: {config_name}")
        print(f"{'=' * 80}")
        print(f"  Params: {params}")

        config = ScanConfig(**params)
        signal_gen = DuckDBSignalGenerator(config=config)

        signals = signal_gen.generate_signals(symbols, start_date, end_date)
        print(f"  Signals: {len(signals)}")

        if not signals:
            print(f"  No signals - skipping")
            results[config_name] = None
            continue

        # Load price data and run backtest
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
            strategy_name=f"2LYNCH_{config_name}",
            signals=vbt_signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
            delisting_dates=None,
        )

        results[config_name] = result

        print(f"  Trades: {len(result.trades)}")
        print(f"  Return: {result.total_return * 100:+.2f}%")
        print(f"  Win Rate: {result.win_rate * 100:.1f}%")
        print(f"  Sharpe: {result.sharpe_ratio:.2f}")

    # Summary comparison
    print(f"\n{'=' * 80}")
    print(f"SUMMARY COMPARISON")
    print(f"{'=' * 80}")

    valid_results = {k: v for k, v in results.items() if v is not None}

    if len(valid_results) < 2:
        print(f"  Not enough valid results for comparison")
        return

    print(
        f"\n{'CONFIG':<15} {'TRADES':>10} {'RETURN %':>12} {'WIN %':>10} {'SHARPE':>10} {'AVG R':>10}"
    )
    print(f"{'-' * 80}")

    for name, result in valid_results.items():
        print(
            f"{name:<15} {len(result.trades):>10} {result.total_return * 100:>12.2f} {result.win_rate * 100:>10.1f} {result.sharpe_ratio:>10.2f} {result.avg_r:>10.2f}"
        )

    # Recommend best configuration
    print(f"\n{'=' * 80}")
    print(f"RECOMMENDATION")
    print(f"{'=' * 80}")

    # Score each configuration
    best_config = None
    best_score = -float("inf")

    for name, result in valid_results.items():
        # Score = Sharpe * 2 + Win Rate + (Avg R / 2)
        score = result.sharpe_ratio * 2 + result.win_rate + (result.avg_r / 2)
        print(f"  {name}: Score = {score:.2f}")

        if score > best_score:
            best_score = score
            best_config = name

    print(f"\n  Best Configuration: {best_config}")
    print(f"  Score: {best_score:.2f}")

    # Show exit reasons for best config
    if best_config and valid_results.get(best_config):
        result = valid_results[best_config]

        print(f"\n{'EXIT REASONS ({best_config})':<40} {'COUNT':>10} {'PCT':>10}")
        print(f"{'-' * 80}")

        exit_reasons = {}
        for t in result.trades:
            if t.exit_reason:
                reason = t.exit_reason.value
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True):
            pct = count / len(result.trades) * 100
            print(f"{reason:<40} {count:>10} {pct:>9.1f}%")

    print(f"\n{'=' * 80}\n")

    return results


if __name__ == "__main__":
    run_tuned_filter_test()
