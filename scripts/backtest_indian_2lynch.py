"""Direct backtest for Indian 2LYNCH strategy.

Uses parquet data directly without integration test overhead.
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    VectorBTConfig,
    VectorBTEngine,
)


def run_backtest_direct():
    """Run backtest directly with Indian 2LYNCH filters in SQL."""

    # Fix encoding for Windows
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    print("\n" + "=" * 80)
    print("INDIAN 2LYNCH BACKTEST - Direct SQL Approach")
    print("=" * 80)

    db = get_market_db()

    # Test different price thresholds
    price_thresholds = [10, 30, 50, 100]
    min_filters_options = [4, 5]

    results = []

    for min_price in price_thresholds:
        for min_filters in min_filters_options:
            config_name = f"Price>Rs.{min_price}, {min_filters}/6 filters"

            print(f"\n{'=' * 80}")
            print(f"CONFIG: {config_name}")
            print(f"{'=' * 80}")

            # Build SQL query with Indian market thresholds
            query = f"""
            WITH numbered_daily AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trading_date) AS rn
                FROM v_daily
                WHERE trading_date BETWEEN '2014-01-01' AND '2024-12-31'
            ),
            with_lag AS (
                SELECT
                    symbol,
                    trading_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    LAG(close) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_close,
                    LAG(high) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_high,
                    LAG(low) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_low,
                    LAG(open) OVER (PARTITION BY symbol ORDER BY trading_date) AS prev_open,
                    (open - LAG(close) OVER (PARTITION BY symbol ORDER BY trading_date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY trading_date) AS gap_pct,
                    close * volume AS value_traded_inr
                FROM numbered_daily
                WHERE rn > 1  -- Skip first row per symbol (no lag data)
            ),
            gap_ups AS (
                SELECT *
                FROM with_lag
                WHERE gap_pct >= 0.04  -- 4% gap up
                  AND prev_close IS NOT NULL
                  AND close >= {min_price}  -- Price filter
                  AND close * volume >= 3000000  -- Rs.30 lakh value traded
                  AND volume >= 50000  -- Min shares
            ),
            with_features AS (
                SELECT
                    g.*,
                    f.close_pos_in_range,
                    f.ma_20,
                    f.ret_5d,
                    f.atr_20,
                    f.vol_dryup_ratio,
                    f.atr_compress_ratio,
                    f.range_percentile,
                    f.prior_breakouts_90d
                FROM gap_ups g
                LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
            )
            SELECT
                symbol,
                trading_date,
                open,
                high,
                low,
                close,
                gap_pct,
                value_traded_inr,
                close_pos_in_range,
                (close > ma_20) AS above_ma20,
                (ret_5d > 0) AS positive_momentum,
                atr_20,
                vol_dryup_ratio,
                atr_compress_ratio,
                range_percentile,
                prior_breakouts_90d,
                -- Calculate filters
                (close_pos_in_range >= 0.70) AS filter_h,
                ((high - low) / close < (atr_20 * 0.5) OR close < open) AS filter_n,
                (prior_breakouts_90d <= 2) AS filter_y,
                (vol_dryup_ratio < 1.3) AS filter_c,
                ((close > ma_20) + (ret_5d > 0) + (COALESCE(NULLIF(r2_65, 0), 0) >= 0.70) >= 2) AS filter_l
            FROM with_features
            WHERE close_pos_in_range IS NOT NULL
            ORDER BY trading_date, symbol
            """

            df = db.con.execute(query).fetchdf()

            if df.is_empty():
                print("  No signals found")
                continue

            # Apply filters
            df = df.with_columns([
                pl.col("filter_h").fill_null(False),
                pl.col("filter_n").fill_null(False),
                pl.col("filter_y").fill_null(True),  # Pass if no data
                pl.col("filter_c").fill_null(False),
                pl.col("filter_l").fill_null(False),
            ])

            # Count filters passed
            df = df.with_columns([
                (pl.col("filter_h").cast(int) +
                 pl.col("filter_n").cast(int) +
                 pl.col("filter_y").cast(int) +
                 pl.col("filter_c").cast(int) +
                 pl.col("filter_l").cast(int)).alias("filters_passed")
            ])

            # Apply minimum filters
            df_filtered = df.filter(pl.col("filters_passed") >= min_filters)

            print(f"  Total gap-ups: {len(df)}")
            print(f"  After {min_filters}/6 filters: {len(df_filtered)}")

            if df_filtered.is_empty():
                print("  No signals after filters - skipping")
                continue

            # Show filter pass rates
            print("\n  Filter Pass Rates:")
            for col in ["filter_h", "filter_n", "filter_y", "filter_c", "filter_l"]:
                pct = df_filtered[col].sum() / len(df_filtered) * 100
                print(f"    {col}: {pct:.1f}%")

            # Get unique symbols
            symbols_list = df_filtered["symbol"].unique().to_list()

            # Convert to VectorBT format
            start_date = date(2014, 1, 1)
            end_date = date(2024, 12, 31)

            # Load price data
            price_data = {}
            value_traded_inr = {}

            for symbol in symbols_list:
                symbol_id = hash(symbol) % 1000000  # Simple hash for ID
                try:
                    price_df = db.query_daily(symbol, start_date.isoformat(), end_date.isoformat())
                    if price_df.is_empty():
                        continue

                    price_data[symbol_id] = {}
                    for row in price_df.iter_rows(named=True):
                        price_data[symbol_id][row["date"]] = {
                            "open_adj": float(row["open"]),
                            "close_adj": float(row["close"]),
                            "high_adj": float(row["high"]),
                            "low_adj": float(row["low"]),
                        }

                    # Get average value traded
                    vol_df = db.get_features_range([symbol], start_date.isoformat(), end_date.isoformat())
                    if not vol_df.is_empty():
                        avg_vol = vol_df.select(pl.col("dollar_vol_20").drop_nulls().mean()).item()
                        value_traded_inr[symbol_id] = avg_vol if avg_vol else 50_000_000.0
                    else:
                        value_traded_inr[symbol_id] = 50_000_000.0
                except Exception:
                    continue

            # Convert signals
            vbt_signals = []
            for row in df_filtered.iter_rows(named=True):
                symbol = row["symbol"]
                symbol_id = hash(symbol) % 1000000
                if symbol_id not in price_data:
                    continue

                vbt_signals.append((
                    row["trading_date"],
                    symbol_id,
                    symbol,
                    row["low"],  # Initial stop at low of day
                    {"gap_pct": row["gap_pct"], "atr": row["atr_20"] if row["atr_20"] else 0.0}
                ))

            if not vbt_signals:
                print("  No valid signals after conversion")
                continue

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

            results.append({
                "config": config_name,
                "min_price": min_price,
                "min_filters": min_filters,
                "total_signals": len(df),
                "filtered_signals": len(df_filtered),
                "trades": len(result.trades),
                "return": result.total_return * 100,
                "win_rate": result.win_rate * 100,
                "sharpe": result.sharpe_ratio,
                "avg_r": result.avg_r,
            })

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

    # Summary comparison
    print(f"\n{'=' * 80}")
    print("SUMMARY COMPARISON")
    print(f"{'=' * 80}")

    print(f"\n{'Config':<30} {'Signals':>10} {'Trades':>10} {'Return %':>12} {'Win %':>10} {'Sharpe':>8} {'R':>8}")
    print(f"{'-' * 80}")

    for r in results:
        print(f"{r['config']:<30} {r['filtered_signals']:>10} {r['trades']:>10} "
              f"{r['return']:>11.2f}% {r['win_rate']:>9.1f}% "
              f"{r['sharpe']:>7.2f} {r['avg_r']:>7.2f}")

    print(f"\n{'=' * 80}")
    print("TEST COMPLETE")
    print(f"{'=' * 80}\n")

    return results


if __name__ == "__main__":
    run_backtest_direct()
