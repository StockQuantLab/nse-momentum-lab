"""Parameterized backtest for the 2LYNCH breakout strategy.

Usage:
    python backtest_2lynch.py                    # Defaults: 100 stocks, Rs.10+, 4/6 filters
    python backtest_2lynch.py --num-stocks 500   # 500 stocks
    python backtest_2lynch.py --min-price 30 --min-filters 5
    python backtest_2lynch.py --all              # Test all combinations
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    VectorBTConfig,
    VectorBTEngine,
)


def get_top_n_liquid_symbols(db: get_market_db, n: int = 100) -> list[str]:
    """Get top N symbols by average traded value (2020-2024)."""
    query = f"""
    SELECT symbol, AVG(close * volume) as avg_value_traded
    FROM v_daily
    WHERE date BETWEEN DATE '2020-01-01' AND DATE '2024-12-31'
      AND close >= 10
    GROUP BY symbol
    ORDER BY avg_value_traded DESC
    LIMIT {n}
    """
    result = db.con.execute(query).fetchdf()
    return result["symbol"].to_list()


def run_single_backtest(
    db: get_market_db,
    symbols: list[str],
    min_price: int,
    min_filters: int,
    vbt_config: VectorBTConfig,
) -> dict | None:
    """Run a single backtest configuration."""

    symbols_list_str = "', '".join(symbols)

    query = f"""
    WITH numbered_daily AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
        FROM v_daily
        WHERE date BETWEEN DATE '2014-01-01' AND DATE '2024-12-31'
          AND symbol IN ('{symbols_list_str}')
    ),
    with_lag AS (
        SELECT
            symbol,
            date as trading_date,
            open,
            high,
            low,
            close,
            volume,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
            (open - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) /
                NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS gap_pct,
            close * volume AS value_traded_inr
        FROM numbered_daily
        WHERE rn > 1
    ),
    gap_ups AS (
        SELECT *
        FROM with_lag
        WHERE gap_pct >= 0.04
          AND prev_close IS NOT NULL
          AND close >= {min_price}
          AND value_traded_inr >= 3000000
          AND volume >= 50000
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
            f.prior_breakouts_90d,
            f.r2_65
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
        close_pos_in_range,
        atr_20,
        prior_breakouts_90d,
        (close_pos_in_range >= 0.70) AS filter_h,
        ((high - low) / NULLIF(close, 0) < (atr_20 * 0.5) OR close < open) AS filter_n,
        (prior_breakouts_90d <= 2) AS filter_y,
        (vol_dryup_ratio < 1.3) AS filter_c,
        (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER) +
            CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l
    FROM with_features
    WHERE close_pos_in_range IS NOT NULL
    ORDER BY trading_date, symbol
    """

    df = db.con.execute(query).fetchdf()
    if df.empty:
        return None

    df_pl = pl.from_pandas(df)
    df_pl = df_pl.with_columns(
        [
            pl.col("filter_h").fill_null(False),
            pl.col("filter_n").fill_null(False),
            pl.col("filter_y").fill_null(True),
            pl.col("filter_c").fill_null(False),
            pl.col("filter_l").fill_null(False),
        ]
    )

    df_pl = df_pl.with_columns(
        [
            (
                pl.col("filter_h").cast(int)
                + pl.col("filter_n").cast(int)
                + pl.col("filter_y").cast(int)
                + pl.col("filter_c").cast(int)
                + pl.col("filter_l").cast(int)
            ).alias("filters_passed")
        ]
    )

    df_filtered = df_pl.filter(pl.col("filters_passed") >= min_filters)
    if df_filtered.height == 0:
        return None

    # Load price data for backtest
    signal_symbols = df_filtered["symbol"].unique().to_list()
    start_date = date(2014, 1, 1)
    end_date = date(2024, 12, 31)

    price_data = {}
    value_traded_inr = {}

    for symbol in signal_symbols:
        symbol_id = hash(symbol) % 1000000
        try:
            price_df = db.query_daily(symbol, start_date.isoformat(), end_date.isoformat())
            if price_df.height == 0:
                continue

            price_data[symbol_id] = {}
            for row in price_df.iter_rows(named=True):
                dt = row["date"]
                if isinstance(dt, datetime):
                    dt = dt.date()
                price_data[symbol_id][dt] = {
                    "open_adj": float(row["open"]),
                    "close_adj": float(row["close"]),
                    "high_adj": float(row["high"]),
                    "low_adj": float(row["low"]),
                }

            vol_df = db.get_features_range([symbol], start_date.isoformat(), end_date.isoformat())
            if vol_df.height > 0:
                avg_vol = vol_df.select(pl.col("dollar_vol_20").drop_nulls().mean()).item()
                value_traded_inr[symbol_id] = avg_vol if avg_vol else 50_000_000.0
            else:
                value_traded_inr[symbol_id] = 50_000_000.0
        except Exception:
            continue

    vbt_signals = []
    for row in df_filtered.iter_rows(named=True):
        symbol = row["symbol"]
        symbol_id = hash(symbol) % 1000000
        if symbol_id not in price_data:
            continue

        sig_date = row["trading_date"]
        if isinstance(sig_date, datetime):
            sig_date = sig_date.date()

        vbt_signals.append(
            (
                sig_date,
                symbol_id,
                symbol,
                row["low"],
                {"gap_pct": row["gap_pct"], "atr": row["atr_20"] if row["atr_20"] else 0.0},
            )
        )

    if not vbt_signals:
        return None

    engine = VectorBTEngine(config=vbt_config)
    result = engine.run_backtest(
        strategy_name=f"thresholdbreakout_{len(symbols)}stocks_{min_filters}filters",
        signals=vbt_signals,
        price_data=price_data,
        value_traded_inr=value_traded_inr,
        delisting_dates=None,
    )

    # Count weak follow-through
    weak_ft = sum(
        1 for t in result.trades if t.exit_reason and "weak" in t.exit_reason.value.lower()
    )

    return {
        "num_stocks": len(symbols),
        "min_price": min_price,
        "min_filters": min_filters,
        "total_signals": len(df),
        "filtered_signals": len(df_filtered),
        "signals_per_year": len(df_filtered) / 11,
        "unique_stocks": len(signal_symbols),
        "trades": len(result.trades),
        "return": result.total_return * 100,
        "win_rate": result.win_rate * 100,
        "sharpe": result.sharpe_ratio,
        "avg_r": result.avg_r,
        "max_drawdown": result.max_drawdown * 100,
        "weak_ft": weak_ft,
        "weak_ft_pct": weak_ft / len(result.trades) * 100 if result.trades else 0,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Backtest 2LYNCH breakout momentum strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--num-stocks",
        "-n",
        type=int,
        default=100,
        help="Number of top liquid stocks to test (default: 100)",
    )
    parser.add_argument(
        "--min-price",
        "-p",
        type=int,
        default=10,
        choices=[10, 30, 50, 100],
        help="Minimum stock price (default: 10)",
    )
    parser.add_argument(
        "--min-filters",
        "-f",
        type=int,
        default=4,
        choices=[3, 4, 5, 6],
        help="Minimum filters to pass (default: 4)",
    )
    parser.add_argument(
        "--all",
        "-a",
        action="store_true",
        help="Test all combinations of parameters",
    )
    parser.add_argument(
        "--risk-per-trade",
        "-r",
        type=float,
        default=1.0,
        help="Risk per trade in %% (default: 1.0)",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Less verbose output",
    )

    args = parser.parse_args()

    # Fix encoding for Windows
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print("\n" + "=" * 80)
    print("2LYNCH BREAKOUT BACKTEST")
    print("=" * 80)

    db = get_market_db()
    symbols = get_top_n_liquid_symbols(db, args.num_stocks)
    print(f"\nSelected {len(symbols)} stocks (top by liquidity 2020-2024)")

    vbt_config = VectorBTConfig(
        default_portfolio_value=1_000_000.0,
        risk_per_trade_pct=args.risk_per_trade / 100,
        fees_per_trade=0.001,
        initial_stop_atr_mult=2.0,
        trail_activation_pct=0.05,
        trail_stop_pct=0.02,
        time_stop_days=3,
        follow_through_threshold=0.02,
    )

    # Define test configurations
    if args.all:
        configs = [
            (10, 4),
            (10, 5),
            (30, 4),
            (30, 5),
            (50, 4),
        ]
    else:
        configs = [(args.min_price, args.min_filters)]

    results = []

    for min_price, min_filters in configs:
        config_name = f"Rs.{min_price}+, {min_filters}/6 filters"

        if not args.quiet:
            print(f"\n{'=' * 60}")
            print(f"Testing: {config_name}")
            print(f"{'=' * 60}")

        result = run_single_backtest(
            db=db,
            symbols=symbols,
            min_price=min_price,
            min_filters=min_filters,
            vbt_config=vbt_config,
        )

        if result:
            results.append(result)
            if not args.quiet:
                print(
                    f"  Signals: {result['filtered_signals']} ({result['signals_per_year']:.1f}/yr)"
                )
                print(f"  Trades: {result['trades']}")
                print(f"  Return: {result['return']:+.2f}%")
                print(f"  Win Rate: {result['win_rate']:.1f}%")
                print(f"  Sharpe: {result['sharpe']:.2f}")
                print(f"  Avg R: {result['avg_r']:.2f}R")
                print(f"  Max DD: {result['max_drawdown']:.2f}%")
                print(f"  Weak FT: {result['weak_ft_pct']:.1f}%")
        else:
            print(f"  No signals for {config_name}")

    # Print summary table
    print(f"\n{'=' * 100}")
    print(f"SUMMARY - {len(symbols)} STOCKS")
    print(f"{'=' * 100}")

    print(
        f"\n{'Config':<20} {'Signals/yr':>12} {'Return %':>12} {'Win %':>10} {'Sharpe':>8} {'DD%':>8} {'WeakFT%':>10}"
    )
    print(f"{'-' * 100}")

    for r in results:
        config = f"Rs.{r['min_price']}+,{r['min_filters']}/6"
        print(
            f"{config:<20} {r['signals_per_year']:>11.1f} "
            f"{r['return']:>11.2f}% {r['win_rate']:>9.1f}% "
            f"{r['sharpe']:>7.2f} {r['max_drawdown']:>7.1f}% {r['weak_ft_pct']:>9.1f}%"
        )

    print(f"\n{'=' * 100}\n")

    return results


if __name__ == "__main__":
    main()
