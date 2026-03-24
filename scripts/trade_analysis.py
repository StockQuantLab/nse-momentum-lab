"""Trade Analysis for 2LYNCH Strategy.

Deep dive into winning and losing trade patterns to understand:
1. What makes a trade successful?
2. What causes failures?
3. Market conditions that favor the strategy
4. Holding period optimization
"""

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
    """Get top N symbols by average traded value."""
    query = f"""
    SELECT symbol, AVG(close * volume) as avg_value_traded
    FROM v_daily
    WHERE date BETWEEN DATE '2018-01-01' AND DATE '2024-12-31'
      AND close >= 10
    GROUP BY symbol
    ORDER BY avg_value_traded DESC
    LIMIT {n}
    """
    result = db.con.execute(query).fetchdf()
    return result["symbol"].to_list()


def run_trade_backtest(
    db: get_market_db,
    symbols: list[str],
    start_year: int = 2015,
    end_year: int = 2024,
) -> list[dict]:
    """Run backtest and return detailed trade data."""

    symbols_list_str = "', '".join(symbols)
    all_trades = []

    for year in range(start_year, end_year + 1):
        query = f"""
        WITH numbered_daily AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
            FROM v_daily
            WHERE date BETWEEN DATE '{year}-01-01' AND DATE '{year}-12-31'
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
              AND close >= 10
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
                f.range_percentile,
                f.prior_breakouts_90d,
                (close_pos_in_range >= 0.70) AS filter_h,
                (prior_breakouts_90d <= 2) AS filter_y,
                (vol_dryup_ratio < 1.3) AS filter_c,
                (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER) >= 1) AS filter_l
            FROM gap_ups g
            LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
            WHERE f.close_pos_in_range IS NOT NULL
        )
        SELECT * FROM with_features
        ORDER BY trading_date, symbol
        """

        df = db.con.execute(query).fetchdf()
        if df.empty:
            continue

        df_pl = pl.from_pandas(df)

        # Apply filters (4/6 = 3/5 since filter_n is always true)
        df_pl = df_pl.with_columns([
            pl.col("filter_h").fill_null(False),
            pl.col("filter_y").fill_null(True),
            pl.col("filter_c").fill_null(False),
            pl.col("filter_l").fill_null(False),
        ])

        df_pl = df_pl.with_columns([
            (pl.col("filter_h").cast(int) +
             pl.col("filter_y").cast(int) +
             pl.col("filter_c").cast(int) +
             pl.col("filter_l").cast(int)).alias("filters_passed")
        ])

        df_filtered = df_pl.filter(pl.col("filters_passed") >= 3)

        if df_filtered.height == 0:
            continue

        # Load price data and run backtest
        signal_symbols = df_filtered["symbol"].unique().to_list()
        price_data = {}
        value_traded_inr = {}

        start_date = date(year - 1, 12, 1)
        end_date = date(year + 1, 1, 31)

        for symbol in signal_symbols:
            symbol_id = hash(symbol) % 1000000
            try:
                price_df = db.query_daily(symbol, start_date.isoformat(), end_date.isoformat())
                if price_df.is_empty():
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
                if not vol_df.is_empty():
                    avg_vol = vol_df.select(pl.col("dollar_vol_20").drop_nulls().mean()).item()
                    value_traded_inr[symbol_id] = avg_vol if avg_vol else 50_000_000.0
                else:
                    value_traded_inr[symbol_id] = 50_000_000.0
            except Exception:
                continue

        # Convert signals
        vbt_signals = []
        signal_map = {}  # Map (symbol_id, date) to signal attributes
        for row in df_filtered.iter_rows(named=True):
            symbol = row["symbol"]
            symbol_id = hash(symbol) % 1000000
            if symbol_id not in price_data:
                continue

            sig_date = row["trading_date"]
            if isinstance(sig_date, datetime):
                sig_date = sig_date.date()

            signal_map[(symbol_id, sig_date)] = row

            vbt_signals.append((
                sig_date,
                symbol_id,
                symbol,
                row["low"],
                {"gap_pct": row["gap_pct"], "atr": row["atr_20"] if row["atr_20"] else 0.0}
            ))

        if not vbt_signals:
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
            strategy_name=f"2LYNCHBreakout_{year}",
            signals=vbt_signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
            delisting_dates=None,
        )

        # Store trades with signal attributes
        for t in result.trades:
            holding_days = 0
            if t.exit_date and t.entry_date:
                holding_days = (t.exit_date - t.entry_date).days

            pct_change = 0.0
            if t.entry_price and t.exit_price and t.entry_price > 0:
                pct_change = ((t.exit_price - t.entry_price) / t.entry_price) * 100

            sig_attrs = signal_map.get((int(t.symbol), t.entry_date), {})

            all_trades.append({
                "year": year,
                "entry_date": t.entry_date,
                "exit_date": t.exit_date,
                "symbol": t.symbol,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "pnl_pct": pct_change,
                "r_multiple": t.pnl_r if t.pnl_r else 0.0,
                "exit_reason": t.exit_reason.value if t.exit_reason else "unknown",
                "holding_days": holding_days,
                "gap_pct": sig_attrs.get("gap_pct", 0),
                "atr": sig_attrs.get("atr", 0),
                "close_pos_in_range": sig_attrs.get("close_pos_in_range", 0),
                "filter_h": sig_attrs.get("filter_h", False),
                "filter_y": sig_attrs.get("filter_y", True),
                "filter_c": sig_attrs.get("filter_c", False),
                "filter_l": sig_attrs.get("filter_l", False),
            })

    return all_trades


def run_trade_analysis():
    """Run comprehensive trade analysis."""

    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    print("\n" + "=" * 80)
    print("2LYNCH TRADE ANALYSIS")
    print("=" * 80)

    db = get_market_db()

    print("\nFetching top 100 liquid symbols...")
    top_symbols = get_top_n_liquid_symbols(db, n=100)
    print(f"Selected {len(top_symbols)} symbols")

    print("\nRunning backtest for trade analysis...")
    trades = run_trade_backtest(db, top_symbols, 2015, 2024)

    if not trades:
        print("No trades found")
        return

    trades_df = pl.DataFrame(trades)

    print(f"\n{'=' * 80}")
    print("OVERALL STATISTICS")
    print(f"{'=' * 80}")

    total = len(trades_df)
    winners = trades_df.filter(pl.col("pnl_pct") > 0)
    losers = trades_df.filter(pl.col("pnl_pct") < 0)

    print(f"\nTotal Trades: {total}")
    print(f"Winners: {len(winners)} ({len(winners)/total*100:.1f}%)")
    print(f"Losers: {len(losers)} ({len(losers)/total*100:.1f}%)")

    avg_win = winners["pnl_pct"].mean() if len(winners) > 0 else 0
    avg_loss = losers["pnl_pct"].mean() if len(losers) > 0 else 0
    print(f"Avg Win: {avg_win:.2f}%")
    print(f"Avg Loss: {avg_loss:.2f}%")

    # Gap size analysis
    print(f"\n{'=' * 80}")
    print("GAP SIZE ANALYSIS")
    print(f"{'=' * 80}")

    trades_df = trades_df.with_columns([
        (pl.col("gap_pct") * 100).alias("gap_pct_display")
    ])

    gap_analysis = trades_df.group_by(
        pl.col("gap_pct_display").cut([6, 8, 10], labels=["4-6%", "6-8%", "8-10%", "10%+"], left_closed=True)
    ).agg(
        pl.len().alias("count"),
        pl.col("pnl_pct").mean().alias("avg_pnl"),
        pl.col("r_multiple").mean().alias("avg_r"),
        (pl.col("pnl_pct") > 0).sum().alias("wins"),
    ).sort("gap_pct_display")

    print(f"\n{'Gap Size':<12} {'Count':>6} {'Avg PnL%':>10} {'Avg R':>7} {'Win%':>6}")
    for row in gap_analysis.iter_rows(named=True):
        win_pct = (row['wins'] / row['count'] * 100) if row['count'] > 0 else 0
        print(f"{row['gap_pct_display']!s:<12} {row['count']:>6} {row['avg_pnl']:>9.2f}% {row['avg_r']:>6.2f}R {win_pct:>5.1f}%")

    # Close position analysis
    print(f"\n{'=' * 80}")
    print("CLOSE POSITION IN RANGE ANALYSIS")
    print(f"{'=' * 80}")

    pos_analysis = trades_df.group_by(
        pl.col("close_pos_in_range").cut([0.5, 0.7, 0.85], labels=["<50%", "50-70%", "70-85%", "85%+"], left_closed=True)
    ).agg(
        pl.len().alias("count"),
        pl.col("pnl_pct").mean().alias("avg_pnl"),
        pl.col("r_multiple").mean().alias("avg_r"),
        (pl.col("pnl_pct") > 0).sum().alias("wins"),
    ).sort("close_pos_in_range")

    print(f"\n{'Close Position':<12} {'Count':>6} {'Avg PnL%':>10} {'Avg R':>7} {'Win%':>6}")
    for row in pos_analysis.iter_rows(named=True):
        win_pct = (row['wins'] / row['count'] * 100) if row['count'] > 0 else 0
        print(f"{row['close_pos_in_range']!s:<12} {row['count']:>6} {row['avg_pnl']:>9.2f}% {row['avg_r']:>6.2f}R {win_pct:>5.1f}%")

    # Day of week analysis
    print(f"\n{'=' * 80}")
    print("DAY OF WEEK ANALYSIS")
    print(f"{'=' * 80}")

    trades_df = trades_df.with_columns([
        pl.col("entry_date").dt.weekday().alias("day_of_week")  # 1=Mon, 7=Sun
    ])

    dow_analysis = trades_df.group_by("day_of_week").agg(
        pl.len().alias("count"),
        pl.col("pnl_pct").mean().alias("avg_pnl"),
        pl.col("r_multiple").mean().alias("avg_r"),
        (pl.col("pnl_pct") > 0).sum().alias("wins"),
    ).sort("day_of_week")

    print(f"\n{'Day':<8} {'Count':>6} {'Avg PnL%':>10} {'Avg R':>7} {'Win%':>6}")
    for row in dow_analysis.iter_rows(named=True):
        win_pct = (row['wins'] / row['count'] * 100) if row['count'] > 0 else 0
        dow_names = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
        day_name = dow_names.get(row['day_of_week'], str(row['day_of_week']))
        print(f"{day_name:<8} {row['count']:>6} {row['avg_pnl']:>9.2f}% {row['avg_r']:>6.2f}R {win_pct:>5.1f}%")

    # Month analysis
    print(f"\n{'=' * 80}")
    print("MONTH ANALYSIS")
    print(f"{'=' * 80}")

    trades_df = trades_df.with_columns([
        pl.col("entry_date").dt.month().alias("month")
    ])

    month_analysis = trades_df.group_by("month").agg(
        pl.len().alias("count"),
        pl.col("pnl_pct").mean().alias("avg_pnl"),
        pl.col("r_multiple").mean().alias("avg_r"),
        (pl.col("pnl_pct") > 0).sum().alias("wins"),
    ).sort("month")

    print(f"\n{'Month':<8} {'Count':>6} {'Avg PnL%':>10} {'Avg R':>7} {'Win%':>6}")
    for row in month_analysis.iter_rows(named=True):
        win_pct = (row['wins'] / row['count'] * 100) if row['count'] > 0 else 0
        month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                       7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
        month_name = month_names.get(row['month'], str(row['month']))
        print(f"{month_name:<8} {row['count']:>6} {row['avg_pnl']:>9.2f}% {row['avg_r']:>6.2f}R {win_pct:>5.1f}%")

    # Exit reason detailed analysis
    print(f"\n{'=' * 80}")
    print("EXIT REASON DETAILED ANALYSIS")
    print(f"{'=' * 80}")

    exit_analysis = trades_df.group_by("exit_reason").agg(
        pl.len().alias("count"),
        pl.col("pnl_pct").mean().alias("avg_pnl"),
        pl.col("pnl_pct").min().alias("min_pnl"),
        pl.col("pnl_pct").max().alias("max_pnl"),
        pl.col("r_multiple").mean().alias("avg_r"),
        pl.col("holding_days").mean().alias("avg_hold"),
        (pl.col("pnl_pct") > 0).sum().alias("wins"),
    ).sort("count", descending=True)

    print(f"\n{'Exit Reason':<30} {'Count':>6} {'Win%':>6} {'Avg PnL%':>9} {'Range%':>12} {'Avg R':>6} {'Hold':>5}")
    for row in exit_analysis.iter_rows(named=True):
        win_pct = (row['wins'] / row['count'] * 100) if row['count'] > 0 else 0
        range_pct = f"{row['min_pnl']:.1f}% to {row['max_pnl']:.1f}%"
        print(f"{row['exit_reason']:<30} {row['count']:>6} {win_pct:>5.1f}% {row['avg_pnl']:>8.2f}% {range_pct:>12} {row['avg_r']:>5.2f}R {row['avg_hold']:>4.1f}d")

    # Winner vs Loser profiles
    print(f"\n{'=' * 80}")
    print("WINNER VS LOSER PROFILES")
    print(f"{'=' * 80}")

    print(f"\n{'Attribute':<20} {'Winners':>12} {'Losers':>12}")
    print(f"{'-' * 45}")

    winner_stats = {
        "Avg Gap %": winners["gap_pct"].mean() * 100 if len(winners) > 0 else 0,
        "Avg Close Pos": winners["close_pos_in_range"].mean() if len(winners) > 0 else 0,
        "Avg ATR": winners["atr"].mean() if len(winners) > 0 else 0,
        "Avg Hold Days": winners["holding_days"].mean() if len(winners) > 0 else 0,
    }

    loser_stats = {
        "Avg Gap %": losers["gap_pct"].mean() * 100 if len(losers) > 0 else 0,
        "Avg Close Pos": losers["close_pos_in_range"].mean() if len(losers) > 0 else 0,
        "Avg ATR": losers["atr"].mean() if len(losers) > 0 else 0,
        "Avg Hold Days": losers["holding_days"].mean() if len(losers) > 0 else 0,
    }

    for key in winner_stats:
        print(f"{key:<20} {winner_stats[key]:>12.2f} {loser_stats[key]:>12.2f}")

    # Filter pass analysis
    print(f"\n{'=' * 80}")
    print("FILTER IMPACT ON TRADES")
    print(f"{'=' * 80}")

    for filter_name, display_name in [
        ("filter_h", "Close Pos >= 70%"),
        ("filter_y", "Prior Breakouts <= 2"),
        ("filter_c", "Volume Dryup < 1.3"),
        ("filter_l", "Long-term Setup"),
    ]:
        filter_pass = trades_df.filter(pl.col(filter_name))
        filter_fail = trades_df.filter(not pl.col(filter_name))

        if len(filter_pass) > 0 and len(filter_fail) > 0:
            win_pass = filter_pass.filter(pl.col("pnl_pct") > 0).height / len(filter_pass) * 100
            win_fail = filter_fail.filter(pl.col("pnl_pct") > 0).height / len(filter_fail) * 100
            avg_pass = filter_pass["pnl_pct"].mean()
            avg_fail = filter_fail["pnl_pct"].mean()

            print(f"\n{display_name}:")
            print(f"  When PASSES: {len(filter_pass):>3} trades, {win_pass:.1f}% win, avg {avg_pass:+.2f}%")
            print(f"  When FAILS:  {len(filter_fail):>3} trades, {win_fail:.1f}% win, avg {avg_fail:+.2f}%")

    # Key insights
    print(f"\n{'=' * 80}")
    print("KEY INSIGHTS")
    print(f"{'=' * 80}")

    best_gap = gap_analysis.sort("avg_pnl", descending=True).row(0, named=True)
    worst_gap = gap_analysis.sort("avg_pnl").row(0, named=True)

    print("\nGap Size:")
    print(f"  Best: {best_gap['gap_pct_display']} with avg {best_gap['avg_pnl']:.2f}% return")
    print(f"  Worst: {worst_gap['gap_pct_display']} with avg {worst_gap['avg_pnl']:.2f}% return")

    best_exit = exit_analysis.sort("avg_pnl", descending=True).row(0, named=True)
    worst_exit = exit_analysis.sort("avg_pnl").row(0, named=True)

    print("\nExit Reasons:")
    print(f"  Best: {best_exit['exit_reason']} with avg {best_exit['avg_pnl']:.2f}% return")
    print(f"  Worst: {worst_exit['exit_reason']} with avg {worst_exit['avg_pnl']:.2f}% return")

    print(f"\n{'=' * 80}")
    print("TRADE ANALYSIS COMPLETE")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    run_trade_analysis()
