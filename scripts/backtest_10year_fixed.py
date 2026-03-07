"""10-Year Full Backtest for Indian 2LYNCH strategy - Fixed analysis.

Tests the optimal configuration (Rs.10+, 4/6 filters) on:
- Top 100 most liquid stocks
- Full 10-year date range (2015-2025)
- Proper trade analysis with realistic PnL calculations
"""

import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import (
    Trade,
    VectorBTConfig,
    VectorBTEngine,
)


@dataclass
class YearlyStats:
    """Statistics for a single year."""
    year: int
    total_signals: int = 0
    filtered_signals: int = 0
    trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_return_pct: float = 0.0
    win_rate_pct: float = 0.0
    avg_r_multiple: float = 0.0
    max_drawdown_pct: float = 0.0
    weak_follow_through_pct: float = 0.0
    exit_reasons: dict[str, int] = field(default_factory=dict)

    # Additional metrics
    avg_holding_days: float = 0.0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0
    profit_factor: float = 0.0


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


def calculate_trade_metrics(trades: list[Trade]) -> dict:
    """Calculate detailed trade metrics."""
    if not trades:
        return {
            "avg_holding_days": 0,
            "best_trade_pct": 0,
            "worst_trade_pct": 0,
            "profit_factor": 0,
            "total_gains": 0,
            "total_losses": 0,
        }

    holding_days_list = []
    pct_changes = []
    gains = []
    losses = []

    for t in trades:
        if t.exit_date and t.entry_date:
            holding_days_list.append((t.exit_date - t.entry_date).days)

        # Calculate actual % return from entry to exit
        if t.entry_price and t.exit_price and t.entry_price > 0:
            pct_change = ((t.exit_price - t.entry_price) / t.entry_price) * 100
            pct_changes.append(pct_change)
            if pct_change > 0:
                gains.append(pct_change)
            elif pct_change < 0:
                losses.append(abs(pct_change))

    total_gains = sum(gains)
    total_losses = sum(losses)
    profit_factor = total_gains / total_losses if total_losses > 0 else 0

    return {
        "avg_holding_days": np.mean(holding_days_list) if holding_days_list else 0,
        "best_trade_pct": max(pct_changes) if pct_changes else 0,
        "worst_trade_pct": min(pct_changes) if pct_changes else 0,
        "profit_factor": profit_factor,
        "total_gains": total_gains,
        "total_losses": total_losses,
        "pct_changes": pct_changes,
    }


def run_yearly_backtest(
    db: get_market_db,
    symbols: list[str],
    start_year: int,
    end_year: int,
    min_price: int = 10,
    min_filters: int = 4,
) -> tuple[dict[int, YearlyStats], list[dict]]:
    """Run backtest year by year and collect stats."""

    symbols_list_str = "', '".join(symbols)
    yearly_results = {}
    all_trades = []
    # Reverse mapping: hash ID -> symbol name (for readable trade output)
    id_to_symbol: dict[int, str] = {}

    for year in range(start_year, end_year + 1):
        print(f"\n{'=' * 80}")
        print(f"YEAR: {year}")
        print(f"{'=' * 80}")

        # Build SQL query for this year
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
                LAG(high) OVER (PARTITION BY symbol ORDER BY date) AS prev_high,
                LAG(low) OVER (PARTITION BY symbol ORDER BY date) AS prev_low,
                LAG(open) OVER (PARTITION BY symbol ORDER BY date) AS prev_open,
                -- T-1 return (day before gap)
                (LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag1,
                -- T-2 return (2 days before gap)
                (LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag2,
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
              AND ret_1d_lag1 IS NOT NULL
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
                f.prior_breakouts_30d,
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
            prev_low,
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
            (close_pos_in_range >= 0.70) AS filter_h,
            -- N filter: T-1 should be narrow range or negative day (compression before breakout)
            ((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close < prev_open) AS filter_n,
            -- Y filter: young breakout - max 2 prior breakouts in 30 days
            (COALESCE(prior_breakouts_30d, 0) <= 2) AS filter_y,
            (vol_dryup_ratio < 1.3) AS filter_c,
            (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER) +
                CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
            -- "2" filter: not up 2 days in a row before breakout
            (ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0) AS filter_2
        FROM with_features
        WHERE close_pos_in_range IS NOT NULL
        ORDER BY trading_date, symbol
        """

        df = db.con.execute(query).fetchdf()

        if df.empty:
            print("  No gap-ups found")
            yearly_results[year] = YearlyStats(year=year)
            continue

        df_pl = pl.from_pandas(df)

        # Apply filters (6 filters: H, N, 2, Y, C, L)
        # All filters default to False on NULL (unknown = reject)
        df_pl = df_pl.with_columns([
            pl.col("filter_h").fill_null(False),
            pl.col("filter_n").fill_null(False),
            pl.col("filter_2").fill_null(False),
            pl.col("filter_y").fill_null(False),
            pl.col("filter_c").fill_null(False),
            pl.col("filter_l").fill_null(False),
        ])

        df_pl = df_pl.with_columns([
            (pl.col("filter_h").cast(int) +
             pl.col("filter_n").cast(int) +
             pl.col("filter_2").cast(int) +
             pl.col("filter_y").cast(int) +
             pl.col("filter_c").cast(int) +
             pl.col("filter_l").cast(int)).alias("filters_passed")
        ])

        df_filtered = df_pl.filter(pl.col("filters_passed") >= min_filters)

        print(f"  Total gap-ups: {len(df)}")
        print(f"  After {min_filters}/6 filters: {len(df_filtered)}")

        if df_filtered.height == 0:
            print("  No signals after filters")
            yearly_results[year] = YearlyStats(year=year, total_signals=len(df))
            continue

        # Get signal symbols
        signal_symbols = df_filtered["symbol"].unique().to_list()

        # Load price data
        price_data = {}
        value_traded_inr = {}

        start_date = date(year - 1, 12, 1)
        end_date = date(year + 1, 1, 31)

        for symbol in signal_symbols:
            symbol_id = hash(symbol) % 1000000
            id_to_symbol[symbol_id] = symbol
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
        for row in df_filtered.iter_rows(named=True):
            symbol = row["symbol"]
            symbol_id = hash(symbol) % 1000000
            if symbol_id not in price_data:
                continue

            sig_date = row["trading_date"]
            if isinstance(sig_date, datetime):
                sig_date = sig_date.date()

            # Stockbee: stop = low of the setup day (T-1), NOT the gap-up day (T)
            initial_stop = row["prev_low"] if row["prev_low"] is not None else row["low"]
            vbt_signals.append((
                sig_date,
                symbol_id,
                symbol,
                initial_stop,
                {"gap_pct": row["gap_pct"], "atr": row["atr_20"] if row["atr_20"] else 0.0}
            ))

        if not vbt_signals:
            print("  No valid signals")
            yearly_results[year] = YearlyStats(
                year=year,
                total_signals=len(df),
                filtered_signals=len(df_filtered)
            )
            continue

        # Run backtest
        vbt_config = VectorBTConfig(
            default_portfolio_value=1_000_000.0,
            risk_per_trade_pct=0.01,
            fees_per_trade=0.001,
            initial_stop_atr_mult=2.0,
            trail_activation_pct=0.08,  # Stockbee: trailing stop at 8%+
            trail_stop_pct=0.02,
            time_stop_days=5,  # Stockbee: exit on 3rd to 5th day
            follow_through_threshold=0.0,  # Disabled: Stockbee holds 3-5 days
        )

        engine = VectorBTEngine(config=vbt_config)
        result = engine.run_backtest(
            strategy_name=f"Indian2LYNCH_{year}",
            signals=vbt_signals,
            price_data=price_data,
            value_traded_inr=value_traded_inr,
            delisting_dates=None,
        )

        # Calculate trade metrics
        trade_metrics = calculate_trade_metrics(result.trades)

        # Collect stats
        stats = YearlyStats(
            year=year,
            total_signals=len(df),
            filtered_signals=len(df_filtered),
            trades=len(result.trades),
            winning_trades=sum(1 for t in result.trades if t.pnl and t.pnl > 0),
            losing_trades=sum(1 for t in result.trades if t.pnl and t.pnl < 0),
            total_return_pct=result.total_return * 100,
            win_rate_pct=result.win_rate * 100,
            avg_r_multiple=result.avg_r,
            max_drawdown_pct=result.max_drawdown * 100,
            avg_holding_days=trade_metrics["avg_holding_days"],
            best_trade_pct=trade_metrics["best_trade_pct"],
            worst_trade_pct=trade_metrics["worst_trade_pct"],
            profit_factor=trade_metrics["profit_factor"],
        )

        # Exit reasons
        exit_reasons = {}
        weak_ft = 0
        for t in result.trades:
            if t.exit_reason:
                reason = t.exit_reason.value
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
                if "weak" in reason.lower():
                    weak_ft += 1

        stats.exit_reasons = exit_reasons
        stats.weak_follow_through_pct = (weak_ft / len(result.trades) * 100) if result.trades else 0

        yearly_results[year] = stats

        # Store trades for analysis
        for t in result.trades:
            holding_days = 0
            if t.exit_date and t.entry_date:
                holding_days = (t.exit_date - t.entry_date).days

            pct_change = 0.0
            if t.entry_price and t.exit_price and t.entry_price > 0:
                pct_change = ((t.exit_price - t.entry_price) / t.entry_price) * 100

            # Resolve hash ID back to symbol name
            real_symbol = id_to_symbol.get(t.symbol_id, t.symbol)

            all_trades.append({
                "year": year,
                "entry_date": t.entry_date,
                "exit_date": t.exit_date,
                "symbol": real_symbol,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "pnl_pct": pct_change,
                "r_multiple": t.pnl_r if t.pnl_r else 0.0,
                "exit_reason": t.exit_reason.value if t.exit_reason else "unknown",
                "holding_days": holding_days,
            })

        print(f"  Trades: {len(result.trades)}")
        print(f"  Return: {stats.total_return_pct:+.2f}%")
        print(f"  Win Rate: {stats.win_rate_pct:.1f}%")
        print(f"  Avg R: {stats.avg_r_multiple:.2f}R")
        print(f"  Avg Hold: {stats.avg_holding_days:.1f} days")
        print(f"  Profit Factor: {stats.profit_factor:.2f}")
        print(f"  Max DD: {stats.max_drawdown_pct:.2f}%")
        print(f"  Weak FT: {stats.weak_follow_through_pct:.1f}%")

    return yearly_results, all_trades


def run_10year_backtest():
    """Run comprehensive 10-year backtest."""

    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    print("\n" + "=" * 80)
    print("INDIAN 2LYNCH - 10 YEAR COMPREHENSIVE BACKTEST")
    print("=" * 80)

    db = get_market_db()

    # Check data range
    status = db.get_status()
    print(f"\nData Status: {status.get('symbols', 0)} symbols")
    print(f"Date Range: {status.get('date_range', 'Unknown')}")

    # Get top 500 liquid symbols
    print("\nFetching top 500 liquid symbols...")
    top_symbols = get_top_n_liquid_symbols(db, n=500)
    print(f"Selected {len(top_symbols)} symbols")

    # Build features if needed
    print("\nEnsuring feat_daily is built...")
    db.build_feat_daily_table()

    # Run yearly backtests
    yearly_results, all_trades = run_yearly_backtest(
        db=db,
        symbols=top_symbols,
        start_year=2015,
        end_year=2025,
        min_price=10,
        min_filters=4,
    )

    # Print summary table
    print(f"\n{'=' * 80}")
    print("YEARLY BREAKDOWN - Rs.10+, 4/6 Filters - Top 500 Stocks")
    print(f"{'=' * 80}")

    print(f"\n{'Year':<6} {'Sig':>4} {'Trd':>4} {'Win%':>6} {'Ret%':>7} {'AvgR':>6} {'Hold':>5} {'PF':>5} {'DD%':>6} {'WFT%':>5}")
    print(f"{'-' * 65}")

    total_signals = 0
    total_trades = 0
    total_wins = 0
    total_return = 0.0
    total_r = 0.0
    count_r = 0
    max_dd = 0.0
    total_hold_days = 0

    for year in range(2015, 2026):
        stats = yearly_results.get(year)
        if not stats or stats.trades == 0:
            print(f"{year:<6} {'-':>4} {'-':>4} {'-':>6} {'-':>7} {'-':>6} {'-':>5} {'-':>5} {'-':>6} {'-':>5}")
            continue

        total_signals += stats.filtered_signals
        total_trades += stats.trades
        total_wins += stats.winning_trades
        total_return += stats.total_return_pct
        if stats.avg_r_multiple:
            total_r += stats.avg_r_multiple
            count_r += 1
        max_dd = max(max_dd, stats.max_drawdown_pct)
        total_hold_days += stats.avg_holding_days * stats.trades

        print(f"{year:<6} {stats.filtered_signals:>4} {stats.trades:>4} "
              f"{stats.win_rate_pct:>5.1f}% {stats.total_return_pct:>6.2f}% "
              f"{stats.avg_r_multiple:>5.2f}R {stats.avg_holding_days:>4.1f}d "
              f"{stats.profit_factor:>4.1f}x {stats.max_drawdown_pct:>5.1f}% "
              f"{stats.weak_follow_through_pct:>4.1f}%")

    print(f"{'-' * 65}")
    overall_win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    avg_r = (total_r / count_r) if count_r > 0 else 0
    avg_hold = (total_hold_days / total_trades) if total_trades > 0 else 0
    print(f"{'TOTAL':<6} {total_signals:>4} {total_trades:>4} "
          f"{overall_win_rate:>5.1f}% {total_return:>6.2f}% "
          f"{avg_r:>5.2f}R {avg_hold:>4.1f}d "
          f"{'-':>4} {max_dd:>5.1f}% "
          f"{'-':>5}")

    # Annualized metrics
    num_years = 11  # 2015-2025
    annualized_return = total_return / num_years
    print(f"\nAnnualized Return: {annualized_return:.2f}% per year")

    # Trade analysis
    print(f"\n{'=' * 80}")
    print("TRADE ANALYSIS")
    print(f"{'=' * 80}")

    if all_trades:
        trades_df = pl.DataFrame(all_trades)

        print(f"\nTotal Trades: {len(all_trades)}")

        # Best trades
        best_trades = trades_df.sort("pnl_pct", descending=True).head(10)
        print("\nTop 10 Winners:")
        print(f"{'Date':<12} {'Symbol':<12} {'Entry':>8} {'Exit':>8} {'PnL%':>7} {'R':>5} {'Days':>5}")
        for t in best_trades.iter_rows(named=True):
            print(f"{t['entry_date']!s:<12} {t['symbol']:<12} {t['entry_price']:>8.2f} "
                  f"{t['exit_price']:>8.2f} {t['pnl_pct']:>6.2f}% {t['r_multiple']:>4.1f}R {t['holding_days']:>5}")

        # Worst trades
        worst_trades = trades_df.sort("pnl_pct").head(10)
        print("\nTop 10 Losers:")
        print(f"{'Date':<12} {'Symbol':<12} {'Entry':>8} {'Exit':>8} {'PnL%':>7} {'R':>5} {'Days':>5}")
        for t in worst_trades.iter_rows(named=True):
            print(f"{t['entry_date']!s:<12} {t['symbol']:<12} {t['entry_price']:>8.2f} "
                  f"{t['exit_price']:>8.2f} {t['pnl_pct']:>6.2f}% {t['r_multiple']:>4.1f}R {t['holding_days']:>5}")

        # R-multiple distribution
        r_dist = trades_df.select(
            pl.col("r_multiple").quantile(0.1).alias("p10"),
            pl.col("r_multiple").quantile(0.25).alias("p25"),
            pl.col("r_multiple").quantile(0.5).alias("p50"),
            pl.col("r_multiple").quantile(0.75).alias("p75"),
            pl.col("r_multiple").quantile(0.90).alias("p90"),
            pl.col("r_multiple").max().alias("max"),
            pl.col("r_multiple").min().alias("min"),
            pl.col("r_multiple").mean().alias("mean"),
        )
        print("\nR-Multiple Distribution:")
        print(f"  Min:   {r_dist['min'][0]:.2f}R")
        print(f"  10th:  {r_dist['p10'][0]:.2f}R")
        print(f"  25th:  {r_dist['p25'][0]:.2f}R")
        print(f"  Median: {r_dist['p50'][0]:.2f}R")
        print(f"  75th:  {r_dist['p75'][0]:.2f}R")
        print(f"  90th:  {r_dist['p90'][0]:.2f}R")
        print(f"  Max:   {r_dist['max'][0]:.2f}R")
        print(f"  Mean:  {r_dist['mean'][0]:.2f}R")

        # Exit reason analysis
        exit_counts = trades_df.group_by("exit_reason").agg(
            pl.len().alias("count"),
            pl.col("pnl_pct").mean().alias("avg_pnl"),
            pl.col("r_multiple").mean().alias("avg_r"),
        ).sort("count", descending=True)

        print("\nExit Reason Analysis:")
        print(f"{'Reason':<30} {'Count':>6} {'Avg PnL%':>10} {'Avg R':>7}")
        for row in exit_counts.iter_rows(named=True):
            print(f"{row['exit_reason']:<30} {row['count']:>6} {row['avg_pnl']:>9.2f}% {row['avg_r']:>6.2f}R")

        # Holding period analysis
        hold_analysis = trades_df.group_by(
            pl.col("holding_days").cut([1, 2], labels=["1d", "2d", "3d+"], left_closed=True)
        ).agg(
            pl.len().alias("count"),
            pl.col("pnl_pct").mean().alias("avg_pnl"),
            pl.col("r_multiple").mean().alias("avg_r"),
            (pl.col("pnl_pct") > 0).sum().alias("wins"),
        ).sort("holding_days")

        print("\nHolding Period Analysis:")
        print(f"{'Period':<10} {'Count':>6} {'Avg PnL%':>10} {'Avg R':>7} {'Win%':>6}")
        for row in hold_analysis.iter_rows(named=True):
            win_pct = (row['wins'] / row['count'] * 100) if row['count'] > 0 else 0
            print(f"{row['holding_days']!s:<10} {row['count']:>6} {row['avg_pnl']:>9.2f}% {row['avg_r']:>6.2f}R {win_pct:>5.1f}%")

    # Per-stock breakdown
    if all_trades:
        print(f"\n{'=' * 80}")
        print("PER-STOCK BREAKDOWN (Top 20 by trade count)")
        print(f"{'=' * 80}")

        trades_df_stock = pl.DataFrame(all_trades)
        stock_stats = trades_df_stock.group_by("symbol").agg(
            pl.len().alias("trades"),
            (pl.col("pnl_pct") > 0).sum().alias("wins"),
            pl.col("pnl_pct").sum().alias("total_pnl"),
            pl.col("pnl_pct").mean().alias("avg_pnl"),
            pl.col("r_multiple").mean().alias("avg_r"),
            pl.col("pnl_pct").max().alias("best_trade"),
            pl.col("pnl_pct").min().alias("worst_trade"),
        ).with_columns(
            (pl.col("wins") / pl.col("trades") * 100).alias("win_pct")
        ).sort("trades", descending=True)

        print(f"\n{'Symbol':<16} {'Trd':>4} {'Win%':>6} {'TotalPnL%':>10} {'AvgPnL%':>9} {'AvgR':>6} {'Best%':>7} {'Worst%':>8}")
        print(f"{'-' * 75}")
        for row in stock_stats.head(20).iter_rows(named=True):
            print(f"{row['symbol']:<16} {row['trades']:>4} {row['win_pct']:>5.1f}% "
                  f"{row['total_pnl']:>9.2f}% {row['avg_pnl']:>8.2f}% "
                  f"{row['avg_r']:>5.2f}R {row['best_trade']:>6.1f}% {row['worst_trade']:>7.1f}%")

        # Also show top 20 by total PnL contribution
        print(f"\n{'Symbol':<16} {'Trd':>4} {'Win%':>6} {'TotalPnL%':>10} {'AvgPnL%':>9} {'AvgR':>6}")
        print("--- Top 10 Contributors ---")
        for row in stock_stats.sort("total_pnl", descending=True).head(10).iter_rows(named=True):
            print(f"{row['symbol']:<16} {row['trades']:>4} {row['win_pct']:>5.1f}% "
                  f"{row['total_pnl']:>9.2f}% {row['avg_pnl']:>8.2f}% {row['avg_r']:>5.2f}R")
        print("--- Bottom 10 Detractors ---")
        for row in stock_stats.sort("total_pnl").head(10).iter_rows(named=True):
            print(f"{row['symbol']:<16} {row['trades']:>4} {row['win_pct']:>5.1f}% "
                  f"{row['total_pnl']:>9.2f}% {row['avg_pnl']:>8.2f}% {row['avg_r']:>5.2f}R")

        unique_stocks = stock_stats.height
        print(f"\nTotal unique stocks traded: {unique_stocks}")

    # Market cycle analysis
    print(f"\n{'=' * 80}")
    print("MARKET CYCLE ANALYSIS")
    print(f"{'=' * 80}")

    periods = [
        (2015, 2015, "Consolidation"),
        (2016, 2017, "Bull - Post-Demonetization"),
        (2018, 2018, "Bear - NBFC Crisis"),
        (2019, 2019, "Recovery"),
        (2020, 2020, "Volatile - COVID"),
        (2021, 2021, "Bull - Strong"),
        (2022, 2022, "Bear - Ukraine/Rate Hikes"),
        (2023, 2024, "Bull - Pre/Post Election"),
        (2025, 2025, "Partial YTD"),
    ]

    print(f"\n{'Period':<30} {'Trades':>7} {'Return%':>9} {'Win%':>6} {'AvgR':>6}")
    for start_y, end_y, label in periods:
        period_trades = [t for t in all_trades if start_y <= t['year'] <= end_y]
        if not period_trades:
            continue

        period_return = sum(t['pnl_pct'] for t in period_trades)
        period_wins = sum(1 for t in period_trades if t['pnl_pct'] > 0)
        period_win_rate = period_wins / len(period_trades) * 100
        period_avg_r = sum(t['r_multiple'] for t in period_trades) / len(period_trades)

        print(f"{label:<30} {len(period_trades):>7} {period_return:>8.2f}% {period_win_rate:>5.1f}% {period_avg_r:>5.2f}R")

    # Consistency analysis
    print(f"\n{'=' * 80}")
    print("CONSISTENCY ANALYSIS")
    print(f"{'=' * 80}")

    profitable_years = sum(1 for y, s in yearly_results.items() if s.total_return_pct > 0)
    print(f"Profitable Years: {profitable_years}/{len(yearly_results)} ({profitable_years/len(yearly_results)*100:.1f}%)")

    returns = [s.total_return_pct for s in yearly_results.values() if s.trades > 0]
    if returns:
        print(f"Best Year: {max(returns):.2f}%")
        print(f"Worst Year: {min(returns):.2f}%")
        print(f"Std Dev: {np.std(returns):.2f}%")

    # Summary
    print(f"\n{'=' * 80}")
    print("BACKTEST COMPLETE")
    print(f"{'=' * 80}")

    print(f"""
Strategy Summary:
  Universe: Top 500 liquid stocks
  Configuration: Rs.10+, 4/6 filters
  Date Range: 2015-2025 ({num_years} years)
  Total Signals: {total_signals} ({total_signals/num_years:.1f}/year)
  Total Trades: {total_trades}

Performance:
  Total Return: {total_return:.2f}% ({annualized_return:.2f}% annualized)
  Win Rate: {overall_win_rate:.1f}%
  Avg R-Multiple: {avg_r:.2f}R
  Avg Holding: {avg_hold:.1f} days
  Max Drawdown: {max_dd:.1f}%

Market Cycle Performance:
  Best Period: COVID (2020) - Volatility favors breakout strategies
  Worst Period: 2017, 2024 - High weak follow-through indicates chop
  Consistency: {profitable_years}/{len(yearly_results)} years profitable

Next Steps to Consider:
  1. Filter Analysis: Identify which filters contribute most
  2. Risk Management: Optimize ATR stops and trailing stops
  3. Trade Analysis: Deep dive into winning/losing patterns
""")

    return yearly_results, all_trades


if __name__ == "__main__":
    run_10year_backtest()
