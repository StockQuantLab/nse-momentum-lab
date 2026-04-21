"""Read-only candidate builder for paper/live workflows.

Extracts candidate query execution, filter scoring, selection ranking, and
intraday entry resolution from ``DuckDBBacktestRunner`` so that daily paper
bootstrap never requires writable backtest storage or research-run assumptions.

The ranking and entry-resolution functions are pure data transforms — they
take Polars DataFrames in and return DataFrames/dicts out.  Only the bulk
5-min candle query needs a DuckDB connection, and it uses a *read-only*
market-db handle (never ``backtest.duckdb``).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import polars as pl

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    IntradayEntry,
)
from nse_momentum_lab.services.backtest.engine import PositionSide
from nse_momentum_lab.services.backtest.intraday_execution import (
    IntradayExecutionResult,
    resolve_intraday_execution_from_5min,
)
from nse_momentum_lab.services.backtest.strategy_registry import StrategyDefinition

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Selection ranking (extracted from DuckDBBacktestRunner private methods)
# ------------------------------------------------------------------


def apply_breakout_selection_ranking(
    df_filtered: pl.DataFrame,
    params: BacktestParams,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Rank and budget daily breakout (long) candidates.

    Pure data transform — no side effects.
    """
    if df_filtered.is_empty():
        return df_filtered, df_filtered

    use_current_day_c = bool(params.breakout_use_current_day_c_quality)

    optional_prev_cols = (
        "prev_vol_dryup_ratio",
        "prev_atr_compress_ratio",
        "prev_range_percentile",
    )
    for col in optional_prev_cols:
        if col not in df_filtered.columns:
            df_filtered = df_filtered.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

    if use_current_day_c:
        c_vol_expr = pl.col("vol_dryup_ratio").fill_null(99.0)
        c_atr_expr = pl.col("atr_compress_ratio").fill_null(99.0)
        c_rng_expr = pl.col("range_percentile").fill_null(99.0)
    else:
        c_vol_expr = pl.coalesce(
            [pl.col("prev_vol_dryup_ratio"), pl.col("vol_dryup_ratio"), pl.lit(99.0)]
        )
        c_atr_expr = pl.coalesce(
            [pl.col("prev_atr_compress_ratio"), pl.col("atr_compress_ratio"), pl.lit(99.0)]
        )
        c_rng_expr = pl.coalesce(
            [pl.col("prev_range_percentile"), pl.col("range_percentile"), pl.lit(99.0)]
        )

    ranked = df_filtered.with_columns(
        (
            (c_vol_expr <= 1.0).cast(pl.Int64)
            + (c_atr_expr <= 1.10).cast(pl.Int64)
            + (c_rng_expr <= 0.60).cast(pl.Int64)
        ).alias("selection_c_strength"),
        (
            pl.when(pl.col("prior_breakouts_30d").fill_null(0) <= 0)
            .then(3)
            .when(pl.col("prior_breakouts_30d").fill_null(0) == 1)
            .then(2)
            .when(pl.col("prior_breakouts_30d").fill_null(0) == 2)
            .then(1)
            .otherwise(0)
        ).alias("selection_y_score"),
        (
            pl.when(
                pl.col("prev_close").is_not_null()
                & pl.col("prev_open").is_not_null()
                & (pl.col("prev_close") < pl.col("prev_open"))
            )
            .then(2)
            .when(
                pl.col("prev_high").is_not_null()
                & pl.col("prev_low").is_not_null()
                & pl.col("atr_20").is_not_null()
                & ((pl.col("prev_high") - pl.col("prev_low")) < (pl.col("atr_20") * 0.5))
            )
            .then(1)
            .otherwise(0)
        ).alias("selection_n_score"),
        pl.col("r2_65").clip(0.0, 1.0).fill_null(0.0).alias("selection_r2_quality"),
    ).with_columns(
        (
            pl.col("selection_c_strength") * 10_000
            + pl.col("selection_y_score") * 1_000
            + ((pl.col("selection_r2_quality") * 100).round(0).cast(pl.Int64) * 5)
            + pl.col("selection_n_score")
        )
        .cast(pl.Float64)
        .alias("selection_score")
    )

    ranked = ranked.sort(
        ["trading_date", "selection_score", "value_traded_inr", "symbol"],
        descending=[False, True, True, False],
    ).with_columns(
        pl.col("symbol").cum_count().over("trading_date").alias("selection_rank"),
        pl.lit(int(params.breakout_daily_candidate_budget)).alias("selection_budget"),
    )

    budget = int(params.breakout_daily_candidate_budget)
    if budget <= 0:
        return ranked, ranked.head(0)
    return (
        ranked.filter(pl.col("selection_rank") <= budget),
        ranked.filter(pl.col("selection_rank") > budget),
    )


def apply_breakdown_selection_ranking(
    df_filtered: pl.DataFrame,
    params: BacktestParams,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Rank and budget daily breakdown (short) candidates.

    Pure data transform — no side effects.
    """
    if df_filtered.is_empty():
        return df_filtered, df_filtered

    r2_score = (pl.col("r2_65").fill_null(0.0).clip(0.0, 1.0) * 5_000.0).alias(
        "selection_r2_quality"
    )
    h_score = (
        ((0.30 - pl.col("close_pos_in_range").fill_null(0.30).clip(0.0, 0.30)) / 0.30) * 2_000.0
    ).alias("selection_h_quality")
    c_score = (
        ((1.3 - pl.col("vol_dryup_ratio").fill_null(1.3).clip(0.0, 1.3)) / 1.3) * 1_000.0
    ).alias("selection_c_strength")
    freshness_score = (
        pl.when(pl.col("prior_breakdowns_90d").fill_null(0) == 0)
        .then(300)
        .when(pl.col("prior_breakdowns_90d").fill_null(0) == 1)
        .then(200)
        .when(pl.col("prior_breakdowns_90d").fill_null(0) == 2)
        .then(100)
        .otherwise(0)
    ).alias("selection_freshness")
    rs_score = (pl.col("rs_252").fill_null(0.0).clip(-0.40, 0.0).abs() / 0.40 * 200.0).alias(
        "selection_rs_score"
    )

    ranked = df_filtered.with_columns(
        h_score, r2_score, c_score, freshness_score, rs_score
    ).with_columns(
        (
            pl.col("selection_h_quality")
            + pl.col("selection_r2_quality")
            + pl.col("selection_c_strength")
            + pl.col("selection_freshness").cast(pl.Float64)
            + pl.col("selection_rs_score")
        )
        .cast(pl.Float64)
        .alias("selection_score")
    )

    ranked = ranked.sort(
        ["trading_date", "selection_score", "value_traded_inr", "symbol"],
        descending=[False, True, True, False],
    ).with_columns(
        pl.col("symbol").cum_count().over("trading_date").alias("selection_rank"),
        pl.lit(int(params.breakdown_daily_candidate_budget)).alias("selection_budget"),
    )
    ranked = ranked.with_columns(
        # Semantic remaps for downstream selection_components_json compatibility:
        # h_quality maps to n_score (filter-N quality signal)
        pl.col("selection_h_quality").alias("selection_n_score"),
        # freshness maps to y_score (filter-Y/young breakout signal)
        pl.col("selection_freshness").alias("selection_y_score"),
    )

    budget = int(params.breakdown_daily_candidate_budget)
    if budget <= 0:
        return ranked, ranked.head(0)
    return (
        ranked.filter(pl.col("selection_rank") <= budget),
        ranked.filter(pl.col("selection_rank") > budget),
    )


# ------------------------------------------------------------------
# Parameter resolvers (static helpers extracted from runner)
# ------------------------------------------------------------------


def resolve_entry_cutoff_minutes(
    params: BacktestParams,
    strategy: StrategyDefinition | None = None,
) -> int:
    if strategy is not None and strategy.direction == PositionSide.SHORT:
        return (
            params.short_entry_cutoff_minutes
            if params.short_entry_cutoff_minutes is not None
            else params.entry_cutoff_minutes
        )
    return params.entry_cutoff_minutes


def resolve_same_day_r_ladder_start_r(
    params: BacktestParams,
    strategy: StrategyDefinition | None = None,
) -> int:
    if (
        params.short_same_day_r_ladder_start_r is not None
        and strategy is not None
        and strategy.direction == PositionSide.SHORT
    ):
        return int(params.short_same_day_r_ladder_start_r)
    return int(params.same_day_r_ladder_start_r)


# ------------------------------------------------------------------
# Intraday entry resolution (bulk, read-only)
# ------------------------------------------------------------------


def resolve_intraday_entry_from_5min(
    candles: pl.DataFrame,
    breakout_price: float,
    entry_cutoff_minutes: int = 30,
    is_short: bool = False,
    orh_window_minutes: int = 0,
    entry_start_minutes: int = 0,
    same_day_r_ladder: bool = False,
    same_day_r_ladder_start_r: int = 2,
    short_initial_stop_atr: float | None = None,
    short_initial_stop_atr_cap_mult: float | None = None,
    short_same_day_take_profit_pct: float | None = None,
) -> IntradayEntry | None:
    """Resolve a single intraday entry from 5-min candles.

    Delegates to ``resolve_intraday_execution_from_5min`` from the engine.
    """
    result: IntradayExecutionResult | None = resolve_intraday_execution_from_5min(
        candles,
        breakout_price=breakout_price,
        entry_cutoff_minutes=entry_cutoff_minutes,
        is_short=is_short,
        orh_window_minutes=orh_window_minutes,
        entry_start_minutes=entry_start_minutes,
        same_day_r_ladder=same_day_r_ladder,
        same_day_r_ladder_start_r=same_day_r_ladder_start_r,
        short_initial_stop_atr=short_initial_stop_atr,
        short_initial_stop_atr_cap_mult=short_initial_stop_atr_cap_mult,
        short_same_day_take_profit_pct=short_same_day_take_profit_pct,
    )
    if result is None:
        return None
    return {
        "entry_price": result.entry_price,
        "initial_stop": result.initial_stop,
        "same_day_stop_hit": result.same_day_exit_ts is not None,
        "entry_ts": result.entry_ts,
        "same_day_exit_price": result.same_day_exit_price,
        "same_day_exit_ts": result.same_day_exit_ts,
        "same_day_exit_time": result.same_day_exit_time,
        "same_day_exit_reason": result.same_day_exit_reason.value
        if result.same_day_exit_reason is not None
        else None,
        "carry_stop_next_session": result.carry_stop_next_session,
        "entry_time": result.entry_time,
    }


def resolve_intraday_entries_bulk(
    db_con: Any,
    *,
    df_filtered: pl.DataFrame,
    breakout_threshold: float,
    entry_cutoff_minutes: int = 30,
    is_short: bool = False,
    orh_window_minutes: int = 0,
    entry_start_minutes: int = 0,
    same_day_r_ladder: bool = False,
    same_day_r_ladder_start_r: int = 2,
    short_initial_stop_atr_cap_mult: float | None = None,
    short_same_day_take_profit_pct: float | None = None,
) -> dict[tuple[str, date], IntradayEntry]:
    """Resolve intraday entries for all signal days with one 5-min batch query.

    Uses a raw DuckDB connection (read-only market db), never
    ``backtest.duckdb``.
    """
    targets = (
        df_filtered.select(["symbol", "trading_date", "prev_close", "atr_20"])
        .drop_nulls(["symbol", "trading_date", "prev_close"])
        .unique(subset=["symbol", "trading_date"], keep="first", maintain_order=True)
    )
    if targets.is_empty():
        return {}

    breakout_price_by_key: dict[tuple[str, date], float] = {}
    short_initial_stop_atr_by_key: dict[tuple[str, date], float] = {}
    for symbol_raw, trading_date_raw, prev_close_raw, atr_20_raw in targets.iter_rows():
        if trading_date_raw is None or prev_close_raw is None:
            continue
        if isinstance(trading_date_raw, datetime):
            trading_day = trading_date_raw.date()
        elif isinstance(trading_date_raw, date):
            trading_day = trading_date_raw
        else:
            try:
                trading_day = date.fromisoformat(str(trading_date_raw))
            except ValueError:
                continue

        prev_close = float(prev_close_raw)
        if prev_close <= 0:
            continue
        symbol = str(symbol_raw)
        if orh_window_minutes > 0:
            breakout_price_by_key[(symbol, trading_day)] = prev_close
        else:
            multiplier = (1 - breakout_threshold) if is_short else (1 + breakout_threshold)
            breakout_price_by_key[(symbol, trading_day)] = prev_close * multiplier
        if is_short and short_initial_stop_atr_cap_mult is not None and atr_20_raw is not None:
            atr_20 = float(atr_20_raw)
            if atr_20 > 0:
                short_initial_stop_atr_by_key[(symbol, trading_day)] = atr_20
    if not breakout_price_by_key:
        return {}

    join_df = pl.DataFrame(
        [
            {"symbol": symbol, "trading_date": trading_day}
            for symbol, trading_day in breakout_price_by_key
        ]
    )
    tmp_name = "tmp_paper_intraday_signal_days"
    db_con.register(tmp_name, join_df.to_arrow())
    try:
        candles = db_con.execute(
            f"""
            SELECT c.symbol, c.date AS trading_date, c.candle_time, c.open, c.high, c.low
            FROM v_5min c
            INNER JOIN {tmp_name} t
              ON c.symbol = t.symbol
             AND c.date = t.trading_date
            ORDER BY c.symbol, c.date, c.candle_time
            """
        ).pl()
    finally:
        try:
            db_con.unregister(tmp_name)
        except Exception:
            pass

    if candles.is_empty():
        return {}

    resolved_entries: dict[tuple[str, date], IntradayEntry] = {}
    grouped = candles.partition_by(["symbol", "trading_date"], as_dict=True, maintain_order=True)
    for group_key, group_candles in grouped.items():
        symbol = str(group_key[0])
        trading_day_raw = group_key[1]
        if isinstance(trading_day_raw, datetime):
            trading_day = trading_day_raw.date()
        elif isinstance(trading_day_raw, date):
            trading_day = trading_day_raw
        else:
            try:
                trading_day = date.fromisoformat(str(trading_day_raw))
            except ValueError:
                continue

        breakout_price = breakout_price_by_key.get((symbol, trading_day))
        if breakout_price is None:
            continue

        intraday_entry = resolve_intraday_entry_from_5min(
            group_candles,
            breakout_price,
            entry_cutoff_minutes,
            is_short=is_short,
            orh_window_minutes=orh_window_minutes,
            entry_start_minutes=entry_start_minutes,
            same_day_r_ladder=same_day_r_ladder,
            same_day_r_ladder_start_r=same_day_r_ladder_start_r,
            short_initial_stop_atr=short_initial_stop_atr_by_key.get((symbol, trading_day)),
            short_initial_stop_atr_cap_mult=short_initial_stop_atr_cap_mult,
            short_same_day_take_profit_pct=short_same_day_take_profit_pct,
        )
        if intraday_entry is not None:
            resolved_entries[(symbol, trading_day)] = intraday_entry

    return resolved_entries
