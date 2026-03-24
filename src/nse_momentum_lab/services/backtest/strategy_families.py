"""
Strategy definitions for threshold_breakout, threshold_breakdown, and episodic_pivot.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from nse_momentum_lab.services.backtest.engine import PositionSide

if TYPE_CHECKING:
    from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams


CandidateQueryBuilder = Callable[
    ["BacktestParams", list[str], date, date],
    tuple[str, list[object]],
]


@dataclass(frozen=True)
class StrategyParams:
    """Strategy-specific parameters."""

    breakout_threshold: float = 0.04
    breakout_reference: str = "prior_close"
    min_price: float = 10.0
    min_value_traded_inr: float = 3_000_000
    min_volume: int = 50_000
    use_filters: bool = True
    min_filters: int = 5


def _build_threshold_breakout_candidate_query(
    params: BacktestParams, symbols: list[str], start: date, end: date
) -> tuple[str, list[object]]:
    """Build candidate query for 2LYNCH threshold breakout strategy (long).

    Uses the identical 2LYNCH filter stack (H, N, 2, Y, C, L) as the legacy 4% baseline.
    The threshold is a configurable parameter; all other filters are canonical 2LYNCH.
    filter_2 = 'Not Up 2 Days': at least one of the 2 days before the breakout was down.
    """
    symbols_placeholders = ",".join("?" for _ in symbols)
    threshold = getattr(params, "breakout_threshold", 0.04)
    min_price = getattr(params, "min_price", 10)
    min_value = getattr(params, "min_value_traded_inr", 3_000_000)
    min_vol = getattr(params, "min_volume", 50000)

    query = f"""
        WITH numbered_daily AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
            FROM v_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND symbol IN ({symbols_placeholders})
        ),
        with_lag AS (
            SELECT
                symbol, date AS trading_date, open, high, low, close, volume,
                LAG(date) OVER (PARTITION BY symbol ORDER BY date) AS prev_trading_date,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                LAG(high) OVER (PARTITION BY symbol ORDER BY date) AS prev_high,
                LAG(low) OVER (PARTITION BY symbol ORDER BY date) AS prev_low,
                LAG(open) OVER (PARTITION BY symbol ORDER BY date) AS prev_open,
                (LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag1,
                (LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag2,
                (open - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS gap_pct,
                close * volume AS value_traded_inr
            FROM numbered_daily WHERE rn > 1
        ),
        breakout_days AS (
            SELECT * FROM with_lag
            WHERE ((high - prev_close) / NULLIF(prev_close, 0)) >= ?
              AND prev_close IS NOT NULL
              AND close >= ?
              AND value_traded_inr >= ?
              AND volume >= ?
              AND ret_1d_lag1 IS NOT NULL
        ),
        with_features AS (
            SELECT g.*,
                f.close_pos_in_range, f.ma_20, f.ret_5d, f.atr_20,
                f.vol_dryup_ratio, f.atr_compress_ratio, f.range_percentile,
                f.prior_breakouts_30d, f.prior_breakouts_90d, f.r2_65,
                f.ma_7, f.ma_65_sma,
                f_prev.vol_dryup_ratio AS prev_vol_dryup_ratio,
                f_prev.atr_compress_ratio AS prev_atr_compress_ratio,
                f_prev.range_percentile AS prev_range_percentile
            FROM breakout_days g
            LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
            LEFT JOIN feat_daily f_prev
              ON g.symbol = f_prev.symbol AND g.prev_trading_date = f_prev.trading_date
        )
        SELECT
            symbol, trading_date, open, high, low, close, prev_close, prev_high, prev_low, prev_open, gap_pct,
            value_traded_inr, close_pos_in_range,
            (close > ma_20) AS above_ma20,
            (ret_5d > 0) AS positive_momentum,
            atr_20, vol_dryup_ratio, atr_compress_ratio, range_percentile, r2_65,
            prev_vol_dryup_ratio, prev_atr_compress_ratio, prev_range_percentile,
            prior_breakouts_30d, prior_breakouts_90d,
            (close_pos_in_range >= 0.70) AS filter_h,
            ((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close < prev_open) AS filter_n,
            (COALESCE(prior_breakouts_30d, 0) <= 2) AS filter_y,
            (vol_dryup_ratio < 1.3) AS filter_c,
            (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER)
             + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
            (ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0) AS filter_2
        FROM with_features
        WHERE close_pos_in_range IS NOT NULL
        ORDER BY trading_date, symbol
        """
    params_tuple: list[object] = [
        start.isoformat(),
        end.isoformat(),
        *symbols,
        float(threshold),
        float(min_price),
        float(min_value),
        int(min_vol),
    ]
    return query, params_tuple


def _build_threshold_breakdown_candidate_query(
    params: BacktestParams, symbols: list[str], start: date, end: date
) -> tuple[str, list[object]]:
    """Build candidate query for 2LYNCH threshold breakdown strategy (short).

    Mirror of the 2LYNCH breakout with SHORT-side filter inversions:
    - filter_h: close near the LOW (selling pressure, close in bottom 30%)
    - filter_n: T-1 day narrow OR bullish (consolidation/up day before breakdown mirrors
      the long-side "narrow OR red day before breakout")
    - filter_y: not over-broken-out (<=2 prior 30-day breakouts) AND stock has negative
      52-week relative return below rs_min threshold (default < 0, tighten via
      breakdown_rs_min param e.g. -0.10 requires >= 10% YTD underperformance).
    - filter_c: prior-day volume dry-up (same as long — quiet day before the move)
    - filter_l: 3 of 4 must confirm downtrend (below MA20, neg 5d ret, orderly R2 >= 0.70,
      close < MA65_SMA). Strengthened from original 2-of-3 by adding medium-term MA gate.
    - filter_2 (SHORT mirror of 'Not Up 2 Days'): at least one of last 2 days was UP
      (stock hasn't already been in free-fall; avoids shorting at the bottom of a cascade)
    """
    symbols_placeholders = ",".join("?" for _ in symbols)
    threshold = getattr(params, "breakout_threshold", 0.04)
    min_price = getattr(params, "min_price", 10)
    min_value = getattr(params, "min_value_traded_inr", 3_000_000)
    min_vol = getattr(params, "min_volume", 50000)
    rs_min = getattr(params, "breakdown_rs_min", 0.0)
    strict_filter_l = getattr(params, "breakdown_strict_filter_l", False)
    narrow_only_filter_n = getattr(params, "breakdown_filter_n_narrow_only", False)
    skip_gap_down = getattr(params, "breakdown_skip_gap_down", False)
    max_prior_breakdowns = getattr(params, "breakdown_max_prior_breakdowns", -1)
    breadth_threshold = getattr(params, "breakdown_breadth_threshold", None)
    ti65_mode = str(getattr(params, "breakdown_ti65_mode", "off")).strip().lower()
    if ti65_mode not in {"off", "bearish"}:
        raise ValueError(f"breakdown_ti65_mode must be one of: off, bearish (got {ti65_mode!r})")

    ti65_gate_sql = ""
    if ti65_mode == "bearish":
        ti65_gate_sql = (
            " AND (CASE "
            "WHEN ma_7 IS NOT NULL AND ma_65_sma IS NOT NULL AND ma_65_sma > 0 "
            "THEN (ma_7 / ma_65_sma) <= 0.95 "
            "ELSE close < ma_20 END)"
        )

    if strict_filter_l:
        filter_l_sql = (
            "(CAST(close < ma_20 AS INTEGER) + CAST(ret_5d < 0 AS INTEGER)"
            " + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER)"
            f" + CAST(close < ma_65_sma AS INTEGER) >= 3){ti65_gate_sql} AS filter_l"
        )
    else:
        filter_l_sql = (
            "(CAST(close < ma_20 AS INTEGER) + CAST(ret_5d < 0 AS INTEGER)"
            f" + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2){ti65_gate_sql} AS filter_l"
        )

    filter_n_sql = (
        "((prev_high - prev_low) < (atr_20 * 0.5)) AS filter_n"
        if narrow_only_filter_n
        else "((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close > prev_open) AS filter_n"
    )

    # Phase 2c: optionally exclude stocks that already gapped down by >= threshold at open
    gap_down_filter = "AND gap_pct > -?" if skip_gap_down else ""
    gap_down_params: list[object] = [float(threshold)] if skip_gap_down else []

    # Phase 1b: optionally cap prior 4%-down breakdown count in 90d (avoid exhausted shorts)
    # max_prior_breakdowns=-1 disables; >=0 adds extra condition to filter_y
    breakdown_counter_clause = (
        " AND COALESCE(prior_breakdowns_90d, 0) <= ?" if max_prior_breakdowns >= 0 else ""
    )
    breakdown_counter_params: list[object] = (
        [int(max_prior_breakdowns)] if max_prior_breakdowns >= 0 else []
    )

    require_atr_expansion = bool(getattr(params, "breakdown_require_atr_expansion", False))
    atr_expansion_where = (
        "            AND atr_20_sma20 IS NOT NULL AND atr_20 > atr_20_sma20"
        if require_atr_expansion
        else ""
    )
    atr_expansion_select = (
        ",\n                AVG(f.atr_20) OVER (\n                    PARTITION BY g.symbol ORDER BY g.trading_date\n                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW\n                ) AS atr_20_sma20"
        if require_atr_expansion
        else ""
    )
    if breadth_threshold is not None:
        breadth_threshold = float(breadth_threshold)
        if not 0.0 <= breadth_threshold <= 1.0:
            raise ValueError("breakdown_breadth_threshold must be between 0.0 and 1.0 when set")
        breadth_cte = """
            , breakdown_breadth AS (
                SELECT
                    d.date AS trading_date,
                    SUM(
                        CASE
                            WHEN f.ma_20 IS NOT NULL AND d.close < f.ma_20 THEN 1
                            ELSE 0
                        END
                    )::DOUBLE / NULLIF(
                        SUM(
                            CASE
                                WHEN f.ma_20 IS NOT NULL THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS pct_below_ma20
                FROM v_daily d
                LEFT JOIN feat_daily f
                    ON d.symbol = f.symbol
                    AND d.date = f.trading_date
                WHERE d.date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                GROUP BY d.date
            )
        """
        breadth_select = (
            "                ,(SELECT COALESCE(pct_below_ma20, 0.0)\n"
            "                  FROM breakdown_breadth b\n"
            "                  WHERE b.trading_date = g.trading_date\n"
            "                  LIMIT 1) AS market_pct_below_ma20\n"
        )
        breadth_where = "        AND COALESCE(market_pct_below_ma20, 0.0) > ?"
        breadth_param = [breadth_threshold]
    else:
        breadth_cte = ""
        breadth_select = ""
        breadth_where = ""
        breadth_param = []

    query = f"""
        WITH numbered_daily AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
            FROM v_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND symbol IN ({symbols_placeholders})
        ),
        with_lag AS (
            SELECT
                symbol, date AS trading_date, open, high, low, close, volume,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                LAG(high) OVER (PARTITION BY symbol ORDER BY date) AS prev_high,
                LAG(low) OVER (PARTITION BY symbol ORDER BY date) AS prev_low,
                LAG(open) OVER (PARTITION BY symbol ORDER BY date) AS prev_open,
                (LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag1,
                (LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag2,
                (open - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS gap_pct,
                close * volume AS value_traded_inr
            FROM numbered_daily WHERE rn > 1
        ),
        breakdown_days AS (
            SELECT * FROM with_lag
            WHERE ((prev_close - low) / NULLIF(prev_close, 0)) >= ?
              AND prev_close IS NOT NULL
              AND close >= ?
              AND value_traded_inr >= ?
              AND volume >= ?
              AND ret_1d_lag1 IS NOT NULL
              {gap_down_filter}
        ){breadth_cte},
        with_features AS (
            SELECT g.*,
                f.close_pos_in_range, f.ma_20, f.ret_5d, f.atr_20,
                f.vol_dryup_ratio, f.atr_compress_ratio, f.range_percentile,
                f.prior_breakouts_30d, f.prior_breakouts_90d, f.r2_65,
            f.prior_breakdowns_90d{atr_expansion_select},
                f.ma_7, f.ma_65_sma, f.rs_252
                {breadth_select}
            FROM breakdown_days g
            LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
        )
        SELECT
            symbol, trading_date, open, high, low, close, prev_close, prev_high, gap_pct,
            value_traded_inr, close_pos_in_range,
            (close < ma_20) AS below_ma20,
            (ret_5d < 0) AS negative_momentum,
            atr_20, vol_dryup_ratio, atr_compress_ratio, range_percentile,
            prior_breakouts_90d, prior_breakdowns_90d, r2_65, rs_252,
            (close_pos_in_range <= 0.30) AS filter_h,
            {filter_n_sql},
            (COALESCE(prior_breakouts_30d, 0) <= 2 AND COALESCE(rs_252, 1.0) < ?{breakdown_counter_clause}) AS filter_y,
            (vol_dryup_ratio < 1.3) AS filter_c,
            {filter_l_sql},
            (ret_1d_lag1 >= 0 OR ret_1d_lag2 >= 0) AS filter_2
        FROM with_features
        WHERE close_pos_in_range IS NOT NULL
        {atr_expansion_where}
        {breadth_where}
        ORDER BY trading_date, symbol
        """
    params_tuple: list[object] = [
        start.isoformat(),
        end.isoformat(),
        *symbols,
        float(threshold),
        float(min_price),
        float(min_value),
        int(min_vol),
        *gap_down_params,  # conditional: threshold for gap-down skip
        *([start.isoformat(), end.isoformat()] if breadth_threshold is not None else []),
        float(rs_min),  # filter_y rs_252 threshold (default 0.0 → rs_252 < 0)
        *breakdown_counter_params,  # conditional: max prior_breakdowns_90d for filter_y
        *breadth_param,  # conditional: minimum market breadth for short entries
    ]
    return query, params_tuple


def _build_episodic_pivot_candidate_query(
    params: BacktestParams, symbols: list[str], start: date, end: date
) -> tuple[str, list[object]]:
    """Build candidate query for episodic pivot strategy.

    Looks for stocks that have had a significant price movement (gap up/down)
    and are now consolidating/pivoting.

    NOTE: EpisodicPivot intentionally uses ret_5d for filter_2, NOT ret_1d_lag1/lag2.
    This strategy is NOT a 2LYNCH breakout variant — it is gap/catalyst-driven. The
    filter_2 here means "the 5-day trend before the gap was flat or negative" (genuine
    catalyst, not momentum continuation). Do not "fix" this to use ret_1d_lag1/lag2 —
    that would be incorrect for this strategy's intent.
    """
    symbols_placeholders = ",".join("?" for _ in symbols)
    min_price = getattr(params, "min_price", 10)
    min_value = getattr(params, "min_value_traded_inr", 3_000_000)
    min_vol = getattr(params, "min_volume", 50000)
    min_gap = 0.05

    query = f"""
        WITH numbered_daily AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
            FROM v_daily
            WHERE date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND symbol IN ({symbols_placeholders})
        ),
        with_lag AS (
            SELECT
                symbol, date AS trading_date, open, high, low, close, volume,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                LAG(high) OVER (PARTITION BY symbol ORDER BY date) AS prev_high,
                LAG(low) OVER (PARTITION BY symbol ORDER BY date) AS prev_low,
                LAG(open) OVER (PARTITION BY symbol ORDER BY date) AS prev_open,
                (open - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0) AS gap_pct,
                close * volume AS value_traded_inr
            FROM numbered_daily WHERE rn > 2
        ),
        pivot_candidates AS (
            SELECT * FROM with_lag
            WHERE ABS(gap_pct) >= ?
              AND prev_close IS NOT NULL
              AND close >= ?
              AND value_traded_inr >= ?
              AND volume >= ?
        ),
        with_features AS (
            SELECT g.*,
                f.close_pos_in_range, f.ma_20, f.ret_5d, f.atr_20,
                f.vol_dryup_ratio, f.atr_compress_ratio, f.range_percentile,
                f.prior_breakouts_30d, f.prior_breakouts_90d, f.r2_65,
                f.ma_7, f.ma_65_sma
            FROM pivot_candidates g
            LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
        )
        SELECT
            symbol, trading_date, open, high, low, close, prev_close, prev_high, prev_low, prev_open, gap_pct,
            value_traded_inr, close_pos_in_range,
            (close > ma_20) AS above_ma20,
            (ret_5d > 0) AS positive_momentum,
            atr_20, vol_dryup_ratio, atr_compress_ratio, range_percentile, r2_65,
            prior_breakouts_30d, prior_breakouts_90d
        FROM with_features
        WHERE close_pos_in_range IS NOT NULL
        ORDER BY trading_date, symbol
        """
    params_tuple: list[object] = [
        start.isoformat(),
        end.isoformat(),
        *symbols,
        float(min_gap),
        float(min_price),
        float(min_value),
        int(min_vol),
    ]
    return query, params_tuple


THRESHOLD_BREAKOUT_DEFINITION = {
    "name": "2LYNCHBreakout",
    "version": "1.1.0",
    "family": "threshold_breakout",
    "description": "2LYNCH breakout with configurable threshold (long). Identical filter stack to the legacy 4% baseline; threshold is the only variable.",
    "direction": PositionSide.LONG,
    "default_params": {
        "breakout_threshold": 0.04,
        "breakout_reference": "prior_close",
        "min_price": 10,
        "min_value_traded_inr": 3_000_000,
        "min_volume": 50000,
    },
    "build_candidate_query": _build_threshold_breakout_candidate_query,
}


THRESHOLD_BREAKDOWN_DEFINITION = {
    "name": "2LYNCHBreakdown",
    "version": "1.1.0",
    "family": "threshold_breakdown",
    "description": "2LYNCH breakdown with configurable threshold (short). SHORT mirror of 2LYNCH filter stack.",
    "direction": PositionSide.SHORT,
    "default_params": {
        "breakout_threshold": 0.04,
        "breakout_reference": "prior_close",
        "min_price": 10,
        "min_value_traded_inr": 3_000_000,
        "min_volume": 50000,
    },
    "build_candidate_query": _build_threshold_breakdown_candidate_query,
}


EPISODIC_PIVOT_DEFINITION = {
    "name": "EpisodicPivot",
    "version": "1.0.0",
    "family": "episodic_pivot",
    "description": "Episodic pivot strategy based on significant gap moves and consolidation",
    "direction": PositionSide.LONG,
    "default_params": {
        "min_gap_pct": 0.05,
        "min_price": 10,
        "min_value_traded_inr": 3_000_000,
        "min_volume": 50000,
    },
    "build_candidate_query": _build_episodic_pivot_candidate_query,
}
