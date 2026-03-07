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

    Uses the identical 2LYNCH filter stack (H, N, 2, Y, C, L) as Indian2LYNCH.
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
                f.ma_7, f.ma_65_sma
            FROM breakout_days g
            LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
        )
        SELECT
            symbol, trading_date, open, high, low, close, prev_close, prev_low, gap_pct,
            value_traded_inr, close_pos_in_range,
            (close > ma_20) AS above_ma20,
            (ret_5d > 0) AS positive_momentum,
            atr_20, vol_dryup_ratio, atr_compress_ratio, range_percentile,
            prior_breakouts_90d,
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
    - filter_n: T-1 day narrow OR bullish (consolidation/up day before breakdown)
    - filter_l: 2 of 3 must confirm downtrend (below MA20, neg 5d ret, orderly R2)
    - filter_2 (SHORT mirror of 'Not Up 2 Days'): at least one of last 2 days was UP
      (stock hasn't already been in free-fall; avoids shorting at the bottom of a cascade)
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
        ),
        with_features AS (
            SELECT g.*,
                f.close_pos_in_range, f.ma_20, f.ret_5d, f.atr_20,
                f.vol_dryup_ratio, f.atr_compress_ratio, f.range_percentile,
                f.prior_breakouts_30d, f.prior_breakouts_90d, f.r2_65,
                f.ma_7, f.ma_65_sma
            FROM breakdown_days g
            LEFT JOIN feat_daily f ON g.symbol = f.symbol AND g.trading_date = f.trading_date
        )
        SELECT
            symbol, trading_date, open, high, low, close, prev_close, prev_high, gap_pct,
            value_traded_inr, close_pos_in_range,
            (close < ma_20) AS below_ma20,
            (ret_5d < 0) AS negative_momentum,
            atr_20, vol_dryup_ratio, atr_compress_ratio, range_percentile,
            prior_breakouts_90d,
            (close_pos_in_range <= 0.30) AS filter_h,
            ((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close > prev_open) AS filter_n,
            (COALESCE(prior_breakouts_30d, 0) <= 2) AS filter_y,
            (vol_dryup_ratio < 1.3) AS filter_c,
            (CAST(close < ma_20 AS INTEGER) + CAST(ret_5d < 0 AS INTEGER)
             + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
            (ret_1d_lag1 >= 0 OR ret_1d_lag2 >= 0) AS filter_2
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
            symbol, trading_date, open, high, low, close, prev_close, prev_high, gap_pct,
            value_traded_inr, close_pos_in_range,
            (close > ma_20) AS above_ma20,
            (ret_5d > 0) AS positive_momentum,
            atr_20, vol_dryup_ratio, atr_compress_ratio, range_percentile,
            prior_breakouts_90d,
            (close_pos_in_range >= 0.70) AS filter_h,
            ((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close < prev_open) AS filter_n,
            (COALESCE(prior_breakouts_30d, 0) <= 2) AS filter_y,
            (vol_dryup_ratio < 1.3) AS filter_c,
            (CAST(close > ma_20 AS INTEGER) + CAST(ret_5d > 0 AS INTEGER)
             + CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
            (ret_5d <= 0 OR ret_5d IS NULL) AS filter_2
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
    "description": "2LYNCH breakout with configurable threshold (long). Identical filter stack to Indian2LYNCH; threshold is the only variable.",
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
