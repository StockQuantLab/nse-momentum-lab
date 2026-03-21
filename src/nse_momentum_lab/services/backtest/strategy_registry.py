from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

from nse_momentum_lab.services.backtest.engine import PositionSide
from nse_momentum_lab.services.backtest.strategy_families import (
    _build_episodic_pivot_candidate_query,
    _build_threshold_breakdown_candidate_query,
    _build_threshold_breakout_candidate_query,
)

if TYPE_CHECKING:
    from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams

CandidateQueryBuilder = Callable[
    ["BacktestParams", list[str], date, date],
    tuple[str, list[object]],
]


def _normalize_strategy_key(raw: str) -> str:
    """Normalize strategy names for resilient resolution."""

    return "".join(ch for ch in raw.strip().lower() if ch.isalnum())


@dataclass(frozen=True)
class StrategyDefinition:
    """Strategy descriptor consumed by the DuckDB backtest runner."""

    name: str
    version: str
    description: str
    family: str
    direction: PositionSide = PositionSide.LONG
    strategy_label: Callable[[int], str] | None = None
    build_candidate_query: CandidateQueryBuilder | None = None
    default_params: dict[str, Any] | None = None
    # Entry admission filters (subset of all filter columns used for counting).
    # If None, falls back to filter_columns then to ALL_FILTERS.
    entry_filter_columns: list[str] | None = None
    # All filter columns emitted by the candidate query (superseded by entry_filter_columns).
    filter_columns: list[str] | None = None
    # Filters evaluated *after* entry admission (hold/carry quality checks).
    hold_quality_filter_columns: list[str] | None = None
    # Override params.min_filters for this strategy. None → use params value.
    min_filters_override: int | None = None

    def label_for_year(self, year: int) -> str:
        if self.strategy_label:
            return self.strategy_label(year)
        return f"{self.name}_{year}"

    def get_default_params(self) -> dict[str, Any]:
        return self.default_params or {}


def _build_2lynch_candidate_query(
    params: BacktestParams, symbols: list[str], start: date, end: date
) -> tuple[str, list[object]]:
    symbols_placeholders = ",".join("?" for _ in symbols)
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
    # Symbol placeholders appear in the IN clause (positions 3..N+2 in the SQL),
    # so symbols must precede the threshold values in the binding list.
    params_tuple: list[object] = [
        start.isoformat(),
        end.isoformat(),
        *symbols,
        float(params.breakout_threshold),
        float(params.min_price),
        float(params.min_value_traded_inr),
        float(params.min_volume),
    ]
    return query, params_tuple


_STRATEGY_REGISTRY: dict[str, StrategyDefinition] = {
    "indian2lynch": StrategyDefinition(
        name="Indian2LYNCH",
        version="1.0.0",
        description="India momentum burst strategy using proven 4% breakout + filter stack.",
        family="indian_2lynch",
        direction=PositionSide.LONG,
        strategy_label=lambda year: f"Indian2LYNCH_{year}",
        build_candidate_query=_build_2lynch_candidate_query,
    ),
    "2lynchbreakout": StrategyDefinition(
        name="2LYNCHBreakout",
        version="1.1.0",
        description=(
            "2LYNCH breakout with configurable threshold (long). "
            "Identical filter stack to Indian2LYNCH (H, N, 2, Y, C, L — 5/6 required); "
            "threshold is the only variable. At 4% matches Indian2LYNCH baseline."
        ),
        family="threshold_breakout",
        direction=PositionSide.LONG,
        strategy_label=lambda year: f"2LYNCHBreakout_{year}",
        build_candidate_query=_build_threshold_breakout_candidate_query,
        default_params={
            "breakout_threshold": 0.04,
            "breakout_reference": "prior_close",
            "min_price": 10,
            "min_value_traded_inr": 3_000_000,
            "min_volume": 50000,
        },
    ),
    # Backward-compat alias
    "thresholdbreakout": StrategyDefinition(
        name="2LYNCHBreakout",
        version="1.1.0",
        description="Alias for 2LYNCHBreakout.",
        family="threshold_breakout",
        direction=PositionSide.LONG,
        strategy_label=lambda year: f"2LYNCHBreakout_{year}",
        build_candidate_query=_build_threshold_breakout_candidate_query,
        default_params={
            "breakout_threshold": 0.04,
            "breakout_reference": "prior_close",
            "min_price": 10,
            "min_value_traded_inr": 3_000_000,
            "min_volume": 50000,
        },
    ),
    "2lynchbreakdown": StrategyDefinition(
        name="2LYNCHBreakdown",
        version="1.2.0",
        description=(
            "2LYNCH breakdown with configurable threshold (short). "
            "SHORT mirror of the 2LYNCH filter stack: close near low, T-1 narrow/bullish, "
            "downtrend quality, not-down-2-days (avoids shorting cascading stocks). "
            "v1.2.0: filter_y now requires rs_252 < 0 (genuine annual underperformer) "
            "in addition to <=2 prior 30d breakouts, acting as a per-stock regime gate."
        ),
        family="threshold_breakdown",
        direction=PositionSide.SHORT,
        strategy_label=lambda year: f"2LYNCHBreakdown_{year}",
        build_candidate_query=_build_threshold_breakdown_candidate_query,
        default_params={
            "breakout_threshold": 0.04,
            "breakout_reference": "prior_close",
            "min_price": 10,
            "min_value_traded_inr": 3_000_000,
            "min_volume": 50000,
        },
    ),
    # Backward-compat alias
    "thresholdbreakdown": StrategyDefinition(
        name="2LYNCHBreakdown",
        version="1.2.0",
        description="Alias for 2LYNCHBreakdown.",
        family="threshold_breakdown",
        direction=PositionSide.SHORT,
        strategy_label=lambda year: f"2LYNCHBreakdown_{year}",
        build_candidate_query=_build_threshold_breakdown_candidate_query,
        default_params={
            "breakout_threshold": 0.04,
            "breakout_reference": "prior_close",
            "min_price": 10,
            "min_value_traded_inr": 3_000_000,
            "min_volume": 50000,
        },
    ),
    "episodicpivot": StrategyDefinition(
        name="EpisodicPivot",
        version="1.0.0",
        description="Episodic pivot strategy based on significant gap moves and consolidation patterns.",
        family="episodic_pivot",
        direction=PositionSide.LONG,
        strategy_label=lambda year: f"EpisodicPivot_{year}",
        build_candidate_query=_build_episodic_pivot_candidate_query,
        default_params={
            "min_gap_pct": 0.05,
            "min_price": 10,
            "min_value_traded_inr": 3_000_000,
            "min_volume": 50000,
        },
    ),
}


def resolve_strategy(strategy_name: str) -> StrategyDefinition:
    key = _normalize_strategy_key(strategy_name)
    if key in _STRATEGY_REGISTRY:
        return _STRATEGY_REGISTRY[key]
    available = ", ".join(sorted({definition.name for definition in _STRATEGY_REGISTRY.values()}))
    raise ValueError(f"Unknown strategy '{strategy_name}'. Available strategies: {available}")


def list_strategies() -> list[StrategyDefinition]:
    """Return one entry per unique strategy (multiple aliases collapse to one)."""
    seen: set[str] = set()
    result: list[StrategyDefinition] = []
    for strategy in _STRATEGY_REGISTRY.values():
        if strategy.name not in seen:
            seen.add(strategy.name)
            result.append(strategy)
    return result
