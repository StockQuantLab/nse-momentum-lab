"""Live watchlist and intraday trigger model for paper trading.

Two-phase candidate model:
1. **Pre-open watchlist** — select symbols from prior-day features only
2. **Intraday trigger** — promote a watched symbol to TRIGGERED when live
   bars confirm the breakout/down condition.

States: WATCH → ARMED → TRIGGERED / REJECTED
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Literal

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db

logger = logging.getLogger(__name__)

CandidateState = Literal["WATCH", "ARMED", "TRIGGERED", "REJECTED"]
_WATCHLIST_CACHE: dict[tuple[Any, ...], pl.DataFrame] = {}
_WATCHLIST_CACHE_MAX_ENTRIES = 32


def _cache_watchlist(key: tuple[Any, ...], frame: pl.DataFrame) -> pl.DataFrame:
    if len(_WATCHLIST_CACHE) >= _WATCHLIST_CACHE_MAX_ENTRIES and key not in _WATCHLIST_CACHE:
        oldest_key = next(iter(_WATCHLIST_CACHE))
        _WATCHLIST_CACHE.pop(oldest_key, None)
    cached = frame.clone()
    _WATCHLIST_CACHE[key] = cached
    return cached.clone()


def _build_latest_bar_fallback_watchlist(
    *,
    symbols: list[str],
    trade_date: date,
    min_price: float,
) -> pl.DataFrame:
    if not symbols or trade_date is None:
        return pl.DataFrame()

    placeholders = ", ".join("?" for _ in symbols)
    market_db = get_market_db(read_only=True)
    query = f"""
    WITH latest_daily AS (
        SELECT symbol, MAX(date) AS watch_date
        FROM v_daily
        WHERE date < CAST(? AS DATE)
          AND symbol IN ({placeholders})
        GROUP BY symbol
    )
    SELECT
        d.symbol,
        d.date AS watch_date,
        d.close AS last_close,
        f.close_pos_in_range,
        f.atr_20,
        f.range_percentile,
        f.vol_dryup_ratio,
        f.r2_65,
        d.close * d.volume AS value_traded_inr,
        FALSE AS filter_h,
        FALSE AS filter_n,
        FALSE AS filter_y,
        FALSE AS filter_c,
        FALSE AS filter_l,
        FALSE AS filter_2,
        0 AS filters_passed
    FROM latest_daily ld
    JOIN v_daily d ON d.symbol = ld.symbol AND d.date = ld.watch_date
    LEFT JOIN feat_daily f ON f.symbol = d.symbol AND f.date = d.date
    WHERE d.close >= ?
    ORDER BY value_traded_inr DESC, d.symbol
    """
    params = [trade_date.isoformat(), *symbols, min_price]
    return market_db.con.execute(query, params).pl()


def build_prior_day_watchlist(
    *,
    symbols: list[str],
    trade_date: date,
    strategy: str = "thresholdbreakout",
    threshold: float = 0.04,
    direction: str = "long",
    min_price: float = 10.0,
    min_filters: int = 5,
) -> pl.DataFrame:
    """Build a watchlist from prior-day features only.

    Returns a Polars DataFrame with one row per symbol that passes the
    prior-day quality filters.  This query uses only ``v_daily`` T-1 data and
    ``feat_daily`` T-1 features — no same-day fields.

    Parameters
    ----------
    symbols:
        Universe to screen.
    trade_date:
        The session date.  The query looks at T-1 features.
    strategy:
        ``thresholdbreakout`` or ``thresholdbreakdown``.
    threshold:
        Breakout/down threshold (e.g. 0.02, 0.04).
    direction:
        ``long`` or ``short``.
    min_price:
        Minimum closing price filter.
    min_filters:
        Minimum filter count to qualify for watchlist.
    """
    if not symbols or trade_date is None:
        return pl.DataFrame()

    cache_key = (
        tuple(symbols),
        trade_date.isoformat(),
        strategy,
        round(float(threshold), 6),
        direction.lower().strip(),
        round(float(min_price), 6),
        int(min_filters),
    )
    cached = _WATCHLIST_CACHE.get(cache_key)
    if cached is not None:
        return cached.clone()

    context_end = trade_date - timedelta(days=1)
    context_start = context_end - timedelta(days=90)

    market_db = get_market_db(read_only=True)

    placeholders = ", ".join("?" for _ in symbols)
    is_long = direction.lower() != "short"
    used_fallback = False

    if is_long:
        query = f"""
        WITH base_daily AS (
            SELECT
                symbol,
                date,
                close,
                high,
                low,
                open,
                volume,
                LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date) AS prev_close_2,
                LAG(open, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_open,
                (LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag1,
                close * volume AS value_traded_inr,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS latest_rn
            FROM v_daily
            WHERE date BETWEEN CAST(? AS DATE)
                  AND CAST(? AS DATE)
              AND symbol IN ({placeholders})
        ),
        latest AS (
            SELECT *
            FROM base_daily
            WHERE latest_rn = 1
        ),
        with_features AS (
            SELECT l.*,
                f.close_pos_in_range, f.ma_20, f.ret_5d, f.atr_20,
                f.vol_dryup_ratio, f.atr_compress_ratio, f.range_percentile,
                f.prior_breakouts_30d, f.r2_65, f.ma_65_sma,
                (f.close_pos_in_range >= 0.70) AS filter_h,
                (
                 (
                  l.prev_close
                  - COALESCE(l.prev_close_2, l.close)
                 )
                 < (f.atr_20 * 0.5)
                 OR l.prev_close
                    < COALESCE(l.prev_open, l.open)
                ) AS filter_n,
                (COALESCE(f.prior_breakouts_30d, 0) <= 2) AS filter_y,
                (f.vol_dryup_ratio < 1.3) AS filter_c,
                (CAST(l.prev_close > f.ma_20 AS INTEGER)
                 + CAST(f.ret_5d > 0 AS INTEGER)
                 + CAST(COALESCE(NULLIF(f.r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
                (l.ret_1d_lag1 <= 0) AS filter_2
            FROM latest l
            LEFT JOIN feat_daily f ON l.symbol = f.symbol AND l.date = f.date
        )
        SELECT
            symbol,
            date AS watch_date,
            close AS last_close,
            close_pos_in_range,
            atr_20,
            range_percentile,
            vol_dryup_ratio,
            r2_65,
            value_traded_inr,
            filter_h, filter_n, filter_y, filter_c, filter_l, filter_2,
            (CAST(filter_h AS INTEGER) + CAST(filter_n AS INTEGER)
             + CAST(filter_y AS INTEGER) + CAST(filter_c AS INTEGER)
             + CAST(filter_l AS INTEGER) + CAST(filter_2 AS INTEGER)) AS filters_passed
        FROM with_features
        WHERE close_pos_in_range IS NOT NULL
          AND close >= ?
        ORDER BY filters_passed DESC, value_traded_inr DESC
        """
        params = [context_start.isoformat(), context_end.isoformat(), *symbols, min_price]
    else:
        query = f"""
        WITH base_daily AS (
            SELECT
                symbol,
                date,
                close,
                high,
                low,
                open,
                volume,
                LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date) AS prev_close_2,
                LAG(open, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_open,
                (LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)
                 - LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date))
                / NULLIF(LAG(close, 2) OVER (PARTITION BY symbol ORDER BY date), 0) AS ret_1d_lag1,
                close * volume AS value_traded_inr,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS latest_rn
            FROM v_daily
            WHERE date BETWEEN CAST(? AS DATE)
                  AND CAST(? AS DATE)
              AND symbol IN ({placeholders})
        ),
        latest AS (
            SELECT *
            FROM base_daily
            WHERE latest_rn = 1
        ),
        with_features AS (
            SELECT l.*,
                f.close_pos_in_range, f.ma_20, f.ret_5d, f.atr_20,
                f.vol_dryup_ratio, f.range_percentile,
                f.prior_breakdowns_90d, f.prior_breakouts_30d, f.r2_65, f.ma_65_sma,
                (f.close_pos_in_range <= 0.30) AS filter_h,
                (
                 (
                  l.prev_close
                  - COALESCE(l.prev_close_2, l.close)
                 )
                 < (f.atr_20 * 0.5)
                 OR l.prev_close
                    > COALESCE(l.prev_open, l.open)
                ) AS filter_n,
                (COALESCE(f.prior_breakdowns_90d, 0) <= 2) AS filter_y,
                (f.vol_dryup_ratio < 1.3) AS filter_c,
                (CAST(l.prev_close < f.ma_20 AS INTEGER)
                 + CAST(f.ret_5d < 0 AS INTEGER)
                 + CAST(COALESCE(NULLIF(f.r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2) AS filter_l,
                (l.ret_1d_lag1 >= 0) AS filter_2
            FROM latest l
            LEFT JOIN feat_daily f ON l.symbol = f.symbol AND l.date = f.date
        )
        SELECT
            symbol,
            date AS watch_date,
            close AS last_close,
            close_pos_in_range,
            atr_20,
            range_percentile,
            vol_dryup_ratio,
            r2_65,
            value_traded_inr,
            filter_h, filter_n, filter_y, filter_c, filter_l, filter_2,
            (CAST(filter_h AS INTEGER) + CAST(filter_n AS INTEGER)
             + CAST(filter_y AS INTEGER) + CAST(filter_c AS INTEGER)
             + CAST(filter_l AS INTEGER) + CAST(filter_2 AS INTEGER)) AS filters_passed
        FROM with_features
        WHERE close_pos_in_range IS NOT NULL
          AND close >= ?
        ORDER BY filters_passed DESC, value_traded_inr DESC
        """
        params = [context_start.isoformat(), context_end.isoformat(), *symbols, min_price]

    try:
        result = market_db.con.execute(query, params).pl()
    except Exception:
        logger.exception(
            "Primary watchlist query failed for %d symbols on %s", len(symbols), trade_date
        )
        try:
            result = _build_latest_bar_fallback_watchlist(
                symbols=symbols,
                trade_date=trade_date,
                min_price=min_price,
            )
            used_fallback = not result.is_empty()
        except Exception:
            logger.exception(
                "Fallback watchlist query failed for %d symbols on %s", len(symbols), trade_date
            )
            result = pl.DataFrame()

    if result.is_empty() and min_filters <= 0:
        logger.info(
            "Falling back to latest-bar watchlist for %d symbols on %s",
            len(symbols),
            trade_date,
        )
        try:
            result = _build_latest_bar_fallback_watchlist(
                symbols=symbols,
                trade_date=trade_date,
                min_price=min_price,
            )
            used_fallback = not result.is_empty()
        except Exception:
            logger.exception(
                "Fallback watchlist query failed for %d symbols on %s", len(symbols), trade_date
            )
            result = pl.DataFrame()

    if result.is_empty() or used_fallback:
        return _cache_watchlist(cache_key, result)

    # Apply min_filters threshold
    result = result.filter(pl.col("filters_passed") >= min_filters)
    return _cache_watchlist(cache_key, result)


def check_intraday_trigger(
    *,
    symbol: str,
    trade_date: date,
    prev_close: float,
    current_high: float | None = None,
    current_low: float | None = None,
    threshold: float = 0.04,
    direction: str = "long",
    entry_cutoff_minutes: int = 30,
    minutes_from_open: int = 0,
) -> dict[str, Any]:
    """Check if a watched symbol meets the intraday trigger condition.

    Returns a dict with:
    - ``triggered``: bool
    - ``trigger_price``: float or None
    - ``state``: CandidateState
    - ``reason``: str

    This is designed to be called on each 5-minute bar tick for a watched symbol.
    """
    is_long = direction.lower() != "short"

    # Entry cutoff check
    if minutes_from_open >= entry_cutoff_minutes:
        return {
            "triggered": False,
            "trigger_price": None,
            "state": "REJECTED",
            "reason": "entry_cutoff_exceeded",
        }

    if is_long:
        breakout_price = prev_close * (1 + threshold)
        triggered = current_high is not None and current_high >= breakout_price
        return {
            "triggered": triggered,
            "trigger_price": breakout_price if triggered else None,
            "state": "TRIGGERED" if triggered else "WATCH",
            "reason": f"high {current_high} >= breakout_price {breakout_price:.2f}"
            if triggered
            else f"waiting for high >= {breakout_price:.2f}",
        }
    else:
        breakdown_price = prev_close * (1 - threshold)
        triggered = current_low is not None and current_low <= breakdown_price
        return {
            "triggered": triggered,
            "trigger_price": breakdown_price if triggered else None,
            "state": "TRIGGERED" if triggered else "WATCH",
            "reason": f"low {current_low} <= breakdown_price {breakdown_price:.2f}"
            if triggered
            else f"waiting for low <= {breakdown_price:.2f}",
        }


def build_operational_universe(
    *,
    trade_date: date,
    lookback_days: int = 7,
) -> list[str]:
    """Return symbols that have both daily and 5-min data on the prior trading day.

    Unlike ``get_available_symbols`` which returns every symbol that ever appeared,
    this filters to symbols actively traded on the most recent session before
    *trade_date*.  This avoids subscribing to delisted or dormant names during
    live sessions.

    Parameters
    ----------
    trade_date:
        The session date.  The query looks for the most recent trading date
        strictly before this date.
    lookback_days:
        How many calendar days before *trade_date* to search for the most
        recent trading date.  7 is enough to skip weekends and short holidays.

    Returns
    -------
    list[str]
        Sorted list of symbols with both ``v_daily`` and ``v_5min`` rows on
        the most recent trading date.
    """
    if trade_date is None:
        return []

    cutoff = trade_date - timedelta(days=lookback_days)

    try:
        market_db = get_market_db(read_only=True)
        row = market_db.con.execute(
            "SELECT MAX(date) AS prev_date FROM v_daily WHERE date >= CAST(? AS DATE) AND date < CAST(? AS DATE)",
            [cutoff.isoformat(), trade_date.isoformat()],
        ).fetchone()

        if row is None or row[0] is None:
            logger.warning(
                "No trading date found within %d days before %s", lookback_days, trade_date
            )
            return []

        prev_date = row[0]

        daily_symbols = {
            r[0]
            for r in market_db.con.execute(
                "SELECT DISTINCT symbol FROM v_daily WHERE date = CAST(? AS DATE)",
                [prev_date],
            ).fetchall()
        }

        five_min_symbols = {
            r[0]
            for r in market_db.con.execute(
                "SELECT DISTINCT symbol FROM v_5min WHERE date = CAST(? AS DATE)",
                [prev_date],
            ).fetchall()
        }

        operational = sorted(daily_symbols & five_min_symbols)
        logger.info(
            "Operational universe: %d symbols (daily=%d, 5min=%d) for trade date %s, prev date %s",
            len(operational),
            len(daily_symbols),
            len(five_min_symbols),
            trade_date.isoformat(),
            prev_date,
        )
        return operational

    except Exception:
        logger.exception("Failed to build operational universe for %s", trade_date)
        return []
