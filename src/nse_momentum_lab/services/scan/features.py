from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import NamedTuple

import numpy as np

logger = logging.getLogger(__name__)


class PriceData(NamedTuple):
    trading_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    value_traded: float | None


@dataclass
class DailyFeatures:
    symbol_id: int
    trading_date: date
    close: float
    high: float
    low: float
    open_adj: float | None = None
    ret_1d: float | None = None
    ret_5d: float | None = None
    atr_20: float | None = None
    range_pct: float | None = None
    close_pos_in_range: float | None = None
    ma_20: float | None = None
    ma_65: float | None = None
    ma_7: float | None = None  # 7-day rolling MA (for TI65)
    ma_65_sma: float | None = None  # 65-day rolling MA (for TI65); ma_65 is LAG(close,65)
    rs_252: float | None = None
    vol_20: float | None = None
    dollar_vol_20: float | None = None
    # 2LYNCH filter features
    r2_65: float | None = None  # R-squared of 65-day linear trend
    atr_compress_ratio: float | None = None  # Current ATR / 50-day avg ATR
    range_percentile: float | None = None  # Price position in 252-day range (0-1)
    vol_dryup_ratio: float | None = None  # Recent volume / 20-day avg volume
    prior_breakouts_90d: int | None = None  # Count of 4%+ gap-ups in last 90 days
    delisting_date: date | None = None
    status: str = "ACTIVE"


class FeatureEngine:
    ATR_PERIOD = 20
    MA_SHORT_PERIOD = 20
    MA_LONG_PERIOD = 65
    RS_LOOKBACK = 252
    VOL_LOOKBACK = 20
    # 2LYNCH filter periods
    R2_PERIOD = 65  # For R² calculation
    ATR_COMPRESS_PERIOD = 50  # For ATR compression ratio
    RANGE_PERIOD = 252  # For range percentile
    BREAKOUT_LOOKBACK = 90  # Days to check for prior breakouts

    def compute_all(
        self,
        symbol_id: int,
        prices: list[PriceData],
        delisting_date: date | None = None,
        status: str = "ACTIVE",
    ) -> list[DailyFeatures]:
        n = len(prices)
        if n < 2:
            return []

        closes = np.array([p.close for p in prices], dtype=np.float64)
        highs = np.array([p.high for p in prices], dtype=np.float64)
        lows = np.array([p.low for p in prices], dtype=np.float64)
        volumes = np.array([p.volume for p in prices], dtype=np.float64)
        values = np.array(
            [p.value_traded if p.value_traded else 0 for p in prices], dtype=np.float64
        )
        dates = [p.trading_date for p in prices]

        ret_1d = self._compute_returns_1d(closes)
        ret_5d = self._compute_returns_5d(closes)
        atr = self._compute_atr_vectorized(highs, lows, closes)
        range_pct = self._compute_range_pct_vectorized(highs, lows, closes)
        close_pos = self._compute_close_pos_vectorized(closes, highs, lows)
        ma_20 = self._compute_ma_vectorized(closes, self.MA_SHORT_PERIOD)
        ma_65 = self._compute_ma_vectorized(closes, self.MA_LONG_PERIOD)
        rs = self._compute_rs_vectorized(closes, self.RS_LOOKBACK)
        vol_20 = self._compute_vol_vectorized(volumes, self.VOL_LOOKBACK)
        dollar_vol_20 = self._compute_vol_vectorized(values, self.VOL_LOOKBACK)

        # 2LYNCH filter features
        r2_65 = self._compute_r2_vectorized(closes, self.R2_PERIOD)
        atr_compress = self._compute_atr_compression_vectorized(atr, self.ATR_COMPRESS_PERIOD)
        range_pctile = self._compute_range_percentile_vectorized(closes, self.RANGE_PERIOD)
        vol_dryup = self._compute_vol_dryup_vectorized(volumes, self.VOL_LOOKBACK)
        prior_breakouts = self._compute_prior_breakouts_vectorized(closes, self.BREAKOUT_LOOKBACK)

        features_list = []
        for i in range(n):
            features = DailyFeatures(
                symbol_id=symbol_id,
                trading_date=dates[i],
                close=float(closes[i]),
                high=float(highs[i]),
                low=float(lows[i]),
                open_adj=float(prices[i].open),
                ret_1d=ret_1d[i],
                ret_5d=ret_5d[i],
                atr_20=atr[i],
                range_pct=range_pct[i],
                close_pos_in_range=close_pos[i],
                ma_20=ma_20[i],
                ma_65=ma_65[i],
                rs_252=rs[i],
                vol_20=vol_20[i],
                dollar_vol_20=dollar_vol_20[i],
                r2_65=r2_65[i],
                atr_compress_ratio=atr_compress[i],
                range_percentile=range_pctile[i],
                vol_dryup_ratio=vol_dryup[i],
                prior_breakouts_90d=prior_breakouts[i],
                delisting_date=delisting_date,
                status=status,
            )
            features_list.append(features)

        return features_list

    def _compute_returns_1d(self, closes: np.ndarray) -> np.ndarray:
        ret = np.empty_like(closes, dtype=object)
        ret[0] = None
        ret[1:] = (closes[1:] - closes[:-1]) / closes[:-1]
        return ret

    def _compute_returns_5d(self, closes: np.ndarray) -> np.ndarray:
        ret = np.empty_like(closes, dtype=object)
        ret[:5] = None
        ret[5:] = (closes[5:] - closes[:-5]) / closes[:-5]
        return ret

    def _compute_true_range_vectorized(
        self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray
    ) -> np.ndarray:
        n = len(highs)
        tr = np.empty(n, dtype=np.float64)
        tr[0] = highs[0] - lows[0]
        high_less_prev_close = np.abs(highs[1:] - closes[:-1])
        low_less_prev_close = np.abs(lows[1:] - closes[:-1])
        high_less_low = highs[1:] - lows[1:]
        tr[1:] = np.maximum.reduce([high_less_low, high_less_prev_close, low_less_prev_close])
        return tr

    def _compute_atr_vectorized(
        self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray
    ) -> np.ndarray:
        tr = self._compute_true_range_vectorized(highs, lows, closes)
        period = self.ATR_PERIOD
        atr = np.empty(len(tr), dtype=object)
        atr[:period] = None
        for i in range(period, len(tr)):
            atr[i] = float(np.mean(tr[i - period + 1 : i + 1]))
        return atr

    def _compute_range_pct_vectorized(
        self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray
    ) -> np.ndarray:
        range_pct = np.empty(len(closes), dtype=object)
        range_pct[0] = None
        prev_closes = closes[:-1]
        range_pct[1:] = (highs[1:] - lows[1:]) / prev_closes
        return range_pct

    def _compute_close_pos_vectorized(
        self, closes: np.ndarray, highs: np.ndarray, lows: np.ndarray
    ) -> np.ndarray:
        close_pos = np.empty(len(closes), dtype=object)
        range_ = highs - lows
        close_pos[0] = None
        valid = range_[1:] != 0
        close_pos[1:][valid] = (closes[1:][valid] - lows[1:][valid]) / range_[1:][valid]
        close_pos[1:][~valid] = None
        return close_pos

    def _compute_ma_vectorized(self, closes: np.ndarray, period: int) -> np.ndarray:
        n = len(closes)
        ma = np.empty(n, dtype=object)
        ma[: period - 1] = None
        for i in range(period - 1, n):
            ma[i] = float(np.mean(closes[i - period + 1 : i + 1]))
        return ma

    def _compute_rs_vectorized(self, closes: np.ndarray, lookback: int) -> np.ndarray:
        n = len(closes)
        rs = np.empty(n, dtype=object)
        rs[: lookback - 1] = None
        for i in range(lookback - 1, n):
            benchmark = np.mean(closes[i - lookback + 1 : i + 1])
            if benchmark == 0:
                rs[i] = None
            else:
                rs[i] = closes[i] / benchmark - 1
        return rs

    def _compute_vol_vectorized(self, volumes: np.ndarray, lookback: int) -> np.ndarray:
        n = len(volumes)
        vol = np.empty(n, dtype=object)
        vol[: lookback - 1] = None
        for i in range(lookback - 1, n):
            vol[i] = float(np.mean(volumes[i - lookback + 1 : i + 1]))
        return vol

    def _compute_r2_vectorized(self, closes: np.ndarray, period: int) -> np.ndarray:
        """
        Compute R-squared of linear trend over period.

        R² measures how well the price fits a linear trend.
        Values closer to 1 indicate strong trend, closer to 0 indicate no trend.
        """
        n = len(closes)
        r2 = np.empty(n, dtype=object)
        r2[: period - 1] = None

        for i in range(period - 1, n):
            window = closes[i - period + 1 : i + 1].astype(np.float64)
            x = np.arange(period)

            # Simple linear regression: y = mx + b
            try:
                # Calculate slope and intercept
                n_points = period
                sum_x = np.sum(x)
                sum_y = np.sum(window)
                sum_xy = np.sum(x * window)
                sum_x2 = np.sum(x**2)

                denominator = n_points * sum_x2 - sum_x**2
                if denominator == 0:
                    r2[i] = 0.0
                    continue

                slope = (n_points * sum_xy - sum_x * sum_y) / denominator
                intercept = (sum_y - slope * sum_x) / n_points

                # Calculate R²
                y_mean = np.mean(window)
                ss_tot = np.sum((window - y_mean) ** 2)
                ss_res = np.sum((window - (slope * x + intercept)) ** 2)

                if ss_tot == 0:
                    r2[i] = 0.0
                else:
                    r2[i] = float(1 - (ss_res / ss_tot))
            except Exception:
                r2[i] = 0.0

        return r2

    def _compute_atr_compression_vectorized(self, atr: np.ndarray, period: int) -> np.ndarray:
        """
        Compute ATR compression ratio.

        Ratio = Current ATR / Average ATR over period
        Values < 1 indicate volatility compression (squeeze)
        """
        n = len(atr)
        ratio = np.empty(n, dtype=object)
        ratio[: period - 1] = None

        for i in range(period - 1, n):
            current_atr = atr[i]
            if current_atr is None or current_atr == 0:
                ratio[i] = None
                continue

            # Calculate average ATR over period
            atr_values = []
            for j in range(i - period + 1, i + 1):
                if atr[j] is not None and atr[j] > 0:
                    atr_values.append(atr[j])

            if not atr_values:
                ratio[i] = None
            else:
                avg_atr = np.mean(atr_values)
                if avg_atr == 0:
                    ratio[i] = None
                else:
                    ratio[i] = float(current_atr / avg_atr)

        return ratio

    def _compute_range_percentile_vectorized(self, closes: np.ndarray, period: int) -> np.ndarray:
        """
        Compute price position in historical range.

        Percentile = (Current Price - Min Price) / (Max Price - Min Price)
        Values closer to 1 indicate price near top of range
        """
        n = len(closes)
        pctile = np.empty(n, dtype=object)
        pctile[: period - 1] = None

        for i in range(period - 1, n):
            window = closes[i - period + 1 : i + 1]
            current = closes[i]
            min_price = np.min(window)
            max_price = np.max(window)

            if max_price - min_price == 0:
                pctile[i] = 0.5  # Middle if no range
            else:
                pctile[i] = float((current - min_price) / (max_price - min_price))

        return pctile

    def _compute_vol_dryup_vectorized(self, volumes: np.ndarray, lookback: int) -> np.ndarray:
        """
        Compute volume dryup ratio.

        Ratio = Recent Volume / Average Volume
        Values < 1 indicate below-average volume (dryup/consolidation)
        """
        n = len(volumes)
        ratio = np.empty(n, dtype=object)
        ratio[: lookback - 1] = None

        for i in range(lookback - 1, n):
            recent_vol = volumes[i]
            if recent_vol == 0:
                ratio[i] = None
                continue

            avg_vol = np.mean(volumes[i - lookback + 1 : i + 1])
            if avg_vol == 0:
                ratio[i] = None
            else:
                ratio[i] = float(recent_vol / avg_vol)

        return ratio

    def _compute_prior_breakouts_vectorized(self, closes: np.ndarray, lookback: int) -> np.ndarray:
        """
        Count prior 4%+ gap-ups in lookback period.

        This helps avoid over-traded symbols that have too many recent breakouts.
        """
        n = len(closes)
        breakouts = np.empty(n, dtype=object)
        breakouts[: lookback - 1] = None

        # Calculate gap-ups (when close opens 4%+ above previous close)
        # Note: We're using close-to-close as proxy since we don't have open here
        # In production, this should use open vs previous close

        for i in range(lookback - 1, n):
            count = 0
            for j in range(max(0, i - lookback + 1), i):
                if closes[j] is not None and closes[j - 1] is not None:
                    gap_pct = (closes[j] - closes[j - 1]) / closes[j - 1]
                    if gap_pct >= 0.04:  # 4%+ gap
                        count += 1
            breakouts[i] = count

        return breakouts


def compute_features(
    symbol_id: int,
    prices: list[PriceData],
    delisting_date: date | None = None,
    status: str = "ACTIVE",
) -> list[DailyFeatures]:
    engine = FeatureEngine()
    return engine.compute_all(symbol_id, prices, delisting_date, status)
