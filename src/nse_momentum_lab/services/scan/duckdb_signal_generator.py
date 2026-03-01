"""Generate gap-up breakout signals using DuckDB data."""

from datetime import date
from typing import Any

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.scan.duckdb_features import DuckDBFeatureEngine
from nse_momentum_lab.services.scan.rules import ScanConfig


class DuckDBSignalGenerator:
    """
    Generate gap-up breakout signals using pre-computed DuckDB features.

    This replaces the PostgreSQL-based signal generation with DuckDB for
    10-100x faster performance.
    """

    def __init__(self, config: ScanConfig | None = None):
        self.config = config or ScanConfig()
        self.db = get_market_db()
        self.feature_engine = DuckDBFeatureEngine()

    def generate_signals(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """
        Generate gap-up signals for 2LYNCH strategy.

        Process:
        1. Load daily OHLCV data from DuckDB
        2. Load pre-computed features from DuckDB
        3. Apply 2LYNCH filter rules (H, N, 2, Y, C, L)
        4. Return list of signals

        Returns:
            List of signal dicts with keys:
            - symbol, trading_date, entry_price, initial_stop, gap_pct
        """
        signals = []

        # Load daily prices
        daily_prices = self.feature_engine.load_daily_prices_for_symbols(
            symbols, start_date, end_date
        )

        # Load features
        features_by_symbol = self.feature_engine.load_features_for_symbols(
            symbols, start_date, end_date
        )

        # Generate signals for each symbol
        for symbol in symbols:
            if symbol not in daily_prices or symbol not in features_by_symbol:
                continue

            prices = daily_prices[symbol]
            features = features_by_symbol[symbol]

            # Create a lookup dict for features by date
            features_by_date = {f.trading_date: f for f in features}

            # Need at least 3 days of history for the "2" filter (ret_1d_lag1/lag2)
            for i, (trading_date, open_price, high, low, _close, _volume) in enumerate(prices):
                if i < 3:
                    continue

                _, prev_open, prev_high, prev_low, prev_close, _prev_vol = prices[i - 1]
                _, _pprev_open, _pprev_high, _pprev_low, pprev_close, _pprev_vol = prices[i - 2]
                _, _ppprev_open, _ppprev_high, _ppprev_low, ppprev_close, _ppprev_vol = prices[
                    i - 3
                ]

                # Calculate gap percentage
                if prev_close <= 0:
                    continue
                gap_pct = (open_price - prev_close) / prev_close

                # Check 4% breakout
                if gap_pct < self.config.breakout_threshold:
                    continue

                # Get features for this date
                feats = features_by_date.get(trading_date)
                if not feats:
                    continue

                # Compute derived values for filters
                ret_1d_lag1 = (prev_close - pprev_close) / pprev_close if pprev_close > 0 else None
                ret_1d_lag2 = (
                    (pprev_close - ppprev_close) / ppprev_close if ppprev_close > 0 else None
                )

                # Apply 2LYNCH filters (H, N, 2, Y, C, L)
                if self._check_2lynch_filters(
                    feats,
                    prev_high,
                    prev_low,
                    prev_close,
                    prev_open,
                    ret_1d_lag1,
                    ret_1d_lag2,
                ):
                    # Initial stop = T-1's low (Stockbee approach)
                    initial_stop = prev_low

                    signal = {
                        "symbol": symbol,
                        "trading_date": trading_date,
                        "entry_price": open_price,
                        "gap_pct": gap_pct,
                        "initial_stop": initial_stop,
                        "prev_close": prev_close,
                        "prev_low": prev_low,
                        "atr": feats.atr_20 if feats.atr_20 else (high - low),
                    }
                    signals.append(signal)

        return signals

    def _check_2lynch_filters(
        self,
        features,
        prev_high: float,
        prev_low: float,
        prev_close: float,
        prev_open: float,
        ret_1d_lag1: float | None,
        ret_1d_lag2: float | None,
    ) -> bool:
        """
        Apply 2LYNCH filter rules to improve signal quality.

        Filters (6 total, require min_filters_pass to pass):
        H - Close position in range >= 0.70
        N - T-1 narrow range or negative day (compression before breakout)
        2 - Not up 2 days in a row before breakout
        Y - Young breakout: max 2 prior breakouts in 30 days
        C - Volume dryup: vol_dryup_ratio < 1.3
        L - Trend quality: 2 of 3 (above MA20, positive momentum, R2 >= 0.70)
        """
        filters_passed = 0

        # Filter H: Close position in range >= 0.70
        if features.close_pos_in_range is not None:
            if features.close_pos_in_range >= self.config.close_pos_threshold:
                filters_passed += 1
        # NULL = fail (conservative)

        # Filter N: T-1 should be narrow range or negative day
        atr = features.atr_20 if features.atr_20 else 0
        if atr > 0:
            narrow = (prev_high - prev_low) < (atr * 0.5)
            negative = prev_close < prev_open
            if narrow or negative:
                filters_passed += 1

        # Filter 2: Not up 2 days in a row before breakout
        if ret_1d_lag1 is not None and ret_1d_lag2 is not None:
            if ret_1d_lag1 <= 0 or ret_1d_lag2 <= 0:
                filters_passed += 1

        # Filter Y: Young breakout — max 2 prior breakouts in 30 days
        prior_breakouts = getattr(features, "prior_breakouts_30d", None)
        if prior_breakouts is None:
            prior_breakouts = getattr(features, "prior_breakouts_90d", None)
        if prior_breakouts is not None:
            if prior_breakouts <= 2:
                filters_passed += 1

        # Filter C: Volume dryup
        if features.vol_dryup_ratio is not None:
            if features.vol_dryup_ratio < 1.3:
                filters_passed += 1

        # Filter L: Trend quality (2 of 3 sub-checks)
        # Check 1: TI65 = MA7/MA65_SMA >= 1.05 (prefer) or close > MA20 (fallback)
        l_score = 0
        ma_7 = getattr(features, "ma_7", None)
        ma_65_sma = getattr(features, "ma_65_sma", None)
        if ma_7 is not None and ma_65_sma is not None and ma_65_sma > 0:
            if ma_7 / ma_65_sma >= 1.05:
                l_score += 1
        elif features.ma_20 is not None:
            close_val = getattr(features, "close", None)
            if close_val is not None and close_val > features.ma_20:
                l_score += 1
        ret_5d = getattr(features, "ret_5d", None)
        if ret_5d is not None and ret_5d > 0:
            l_score += 1
        r2 = getattr(features, "r2_65", None)
        if r2 is not None and r2 >= 0.70:
            l_score += 1
        if l_score >= 2:
            filters_passed += 1

        min_required = getattr(self.config, "min_filters_pass", 4)
        return filters_passed >= min_required

    def get_5min_data_for_entry(
        self,
        symbol: str,
        trading_date: date,
    ) -> pl.DataFrame | None:
        """
        Get 5-minute candles for a trading day (for precise entry timing).

        Returns:
            Polars DataFrame with columns: candle_time, open, high, low, close, volume
        """
        return self.db.query_5min(symbol, trading_date.isoformat(), trading_date.isoformat())
