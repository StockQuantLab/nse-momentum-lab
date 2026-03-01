"""Unified 2LYNCH signal generator - Consolidated and corrected.

This replaces both duckdb_signal_generator.py and duckdb_signal_generator_complete.py
with a single, correct implementation.

ADR-007 Compliance:
- Letter 2: Not up 2 days in a row
- Letter L: Linear first leg (R² > threshold)
- Letter Y: Young trend (few prior breakouts)
- Letter N: Narrow range or negative day pre-breakout
- Letter C: Consolidation (shallow, low volume)
- Letter H: Close near high
"""

from datetime import date
from typing import Any

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.scan.duckdb_features import DuckDBFeatureEngine
from nse_momentum_lab.services.scan.rules import ScanConfig


class SignalGeneratorV2:
    """
    Unified signal generator with proper 2LYNCH filtering.

    CRITICAL FIXES from previous versions:
    1. Narrow range filter now correctly rejects wide-range days
    2. Filter threshold uses config.min_filters_pass
    3. R² threshold configurable via config
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
        """Generate gap-up signals with proper 2LYNCH filters."""
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

            if len(prices) < 5:
                continue

            # Create lookup dict
            features_by_date = {f.trading_date: f for f in features}

            # Check each trading day for gap-up
            for i, (trading_date, open_price, high, low, close, volume) in enumerate(prices):
                if i == 0:
                    continue

                _, _, _, _, prev_close, _ = prices[i - 1]

                # Calculate gap percentage
                gap_pct = (open_price - prev_close) / prev_close if prev_close > 0 else 0

                # ADR-007: 4% breakout
                if gap_pct < self.config.breakout_threshold:
                    continue

                # ADR-007: Volume filter
                if volume < 100000:
                    continue

                # ADR-007: Price filter
                if close < self.config.min_price:
                    continue

                # Get features for this date
                feats = features_by_date.get(trading_date)
                if not feats:
                    continue

                # Apply 2LYNCH filters
                if self._check_2lynch_filters(feats, prices, i, gap_pct):
                    signal = {
                        "symbol": symbol,
                        "trading_date": trading_date,
                        "entry_price": open_price,
                        "gap_pct": gap_pct,
                        "initial_stop": low,
                        "prev_close": prev_close,
                        "atr": feats.atr_20 if feats.atr_20 else (high - low),
                    }
                    signals.append(signal)

        return signals

    def _check_2lynch_filters(
        self,
        features,
        prices,
        current_index: int,
        gap_pct: float,
    ) -> bool:
        """
        Apply 2LYNCH filters per ADR-007 and Stockbee.

        Returns True if setup passes required number of filters.
        """
        filters_passed = 0
        filters_total = 0

        # Letter H: Close near high
        filters_total += 1
        if features.close_pos_in_range is not None:
            if features.close_pos_in_range >= self.config.close_pos_threshold:
                filters_passed += 1

        # Letter L: Linearity (R²)
        filters_total += 1
        if features.r2_65 is not None and features.r2_65 > 0:
            if features.r2_65 >= self.config.min_r2_l:
                filters_passed += 1

        # Letter 2: Not up 2 days in a row
        filters_total += 1
        if current_index >= 2:
            prev_1_close = prices[current_index - 1][4]
            prev_2_close = prices[current_index - 2][4]
            prev_1_high = prices[current_index - 1][2]
            prev_1_low = prices[current_index - 1][3]
            current_high = prices[current_index][2]
            current_low = prices[current_index][3]

            ret_1 = (prev_1_close - prev_2_close) / prev_2_close if prev_2_close > 0 else 0
            ret_2 = gap_pct

            if ret_1 > 0 and ret_2 > 0:
                range_1 = (prev_1_high - prev_1_low) / prev_1_close if prev_1_close > 0 else 0
                range_2 = (
                    (current_high - current_low) / prices[current_index][4]
                    if prices[current_index][4] > 0
                    else 0
                )

                if range_1 > 0.02 and range_2 > 0.02:
                    return False  # Extended move - reject
                elif range_1 <= 0.02 and range_2 <= 0.02:
                    filters_passed += 1  # Small range OK
                else:
                    pass  # Mixed - neither pass nor fail
            else:
                filters_passed += 1  # Not up 2 days
        else:
            filters_total -= 1  # Not enough history

        # Letter N: Narrow range or negative day pre-breakout
        filters_total += 1
        if current_index >= 1:
            prev_high = prices[current_index - 1][2]
            prev_low = prices[current_index - 1][3]
            prev_close = prices[current_index - 1][4]
            prev_open = prices[current_index - 1][1]

            yesterday_range = (prev_high - prev_low) / prev_close if prev_close > 0 else 0
            atr_yest = features.atr_20 if features.atr_20 else yesterday_range

            # Narrow range OR negative day
            is_narrow = yesterday_range < atr_yest * 0.5
            is_negative = prev_close < prev_open

            if is_narrow or is_negative:
                filters_passed += 1
            # CRITICAL FIX: Don't add filter_passed for wide-range positive days

        # Letter Y: Young trend
        filters_total += 1
        if features.prior_breakouts_90d is not None:
            if features.prior_breakouts_90d <= self.config.max_prior_breakouts:
                filters_passed += 1
        else:
            filters_total -= 1

        # Letter C: Consolidation characteristics
        filters_total += 1
        if features.vol_dryup_ratio is not None:
            if features.vol_dryup_ratio < self.config.max_vol_dryup_ratio:
                filters_passed += 1
        else:
            filters_total -= 1

        # Use config-specified minimum filters
        min_filters = self.config.min_filters_pass

        return filters_passed >= min_filters
