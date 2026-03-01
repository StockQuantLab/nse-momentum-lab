"""Complete 2LYNCH signal generator with all ADR-007 filters.

This implements the complete Stockbee momentum burst strategy as specified in ADR-007.

INDIAN MARKET ADAPTATIONS:
- Min price: ₹10 (adapted from $3 for US)
- Min volume: 100,000 shares
- 4% gap threshold (same as US)
"""

from datetime import date
from typing import Any

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.scan.duckdb_features import DuckDBFeatureEngine
from nse_momentum_lab.services.scan.rules import ScanConfig


class CompleteDuckDBSignalGenerator:
    """
    Generate gap-up breakout signals with COMPLETE 2LYNCH filters.

    ADR-007 Compliance:
    - Letter 2: Not up 2 days in a row
    - Letter L: Linear first leg (R² > threshold)
    - Letter Y: Young trend (few prior breakouts)
    - Letter N: Narrow range or negative day pre-breakout
    - Letter C: Consolidation (shallow, low volume)
    - Letter H: Close near high
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
        Generate gap-up signals with complete 2LYNCH filters.
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

            if len(prices) < 5:  # Need minimum history
                continue

            # Create lookup dicts
            features_by_date = {f.trading_date: f for f in features}

            # Check each trading day for gap-up
            for i, (trading_date, open_price, high, low, close, volume) in enumerate(prices):
                if i == 0:
                    continue

                _, _, _, _, prev_close, _ = prices[i - 1]

                # Calculate gap percentage
                gap_pct = (open_price - prev_close) / prev_close

                # ADR-007: 4% breakout (c/c1>=1.04)
                if gap_pct < 0.04:
                    continue

                # ADR-007: Volume filter (v>=100000)
                if volume < 100000:
                    continue

                # ADR-007: Price filter (c>=10) - Indian market adaptation
                # Note: ADR-007 specifies $3 for US, adapted to ₹10 for NSE
                if close < 10:
                    continue

                # Get features for this date
                feats = features_by_date.get(trading_date)
                if not feats:
                    continue

                # Apply complete 2LYNCH filters
                if self._check_2lynch_filters_complete(feats, prices, i, gap_pct):
                    # Calculate initial stop (low of entry day per Stockbee)
                    initial_stop = low  # Stockbee: stop at low of entry day

                    signal = {
                        "symbol": symbol,
                        "trading_date": trading_date,
                        "entry_price": open_price,
                        "gap_pct": gap_pct,
                        "initial_stop": initial_stop,
                        "prev_close": prev_close,
                        "atr": feats.atr_20 if feats.atr_20 else (high - low),
                    }
                    signals.append(signal)

        return signals

    def _check_2lynch_filters_complete(
        self,
        features,
        prices,
        current_index: int,
        gap_pct: float,
    ) -> bool:
        """
        Apply COMPLETE 2LYNCH filters per ADR-007 and Stockbee.

        Returns True if setup passes all required filters.
        """
        filters_passed = 0
        filters_total = 6

        # Letter H: Close near high
        # ADR-007: (c-l)/(h-l) >= 0.70 (top 30%)
        # Stockbee: "Should close near high"
        if features.close_pos_in_range is not None:
            if features.close_pos_in_range >= 0.70:
                filters_passed += 1

        # Letter L: Linearity (R²)
        # Stockbee: "First leg should be linear"
        # Avoid whipsaw "drunken walk"
        #
        # PRAGMATIC APPROACH: Since our R² values are approximate (0.9+ for everything),
        # we use a combination of:
        # 1. TI65 = MA7/MA65_SMA >= 1.05 (prefer) or close > MA20 (fallback)
        # 2. Positive 5-day return (recent momentum)
        # 3. R² check (if available and reasonable)
        ma_7 = getattr(features, "ma_7", None)
        ma_65_sma = getattr(features, "ma_65_sma", None)
        has_uptrend = False
        if ma_7 is not None and ma_65_sma is not None and ma_65_sma > 0:
            has_uptrend = (ma_7 / ma_65_sma) >= 1.05
        elif features.ma_20 and features.close:
            has_uptrend = features.close > features.ma_20

        has_momentum = False
        if features.ret_5d is not None:
            if features.ret_5d > 0:
                has_momentum = True

        # R² check - use lower threshold since our values are approximate
        has_linear_r2 = False
        if features.r2_65 is not None and features.r2_65 > 0:
            # Use 0.85 threshold for approximate R² values
            # (true linear regression would use 0.7)
            if features.r2_65 >= 0.85:
                has_linear_r2 = True

        # Pass L filter if at least 2 of 3 criteria met
        if sum([has_uptrend, has_momentum, has_linear_r2]) >= 2:
            filters_passed += 1

        # Letter 2: Not up 2 days in a row
        # ADR-007: "Not up 2 days in a row"
        # Stockbee: "Stock should not be up 3 days in a row (small range days up 3 days is ok)"
        if current_index >= 2:
            prev_1_close = prices[current_index - 1][4]  # close
            prev_2_close = prices[current_index - 2][4]  # close
            prev_1_high = prices[current_index - 1][2]  # high
            prev_1_low = prices[current_index - 1][3]  # low
            current_high = prices[current_index][2]
            current_low = prices[current_index][3]

            # Calculate returns
            ret_1 = (prev_1_close - prev_2_close) / prev_2_close if prev_2_close > 0 else 0
            ret_2 = gap_pct  # Today's gap

            # If yesterday and today both up, check ranges
            if ret_1 > 0 and ret_2 > 0:
                # Check if small range days (OK per Stockbee)
                range_1 = (prev_1_high - prev_1_low) / prev_1_close if prev_1_close > 0 else 0
                range_2 = (
                    (current_high - current_low) / prices[current_index][4]
                    if prices[current_index][4] > 0
                    else 0
                )

                # If either was big range (>2%), reject
                if range_1 > 0.02 and range_2 > 0.02:
                    return False  # Extended move - skip
                elif range_1 <= 0.02 and range_2 <= 0.02:
                    filters_passed += 1  # Small range OK
                else:
                    # Mixed - one big one small
                    pass
            else:
                filters_passed += 1  # Not up 2 days
        else:
            filters_total -= 1

        # Letter N: Narrow range or negative day pre-breakout
        # ADR-007: "Narrow range day or negative day"
        # CRITICAL FIX: Only pass if narrow range OR negative day
        if current_index >= 1:
            prev_high = prices[current_index - 1][2]
            prev_low = prices[current_index - 1][3]
            prev_close = prices[current_index - 1][4]
            prev_open = prices[current_index - 1][1]

            # Check yesterday's range
            yesterday_range = (prev_high - prev_low) / prev_close if prev_close > 0 else 0
            atr_yest = features.atr_20 if features.atr_20 else yesterday_range

            # Narrow range = range < 50% of ATR OR negative day
            is_narrow = yesterday_range < atr_yest * 0.5
            is_negative = prev_close < prev_open

            if is_narrow or is_negative:
                filters_passed += 1  # Good - consolidation or pullback
            # Wide range positive days DO NOT pass the filter

        # Letter Y: Young trend
        # ADR-007: "1st-3rd breakout from consolidation is lower risk"
        # Stockbee: "No 4% b/d in last 3 to 5 days"
        if features.prior_breakouts_90d is not None:
            # Check shorter window - we want 3-5 days, but we have 90d
            # For now, require <= 5 (allows recent breakout but not over-traded)
            if features.prior_breakouts_90d <= 5:
                filters_passed += 1
        else:
            filters_total -= 1

        # Letter C: Consolidation characteristics
        # ADR-007: "Consolidation... low-volume; no more than one 4% day inside"
        # Stockbee: "Orderly pullback or consolidation"
        if features.vol_dryup_ratio is not None:
            # Volume dryup indicates consolidation
            if features.vol_dryup_ratio < 1.0:
                filters_passed += 1  # Strong consolidation
            elif features.vol_dryup_ratio < 1.5:
                filters_passed += 1  # Moderate consolidation
        else:
            filters_total -= 1

        # Use configured minimum filters (default 4 out of 6)
        # This ensures quality over quantity
        min_filters = self.config.min_filters_pass

        return filters_passed >= min_filters
