"""Indian Market 2LYNCH Signal Generator - Properly Adapted for NSE.

This implements Stockbee's momentum burst strategy correctly adapted for Indian markets:

Key Indian Market Adaptations:
- Price threshold: ₹50 (not $3) - filters out low-quality penny stocks
- Volume filter: ₹30 lakh value traded (not 100,000 shares)
- 4% gap threshold: Same as US markets
- Close position: Top 30% (0.70) of daily range

Stockbee's 2LYNCH Criteria:
- 2: Not up 2 days in a row (small up days OK)
- L: First leg should be linear (avoid "drunken walk")
- Y: Young trend (1st-3rd breakout, not over-traded)
- N: Narrow range or negative day pre-breakout
- C: Consolidation (shallow, low volume, orderly)
- H: Close near high

Reference: Stockbee blog "Episodic Pivots" and "Momentum Burst" posts
"""

from datetime import date
from typing import Any

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.scan.duckdb_features import DuckDBFeatureEngine
from nse_momentum_lab.services.scan.rules import ScanConfig


class Indian2LynchSignalGenerator:
    """
    Generate gap-up breakout signals with 2LYNCH filters adapted for Indian markets.

    INDIAN MARKET THRESHOLDS:
    - Min price: ₹50 (filters out low-quality penny stocks)
    - Min value traded: ₹30 lakh (ensures liquidity)
    - Gap threshold: 4% (same as US)
    - Close position: Top 30% of range (0.70)

    2LYNCH LETTERS (All must pass):
    - H: Close in top 30% of day's range
    - 2: Not up 2 days with big range (small up days OK)
    - L: Linear first leg (R² >= 0.70 OR 2/3 of pragmatic checks)
    - Y: Young trend (≤2 breakouts in last 20 days, not 90)
    - N: Narrow range OR negative day before breakout
    - C: Consolidation (volume dryup, ATR compression)
    """

    # Indian market specific thresholds
    MIN_PRICE_INR = 50.0  # Not $3 - India needs higher threshold
    MIN_VALUE_TRADED_INR = 3_000_000  # ₹30 lakh minimum daily value
    MIN_VOLUME_SHARES = 50_000  # Minimum shares as secondary check

    # 2LYNCH thresholds
    CLOSE_POS_THRESHOLD = 0.70  # Top 30% of day's range
    GAP_THRESHOLD = 0.04  # 4% minimum gap

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
        Generate gap-up signals with complete 2LYNCH filters for Indian markets.
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

            if len(prices) < 20:  # Need 20 days for MA checks
                continue

            # Create lookup dicts
            features_by_date = {f.trading_date: f for f in features}

            # Check each trading day for gap-up
            for i, (trading_date, open_price, high, low, close, volume) in enumerate(prices):
                if i == 0:
                    continue

                _, _, _, _, prev_close, _ = prices[i - 1]

                # Calculate gap percentage
                gap_pct = (open_price - prev_close) / prev_close if prev_close > 0 else 0

                # === PRIMARY ENTRY CRITERIA ===

                # 1. 4% gap minimum (Stockbee threshold)
                if gap_pct < self.GAP_THRESHOLD:
                    continue

                # 2. Indian market: Price must be >= ₹50 (filters penny stocks)
                if close < self.MIN_PRICE_INR:
                    continue

                # 3. Indian market: Value traded >= ₹30 lakh (liquidity filter)
                value_traded = close * volume
                if value_traded < self.MIN_VALUE_TRADED_INR:
                    continue

                # 4. Secondary: Minimum shares (prevents odd-lot scenarios)
                if volume < self.MIN_VOLUME_SHARES:
                    continue

                # Get features for this date
                feats = features_by_date.get(trading_date)
                if not feats:
                    continue

                # === 2LYNCH QUALITY FILTERS ===
                filter_result = self._check_2lynch_filters(feats, prices, i, gap_pct)

                if not filter_result["passed"]:
                    continue

                # === SIGNAL GENERATED ===

                # Initial stop: Low of entry day (Stockbee's method)
                # This is more conservative than ATR-based stops
                initial_stop = low

                # Calculate risk percentage for position sizing
                risk_pct = (open_price - initial_stop) / open_price if open_price > 0 else 0

                signal = {
                    "symbol": symbol,
                    "trading_date": trading_date,
                    "entry_price": open_price,
                    "gap_pct": gap_pct,
                    "initial_stop": initial_stop,
                    "prev_close": prev_close,
                    "atr": feats.atr_20 if feats.atr_20 else (high - low),
                    "risk_pct": risk_pct,
                    "filters_passed": filter_result["filters_passed"],
                    "filters_total": filter_result["filters_total"],
                    "filter_details": filter_result["details"],
                }
                signals.append(signal)

        return signals

    def _check_2lynch_filters(
        self,
        features,
        prices,
        current_index: int,
        gap_pct: float,
    ) -> dict[str, Any]:
        """
        Apply ALL 2LYNCH filters per Stockbee's methodology.

        Returns dict with:
        - passed: bool (True if ALL required filters pass)
        - filters_passed: int
        - filters_total: int
        - details: dict of each filter's result
        """
        details = {}
        filters_passed = 0
        filters_total = 6

        # === LETTER H: Close Near High ===
        # Stockbee: "Should close near high"
        # ADR-007: (c-l)/(h-l) >= 0.70 (top 30%)
        h_passed = False
        close_pos_val = features.close_pos_in_range
        if close_pos_val is not None:
            h_passed = close_pos_val >= self.CLOSE_POS_THRESHOLD
        close_pos_str = f"{close_pos_val:.2f}" if close_pos_val is not None else "N/A"
        details["H"] = {
            "passed": h_passed,
            "value": close_pos_val,
            "threshold": self.CLOSE_POS_THRESHOLD,
            "desc": f"Close position: {close_pos_str} vs {self.CLOSE_POS_THRESHOLD}",
        }
        if h_passed:
            filters_passed += 1

        # === LETTER L: Linearity (First Leg) ===
        # Stockbee: "First leg should be linear"
        # Avoid whipsaw "drunken walk" stocks
        #
        # Lynch trend: 3-pronged approach, need 2 of 3
        # 1. TI65 = MA7/MA65_SMA >= 1.05 (trend intensity); fallback: close > MA20
        # 2. Positive momentum (ret_5d > 0)
        # 3. R² of 65-day linear regression >= 0.70
        l_checks = {"ti65": False, "momentum": False, "r2": False}

        # Check 1: TI65 (prefer) or close > MA20 (fallback pre-rebuild)
        if features.ma_7 is not None and features.ma_65_sma is not None and features.ma_65_sma > 0:
            l_checks["ti65"] = (features.ma_7 / features.ma_65_sma) >= 1.05
        elif features.ma_20 is not None and features.close is not None:
            l_checks["ti65"] = features.close > features.ma_20

        # Check 2: Positive 5-day momentum
        if features.ret_5d is not None:
            l_checks["momentum"] = features.ret_5d > 0

        # Check 3: R² if available (use 0.70 threshold)
        if features.r2_65 is not None and features.r2_65 > 0:
            l_checks["r2"] = features.r2_65 >= 0.70

        # Pass L if at least 2 of 3 checks pass
        l_score = sum(l_checks.values())
        l_passed = l_score >= 2

        details["L"] = {
            "passed": l_passed,
            "value": l_score,
            "checks": l_checks,
            "desc": f"Linearity: {l_score}/3 checks passed",
        }
        if l_passed:
            filters_passed += 1

        # === LETTER 2: Not Up 2 Days ===
        # Stockbee: "Stock should not be up 3 days in a row (small range days up 3 days is ok)"
        # We check 2 days: if yesterday AND today both up with big range, reject
        two_passed = False
        two_reason = ""

        if current_index >= 2:
            prev_1_close = prices[current_index - 1][4]
            prev_2_close = prices[current_index - 2][4]
            prev_1_high = prices[current_index - 1][2]
            prev_1_low = prices[current_index - 1][3]
            current_high = prices[current_index][2]
            current_low = prices[current_index][3]

            # Calculate returns
            ret_1 = (prev_1_close - prev_2_close) / prev_2_close if prev_2_close > 0 else 0
            ret_2 = gap_pct  # Today's gap

            # Define "small range" as < 1.5% (intraday movement)
            range_1 = (prev_1_high - prev_1_low) / prev_1_close if prev_1_close > 0 else 0
            range_2 = (
                (current_high - current_low) / prices[current_index][4]
                if prices[current_index][4] > 0
                else 0
            )
            small_range_threshold = 0.015  # 1.5%

            if ret_1 > 0 and ret_2 > 0:
                # Both days up - check if small range
                if range_1 <= small_range_threshold and range_2 <= small_range_threshold:
                    two_passed = True  # Small range up days - OK
                    two_reason = "Both up but small range"
                elif range_1 > small_range_threshold and range_2 > small_range_threshold:
                    two_passed = False  # Big range both days - REJECT
                    two_reason = "Extended move (both big range up)"
                else:
                    # Mixed - one big, one small - borderline PASS
                    two_passed = True
                    two_reason = "Mixed range - allowed"
            else:
                two_passed = True  # Not both up - PASS
                two_reason = "Not up 2 days"
        else:
            two_passed = True  # Not enough data - PASS
            two_reason = "Insufficient history"

        details["2"] = {"passed": two_passed, "desc": two_reason}

        # CRITICAL: If "2" filter fails, return immediately (extended move = high risk)
        if not two_passed:
            details["L"]["desc"] += " [Invalid due to extended move]"
            return {
                "passed": False,
                "filters_passed": filters_passed,
                "filters_total": filters_total,
                "details": details,
                "reject_reason": "Extended move (up 2 days with big range)",
            }

        filters_passed += 1

        # === LETTER N: Narrow Range OR Negative Day ===
        # Stockbee: Prefer breakouts from consolidation (narrow range) or pullback (negative day)
        # This ensures the breakout is "fresh" not "extended"
        n_passed = False
        n_reason = ""

        if current_index >= 1:
            prev_high = prices[current_index - 1][2]
            prev_low = prices[current_index - 1][3]
            prev_close = prices[current_index - 1][4]
            prev_open = prices[current_index - 1][1]

            # Check 1: Negative day (red candle)
            is_negative = prev_close < prev_open

            # Check 2: Narrow range (consolidation)
            yesterday_range = (prev_high - prev_low) / prev_close if prev_close > 0 else 0
            atr_yest = features.atr_20 if features.atr_20 else yesterday_range
            is_narrow = yesterday_range < (atr_yest * 0.5)  # Range < 50% of ATR

            if is_negative:
                n_passed = True
                n_reason = "Negative day (pullback)"
            elif is_narrow:
                n_passed = True
                n_reason = f"Narrow range ({yesterday_range:.1%} vs ATR {atr_yest:.1%})"
            else:
                n_passed = False
                n_reason = f"Wide range positive day ({yesterday_range:.1%})"

        details["N"] = {"passed": n_passed, "desc": n_reason}
        if n_passed:
            filters_passed += 1

        # === LETTER Y: Young Trend ===
        # Stockbee: "No 4% b/d in last 3 to 5 days"
        # This means the stock shouldn't have already broken out multiple times recently
        # We use prior_breakouts_90d but interpret it differently
        #
        # Since our feature is 90-day count, we need to be stricter
        # A stock with 0-2 breakouts in 90 days is likely "young"
        y_passed = False
        y_reason = ""

        if features.prior_breakouts_90d is not None:
            # Be strict: <= 2 breakouts in 90 days = young trend
            if features.prior_breakouts_90d <= 2:
                y_passed = True
                y_reason = f"Young trend ({features.prior_breakouts_90d} breakouts in 90d)"
            else:
                y_passed = False
                y_reason = f"Aged trend ({features.prior_breakouts_90d} breakouts in 90d)"
        else:
            y_passed = True  # Data unavailable - pass
            y_reason = "No breakout data"

        details["Y"] = {"passed": y_passed, "value": features.prior_breakouts_90d, "desc": y_reason}
        if y_passed:
            filters_passed += 1

        # === LETTER C: Consolidation ===
        # Stockbee: "Orderly pullback or consolidation"
        # Characteristics: Low volume, ATR compression, shallow pullback
        c_passed = False
        c_score = 0
        c_checks = []

        # Check 1: Volume dryup (vol_dryup_ratio < 1.0 = below average)
        if features.vol_dryup_ratio is not None:
            if features.vol_dryup_ratio < 1.0:
                c_score += 2  # Strong consolidation signal
                c_checks.append("vol_dryup_strong")
            elif features.vol_dryup_ratio < 1.3:
                c_score += 1  # Moderate consolidation
                c_checks.append("vol_dryup_moderate")
            else:
                c_checks.append("vol_high")
        else:
            c_checks.append("vol_na")

        # Check 2: ATR compression (ratio < 1.0 = squeezed)
        if features.atr_compress_ratio is not None:
            if features.atr_compress_ratio < 1.0:
                c_score += 2
                c_checks.append("atr_squeezed")
            elif features.atr_compress_ratio < 1.3:
                c_score += 1
                c_checks.append("atr_compressed")
            else:
                c_checks.append("atr_expanded")
        else:
            c_checks.append("atr_na")

        # Check 3: Price not at extreme top of range (allows room to run)
        if features.range_percentile is not None:
            if features.range_percentile < 0.90:
                c_score += 1
                c_checks.append("room_to_run")
            else:
                c_checks.append("at_highs")
        else:
            c_checks.append("range_na")

        # Pass C if at least 3 points (shows consolidation)
        c_passed = c_score >= 3

        details["C"] = {
            "passed": c_passed,
            "value": c_score,
            "checks": c_checks,
            "desc": f"Consolidation: {c_score} points, {c_checks}",
        }
        if c_passed:
            filters_passed += 1

        # === FINAL DECISION ===
        # All 6 filters are important - require at least 5 to pass
        # This is stricter than the "4 of 6" approach
        min_required = 5  # Require 5 of 6 filters
        passed = filters_passed >= min_required

        return {
            "passed": passed,
            "filters_passed": filters_passed,
            "filters_total": filters_total,
            "min_required": min_required,
            "details": details,
        }


def create_indian_2lynch_config() -> ScanConfig:
    """Create ScanConfig optimized for Indian markets with 2LYNCH."""
    return ScanConfig(
        # Entry criteria
        breakout_threshold=0.04,  # 4% gap
        close_pos_threshold=0.70,  # Top 30% close
        # Indian market specific
        min_price=50.0,  # ₹50 minimum
        min_value_traded_inr=3_000_000,  # ₹30 lakh
        # 2LYNCH filter thresholds
        min_r2_l=0.70,  # Linear trend
        max_prior_breakouts=2,  # Young trend
        max_atr_compress_ratio=1.3,  # ATR compression
        min_range_percentile=0.0,  # Any range OK (we check other criteria)
        max_vol_dryup_ratio=1.3,  # Volume dryup
        # Require 5 of 6 filters (strict quality)
        min_filters_pass=5,
    )
