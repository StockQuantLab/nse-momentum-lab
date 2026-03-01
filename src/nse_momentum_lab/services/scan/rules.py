from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import numpy as np

from nse_momentum_lab.services.scan.features import DailyFeatures

logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
    """Convert value to JSON-serializable type."""
    if value is None:
        return None
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


@dataclass
class ScanConfig:
    breakout_threshold: float = 0.04
    close_pos_threshold: float = 0.70
    nr_percentile: float = 0.20
    nr_lookback: int = 20
    min_r2_l: float = 0.70
    max_down_days_l: int = 7
    lookback_l: int = 20
    lookback_high: int = 20
    lookback_y: int = 90
    max_prior_breakouts: int = 2
    atr_compress_ratio: float = 0.80
    range_percentile: float = 0.20
    range_ref_window: int = 60
    vol_dryup_ratio: float = 0.80
    lookback_c: int = 15
    max_big_moves_in_consolidation: int = 1
    min_value_traded_inr: float = 3000000.0
    min_price: float = 3.0
    avg_c7_c65_ratio: float = 1.05
    slippage_large_threshold_inr: float = 1_000_000_000.0
    slippage_small_threshold_inr: float = 20_000_000.0
    holding_period_days: int = 3
    # 2LYNCH filter additional parameters
    max_atr_compress_ratio: float = 1.2  # Allow ATR up to 1.2x average
    min_range_percentile: float = 0.5  # Price in top 50% of range
    max_vol_dryup_ratio: float = 1.5  # Volume up to 1.5x average
    min_filters_pass: int = 4  # Minimum filters to pass (out of 6)


@dataclass
class ScanCheck:
    letter: str
    passed: bool
    reason: str
    value: float | None = None


@dataclass
class ScanCandidate:
    symbol_id: int
    symbol: str
    trading_date: date
    score: float
    checks: list[ScanCheck]
    passed: bool
    reason_json: dict[str, Any]
    gap_pct: float | None = None
    entry_price: float | None = None
    initial_stop: float | None = None


@dataclass
class ScanDiagnostics:
    total_scanned: int = 0
    passed_breakout_4p: int = 0
    passed_h: int = 0
    passed_n: int = 0
    passed_2: int = 0
    passed_y: int = 0
    passed_l: int = 0
    passed_c: int = 0
    passed_liquidity: int = 0
    passed_survivorship: int = 0
    failed_survivorship: int = 0
    passed_all: int = 0
    fail_reason_counts: dict[str, int] = field(default_factory=dict)


class ScanRuleEngine:
    def __init__(self, config: ScanConfig | None = None) -> None:
        self.config = config or ScanConfig()

    def check_gap_breakout(
        self,
        features: DailyFeatures,
        prev_features: DailyFeatures | None,
    ) -> ScanCheck:
        """Check for gap-up breakout at T's open.

        GAP BREAKOUT: Today's open >= (1 + breakout_threshold) * yesterday's close

        This is the realistic entry scenario:
        - At 9:15 AM (market open), we see the stock gapped up 4%+
        - We can enter immediately at T's open price
        - All 2LYNCH criteria use T-1 and prior data
        """
        if prev_features is None:
            return ScanCheck(
                letter="4P",
                passed=False,
                reason="No prior day data",
                value=None,
            )

        prev_close = prev_features.close
        today_open = features.open_adj if hasattr(features, "open_adj") else None

        if prev_close is None or prev_close <= 0:
            return ScanCheck(
                letter="4P",
                passed=False,
                reason="Invalid prior close",
                value=None,
            )

        if today_open is None:
            return ScanCheck(
                letter="4P",
                passed=False,
                reason="No today's open data",
                value=None,
            )

        gap_pct = (today_open - prev_close) / prev_close
        passed = gap_pct >= self.config.breakout_threshold

        return ScanCheck(
            letter="4P",
            passed=passed,
            reason=f"Gap {gap_pct * 100:.2f}% {'>=' if passed else '<'} {self.config.breakout_threshold * 100}% (Open ₹{today_open:.2f} vs PrevClose ₹{prev_close:.2f})",
            value=gap_pct,
        )

    def check_breakout(
        self,
        features: DailyFeatures,
        prev_features: DailyFeatures | None,
    ) -> ScanCheck:
        """Check for gap-up breakout (primary method for realistic entry)."""
        return self.check_gap_breakout(features, prev_features)

    def check_volume_increase(
        self,
        features: DailyFeatures | None,
        prev_features: DailyFeatures | None,
    ) -> ScanCheck:
        if features is None or prev_features is None:
            return ScanCheck(
                letter="VOL",
                passed=True,
                reason="No prior volume data (skipped)",
                value=None,
            )
        if prev_features.vol_20 is None or prev_features.vol_20 == 0:
            return ScanCheck(
                letter="VOL",
                passed=True,
                reason="No prior volume data (skipped)",
                value=None,
            )

        vol_ratio = (
            float(features.vol_20) / float(prev_features.vol_20)
            if features.vol_20 and prev_features.vol_20
            else 0.0
        )
        vol_prev = float(prev_features.vol_20) if prev_features.vol_20 else 0.0
        passed = bool(features.vol_20 and vol_prev and features.vol_20 > vol_prev)
        return ScanCheck(
            letter="VOL",
            passed=passed,
            reason=f"Volume ratio {vol_ratio:.2f}",
            value=vol_ratio,
        )

    def check_liquidity(
        self,
        features: DailyFeatures | None,
    ) -> ScanCheck:
        """Liquidity check using 20-day average value traded in INR.

        For Indian markets, we use value_traded_inr instead of dollar volume.
        The threshold is adapted to NSE liquidity characteristics.

        Stockbee's minv3.1 uses dollar volume; we adapt to INR:
        - Original: $300,000 ~ Rs 25 lakh at current rates
        - Adapted: Rs 30 lakh minimum for Indian stocks
        """
        if features is None:
            return ScanCheck(
                letter="LIQ",
                passed=False,
                reason="No value traded data",
                value=None,
            )

        value_traded = features.dollar_vol_20  # This field stores value_traded in INR
        if value_traded is None:
            return ScanCheck(
                letter="LIQ",
                passed=False,
                reason="No value traded data",
                value=None,
            )

        passed = value_traded >= self.config.min_value_traded_inr
        return ScanCheck(
            letter="LIQ",
            passed=passed,
            reason=f"Value traded: Rs {value_traded:,.0f} {'>=' if passed else '<'} Rs {self.config.min_value_traded_inr:,.0f}",
            value=value_traded,
        )

    def check_survivorship(
        self,
        features: DailyFeatures | None,
        asof_date: date,
    ) -> ScanCheck:
        """Survivorship bias check: Skip stocks that are delisted or suspended.

        This is critical for avoiding survivorship bias in backtests.
        We should NOT scan stocks that:
        1. Are already delisted before the scan date
        2. Will be delisted during the holding period (adds forced exit risk)

        Args:
            features: Daily features for the scan date
            asof_date: The date we're scanning

        Returns:
            ScanCheck with pass/fail for survivorship
        """
        if features is None:
            return ScanCheck(
                letter="SURV",
                passed=False,
                reason="No features data",
                value=None,
            )

        if features.status not in ("ACTIVE", None):
            return ScanCheck(
                letter="SURV",
                passed=False,
                reason=f"Stock status is {features.status}",
                value=None,
            )

        if features.delisting_date is not None:
            from datetime import timedelta

            holding_end = asof_date + timedelta(days=self.config.holding_period_days)
            if features.delisting_date <= holding_end:
                return ScanCheck(
                    letter="SURV",
                    passed=False,
                    reason=f"Delisting scheduled {features.delisting_date} within holding period",
                    value=None,
                )

        return ScanCheck(
            letter="SURV",
            passed=True,
            reason="Active stock with no imminent delisting",
            value=None,
        )

    def check_h(
        self,
        features: DailyFeatures | None,
    ) -> ScanCheck:
        """H: Close near high of the day (T-1's data for gap-up breakout)."""
        if features is None:
            return ScanCheck(
                letter="H",
                passed=False,
                reason="No data",
                value=None,
            )
        cpr = features.close_pos_in_range
        if cpr is None:
            return ScanCheck(
                letter="H",
                passed=False,
                reason="No close pos in range",
                value=None,
            )

        passed = cpr >= self.config.close_pos_threshold
        return ScanCheck(
            letter="H",
            passed=passed,
            reason=f"Close pos {cpr:.2f} {'>=' if passed else '<'} {self.config.close_pos_threshold}",
            value=cpr,
        )

    def check_n(
        self,
        features: DailyFeatures | None,
        prev_features: DailyFeatures | None,
        all_features: list[DailyFeatures],
        current_idx: int,
    ) -> ScanCheck:
        """N (Narrow/Negative prior day): Prefer breakouts from a narrow range day
        or a negative day. This indicates the breakout is fresh, not extended.

        Passes if:
        1. Prior day had negative return, OR
        2. Prior day's True Range was in bottom percentile of recent history
        """
        if features is None or prev_features is None:
            return ScanCheck(
                letter="N",
                passed=False,
                reason="No prior day data",
                value=None,
            )

        # First check: prior day negative return
        if prev_features.ret_1d is not None and prev_features.ret_1d <= 0:
            return ScanCheck(
                letter="N",
                passed=True,
                reason=f"Prior day negative return ({prev_features.ret_1d * 100:.2f}%)",
                value=prev_features.ret_1d,
            )

        # Second check: prior day's ATR in bottom percentile
        nr_lookback = min(self.config.nr_lookback, current_idx)

        if prev_features.atr_20 is None or nr_lookback < 5:
            return ScanCheck(
                letter="N",
                passed=False,
                reason="Insufficient ATR history",
                value=None,
            )

        # Collect ATR values from the lookback window
        tr_values = []
        start_idx = max(0, current_idx - nr_lookback)
        for i in range(start_idx, current_idx):
            if all_features[i].atr_20 is not None:
                tr_values.append(all_features[i].atr_20)

        if len(tr_values) < 5:
            return ScanCheck(
                letter="N",
                passed=False,
                reason="Insufficient true range history",
                value=None,
            )

        current_tr = prev_features.atr_20
        percentile = np.percentile(tr_values, self.config.nr_percentile * 100)
        passed = bool(current_tr <= percentile)
        return ScanCheck(
            letter="N",
            passed=passed,
            reason=(
                f"Prior TR {current_tr:.2f} {'<=' if passed else '>'} "
                f"{self.config.nr_percentile * 100:.0f}th percentile {percentile:.2f}"
            ),
            value=current_tr,
        )

    def check_2(
        self,
        features: DailyFeatures | None,
        prev1: DailyFeatures | None,
        prev2: DailyFeatures | None,
    ) -> ScanCheck:
        """2: Not up 2 days in a row before breakout (T-2 and T-3 data for gap-up)."""
        if features is None:
            return ScanCheck(
                letter="2",
                passed=False,
                reason="No data",
                value=None,
            )
        ret1 = prev1.ret_1d if prev1 else None
        ret2 = prev2.ret_1d if prev2 else None

        if ret1 is None or ret2 is None:
            return ScanCheck(
                letter="2",
                passed=False,
                reason="Insufficient return history",
                value=None,
            )

        passed = not (ret1 > 0 and ret2 > 0)
        return ScanCheck(
            letter="2",
            passed=passed,
            reason=f"Prior 2 days: {ret1 * 100:.2f}%, {ret2 * 100:.2f}% {'not both up' if passed else 'both up'}",
            value=ret1,
        )

    def check_y(
        self,
        features: DailyFeatures | None,
        all_features: list[DailyFeatures],
        current_idx: int,
    ) -> ScanCheck:
        """Y (Young trend): Count prior breakouts in the lookback window.

        A breakout is defined as a day where close >= max close of prior N days.
        This check ensures we're in the 1st-3rd breakout from consolidation,
        not an aged trend where failure risk increases.
        """
        if features is None or current_idx < 1:
            return ScanCheck(
                letter="Y",
                passed=False,
                reason="No prior data",
                value=None,
            )

        lookback_high = min(self.config.lookback_high, current_idx)
        lookback_y = min(self.config.lookback_y, current_idx)

        # Count breakouts in the Y-day window (excluding current day)
        breakout_count = 0
        for i in range(current_idx - lookback_y, current_idx):
            if i < lookback_high:
                # Not enough history to determine if this was a breakout
                continue

            # Get the maximum close in the lookback_high window BEFORE day i
            # This is the proper definition: a close >= max of prior N days
            window_start = i - lookback_high
            window_closes = [all_features[j].close for j in range(window_start, i)]
            max_close = max(window_closes) if window_closes else all_features[i].close

            # Day i is a breakout if its close made a new high
            if all_features[i].close >= max_close:
                breakout_count += 1

        passed = breakout_count <= self.config.max_prior_breakouts
        return ScanCheck(
            letter="Y",
            passed=passed,
            reason=f"Prior breakouts: {breakout_count} {'<=' if passed else '>'} {self.config.max_prior_breakouts} in {lookback_y}d window",
            value=float(breakout_count),
        )

    def check_l(
        self,
        features: DailyFeatures | None,
        all_features: list[DailyFeatures],
        current_idx: int,
    ) -> ScanCheck:
        """L: Linearity of prior move (T-1 and prior for gap-up breakout)."""
        if features is None:
            return ScanCheck(
                letter="L",
                passed=False,
                reason="No data",
                value=None,
            )
        lookback = min(self.config.lookback_l, current_idx)
        if lookback < 5:
            return ScanCheck(
                letter="L",
                passed=False,
                reason="Insufficient lookback history",
                value=None,
            )

        closes = np.array(
            [f.close for f in all_features[current_idx - lookback : current_idx + 1]],
            dtype=np.float64,
        )
        x = np.arange(len(closes))
        coeffs = np.polyfit(x, closes, 1)
        y_pred = coeffs[0] * x + coeffs[1]
        ss_res = np.sum((closes - y_pred) ** 2)
        ss_tot = np.sum((closes - np.mean(closes)) ** 2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

        down_days = sum(
            1
            for f in all_features[current_idx - lookback : current_idx]
            if f.ret_1d and f.ret_1d < 0
        )

        r2_passed = r2 >= self.config.min_r2_l and coeffs[0] > 0
        down_days_passed = down_days <= self.config.max_down_days_l
        passed = r2_passed and down_days_passed

        return ScanCheck(
            letter="L",
            passed=passed,
            reason=f"R2={r2:.3f} {'>=' if r2_passed else '<'} {self.config.min_r2_l}, down_days={down_days} {'<=' if down_days_passed else '>'} {self.config.max_down_days_l}",
            value=r2,
        )

    def check_c(
        self,
        features: DailyFeatures | None,
        all_features: list[DailyFeatures],
        current_idx: int,
    ) -> ScanCheck:
        """C: Consolidation quality (T-1 and prior for gap-up breakout)."""
        if features is None:
            return ScanCheck(
                letter="C",
                passed=False,
                reason="No data",
                value=None,
            )
        lookback = min(self.config.lookback_c, current_idx)
        if lookback < 3:
            return ScanCheck(
                letter="C",
                passed=False,
                reason="Insufficient lookback for consolidation check",
                value=None,
            )

        atr_short = (
            np.mean([f.atr_20 for f in all_features[current_idx - 5 : current_idx] if f.atr_20])
            if current_idx >= 5
            else None
        )
        atr_long = features.atr_20
        atr_ratio = float(atr_short) / float(atr_long) if atr_short and atr_long else None
        atr_passed = atr_ratio is not None and atr_ratio <= self.config.atr_compress_ratio

        range_vals = [
            (f.high - f.low) / f.close
            for f in all_features[current_idx - lookback : current_idx]
            if f.close > 0
        ]
        if len(range_vals) < 3:
            return ScanCheck(
                letter="C",
                passed=False,
                reason="Insufficient range data",
                value=None,
            )

        range_percentile = (
            np.percentile(
                [
                    (f.high - f.low) / f.close
                    for f in all_features[
                        max(0, current_idx - self.config.range_ref_window) : current_idx
                    ]
                    if f.close > 0
                ],
                self.config.range_percentile * 100,
            )
            if range_vals
            else 0
        )
        range_passed = np.median(range_vals) <= range_percentile if range_vals else False

        vol_prev_raw = all_features[current_idx - 1].vol_20 if current_idx > 0 else None
        vol_prev = float(vol_prev_raw) if vol_prev_raw else 0.0
        vol_curr = float(features.vol_20) if features.vol_20 else 0.0
        vol_dryup_passed = vol_prev > 0 and (vol_curr / vol_prev) <= self.config.vol_dryup_ratio

        big_moves = sum(
            1
            for f in all_features[current_idx - lookback : current_idx]
            if f.ret_1d and float(f.ret_1d) >= 0.04
        )
        big_move_passed = big_moves <= self.config.max_big_moves_in_consolidation

        passed = bool(atr_passed and range_passed and vol_dryup_passed and big_move_passed)

        return ScanCheck(
            letter="C",
            passed=passed,
            reason=f"ATR compression: {'PASS' if atr_passed else 'FAIL'}, Range compression: {'PASS' if range_passed else 'FAIL'}, Vol dryup: {'PASS' if vol_dryup_passed else 'FAIL'}, Big moves: {big_moves}/{self.config.max_big_moves_in_consolidation}",
            value=float(atr_short) / float(atr_long) if atr_short and atr_long else None,
        )

    def run_scan(
        self,
        symbol_id: int,
        symbol: str,
        features_list: list[DailyFeatures],
        asof_date: date,
    ) -> list[ScanCandidate]:
        """Run scan for gap-up breakout with 2LYNCH quality filter.

        TIMING (Critical for avoiding look-ahead bias):
        - Signal detected at T's open (9:15 AM market open)
        - Gap breakout: T's open >= (1 + threshold) * T-1's close
        - Entry: T's open price (immediate execution)
        - 2LYNCH checks: Use T-1 and prior data ONLY

        Args:
            symbol_id: Symbol ID
            symbol: Symbol string
            features_list: List of daily features (sorted by date)
            asof_date: Date to scan for breakouts

        Returns:
            List of ScanCandidate with pass/fail results
        """
        candidates = []

        for i, features in enumerate(features_list):
            if features.trading_date != asof_date:
                continue

            prev_features = features_list[i - 1] if i > 0 else None
            prev2_features = features_list[i - 2] if i > 1 else None

            checks = []

            # 4P: Gap breakout check (T's open vs T-1's close)
            breakout_check = self.check_gap_breakout(features, prev_features)
            checks.append(breakout_check)

            # Volume increase check (T-1 vs T-2 for 2LYNCH context)
            checks.append(self.check_volume_increase(prev_features, prev2_features))

            # Liquidity check (use T-1's data since we're checking before entry)
            checks.append(self.check_liquidity(prev_features))

            # Survivorship bias check - skip delisted/suspended stocks
            checks.append(self.check_survivorship(features, asof_date))

            # H: Close position in range (T-1's data)
            checks.append(self.check_h(prev_features))

            # N: Narrow range or negative day (T-1 vs T-2)
            checks.append(self.check_n(prev_features, prev2_features, features_list, i - 1))

            # 2: Not up 2 days in a row (T-2 and T-3 data)
            checks.append(
                self.check_2(
                    prev_features,
                    prev2_features,
                    features_list[i - 3] if i > 2 else None,
                )
            )

            # Y: Young trend (count breakouts in T-1 and prior)
            checks.append(self.check_y(prev_features, features_list, i - 1))

            # L: Linearity of prior move (T-1 and prior)
            checks.append(self.check_l(prev_features, features_list, i - 1))

            # C: Consolidation quality (T-1 and prior)
            checks.append(self.check_c(prev_features, features_list, i - 1))

            base_4p = any(c.letter == "4P" and c.passed for c in checks)
            all_2lynch = all(c.passed for c in checks if c.letter in ["2", "N", "H", "C", "L", "Y"])
            liquidity_passed = any(c.letter == "LIQ" and c.passed for c in checks)
            survivorship_passed = any(c.letter == "SURV" and c.passed for c in checks)

            passed = base_4p and all_2lynch and liquidity_passed and survivorship_passed
            score = sum(1 for c in checks if c.passed) / len(checks)

            # Calculate entry price and initial stop
            entry_price = features.open_adj
            gap_pct = breakout_check.value
            initial_stop = None
            if prev_features and prev_features.atr_20:
                initial_stop = entry_price - (prev_features.atr_20 * 2.0) if entry_price else None

            candidates.append(
                ScanCandidate(
                    symbol_id=symbol_id,
                    symbol=symbol,
                    trading_date=asof_date,
                    score=score,
                    checks=checks,
                    passed=passed,
                    gap_pct=gap_pct,
                    entry_price=entry_price,
                    initial_stop=initial_stop,
                    reason_json={
                        "checks": [
                            {
                                "letter": c.letter,
                                "passed": _json_safe(c.passed),
                                "reason": c.reason,
                                "value": _json_safe(c.value),
                            }
                            for c in checks
                        ],
                        "base_4p": _json_safe(base_4p),
                        "all_2lynch": _json_safe(all_2lynch),
                        "gap_pct": _json_safe(gap_pct),
                        "entry_price": _json_safe(entry_price),
                        "initial_stop": _json_safe(initial_stop),
                    },
                )
            )

        return candidates


def aggregate_scan_diagnostics(candidates: list[ScanCandidate]) -> ScanDiagnostics:
    diagnostics = ScanDiagnostics()
    diagnostics.total_scanned = len(candidates)

    for candidate in candidates:
        for check in candidate.checks:
            if check.letter == "4P" and check.passed:
                diagnostics.passed_breakout_4p += 1
            elif check.letter == "H" and check.passed:
                diagnostics.passed_h += 1
            elif check.letter == "N" and check.passed:
                diagnostics.passed_n += 1
            elif check.letter == "2" and check.passed:
                diagnostics.passed_2 += 1
            elif check.letter == "Y" and check.passed:
                diagnostics.passed_y += 1
            elif check.letter == "L" and check.passed:
                diagnostics.passed_l += 1
            elif check.letter == "C" and check.passed:
                diagnostics.passed_c += 1
            elif check.letter == "LIQ" and check.passed:
                diagnostics.passed_liquidity += 1
            elif check.letter == "SURV" and check.passed:
                diagnostics.passed_survivorship += 1
            elif check.letter == "SURV" and not check.passed:
                diagnostics.failed_survivorship += 1

        if candidate.passed:
            diagnostics.passed_all += 1
        else:
            for check in candidate.checks:
                if not check.passed:
                    fail_key = f"{check.letter}_{check.reason.split('.')[0] if '.' in check.reason else check.reason[:20]}"
                    diagnostics.fail_reason_counts[fail_key] = (
                        diagnostics.fail_reason_counts.get(fail_key, 0) + 1
                    )

    return diagnostics
