"""
Consolidated 2LYNCH filter logic for backtesting.

This module centralizes the filter definitions that were previously duplicated
across:
- services/backtest/duckdb_backtest_runner.py (SQL CTE)
- services/scan/rules.py (Python FilterChecker class)
- services/scan/legacy 2LYNCH signal generator code

Filters are defined once with both SQL expression templates and Python
validation functions.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class FilterDefinition:
    """Definition of a 2LYNCH filter with SQL and Python implementations."""

    name: str
    """Filter letter (H, N, 2, Y, C, L)."""

    description: str
    """Human-readable description of what the filter checks."""

    sql_expression: str
    """SQL expression template for use in DuckDB queries.
    May use placeholders like {threshold} for configurable values.
    """

    def build_sql(self, **kwargs) -> str:
        """Build SQL expression with substituted parameters."""
        return self.sql_expression.format(**kwargs)


# 2LYNCH Filter Definitions
# These are the canonical definitions used across all backtesting code

FILTER_H = FilterDefinition(
    name="H",
    description="Close in top 30% of day's range (high close position)",
    sql_expression="close_pos_in_range >= 0.70",
)

FILTER_N = FilterDefinition(
    name="N",
    description="Narrow or negative prior day (T-1 not extended)",
    sql_expression="((prev_high - prev_low) < (atr_20 * 0.5) OR prev_close < prev_open)",
)

FILTER_2 = FilterDefinition(
    name="2",
    description="Not up 2 days in a row (avoid extended moves)",
    sql_expression="(ret_1d_lag1 <= 0 OR ret_1d_lag2 <= 0)",
)

FILTER_Y = FilterDefinition(
    name="Y",
    description="Young breakout (<= 2 prior breakouts in 30 days)",
    sql_expression="(COALESCE(prior_breakouts_30d, 0) <= 2)",
)

FILTER_C = FilterDefinition(
    name="C",
    description="Volume compression/dryup (low volume before breakout)",
    sql_expression="(vol_dryup_ratio < 1.3)",
)

FILTER_L = FilterDefinition(
    name="L",
    description="Lynch trend (at least 2 of: above MA20, positive 5d return, strong R²)",
    sql_expression=(
        "(CAST(close > ma_20 AS INTEGER) + "
        "CAST(ret_5d > 0 AS INTEGER) + "
        "CAST(COALESCE(NULLIF(r2_65, 0), 0) >= 0.70 AS INTEGER) >= 2)"
    ),
)

# All filters as a dict and list for easy access
ALL_FILTER_DEFS: dict[str, FilterDefinition] = {
    "H": FILTER_H,
    "N": FILTER_N,
    "2": FILTER_2,
    "Y": FILTER_Y,
    "C": FILTER_C,
    "L": FILTER_L,
}

FILTER_LIST: list[FilterDefinition] = list(ALL_FILTER_DEFS.values())

# Default minimum filters required (production default)
DEFAULT_MIN_FILTERS = 5


def build_filter_sql_clause(
    filters: list[str] | None = None,
    min_filters: int = DEFAULT_MIN_FILTERS,
) -> str:
    """Build SQL WHERE clause for 2LYNCH filters.

    Args:
        filters: List of filter letters to include (default: all)
        min_filters: Minimum number of filters that must pass

    Returns:
        SQL expression for filter logic

    Example:
        >>> build_filter_sql_clause(min_filters=5)
        "(filter_h + filter_n + filter_2 + filter_y + filter_c + filter_l) >= 5"
    """
    if filters is None:
        filters = list(ALL_FILTER_DEFS.keys())

    filter_cols = [f"filter_{f.lower()}" for f in filters]
    return f"({' + '.join(filter_cols)}) >= {min_filters}"


def build_filter_ctes(
    table_name: str = "with_features",
    min_filters: int = DEFAULT_MIN_FILTERS,
) -> str:
    """Build complete CTE SQL for all 2LYNCH filters.

    Args:
        table_name: Name of the input CTE/table with feature columns
        min_filters: Minimum number of filters that must pass

    Returns:
        SQL SELECT clause with all filter columns

    Example:
        >>> sql = build_filter_ctes()
        >>> print(sql)
    """
    filter_expressions = ",\n            ".join(
        f"({f.sql_expression}) AS filter_{f.name.lower()}" for f in FILTER_LIST
    )

    return f"""
        SELECT
            symbol, trading_date, open, high, low, close, prev_close, prev_low, gap_pct,
            value_traded_inr, close_pos_in_range,
            (close > ma_20) AS above_ma20,
            (ret_5d > 0) AS positive_momentum,
            atr_20, vol_dryup_ratio, atr_compress_ratio, range_percentile,
            prior_breakouts_90d,
            {filter_expressions}
        FROM {table_name}
        WHERE close_pos_in_range IS NOT NULL
    """


@dataclass
class FilterResult:
    """Result of checking a single filter on a signal."""

    filter_name: str
    passed: bool
    reason: str
    value: float | None = None


class FilterChecker:
    """Python implementation of 2LYNCH filter checking.

    This provides the same logic as the SQL expressions but for
    Python-based signal generation and testing.
    """

    def __init__(
        self,
        close_pos_threshold: float = 0.70,
        nr_atr_multiplier: float = 0.5,
        max_prior_breakouts: int = 2,
        vol_dryup_threshold: float = 1.3,
        lynch_min_passed: int = 2,
        min_filters: int = DEFAULT_MIN_FILTERS,
    ):
        self.close_pos_threshold = close_pos_threshold
        self.nr_atr_multiplier = nr_atr_multiplier
        self.max_prior_breakouts = max_prior_breakouts
        self.vol_dryup_threshold = vol_dryup_threshold
        self.lynch_min_passed = lynch_min_passed
        self.min_filters = min_filters

    def check_h(self, close_pos_in_range: float | None, is_short: bool = False) -> FilterResult:
        """H: Close near high of the day (long) or near low of the day (short)."""
        if close_pos_in_range is None:
            return FilterResult("H", False, "No close position data")

        if is_short:
            short_threshold = round(1.0 - self.close_pos_threshold, 6)
            passed = close_pos_in_range <= short_threshold
            return FilterResult(
                "H",
                passed,
                f"Close pos {close_pos_in_range:.2f} {'<=' if passed else '>'} {short_threshold} (short)",
                close_pos_in_range,
            )
        passed = close_pos_in_range >= self.close_pos_threshold
        return FilterResult(
            "H",
            passed,
            f"Close pos {close_pos_in_range:.2f} {'>=' if passed else '<'} {self.close_pos_threshold}",
            close_pos_in_range,
        )

    def check_n(
        self,
        prev_high: float | None,
        prev_low: float | None,
        prev_close: float | None,
        prev_open: float | None,
        atr_20: float | None,
        is_short: bool = False,
    ) -> FilterResult:
        """N: Narrow range or directional prior day.

        For longs: prior day narrow OR red (close < open) — exhaustion before breakout.
        For shorts: prior day narrow OR green (close > open) — buying exhaustion before breakdown.
        """
        if None in (prev_high, prev_low, prev_close, prev_open, atr_20):
            return FilterResult("N", False, "Missing prior day data")

        # Narrow types for mypy
        assert prev_high is not None
        assert prev_low is not None
        assert prev_close is not None
        assert prev_open is not None
        assert atr_20 is not None

        # Check 2: Narrow range (True Range in bottom half) — shared for both directions
        prior_tr = prev_high - prev_low
        is_narrow = prior_tr < (atr_20 * self.nr_atr_multiplier)
        if is_narrow:
            return FilterResult(
                "N",
                True,
                f"Prior TR {prior_tr:.2f} < ATR*{self.nr_atr_multiplier} ({atr_20 * self.nr_atr_multiplier:.2f}) [narrow]",
                prior_tr,
            )

        # Check 1: Directional candle — red for longs, green for shorts
        if is_short:
            directional = prev_close > prev_open
            direction_label = "green (exhaustion)"
        else:
            directional = prev_close < prev_open
            direction_label = "red (exhaustion)"

        return FilterResult(
            "N",
            directional,
            f"Prior day {direction_label}: close {prev_close:.2f} {'>' if is_short else '<'} open {prev_open:.2f}",
            prev_close - prev_open,
        )

    def check_2(self, ret_1d_lag1: float | None, ret_1d_lag2: float | None) -> FilterResult:
        """2: Not up 2 days in a row."""
        if ret_1d_lag1 is None and ret_1d_lag2 is None:
            return FilterResult("2", False, "No lag return data")

        passed = (ret_1d_lag1 is not None and ret_1d_lag1 <= 0) or (
            ret_1d_lag2 is not None and ret_1d_lag2 <= 0
        )
        t1_str = f"{ret_1d_lag1:.2f}" if ret_1d_lag1 is not None else "None"
        t2_str = f"{ret_1d_lag2:.2f}" if ret_1d_lag2 is not None else "None"
        return FilterResult(
            "2",
            passed,
            f"Lag returns: T-1={t1_str}, T-2={t2_str}",
        )

    def check_y(self, prior_breakouts_30d: int | None) -> FilterResult:
        """Y: Young breakout (few prior breakouts)."""
        if prior_breakouts_30d is None:
            return FilterResult("Y", False, "No breakout history data")

        passed = prior_breakouts_30d <= self.max_prior_breakouts
        return FilterResult(
            "Y",
            passed,
            f"Prior breakouts (30d): {prior_breakouts_30d} "
            f"{'<=' if passed else '>'} {self.max_prior_breakouts}",
            prior_breakouts_30d,
        )

    def check_c(self, vol_dryup_ratio: float | None) -> FilterResult:
        """C: Volume compression."""
        if vol_dryup_ratio is None:
            return FilterResult("C", False, "No volume ratio data")

        passed = vol_dryup_ratio < self.vol_dryup_threshold
        return FilterResult(
            "C",
            passed,
            f"Vol dryup ratio {vol_dryup_ratio:.2f} "
            f"{'<' if passed else '>='} {self.vol_dryup_threshold}",
            vol_dryup_ratio,
        )

    def check_l(
        self,
        close: float | None,
        ma_20: float | None,
        ret_5d: float | None,
        r2_65: float | None,
    ) -> FilterResult:
        """L: Lynch trend (at least 2 of 3 conditions)."""
        conditions_passed = 0
        reasons = []

        if close is not None and ma_20 is not None:
            above_ma = close > ma_20
            if above_ma:
                conditions_passed += 1
            reasons.append(f"above MA20: {above_ma}")

        if ret_5d is not None:
            positive_momentum = ret_5d > 0
            if positive_momentum:
                conditions_passed += 1
            reasons.append(f"5d return > 0: {positive_momentum}")

        if r2_65 is not None and r2_65 != 0:
            strong_r2 = r2_65 >= 0.70
            if strong_r2:
                conditions_passed += 1
            reasons.append(f"R² >= 0.70: {strong_r2}")

        passed = conditions_passed >= self.lynch_min_passed
        return FilterResult(
            "L",
            passed,
            f"Lynch: {conditions_passed}/{self.lynch_min_passed} passed ({', '.join(reasons)})",
            conditions_passed,
        )

    def check_all(
        self,
        # H
        close_pos_in_range: float | None,
        # N
        prev_high: float | None,
        prev_low: float | None,
        prev_close: float | None,
        prev_open: float | None,
        atr_20: float | None,
        # 2
        ret_1d_lag1: float | None,
        ret_1d_lag2: float | None,
        # Y
        prior_breakouts_30d: int | None,
        # C
        vol_dryup_ratio: float | None,
        # L
        close: float | None,
        ma_20: float | None,
        ret_5d: float | None,
        r2_65: float | None,
        *,
        is_short: bool = False,
    ) -> dict[str, FilterResult]:
        """Check all filters and return results.

        Args:
            is_short: When True, inverts direction-sensitive filters H and N for short trades.

        Returns:
            Dict mapping filter letter to FilterResult
        """
        return {
            "H": self.check_h(close_pos_in_range, is_short=is_short),
            "N": self.check_n(
                prev_high, prev_low, prev_close, prev_open, atr_20, is_short=is_short
            ),
            "2": self.check_2(ret_1d_lag1, ret_1d_lag2),
            "Y": self.check_y(prior_breakouts_30d),
            "C": self.check_c(vol_dryup_ratio),
            "L": self.check_l(close, ma_20, ret_5d, r2_65),
        }

    def count_passed(self, results: dict[str, FilterResult]) -> int:
        """Count how many filters passed."""
        return sum(1 for r in results.values() if r.passed)

    def passes_min_filters(self, results: dict[str, FilterResult]) -> bool:
        """Check if enough filters passed."""
        return self.count_passed(results) >= self.min_filters
