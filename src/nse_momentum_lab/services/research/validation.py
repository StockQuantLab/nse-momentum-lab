"""
Quality gates for backtest and research validation.

Implements sanity checks and statistical validation to catch:
- Data errors (missing prices, wrong symbols)
- Logic errors (wrong P&L calculation)
- Overfitting (too few trades, unrealistic metrics)
- Implementation bugs (extreme outliers)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from nse_momentum_lab.services.research.benchmarks import BenchmarkMetrics

logger = logging.getLogger(__name__)


class ValidationSeverity(StrEnum):
    """Severity level of a validation issue."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationIssue:
    """A single validation issue found during checks."""

    check_name: str
    severity: ValidationSeverity
    message: str
    actual_value: float | None = None
    expected_range: str | None = None
    impact: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_name": self.check_name,
            "severity": self.severity.value,
            "message": self.message,
            "actual_value": self.actual_value,
            "expected_range": self.expected_range,
            "impact": self.impact,
        }


@dataclass
class QualityGateResult:
    """Result of running quality gates on a backtest."""

    passed: bool
    warnings: int
    errors: int
    criticals: int
    issues: list[ValidationIssue] = field(default_factory=list)
    validated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0

    @property
    def can_proceed(self) -> bool:
        """Check if research can proceed despite issues."""
        return self.criticals == 0

    def add_issue(self, issue: ValidationIssue) -> None:
        self.issues.append(issue)
        if issue.severity == ValidationSeverity.WARNING:
            self.warnings += 1
        elif issue.severity == ValidationSeverity.ERROR:
            self.errors += 1
        elif issue.severity == ValidationSeverity.CRITICAL:
            self.criticals += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "can_proceed": self.can_proceed,
            "warnings": self.warnings,
            "errors": self.errors,
            "criticals": self.criticals,
            "issues": [i.to_dict() for i in self.issues],
            "validated_at": self.validated_at.isoformat(),
        }


@dataclass
class QualityThresholds:
    """Configurable thresholds for quality gates."""

    # Trade count thresholds
    min_trades: int = 50
    min_trades_per_year: int = 10

    # Drawdown sanity
    max_max_drawdown_pct: float = 80.0  # Extreme DD suggests data errors
    suspicious_max_drawdown_pct: float = 50.0  # Very high DD

    # Return sanity
    max_annual_return_pct: float = 500.0  # >500% annual suggests error
    min_annual_return_pct: float = -100.0  # Can't lose more than 100%

    # Win rate sanity
    min_win_rate: float = 0.0
    max_win_rate: float = 1.0
    suspicious_win_rate: tuple[float, float] = (0.85, 1.0)  # Too good to be true

    # Calmar ratio sanity
    max_calmar: float = 50.0  # Extremely high suggests error

    # Trade duration sanity
    max_holding_days: int = 365  # Trades should not hold >1 year
    avg_holding_days_max: int = 90  # Avg holding period

    # P&L distribution sanity
    max_single_trade_loss_pct: float = -50.0  # Single trade loses >50%
    max_single_trade_gain_pct: float = 200.0  # Single trade gains >200%

    # Position sizing
    max_position_size_pct: float = 0.5  # No position >50% of portfolio

    # Performance consistency
    min_sharpe_ratio: float = -5.0  # Extremely negative suggests error


# Strategy-specific thresholds
STRATEGY_THRESHOLDS: dict[str, QualityThresholds] = {
    "indian_2lynch": QualityThresholds(
        min_trades=100,
        min_trades_per_year=20,
        max_max_drawdown_pct=60.0,
        suspicious_max_drawdown_pct=40.0,
        max_annual_return_pct=300.0,
        suspicious_win_rate=(0.70, 1.0),
        max_calmar=30.0,
        max_holding_days=60,
        avg_holding_days_max=10,
    ),
    "threshold_breakout": QualityThresholds(
        min_trades=100,
        min_trades_per_year=20,
        max_max_drawdown_pct=60.0,
        suspicious_max_drawdown_pct=40.0,
        max_annual_return_pct=300.0,
        suspicious_win_rate=(0.70, 1.0),
        max_calmar=30.0,
        max_holding_days=60,
        avg_holding_days_max=10,
    ),
    "threshold_breakdown": QualityThresholds(
        min_trades=50,
        min_trades_per_year=10,
        max_max_drawdown_pct=70.0,  # Shorts can have higher DD
        suspicious_max_drawdown_pct=50.0,
        max_annual_return_pct=300.0,
        suspicious_win_rate=(0.65, 1.0),  # Short win rates typically lower
        max_calmar=25.0,
        max_holding_days=60,
        avg_holding_days_max=10,
    ),
    "episodic_pivot": QualityThresholds(
        min_trades=30,  # Fewer signals expected
        min_trades_per_year=5,
        max_max_drawdown_pct=50.0,
        suspicious_max_drawdown_pct=35.0,
        max_annual_return_pct=400.0,  # Episodic strategies can have higher returns
        suspicious_win_rate=(0.75, 1.0),
        max_calmar=40.0,
        max_holding_days=30,  # Shorter holding for event-driven
        avg_holding_days_max=7,
    ),
}


def validate_backtest_result(
    metrics: dict[str, float],
    trade_data: dict[str, Any] | None = None,
    strategy_name: str = "indian_2lynch",
    custom_thresholds: QualityThresholds | None = None,
) -> QualityGateResult:
    """
    Run quality gates on backtest results.

    Args:
        metrics: Dictionary of backtest metrics (total_trades, win_rate, etc.)
        trade_data: Optional detailed trade data for deeper checks
        strategy_name: Name of strategy for specific thresholds
        custom_thresholds: Optional custom thresholds

    Returns:
        QualityGateResult with all validation issues
    """
    thresholds = custom_thresholds or STRATEGY_THRESHOLDS.get(
        strategy_name,
        QualityThresholds(),
    )

    result = QualityGateResult(
        passed=True,
        warnings=0,
        errors=0,
        criticals=0,
    )

    # Extract metrics with safe defaults
    total_trades = int(metrics.get("total_trades", 0))
    _winning_trades = int(metrics.get("winning_trades", 0))
    _losing_trades = int(metrics.get("losing_trades", 0))
    win_rate = metrics.get("win_rate", 0.0)
    annual_return_pct = metrics.get("annual_return_pct", 0.0)
    max_drawdown_pct = metrics.get("max_drawdown_pct", 0.0)
    calmar_ratio = metrics.get("calmar_ratio", 0.0)
    sharpe_ratio = metrics.get("sharpe_ratio", None)
    profit_factor = metrics.get("profit_factor", None)
    avg_holding_days = metrics.get("avg_holding_days", None)
    years_count = metrics.get("years_count", 1)

    # 1. Trade count checks
    if total_trades == 0:
        result.add_issue(
            ValidationIssue(
                check_name="trade_count",
                severity=ValidationSeverity.CRITICAL,
                message="No trades generated - candidate generation may be broken",
                actual_value=0,
                expected_range=f">= {thresholds.min_trades}",
                impact="Cannot evaluate strategy with no trades",
            )
        )
        result.passed = False
    elif total_trades < thresholds.min_trades:
        result.add_issue(
            ValidationIssue(
                check_name="trade_count",
                severity=ValidationSeverity.WARNING,
                message=f"Low trade count: {total_trades} < {thresholds.min_trades}",
                actual_value=total_trades,
                expected_range=f">= {thresholds.min_trades}",
                impact="Low statistical significance",
            )
        )
        # Don't fail on low trade count, just warn

    trades_per_year = total_trades / max(years_count, 1)
    if trades_per_year < thresholds.min_trades_per_year:
        result.add_issue(
            ValidationIssue(
                check_name="trade_density",
                severity=ValidationSeverity.WARNING,
                message=f"Low trade density: {trades_per_year:.1f} trades/year",
                actual_value=trades_per_year,
                expected_range=f">= {thresholds.min_trades_per_year} trades/year",
                impact="Sparse signals may indicate overfitting",
            )
        )

    # 2. Drawdown sanity checks
    abs_max_dd = abs(max_drawdown_pct)
    if abs_max_dd > thresholds.max_max_drawdown_pct:
        result.add_issue(
            ValidationIssue(
                check_name="drawdown_sanity",
                severity=ValidationSeverity.CRITICAL,
                message=f"Extreme drawdown: {abs_max_dd:.1f}% suggests data or logic error",
                actual_value=abs_max_dd,
                expected_range=f"<= {thresholds.max_max_drawdown_pct}%",
                impact="Results not trustworthy",
            )
        )
        result.passed = False
    elif abs_max_dd > thresholds.suspicious_max_drawdown_pct:
        result.add_issue(
            ValidationIssue(
                check_name="drawdown_sanity",
                severity=ValidationSeverity.WARNING,
                message=f"High drawdown: {abs_max_dd:.1f}%",
                actual_value=abs_max_dd,
                expected_range=f"<= {thresholds.suspicious_max_drawdown_pct}%",
                impact="Consider risk reduction",
            )
        )

    # 3. Return sanity checks
    if annual_return_pct > thresholds.max_annual_return_pct:
        result.add_issue(
            ValidationIssue(
                check_name="return_sanity",
                severity=ValidationSeverity.ERROR,
                message=f"Unrealistic annual return: {annual_return_pct:.1f}%",
                actual_value=annual_return_pct,
                expected_range=f"<= {thresholds.max_annual_return_pct}%",
                impact="Likely calculation error or look-ahead bias",
            )
        )
        result.passed = False
    elif annual_return_pct < thresholds.min_annual_return_pct:
        result.add_issue(
            ValidationIssue(
                check_name="return_sanity",
                severity=ValidationSeverity.ERROR,
                message=f"Impossible return: {annual_return_pct:.1f}%",
                actual_value=annual_return_pct,
                expected_range=f">= {thresholds.min_annual_return_pct}%",
                impact="Calculation error - cannot lose >100%",
            )
        )
        result.passed = False

    # 4. Win rate sanity checks
    if thresholds.suspicious_win_rate[0] <= win_rate <= 1.0:
        if win_rate >= thresholds.suspicious_win_rate[0]:
            result.add_issue(
                ValidationIssue(
                    check_name="win_rate_sanity",
                    severity=ValidationSeverity.WARNING,
                    message=f"Suspiciously high win rate: {win_rate:.1%}",
                    actual_value=win_rate,
                    expected_range=f"< {thresholds.suspicious_win_rate[0]:.0%}",
                    impact="Possible look-ahead bias or overfitting",
                )
            )

    # 5. Calmar ratio sanity
    if calmar_ratio > thresholds.max_calmar:
        result.add_issue(
            ValidationIssue(
                check_name="calmar_sanity",
                severity=ValidationSeverity.WARNING,
                message=f"Unusually high Calmar: {calmar_ratio:.1f}",
                actual_value=calmar_ratio,
                expected_range=f"<= {thresholds.max_calmar}",
                impact="Verify results are not overfitted",
            )
        )

    # 6. Holding period checks
    if avg_holding_days and trade_data:
        if avg_holding_days > thresholds.avg_holding_days_max:
            result.add_issue(
                ValidationIssue(
                    check_name="holding_period",
                    severity=ValidationSeverity.INFO,
                    message=f"Long avg holding: {avg_holding_days:.1f} days",
                    actual_value=avg_holding_days,
                    expected_range=f"<= {thresholds.avg_holding_days_max} days",
                    impact="May indicate exit logic issue",
                )
            )

        max_holding = trade_data.get("max_holding_days", 0)
        if max_holding > thresholds.max_holding_days:
            result.add_issue(
                ValidationIssue(
                    check_name="max_holding_period",
                    severity=ValidationSeverity.WARNING,
                    message=f"Excessive max holding: {max_holding} days",
                    actual_value=max_holding,
                    expected_range=f"<= {thresholds.max_holding_days} days",
                    impact="Exit logic may not be triggering",
                )
            )

    # 7. Sharpe ratio sanity
    if sharpe_ratio is not None and sharpe_ratio < thresholds.min_sharpe_ratio:
        result.add_issue(
            ValidationIssue(
                check_name="sharpe_sanity",
                severity=ValidationSeverity.WARNING,
                message=f"Very low Sharpe: {sharpe_ratio:.2f}",
                actual_value=sharpe_ratio,
                expected_range=f">= {thresholds.min_sharpe_ratio}",
                impact="Poor risk-adjusted returns",
            )
        )

    # 8. Profit factor check
    if profit_factor is not None:
        if profit_factor < 1.0:
            result.add_issue(
                ValidationIssue(
                    check_name="profit_factor",
                    severity=ValidationSeverity.INFO,
                    message=f"Losing strategy: profit factor {profit_factor:.2f}",
                    actual_value=profit_factor,
                    expected_range=">= 1.0",
                    impact="Gross losses exceed gross wins",
                )
            )
        elif profit_factor > 5.0:
            result.add_issue(
                ValidationIssue(
                    check_name="profit_factor",
                    severity=ValidationSeverity.WARNING,
                    message=f"Unusually high profit factor: {profit_factor:.2f}",
                    actual_value=profit_factor,
                    expected_range="1.0 - 5.0",
                    impact="Verify not overfitted",
                )
            )

    # 9. Trade distribution checks (if trade_data available)
    if trade_data:
        _validate_trade_distribution(trade_data, result, thresholds)

    return result


def _validate_trade_distribution(
    trade_data: dict[str, Any],
    result: QualityGateResult,
    thresholds: QualityThresholds,
) -> None:
    """Validate distribution of individual trade P&L."""
    losses = trade_data.get("losses", [])
    gains = trade_data.get("gains", [])

    if losses and len(losses) > 0:
        _avg_loss = np.mean(losses)
        max_loss = np.min(losses) if losses else 0  # Most negative

        if max_loss < thresholds.max_single_trade_loss_pct:
            result.add_issue(
                ValidationIssue(
                    check_name="max_loss",
                    severity=ValidationSeverity.WARNING,
                    message=f"Extreme single loss: {max_loss:.1%}",
                    actual_value=max_loss * 100,
                    expected_range=f">= {thresholds.max_single_trade_loss_pct:.0%}",
                    impact="Stop loss may not be working correctly",
                )
            )

    if gains and len(gains) > 0:
        max_gain = np.max(gains) if gains else 0
        if max_gain > thresholds.max_single_trade_gain_pct:
            result.add_issue(
                ValidationIssue(
                    check_name="max_gain",
                    severity=ValidationSeverity.WARNING,
                    message=f"Extreme single gain: {max_gain:.1%}",
                    actual_value=max_gain * 100,
                    expected_range=f"<= {thresholds.max_single_trade_gain_pct:.0%}",
                    impact="May indicate data error (e.g., split adjustment issue)",
                )
            )


def validate_research_run(
    results: list[dict[str, Any]],
    protocol_name: str,
    min_folds: int = 3,
    min_trades_per_fold: int = 20,
    allow_fold_failure_rate: float = 0.2,
) -> QualityGateResult:
    """
    Validate a research protocol run (walk-forward, optimization, etc.).

    Args:
        results: List of fold results from the protocol
        protocol_name: Name of protocol (walk_forward, optimization, etc.)
        min_folds: Minimum number of folds required
        min_trades_per_fold: Minimum trades per fold
        allow_fold_failure_rate: Max allowed fraction of failed folds

    Returns:
        QualityGateResult with validation issues
    """
    result = QualityGateResult(
        passed=True,
        warnings=0,
        errors=0,
        criticals=0,
    )

    if len(results) < min_folds:
        result.add_issue(
            ValidationIssue(
                check_name="fold_count",
                severity=ValidationSeverity.ERROR,
                message=f"Insufficient folds: {len(results)} < {min_folds}",
                actual_value=len(results),
                expected_range=f">= {min_folds}",
                impact=f"{protocol_name} requires more historical data",
            )
        )
        result.passed = False

    failed_folds = sum(1 for r in results if not r.get("success", True))
    failure_rate = failed_folds / len(results) if results else 0

    if failure_rate > allow_fold_failure_rate:
        result.add_issue(
            ValidationIssue(
                check_name="fold_failure_rate",
                severity=ValidationSeverity.ERROR,
                message=f"High fold failure rate: {failure_rate:.1%}",
                actual_value=failure_rate * 100,
                expected_range=f"<= {allow_fold_failure_rate:.0%}",
                impact="Protocol results incomplete",
            )
        )
        result.passed = False

    # Check trade counts per fold
    low_trade_folds = []
    for i, fold in enumerate(results):
        trades = fold.get("total_trades", 0)
        if trades < min_trades_per_fold:
            low_trade_folds.append((i, trades))

    if low_trade_folds:
        fold_str = ", ".join(f"#{i}({t})" for i, t in low_trade_folds[:3])
        result.add_issue(
            ValidationIssue(
                check_name="fold_trade_count",
                severity=ValidationSeverity.WARNING,
                message=f"Folds with low trades: {fold_str}",
                actual_value=len(low_trade_folds),
                expected_range=f"Each fold >= {min_trades_per_fold} trades",
                impact="Low statistical significance in some folds",
            )
        )

    # Check for metric consistency across folds
    if len(results) >= 3:
        calmars = [r.get("calmar_ratio", 0) for r in results if r.get("success", True)]
        if calmars:
            cv = np.std(calmars) / (np.mean(calmars) + 1e-9)  # Coefficient of variation
            if cv > 1.0:
                result.add_issue(
                    ValidationIssue(
                        check_name="metric_consistency",
                        severity=ValidationSeverity.WARNING,
                        message=f"High fold variability: CV={cv:.2f}",
                        actual_value=cv,
                        expected_range="< 1.0",
                        impact="Strategy performance may not be stable",
                    )
                )

    return result


def validate_performance_regressions(
    baseline: dict[str, float],
    current: dict[str, float],
    thresholds: dict[str, float] | None = None,
) -> QualityGateResult:
    """
    Check for performance regressions vs baseline.

    Args:
        baseline: Baseline metrics
        current: Current metrics
        thresholds: Custom thresholds for regression detection

    Returns:
        QualityGateResult with regression issues
    """
    result = QualityGateResult(
        passed=True,
        warnings=0,
        errors=0,
        criticals=0,
    )

    # Default regression thresholds
    default_thresholds = {
        "calmar_ratio_pct": -10.0,  # 10% decline is warning
        "annual_return_pct_pct": -15.0,  # 15% decline is warning
        "sharpe_ratio_pct": -20.0,  # 20% decline is warning
        "max_drawdown_pct_pct": 10.0,  # 10% increase is warning (worse DD)
    }

    regs = thresholds or default_thresholds

    for metric, threshold in regs.items():
        base_val = baseline.get(metric.replace("_pct", ""))
        curr_val = current.get(metric.replace("_pct", ""))

        if base_val is None or curr_val is None:
            continue

        if base_val == 0:
            continue

        change_pct = ((curr_val - base_val) / abs(base_val)) * 100

        if isinstance(threshold, tuple):
            # Range: (min_decline_is_error, min_decline_is_warning)
            error_thresh, warn_thresh = threshold
            if change_pct < error_thresh:
                result.add_issue(
                    ValidationIssue(
                        check_name=f"regression_{metric}",
                        severity=ValidationSeverity.ERROR,
                        message=f"Significant regression in {metric}: {change_pct:+.1f}%",
                        actual_value=change_pct,
                        expected_range=f">= {error_thresh:.0f}%",
                        impact="Performance degradation detected",
                    )
                )
                result.passed = False
            elif change_pct < warn_thresh:
                result.add_issue(
                    ValidationIssue(
                        check_name=f"regression_{metric}",
                        severity=ValidationSeverity.WARNING,
                        message=f"Regression in {metric}: {change_pct:+.1f}%",
                        actual_value=change_pct,
                        expected_range=f">= {warn_thresh:.0f}%",
                        impact="Performance may have degraded",
                    )
                )
        else:
            # Single threshold
            if change_pct < threshold:
                severity = (
                    ValidationSeverity.ERROR
                    if change_pct < threshold * 1.5
                    else ValidationSeverity.WARNING
                )
                result.add_issue(
                    ValidationIssue(
                        check_name=f"regression_{metric}",
                        severity=severity,
                        message=f"Regression in {metric}: {change_pct:+.1f}%",
                        actual_value=change_pct,
                        expected_range=f">= {threshold:.0f}%",
                        impact="Performance degradation",
                    )
                )
                if severity == ValidationSeverity.ERROR:
                    result.passed = False

    return result


def validate_benchmark(
    metrics: BenchmarkMetrics,
    strategy_name: str = "indian_2lynch",
) -> QualityGateResult:
    """
    Validate performance benchmarks.

    Checks for:
    - Runtime within expected bounds
    - Memory usage within limits
    - Throughput reasonable
    """
    result = QualityGateResult(
        passed=True,
        warnings=0,
        errors=0,
        criticals=0,
    )

    # Runtime sanity
    if metrics.total_duration_seconds > 3600:  # > 1 hour
        result.add_issue(
            ValidationIssue(
                check_name="runtime",
                severity=ValidationSeverity.WARNING,
                message=f"Long runtime: {metrics.total_duration_seconds / 60:.1f} minutes",
                actual_value=metrics.total_duration_seconds,
                expected_range="<= 3600 seconds",
                impact="Consider optimization or sampling",
            )
        )

    # Memory sanity
    if metrics.peak_memory_mb > 4096:  # > 4GB
        result.add_issue(
            ValidationIssue(
                check_name="memory_usage",
                severity=ValidationSeverity.WARNING,
                message=f"High memory usage: {metrics.peak_memory_mb:.0f}MB",
                actual_value=metrics.peak_memory_mb,
                expected_range="<= 4096 MB",
                impact="May cause issues on resource-constrained systems",
            )
        )

    # Throughput sanity
    if metrics.signals_generated > 0:
        signals_per_sec = metrics.signals_generated / max(
            metrics.candidate_generation_seconds, 0.001
        )
        if signals_per_sec < 1.0:  # < 1 signal/second
            result.add_issue(
                ValidationIssue(
                    check_name="throughput",
                    severity=ValidationSeverity.INFO,
                    message=f"Low throughput: {signals_per_sec:.1f} signals/sec",
                    actual_value=signals_per_sec,
                    expected_range=">= 1.0 signals/sec",
                    impact="Candidate generation may be slow",
                )
            )

    # Zero output checks
    if metrics.signals_generated == 0:
        result.add_issue(
            ValidationIssue(
                check_name="zero_signals",
                severity=ValidationSeverity.CRITICAL,
                message="No signals generated",
                actual_value=0,
                expected_range="> 0",
                impact="Candidate generation or filtering broken",
            )
        )
        result.passed = False

    return result
