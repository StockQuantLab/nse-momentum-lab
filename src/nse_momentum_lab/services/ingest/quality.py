"""Backward-compatible quality checks used by legacy ingest tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class QualityIssue:
    symbol: str
    issue_type: str
    details: str
    severity: str = "ERROR"
    trading_date: date | None = None


class IngestQualityChecks:
    """Lightweight OHLCV quality checks kept for compatibility."""

    def __init__(self, min_price: float = 0.01, extreme_move_threshold: float = 0.50) -> None:
        self.min_price = min_price
        self.extreme_move_threshold = extreme_move_threshold

    def check_row(self, symbol: str, trading_date: date, row: dict) -> list[QualityIssue]:
        issues: list[QualityIssue] = []

        for field in ("open", "high", "low", "close"):
            price = row.get(field)
            if price is not None and price <= self.min_price:
                issues.append(
                    QualityIssue(
                        symbol=symbol,
                        trading_date=trading_date,
                        issue_type="INVALID_PRICE",
                        details=f"{field}={price} <= {self.min_price}",
                        severity="ERROR",
                    )
                )
                break

        open_price = row.get("open")
        high_price = row.get("high")
        low_price = row.get("low")
        close_price = row.get("close")

        if all(
            isinstance(v, int | float) for v in (open_price, high_price, low_price, close_price)
        ):
            open_px = float(open_price)
            high_px = float(high_price)
            low_px = float(low_price)
            close_px = float(close_price)
            if (
                high_px < low_px
                or high_px < max(open_px, close_px)
                or low_px > min(open_px, close_px)
            ):
                issues.append(
                    QualityIssue(
                        symbol=symbol,
                        trading_date=trading_date,
                        issue_type="INVALID_OHLC",
                        details="OHLC constraint violation",
                        severity="ERROR",
                    )
                )

        return issues

    def check_extreme_moves(
        self,
        symbol: str,
        prev_close: float,
        close: float,
        volume: int | float,
    ) -> list[QualityIssue]:
        del volume  # volume-based checks are out of scope in the compatibility shim.
        if prev_close <= 0:
            return []

        move = (close - prev_close) / prev_close
        if abs(move) <= self.extreme_move_threshold:
            return []

        return [
            QualityIssue(
                symbol=symbol,
                trading_date=None,
                issue_type="EXTREME_MOVE",
                details=f"Move {move:.2%} exceeds threshold {self.extreme_move_threshold:.0%}",
                severity="WARNING",
            )
        ]
