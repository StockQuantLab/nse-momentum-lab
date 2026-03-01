"""Input validation utilities for API endpoints."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any


class ValidationError(Exception):
    def __init__(self, message: str, field: str | None = None) -> None:
        self.message = message
        self.field = field
        super().__init__(message)


def validate_date_string(date_str: str | None, field_name: str = "date") -> date | None:
    if date_str is None:
        return None
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
        today = date.today()
        if parsed > today:
            raise ValidationError(f"{field_name} cannot be in the future", field_name)
        if parsed < date(2000, 1, 1):
            raise ValidationError(f"{field_name} cannot be before 2000-01-01", field_name)
        return parsed
    except ValueError as e:
        raise ValidationError(f"Invalid {field_name} format. Use YYYY-MM-DD", field_name) from e


def validate_symbol(symbol: str | None) -> str | None:
    if symbol is None:
        return None
    if not symbol:
        raise ValidationError("Symbol cannot be empty", "symbol")
    sanitized = re.sub(r"[^A-Z0-9\-]", "", symbol.upper())
    if not sanitized:
        raise ValidationError("Invalid symbol format", "symbol")
    if len(sanitized) > 20:
        raise ValidationError("Symbol too long (max 20 chars)", "symbol")
    return sanitized


def validate_symbols_csv(symbols_csv: str | None, max_symbols: int = 50) -> list[str] | None:
    if symbols_csv is None:
        return None
    if not symbols_csv.strip():
        return None
    parts = symbols_csv.split(",")
    symbols = []
    seen = set()
    for part in parts[:max_symbols]:
        symbol = validate_symbol(part.strip())
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return symbols if symbols else None


def validate_positive_int(
    value: int | None,
    field_name: str = "value",
    min_val: int = 1,
    max_val: int = 10000,
) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int):
        try:
            value = int(value)
        except (ValueError, TypeError) as e:
            raise ValidationError(f"{field_name} must be an integer", field_name) from e
    if value < min_val:
        raise ValidationError(f"{field_name} must be >= {min_val}", field_name)
    if value > max_val:
        raise ValidationError(f"{field_name} must be <= {max_val}", field_name)
    return value


def validate_hash(hash_str: str | None, field_name: str = "hash") -> str | None:
    if hash_str is None:
        return None
    if not hash_str:
        raise ValidationError(f"{field_name} cannot be empty", field_name)
    if not re.match(r"^[a-fA-F0-9]{8,64}$", hash_str):
        raise ValidationError(f"Invalid {field_name} format", field_name)
    return hash_str.lower()


def validate_series(series: str | None) -> str:
    valid_series = {
        "EQ",
        "BE",
        "BL",
        "BT",
        "GS",
        "N1",
        "N2",
        "N3",
        "N4",
        "N5",
        "N6",
        "N7",
        "N8",
        "N9",
        "SM",
        "ST",
    }
    if series is None:
        return "EQ"
    series = series.upper()
    if series not in valid_series:
        raise ValidationError(f"Invalid series. Valid: {', '.join(sorted(valid_series))}", "series")
    return series


def validate_status(status: str | None, valid_statuses: set[str] | None = None) -> str | None:
    if status is None:
        return None
    if valid_statuses is None:
        valid_statuses = {"ACTIVE", "INACTIVE", "SUSPENDED", "DELISTED"}
    status = status.upper()
    if status not in valid_statuses:
        raise ValidationError(
            f"Invalid status. Valid: {', '.join(sorted(valid_statuses))}", "status"
        )
    return status


def validate_entry_mode(mode: str | None) -> str:
    valid_modes = {"open", "close"}
    if mode is None:
        return "close"
    mode = mode.lower()
    if mode not in valid_modes:
        raise ValidationError(f"Invalid entry_mode. Valid: {', '.join(valid_modes)}", "entry_mode")
    return mode


def validate_exit_reason(reason: str | None) -> str | None:
    valid_reasons = {
        "STOP_INITIAL",
        "STOP_BREAKEVEN",
        "STOP_TRAIL",
        "STOP_POST_DAY3",
        "TIME_STOP",
        "EXIT_EOD",
        "GAP_THROUGH_STOP",
        "ABNORMAL_PROFIT",
        "ABNORMAL_GAP_EXIT",
        "DELISTING",
        "SUSPENSION",
    }
    if reason is None:
        return None
    reason = reason.upper()
    if reason not in valid_reasons:
        raise ValidationError(
            f"Invalid exit_reason. Valid: {', '.join(valid_reasons)}", "exit_reason"
        )
    return reason


def sanitize_string(value: str | None, max_length: int = 500) -> str | None:
    if value is None:
        return None
    if not value.strip():
        return None
    sanitized = value.strip()[:max_length]
    sanitized = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", sanitized)
    return sanitized


def validate_json_dict(value: dict[str, Any] | None, max_depth: int = 3) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValidationError("Expected a JSON object", "json")

    def _check_depth(obj: Any, current_depth: int) -> None:
        if current_depth > max_depth:
            raise ValidationError(f"JSON depth exceeds maximum ({max_depth})", "json")
        if isinstance(obj, dict):
            for k, v in obj.items():
                if not isinstance(k, str):
                    raise ValidationError("JSON keys must be strings", "json")
                _check_depth(v, current_depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                _check_depth(item, current_depth + 1)

    _check_depth(value, 0)
    return value


def validate_pagination(limit: int | None, offset: int | None) -> tuple[int, int]:
    limit = validate_positive_int(limit, "limit", min_val=1, max_val=1000) or 20
    offset = validate_positive_int(offset, "offset", min_val=0, max_val=100000) or 0
    return limit, offset
