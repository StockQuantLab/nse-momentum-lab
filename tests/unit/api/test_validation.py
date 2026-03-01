"""Tests for api/validation.py"""

from datetime import date, timedelta

import pytest

from nse_momentum_lab.api.validation import (
    ValidationError,
    sanitize_string,
    validate_date_string,
    validate_entry_mode,
    validate_exit_reason,
    validate_hash,
    validate_json_dict,
    validate_pagination,
    validate_positive_int,
    validate_series,
    validate_status,
    validate_symbol,
    validate_symbols_csv,
)


class TestValidationError:
    def test_validation_error_attributes(self) -> None:
        error = ValidationError("test message", "test_field")
        assert error.message == "test message"
        assert error.field == "test_field"
        assert str(error) == "test message"

    def test_validation_error_no_field(self) -> None:
        error = ValidationError("test message")
        assert error.message == "test message"
        assert error.field is None


class TestValidateDateString:
    def test_valid_date(self) -> None:
        result = validate_date_string("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_none_returns_none(self) -> None:
        assert validate_date_string(None) is None

    def test_invalid_format(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_date_string("15-01-2024")
        assert "Invalid date format" in str(exc.value)

    def test_future_date_rejected(self) -> None:
        future = (date.today() + timedelta(days=10)).isoformat()
        with pytest.raises(ValidationError) as exc:
            validate_date_string(future)
        assert "cannot be in the future" in str(exc.value)

    def test_date_before_2000_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_date_string("1999-12-31")
        assert "cannot be before 2000-01-01" in str(exc.value)


class TestValidateSymbol:
    def test_valid_symbol(self) -> None:
        assert validate_symbol("RELIANCE") == "RELIANCE"
        assert validate_symbol("tcs") == "TCS"
        assert validate_symbol("INFY-TECH") == "INFY-TECH"
        assert validate_symbol("HDFC123") == "HDFC123"

    def test_none_returns_none(self) -> None:
        assert validate_symbol(None) is None

    def test_empty_symbol_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_symbol("")
        assert "empty" in str(exc.value).lower()

    def test_invalid_chars_removed(self) -> None:
        assert validate_symbol("ABC@#$") == "ABC"
        assert validate_symbol("RELIANCE.Ltd") == "RELIANCELTD"

    def test_symbol_too_long(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_symbol("A" * 21)
        assert "too long" in str(exc.value)

    def test_special_chars_only_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_symbol("@#$")
        assert "Invalid" in str(exc.value)


class TestValidateSymbolsCsv:
    def test_valid_csv(self) -> None:
        result = validate_symbols_csv("RELIANCE,TCS,INFY")
        assert result == ["RELIANCE", "TCS", "INFY"]

    def test_none_returns_none(self) -> None:
        assert validate_symbols_csv(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert validate_symbols_csv("   ") is None

    def test_duplicates_removed(self) -> None:
        result = validate_symbols_csv("RELIANCE,TCS,RELIANCE,INFY")
        assert result == ["RELIANCE", "TCS", "INFY"]

    def test_whitespace_trimmed(self) -> None:
        result = validate_symbols_csv("RELIANCE, TCS , INFY")
        assert result == ["RELIANCE", "TCS", "INFY"]

    def test_max_symbols_enforced(self) -> None:
        symbols = ",".join(f"SYM{i}" for i in range(55))
        result = validate_symbols_csv(symbols, max_symbols=50)
        assert len(result) == 50

    def test_invalid_symbol_raises_error(self) -> None:
        # Invalid symbols raise ValidationError, not skipped
        with pytest.raises(ValidationError):
            validate_symbols_csv("RELIANCE,@,TCS")

    def test_empty_list_returns_none(self) -> None:
        # All invalid symbols will raise error, not return None
        with pytest.raises(ValidationError):
            validate_symbols_csv("@,@")


class TestValidatePositiveInt:
    def test_valid_int(self) -> None:
        assert validate_positive_int(5) == 5
        assert validate_positive_int(100) == 100

    def test_none_returns_none(self) -> None:
        assert validate_positive_int(None) is None

    def test_string_converted(self) -> None:
        assert validate_positive_int("42") == 42

    def test_below_minimum_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_positive_int(0, min_val=1)
        assert ">= 1" in str(exc.value)

    def test_above_maximum_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_positive_int(10001, max_val=10000)
        assert "<= 10000" in str(exc.value)

    def test_invalid_type_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_positive_int("abc")
        assert "must be an integer" in str(exc.value)

    def test_custom_field_name(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_positive_int(-1, field_name="count")
        assert exc.value.field == "count"


class TestValidateHash:
    def test_valid_hash(self) -> None:
        result = validate_hash("ABC123DEF456")
        assert result == "abc123def456"

    def test_none_returns_none(self) -> None:
        assert validate_hash(None) is None

    def test_empty_rejected(self) -> None:
        with pytest.raises(ValidationError):
            validate_hash("")

    def test_invalid_chars_rejected(self) -> None:
        with pytest.raises(ValidationError):
            validate_hash("XYZ-123")

    def test_too_short_rejected(self) -> None:
        with pytest.raises(ValidationError):
            validate_hash("ABC123")

    def test_too_long_rejected(self) -> None:
        with pytest.raises(ValidationError):
            validate_hash("A" * 65)


class TestValidateSeries:
    def test_valid_series(self) -> None:
        assert validate_series("EQ") == "EQ"
        assert validate_series("eq") == "EQ"
        assert validate_series("BE") == "BE"

    def test_none_defaults_to_eq(self) -> None:
        assert validate_series(None) == "EQ"

    def test_invalid_series_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_series("XX")
        assert "Invalid series" in str(exc.value)
        assert exc.value.field == "series"


class TestValidateStatus:
    def test_valid_status(self) -> None:
        assert validate_status("ACTIVE") == "ACTIVE"
        assert validate_status("active") == "ACTIVE"

    def test_none_returns_none(self) -> None:
        assert validate_status(None) is None

    def test_custom_valid_statuses(self) -> None:
        assert validate_status("PENDING", valid_statuses={"PENDING", "DONE"}) == "PENDING"

    def test_invalid_status_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_status("UNKNOWN")
        assert "Invalid status" in str(exc.value)


class TestValidateEntryMode:
    def test_valid_modes(self) -> None:
        assert validate_entry_mode("open") == "open"
        assert validate_entry_mode("OPEN") == "open"
        assert validate_entry_mode("close") == "close"

    def test_none_defaults_to_close(self) -> None:
        assert validate_entry_mode(None) == "close"

    def test_invalid_mode_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_entry_mode("limit")
        assert "Invalid entry_mode" in str(exc.value)


class TestValidateExitReason:
    def test_valid_reasons(self) -> None:
        assert validate_exit_reason("STOP_INITIAL") == "STOP_INITIAL"
        assert validate_exit_reason("stop_initial") == "STOP_INITIAL"
        assert validate_exit_reason("TIME_STOP") == "TIME_STOP"

    def test_none_returns_none(self) -> None:
        assert validate_exit_reason(None) is None

    def test_invalid_reason_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_exit_reason("WEAK_FOLLOW_THROUGH")
        assert "Invalid exit_reason" in str(exc.value)


class TestSanitizeString:
    def test_valid_string(self) -> None:
        assert sanitize_string("hello world") == "hello world"

    def test_none_returns_none(self) -> None:
        assert sanitize_string(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert sanitize_string("   ") is None

    def test_whitespace_trimmed(self) -> None:
        assert sanitize_string("  hello  ") == "hello"

    def test_max_length_enforced(self) -> None:
        assert len(sanitize_string("a" * 1000, max_length=100)) == 100

    def test_control_chars_removed(self) -> None:
        assert sanitize_string("hello\x00world") == "helloworld"
        assert sanitize_string("test\x1fstring") == "teststring"


class TestValidateJsonDict:
    def test_valid_dict(self) -> None:
        result = validate_json_dict({"key": "value"})
        assert result == {"key": "value"}

    def test_none_returns_none(self) -> None:
        assert validate_json_dict(None) is None

    def test_non_dict_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_json_dict(["not", "a", "dict"])
        assert "JSON object" in str(exc.value)

    def test_nested_dict_allowed(self) -> None:
        result = validate_json_dict({"a": {"b": {"c": "d"}}, "max_depth": 3})
        assert result == {"a": {"b": {"c": "d"}}, "max_depth": 3}

    def test_depth_limit_enforced(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_json_dict({"a": {"b": {"c": {"d": "e"}}}}, max_depth=3)
        assert "depth exceeds maximum" in str(exc.value)

    def test_non_string_key_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc:
            validate_json_dict({123: "value"})
        assert "keys must be strings" in str(exc.value)

    def test_list_values_allowed(self) -> None:
        result = validate_json_dict({"items": [1, 2, 3]})
        assert result == {"items": [1, 2, 3]}


class TestValidatePagination:
    def test_default_values(self) -> None:
        limit, offset = validate_pagination(None, None)
        assert limit == 20
        assert offset == 0

    def test_custom_values(self) -> None:
        limit, offset = validate_pagination(50, 100)
        assert limit == 50
        assert offset == 100

    def test_limit_max_enforced(self) -> None:
        # validate_positive_int raises error for values > max_val
        with pytest.raises(ValidationError):
            validate_pagination(2000, 0)

    def test_negative_offset_rejected(self) -> None:
        with pytest.raises(ValidationError):
            validate_pagination(10, -5)
