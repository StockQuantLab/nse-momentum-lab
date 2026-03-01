from datetime import date

from nse_momentum_lab.services.ingest.quality import IngestQualityChecks, QualityIssue


class TestQualityIssue:
    def test_quality_issue_creation(self) -> None:
        issue = QualityIssue(
            symbol="TEST",
            issue_type="INVALID_PRICE",
            details="open=0.001 <= 0.01",
            severity="ERROR",
        )
        assert issue.symbol == "TEST"
        assert issue.issue_type == "INVALID_PRICE"
        assert issue.severity == "ERROR"


class TestIngestQualityChecks:
    def setup_method(self) -> None:
        self.checks = IngestQualityChecks()

    def test_check_row_valid(self) -> None:
        row = {
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 102.0,
            "volume": 1000000,
        }
        issues = self.checks.check_row("TEST", date(2024, 1, 1), row)
        assert len(issues) == 0

    def test_check_row_invalid_open(self) -> None:
        row = {
            "open": 0.001,
            "high": 105.0,
            "low": 0.001,
            "close": 102.0,
            "volume": 1000000,
        }
        issues = self.checks.check_row("TEST", date(2024, 1, 1), row)
        assert any(i.issue_type == "INVALID_PRICE" for i in issues)

    def test_check_row_invalid_high(self) -> None:
        row = {
            "open": 100.0,
            "high": 0.001,
            "low": 0.001,
            "close": 102.0,
            "volume": 1000000,
        }
        issues = self.checks.check_row("TEST", date(2024, 1, 1), row)
        assert any(i.issue_type == "INVALID_PRICE" for i in issues)

    def test_check_row_invalid_low(self) -> None:
        row = {
            "open": 100.0,
            "high": 105.0,
            "low": 0.001,
            "close": 102.0,
            "volume": 1000000,
        }
        issues = self.checks.check_row("TEST", date(2024, 1, 1), row)
        assert len(issues) == 1

    def test_check_row_high_less_than_low(self) -> None:
        row = {
            "open": 100.0,
            "high": 99.0,
            "low": 105.0,
            "close": 102.0,
            "volume": 1000000,
        }
        issues = self.checks.check_row("TEST", date(2024, 1, 1), row)
        assert len(issues) == 1
        assert "OHLC constraint" in issues[0].details

    def test_check_row_close_outside_range(self) -> None:
        row = {
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 110.0,
            "volume": 1000000,
        }
        issues = self.checks.check_row("TEST", date(2024, 1, 1), row)
        assert len(issues) == 1
        assert "OHLC constraint" in issues[0].details

    def test_check_row_extreme_volume(self) -> None:
        issues = self.checks.check_extreme_moves("TEST", 100.0, 102.0, 100000000)
        assert len(issues) == 0

    def test_check_row_extreme_move(self) -> None:
        issues = self.checks.check_extreme_moves("TEST", 100.0, 180.0, 1000000)
        assert len(issues) == 1
        assert issues[0].issue_type == "EXTREME_MOVE"

    def test_check_row_missing_prices(self) -> None:
        row = {
            "volume": 1000000,
        }
        issues = self.checks.check_row("TEST", date(2024, 1, 1), row)
        assert len(issues) == 0
