"""Tests for data quality validation."""

from datetime import date

from nse_momentum_lab.services.ingest.data_quality import (
    DataQualityConfig,
    DataQualityValidator,
    QualityIssueType,
    validate_ingestion_batch,
)


class TestDataQualityConfig:
    def test_default_config(self) -> None:
        config = DataQualityConfig()
        assert config.max_price_change_pct == 0.30
        assert config.max_volume_spike_mult == 20.0
        assert config.min_price == 0.01
        assert config.max_price == 100000.0
        assert config.allowed_gap_days == 5

    def test_custom_config(self) -> None:
        config = DataQualityConfig(
            max_price_change_pct=0.50,
            min_price=1.0,
        )
        assert config.max_price_change_pct == 0.50
        assert config.min_price == 1.0


class TestDataQualityValidator:
    def setup_method(self) -> None:
        self.validator = DataQualityValidator()

    def test_validate_empty_data(self) -> None:
        report = self.validator.validate_symbol_data("TEST", [])
        assert report.total_rows == 0
        assert report.passed is False
        assert report.quality_score == 0.0

    def test_validate_valid_row(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 100.0,
                "high": 105.0,
                "low": 98.0,
                "close": 102.0,
                "volume": 1000000,
            }
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert report.total_rows == 1
        assert report.passed is True
        assert report.quality_score == 1.0

    def test_validate_ohlc_invalid_high_low(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 100.0,
                "high": 98.0,
                "low": 105.0,
                "close": 102.0,
                "volume": 1000000,
            }
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert report.passed is False
        assert any(i.issue_type == QualityIssueType.OHLC_INVALID for i in report.issues)

    def test_validate_ohlc_invalid_high_below_open(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 110.0,
                "high": 105.0,
                "low": 98.0,
                "close": 102.0,
                "volume": 1000000,
            }
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert report.passed is False
        assert any(i.issue_type == QualityIssueType.OHLC_INVALID for i in report.issues)

    def test_validate_zero_price(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 0.001,
                "high": 105.0,
                "low": 98.0,
                "close": 102.0,
                "volume": 1000000,
            }
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert any(i.issue_type == QualityIssueType.ZERO_PRICE for i in report.issues)

    def test_validate_negative_volume(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 100.0,
                "high": 105.0,
                "low": 98.0,
                "close": 102.0,
                "volume": -1000,
            }
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert report.passed is False
        assert any(i.issue_type == QualityIssueType.NEGATIVE_VALUE for i in report.issues)

    def test_validate_zero_volume_info(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 100.0,
                "high": 105.0,
                "low": 98.0,
                "close": 102.0,
                "volume": 0,
            }
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert report.passed is True
        assert any(i.issue_type == QualityIssueType.VOLUME_ANOMALY for i in report.issues)

    def test_validate_date_gap(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 100.0,
                "high": 105.0,
                "low": 98.0,
                "close": 102.0,
                "volume": 1000000,
            },
            {
                "trading_date": date(2024, 1, 15),
                "open": 102.0,
                "high": 107.0,
                "low": 100.0,
                "close": 105.0,
                "volume": 1200000,
            },
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert any(i.issue_type == QualityIssueType.DATE_GAP for i in report.issues)

    def test_validate_duplicate_date(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 100.0,
                "high": 105.0,
                "low": 98.0,
                "close": 102.0,
                "volume": 1000000,
            },
            {
                "trading_date": date(2024, 1, 1),
                "open": 101.0,
                "high": 106.0,
                "low": 99.0,
                "close": 103.0,
                "volume": 1100000,
            },
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert any(i.issue_type == QualityIssueType.DUPLICATE_DATE for i in report.issues)

    def test_validate_missing_ohlc(self) -> None:
        rows = [
            {
                "trading_date": date(2024, 1, 1),
                "open": 100.0,
                "high": None,
                "low": 98.0,
                "close": 102.0,
                "volume": 1000000,
            }
        ]
        report = self.validator.validate_symbol_data("TEST", rows)
        assert report.passed is False
        assert any(i.issue_type == QualityIssueType.MISSING_DATA for i in report.issues)

    def test_validate_extreme_move(self) -> None:
        rows = []
        base_price = 100.0
        for i in range(10):
            if i < 5:
                price = base_price
            else:
                price = base_price * (1.6 if i == 5 else 1.0)
            rows.append(
                {
                    "trading_date": date(2024, 1, 1 + i),
                    "open": price,
                    "high": price * 1.02,
                    "low": price * 0.98,
                    "close": price,
                    "volume": 1000000,
                }
            )

        report = self.validator.validate_symbol_data("TEST", rows)
        extreme_issues = [i for i in report.issues if i.issue_type == QualityIssueType.EXTREME_MOVE]
        assert len(extreme_issues) > 0

    def test_generate_summary_report(self) -> None:
        reports = [
            self.validator.validate_symbol_data(
                "PASS1",
                [
                    {
                        "trading_date": date(2024, 1, 1),
                        "open": 100.0,
                        "high": 105.0,
                        "low": 98.0,
                        "close": 102.0,
                        "volume": 1000000,
                    }
                ],
            ),
            self.validator.validate_symbol_data(
                "FAIL1",
                [
                    {
                        "trading_date": date(2024, 1, 1),
                        "open": 100.0,
                        "high": 98.0,
                        "low": 105.0,
                        "close": 102.0,
                        "volume": 1000000,
                    }
                ],
            ),
        ]
        summary = self.validator.generate_summary_report(reports)
        assert summary["total_symbols"] == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1
        assert summary["pass_rate"] == 0.5


class TestValidateIngestionBatch:
    def test_batch_validation(self) -> None:
        data = {
            "PASS1": [
                {
                    "trading_date": date(2024, 1, 1),
                    "open": 100.0,
                    "high": 105.0,
                    "low": 98.0,
                    "close": 102.0,
                    "volume": 1000000,
                }
            ],
            "FAIL1": [
                {
                    "trading_date": date(2024, 1, 1),
                    "open": 100.0,
                    "high": 98.0,
                    "low": 105.0,
                    "close": 102.0,
                    "volume": 1000000,
                }
            ],
        }

        reports, summary = validate_ingestion_batch(data)
        assert len(reports) == 2
        assert summary["total_symbols"] == 2
        assert summary["passed"] == 1
        assert summary["failed"] == 1
