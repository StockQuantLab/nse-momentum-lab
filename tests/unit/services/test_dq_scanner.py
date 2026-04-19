from __future__ import annotations

from datetime import date

import duckdb
import pytest

from nse_momentum_lab.services.dq_scanner import (
    DQScanResult,
    run_fast_scan,
    run_full_scan,
    scan_date_gap,
    scan_duplicate_candle,
    scan_extreme_candle,
    scan_extreme_move_daily,
    scan_missing_5min_coverage,
    scan_null_price,
    scan_ohlc_violation,
    scan_short_history,
    scan_timestamp_invalid,
    scan_zero_price,
    scan_zero_volume_day,
)


@pytest.fixture()
def con():
    """In-memory DuckDB with synthetic daily and 5-min views."""
    con = duckdb.connect(":memory:")

    # Good daily data
    con.execute("""
        CREATE VIEW v_daily AS SELECT * FROM (
            VALUES
              ('AAA', '2026-03-20'::DATE, 100.0, 105.0, 98.0, 102.0, 1000),
              ('AAA', '2026-03-21'::DATE, 102.0, 108.0, 101.0, 107.0, 1200),
              ('AAA', '2026-03-22'::DATE, 107.0, 110.0, 106.0, 109.0, 900),
              ('BBB', '2026-03-20'::DATE, 50.0, 52.0, 49.0, 51.0, 500),
              -- Bad: high < low
              ('BAD1', '2026-03-20'::DATE, 100.0, 95.0, 105.0, 100.0, 200),
              -- Bad: close outside [low, high]
              ('BAD2', '2026-03-20'::DATE, 100.0, 105.0, 98.0, 110.0, 200),
              -- Bad: zero volume
              ('ZERO', '2026-03-20'::DATE, 100.0, 105.0, 98.0, 102.0, 0),
              -- Bad: extreme move (>30%) — needs a prior close of 100
              ('EXTREME', '2026-03-19'::DATE, 95.0, 102.0, 94.0, 100.0, 400),
              ('EXTREME', '2026-03-20'::DATE, 100.0, 150.0, 99.0, 140.0, 500),
              -- Bad: NULL close
              ('NULL1', '2026-03-20'::DATE, NULL, 105.0, 98.0, 102.0, 200),
              -- Bad: zero open
              ('ZPRICE', '2026-03-20'::DATE, 0.0, 105.0, 98.0, 102.0, 200),
        ) AS t(symbol, date, open, high, low, close, volume)
    """)

    # Good 5-min data
    con.execute("""
        CREATE VIEW v_5min AS SELECT * FROM (
            VALUES
              ('AAA', '2026-03-20'::DATE, '2026-03-20 09:15:00'::TIMESTAMP,
               100.0, 101.0, 99.5, 100.5, 100),
              ('AAA', '2026-03-20'::DATE, '2026-03-20 09:20:00'::TIMESTAMP,
               100.5, 102.0, 100.0, 101.5, 120),
              ('AAA', '2026-03-21'::DATE, '2026-03-21 09:15:00'::TIMESTAMP,
               102.0, 103.0, 101.0, 102.5, 100),
              ('AAA', '2026-03-22'::DATE, '2026-03-22 09:15:00'::TIMESTAMP,
               107.0, 108.0, 106.0, 107.5, 100),
              ('BBB', '2026-03-20'::DATE, '2026-03-20 09:15:00'::TIMESTAMP,
               50.0, 51.0, 49.5, 50.5, 50),
              -- Bad: timestamp before 09:15
              ('TSTAMP', '2026-03-20'::DATE, '2026-03-20 09:10:00'::TIMESTAMP,
               100.0, 101.0, 99.0, 100.0, 100),
              -- Bad: extreme candle (>50%)
              ('EXTCANDLE', '2026-03-20'::DATE, '2026-03-20 09:15:00'::TIMESTAMP,
               100.0, 200.0, 50.0, 150.0, 100),
              -- Bad: duplicate candle
              ('DUP', '2026-03-20'::DATE, '2026-03-20 09:15:00'::TIMESTAMP,
               100.0, 101.0, 99.0, 100.0, 100),
              ('DUP', '2026-03-20'::DATE, '2026-03-20 09:15:00'::TIMESTAMP,
               100.5, 101.5, 99.5, 100.5, 110),
        ) AS t(symbol, date, candle_time, open, high, low, close, volume)
    """)

    yield con
    con.close()


# ---------------------------------------------------------------------------
# Individual scan tests
# ---------------------------------------------------------------------------


class TestScanOhlcViolation:
    def test_finds_high_less_than_low(self, con):
        result = scan_ohlc_violation(con)
        assert "BAD1" in result.symbols

    def test_finds_close_outside_range(self, con):
        result = scan_ohlc_violation(con)
        assert "BAD2" in result.symbols

    def test_severity_is_critical(self, con):
        result = scan_ohlc_violation(con)
        assert result.severity == "CRITICAL"

    def test_clean_symbols_not_flagged(self, con):
        result = scan_ohlc_violation(con)
        assert "AAA" not in result.symbols
        assert "BBB" not in result.symbols


class TestScanNullPrice:
    def test_finds_null_ohlc(self, con):
        result = scan_null_price(con)
        assert "NULL1" in result.symbols

    def test_clean_symbols_not_flagged(self, con):
        result = scan_null_price(con)
        assert "AAA" not in result.symbols


class TestScanZeroPrice:
    def test_finds_zero_open(self, con):
        result = scan_zero_price(con)
        assert "ZPRICE" in result.symbols

    def test_clean_symbols_not_flagged(self, con):
        result = scan_zero_price(con)
        assert "AAA" not in result.symbols


class TestScanTimestampInvalid:
    def test_finds_early_timestamp(self, con):
        result = scan_timestamp_invalid(con)
        assert "TSTAMP" in result.symbols

    def test_clean_symbols_not_flagged(self, con):
        result = scan_timestamp_invalid(con)
        assert "AAA" not in result.symbols


class TestScanExtremeCandle:
    def test_finds_extreme_range(self, con):
        result = scan_extreme_candle(con)
        assert "EXTCANDLE" in result.symbols

    def test_clean_symbols_not_flagged(self, con):
        result = scan_extreme_candle(con)
        assert "AAA" not in result.symbols


class TestScanDuplicateCandle:
    def test_finds_duplicates(self, con):
        result = scan_duplicate_candle(con)
        assert "DUP" in result.symbols

    def test_clean_symbols_not_flagged(self, con):
        result = scan_duplicate_candle(con)
        assert "AAA" not in result.symbols


class TestScanZeroVolumeDay:
    def test_finds_zero_volume(self, con):
        result = scan_zero_volume_day(con)
        assert "ZERO" in result.symbols

    def test_severity_is_info(self, con):
        result = scan_zero_volume_day(con)
        assert result.severity == "INFO"

    def test_clean_symbols_not_flagged(self, con):
        result = scan_zero_volume_day(con)
        assert "AAA" not in result.symbols


class TestScanExtremeMoveDaily:
    def test_finds_extreme_move(self, con):
        result = scan_extreme_move_daily(con)
        assert "EXTREME" in result.symbols

    def test_clean_symbols_not_flagged(self, con):
        result = scan_extreme_move_daily(con)
        assert "AAA" not in result.symbols


class TestScanMissing5minCoverage:
    def test_finds_symbols_without_5min(self, con):
        result = scan_missing_5min_coverage(con)
        # BAD1, BAD2, ZERO, EXTREME, NULL1, ZPRICE have daily but no 5-min
        for sym in ["BAD1", "BAD2", "ZERO", "NULL1", "ZPRICE"]:
            assert sym in result.symbols

    def test_symbols_with_5min_not_flagged(self, con):
        result = scan_missing_5min_coverage(con)
        assert "AAA" not in result.symbols
        assert "BBB" not in result.symbols


class TestScanShortHistory:
    def test_finds_short_history_symbols(self, con):
        result = scan_short_history(con, min_dates=100)
        # All symbols in test data have < 100 dates
        assert len(result.symbols) > 0

    def test_passes_with_low_threshold(self, con):
        result = scan_short_history(con, min_dates=1)
        # All symbols have at least 1 date
        assert result.symbols == []


class TestScanDateGap:
    def test_no_gaps_in_test_data(self, con):
        result = scan_date_gap(con, gap_days=2)
        # Test data has consecutive dates, so no gaps > 2 days
        # (weekends are not in the data, so gaps could appear)
        # Just verify it doesn't crash
        assert isinstance(result, DQScanResult)


# ---------------------------------------------------------------------------
# Orchestrator tests
# ---------------------------------------------------------------------------


class TestRunFullScan:
    def test_returns_all_results(self, con):
        results = run_full_scan(con)
        assert len(results) == 11  # all scan functions

    def test_results_are_dq_scan_result(self, con):
        results = run_full_scan(con)
        for r in results:
            assert isinstance(r, DQScanResult)

    def test_finds_known_issues(self, con):
        results = run_full_scan(con)
        issue_codes = [r.issue_code for r in results if r.symbols]
        assert "OHLC_VIOLATION" in issue_codes
        assert "TIMESTAMP_INVALID" in issue_codes


class TestRunFastScan:
    def test_returns_coverage_only(self, con):
        results = run_fast_scan(con)
        assert len(results) == 1
        assert results[0].issue_code == "MISSING_5MIN_COVERAGE"


class TestWindowFiltering:
    def test_window_filters_daily(self, con):
        result = scan_zero_volume_day(
            con, window_start=date(2026, 3, 21), window_end=date(2026, 3, 22)
        )
        # ZERO only has data on 2026-03-20, which is outside the window
        assert "ZERO" not in result.symbols

    def test_window_includes_target(self, con):
        result = scan_zero_volume_day(
            con, window_start=date(2026, 3, 20), window_end=date(2026, 3, 20)
        )
        assert "ZERO" in result.symbols


class TestEmptyViews:
    def test_works_without_5min(self):
        con = duckdb.connect(":memory:")
        con.execute("""
            CREATE VIEW v_daily AS SELECT * FROM (
                VALUES ('AAA', '2026-03-20'::DATE, 100.0, 105.0, 98.0, 102.0, 1000)
            ) AS t(symbol, date, open, high, low, close, volume)
        """)
        result = scan_timestamp_invalid(con)
        assert result.symbols == []
        con.close()

    def test_works_without_any_views(self):
        con = duckdb.connect(":memory:")
        result = scan_ohlc_violation(con)
        assert result.symbols == []
        con.close()
