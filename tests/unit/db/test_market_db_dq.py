from __future__ import annotations

from types import MethodType

import duckdb

from nse_momentum_lab.db.market_db import MarketDataDB


def _make_db() -> MarketDataDB:
    db = MarketDataDB.__new__(MarketDataDB)
    db.con = duckdb.connect(":memory:")
    db._read_only = False
    db._data_source = "local"
    db._daily_glob = ""
    db._five_min_glob = ""
    db._has_5min = False
    db._has_daily = False
    db.lake = None
    db.get_dataset_snapshot = MethodType(lambda self: {"dataset_hash": "test-hash"}, db)
    db._get_materialization_state = MethodType(lambda self, table: None, db)
    db._upsert_materialization_state = MethodType(lambda self, **kwargs: None, db)
    db.register_dataset_snapshot = MethodType(lambda self, snapshot: None, db)
    db._ensure_dq_tables()
    return db


class TestUpsertDataQualityIssues:
    def test_inserts_new_rows(self):
        db = _make_db()
        count = db.upsert_data_quality_issues(
            symbols=["AAA", "BBB"], issue_code="TEST_ISSUE", details="test detail"
        )
        assert count == 2

        issues = db.query_active_dq_issues()
        assert len(issues) == 2
        assert set(issues["symbol"].to_list()) == {"AAA", "BBB"}
        assert all(code == "TEST_ISSUE" for code in issues["issue_code"].to_list())

    def test_updates_existing_rows(self):
        db = _make_db()
        db.upsert_data_quality_issues(
            symbols=["AAA"], issue_code="TEST_ISSUE", details="first", severity="WARNING"
        )
        db.upsert_data_quality_issues(
            symbols=["AAA"], issue_code="TEST_ISSUE", details="updated", severity="CRITICAL"
        )

        issues = db.query_active_dq_issues()
        assert len(issues) == 1
        row = issues.row(0, named=True)
        assert row["details"] == "updated"
        assert row["severity"] == "CRITICAL"

    def test_reactivates_deactivated_rows(self):
        db = _make_db()
        db.upsert_data_quality_issues(symbols=["AAA"], issue_code="TEST_ISSUE")
        db.deactivate_data_quality_issue("TEST_ISSUE")

        # Verify deactivated
        active = db.query_active_dq_issues()
        assert len(active) == 0

        # Reactivate
        db.upsert_data_quality_issues(symbols=["AAA"], issue_code="TEST_ISSUE")
        active = db.query_active_dq_issues()
        assert len(active) == 1
        assert active["symbol"][0] == "AAA"

    def test_empty_symbols_returns_zero(self):
        db = _make_db()
        count = db.upsert_data_quality_issues(symbols=[], issue_code="TEST")
        assert count == 0


class TestDeactivateDataQualityIssue:
    def test_deactivates_all_for_issue_code(self):
        db = _make_db()
        db.upsert_data_quality_issues(symbols=["AAA", "BBB"], issue_code="ISSUE1")
        db.upsert_data_quality_issues(symbols=["CCC"], issue_code="ISSUE2")

        db.deactivate_data_quality_issue("ISSUE1")

        active = db.query_active_dq_issues()
        assert len(active) == 1
        assert active["symbol"][0] == "CCC"

    def test_keep_symbols_exempts_specific_symbols(self):
        db = _make_db()
        db.upsert_data_quality_issues(symbols=["AAA", "BBB", "CCC"], issue_code="ISSUE1")

        db.deactivate_data_quality_issue("ISSUE1", keep_symbols=["BBB"])

        active = db.query_active_dq_issues()
        assert len(active) == 1
        assert active["symbol"][0] == "BBB"

    def test_deactivate_nonexistent_code_is_noop(self):
        db = _make_db()
        db.upsert_data_quality_issues(symbols=["AAA"], issue_code="REAL")
        db.deactivate_data_quality_issue("NONEXISTENT")

        active = db.query_active_dq_issues()
        assert len(active) == 1


class TestQueryActiveDqIssues:
    def test_filter_by_issue_code(self):
        db = _make_db()
        db.upsert_data_quality_issues(symbols=["AAA"], issue_code="ISSUE1")
        db.upsert_data_quality_issues(symbols=["BBB"], issue_code="ISSUE2")

        issues = db.query_active_dq_issues(issue_code="ISSUE1")
        assert len(issues) == 1
        assert issues["symbol"][0] == "AAA"

    def test_filter_by_severity(self):
        db = _make_db()
        db.upsert_data_quality_issues(
            symbols=["AAA"], issue_code="ISSUE1", severity="CRITICAL"
        )
        db.upsert_data_quality_issues(
            symbols=["BBB"], issue_code="ISSUE2", severity="WARNING"
        )

        issues = db.query_active_dq_issues(severity="CRITICAL")
        assert len(issues) == 1
        assert issues["symbol"][0] == "AAA"

    def test_returns_empty_when_no_matches(self):
        db = _make_db()
        issues = db.query_active_dq_issues(issue_code="NONEXISTENT")
        assert len(issues) == 0

    def test_excludes_inactive(self):
        db = _make_db()
        db.upsert_data_quality_issues(symbols=["AAA"], issue_code="ISSUE1")
        db.deactivate_data_quality_issue("ISSUE1")

        issues = db.query_active_dq_issues()
        assert len(issues) == 0


class TestDqTableInGetStatus:
    def test_data_quality_issues_in_status(self):
        db = _make_db()
        status = db.get_status()
        assert "data_quality_issues" in status["tables"]
