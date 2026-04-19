"""Unit tests for paper feed audit — record, query, purge, and compare."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone

import pytest

_IST = timezone(timedelta(hours=5, minutes=30))


# ---------------------------------------------------------------------------
# Minimal ClosedCandle stub (avoids import of Kite auth / settings)
# ---------------------------------------------------------------------------


@dataclass
class _FakeCandle:
    symbol: str
    bar_start: float
    bar_end: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    first_snapshot_ts: float = 0.0
    last_snapshot_ts: float = 0.0


def _ist_epoch(hour: int, minute: int, day: int = 18) -> float:
    """Return epoch seconds for an IST wall-clock time on 2026-04-<day>."""
    dt = datetime(2026, 4, day, hour, minute, 0, tzinfo=_IST)
    return dt.timestamp()


def _make_candle(
    symbol: str = "RELIANCE",
    bar_end_ist: tuple[int, int] = (9, 20),
    *,
    open: float = 100.0,
    high: float = 105.0,
    low: float = 98.0,
    close: float = 103.0,
    volume: float = 10000.0,
) -> _FakeCandle:
    bar_end = _ist_epoch(*bar_end_ist)
    bar_start = bar_end - 300
    return _FakeCandle(
        symbol=symbol,
        bar_start=bar_start,
        bar_end=bar_end,
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
        first_snapshot_ts=bar_start + 1,
        last_snapshot_ts=bar_end - 1,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def paper_db():
    from nse_momentum_lab.services.paper.db.paper_db import PaperDB

    db = PaperDB(":memory:")
    yield db
    db.close()


@pytest.fixture()
def session_id(paper_db):
    s = paper_db.create_session(
        strategy_name="thresholdbreakout",
        mode="replay",
        trade_date="2026-04-18",
        strategy_params={},
    )
    return s["session_id"]


# ---------------------------------------------------------------------------
# PaperDB feed audit CRUD
# ---------------------------------------------------------------------------


class TestPaperDBFeedAudit:
    def _row(self, session_id: str, symbol: str = "RELIANCE", hhmm: str = "09:20") -> dict:
        h, m = map(int, hhmm.split(":"))
        bar_end = datetime.fromtimestamp(_ist_epoch(h, m), tz=UTC)
        bar_start = bar_end - timedelta(seconds=300)
        return {
            "session_id": session_id,
            "trade_date": "2026-04-18",
            "feed_source": "replay",
            "transport": "local",
            "symbol": symbol,
            "bar_start": bar_start,
            "bar_end": bar_end,
            "open": 100.0,
            "high": 105.0,
            "low": 98.0,
            "close": 103.0,
            "volume": 10000.0,
            "first_snapshot_ts": None,
            "last_snapshot_ts": None,
            "created_at": datetime.now(UTC),
        }

    def test_upsert_returns_count(self, paper_db, session_id):
        rows = [self._row(session_id)]
        count = paper_db.upsert_feed_audit_rows(rows)
        assert count == 1

    def test_upsert_empty_returns_zero(self, paper_db):
        assert paper_db.upsert_feed_audit_rows([]) == 0

    def test_upsert_idempotent(self, paper_db, session_id):
        row = self._row(session_id)
        paper_db.upsert_feed_audit_rows([row])
        paper_db.upsert_feed_audit_rows([row])  # second write same PK
        result = paper_db.get_feed_audit_rows(trade_date="2026-04-18", session_id=session_id)
        assert len(result) == 1

    def test_get_filters_by_session(self, paper_db, session_id):
        paper_db.upsert_feed_audit_rows([self._row(session_id)])
        # Create a second session.
        other = paper_db.create_session(
            strategy_name="thresholdbreakout",
            mode="replay",
            trade_date="2026-04-18",
            strategy_params={},
        )
        paper_db.upsert_feed_audit_rows([self._row(other["session_id"], symbol="TCS")])
        result = paper_db.get_feed_audit_rows(trade_date="2026-04-18", session_id=session_id)
        assert all(r.session_id == session_id for r in result)
        assert len(result) == 1

    def test_get_filters_by_feed_source(self, paper_db, session_id):
        r1 = self._row(session_id)
        r2 = {**r1, "symbol": "TCS", "feed_source": "kite"}
        paper_db.upsert_feed_audit_rows([r1, r2])
        result = paper_db.get_feed_audit_rows(trade_date="2026-04-18", feed_source="kite")
        assert len(result) == 1
        assert result[0].symbol == "TCS"

    def test_get_returns_correct_ohlcv(self, paper_db, session_id):
        row = self._row(session_id)
        paper_db.upsert_feed_audit_rows([row])
        rows = paper_db.get_feed_audit_rows(trade_date="2026-04-18")
        assert len(rows) == 1
        r = rows[0]
        assert r.open == pytest.approx(100.0)
        assert r.high == pytest.approx(105.0)
        assert r.low == pytest.approx(98.0)
        assert r.close == pytest.approx(103.0)
        assert r.volume == pytest.approx(10000.0)

    def test_purge_removes_old_rows(self, paper_db, session_id):
        """Rows with trade_date before retention window should be deleted."""
        from datetime import date

        today_str = date.today().isoformat()
        # Insert a row for a very old date (always outside any retention window).
        old_row = {**self._row(session_id), "trade_date": "2020-01-01"}
        paper_db.upsert_feed_audit_rows([old_row])
        # Insert a row for today using a distinct symbol to avoid PK collision.
        today_row = {**self._row(session_id, symbol="TCS"), "trade_date": today_str}
        paper_db.upsert_feed_audit_rows([today_row])

        # Purge with 7-day retention (2020-01-01 is stale; today is retained).
        paper_db.purge_old_feed_audit_rows(retention_days=7)

        remaining = paper_db.get_feed_audit_rows(trade_date="2020-01-01")
        assert len(remaining) == 0
        recent = paper_db.get_feed_audit_rows(trade_date=today_str)
        assert len(recent) == 1

    def test_purge_no_crash_on_empty_table(self, paper_db):
        """purge should not raise even when the table is empty."""
        paper_db.purge_old_feed_audit_rows(retention_days=7)  # must not raise


# ---------------------------------------------------------------------------
# record_closed_candles()
# ---------------------------------------------------------------------------


class TestRecordClosedCandles:
    def test_basic_record(self, paper_db, session_id):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

        candles = [_make_candle("RELIANCE", (9, 20)), _make_candle("TCS", (9, 20))]
        n = record_closed_candles(
            bar_candles=candles,
            session_id=session_id,
            trade_date="2026-04-18",
            feed_source="replay",
            paper_db=paper_db,
            transport="local",
        )
        assert n == 2
        rows = paper_db.get_feed_audit_rows(trade_date="2026-04-18", session_id=session_id)
        assert len(rows) == 2
        symbols = {r.symbol for r in rows}
        assert symbols == {"RELIANCE", "TCS"}

    def test_empty_list_returns_zero(self, paper_db, session_id):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

        n = record_closed_candles(
            bar_candles=[],
            session_id=session_id,
            trade_date="2026-04-18",
            feed_source="replay",
            paper_db=paper_db,
        )
        assert n == 0

    def test_none_paper_db_returns_zero(self, session_id):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

        n = record_closed_candles(
            bar_candles=[_make_candle()],
            session_id=session_id,
            trade_date="2026-04-18",
            feed_source="replay",
            paper_db=None,
        )
        assert n == 0

    def test_idempotent_redelivery(self, paper_db, session_id):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

        candles = [_make_candle()]
        record_closed_candles(
            bar_candles=candles,
            session_id=session_id,
            trade_date="2026-04-18",
            feed_source="replay",
            paper_db=paper_db,
        )
        record_closed_candles(
            bar_candles=candles,
            session_id=session_id,
            trade_date="2026-04-18",
            feed_source="replay",
            paper_db=paper_db,
        )
        rows = paper_db.get_feed_audit_rows(trade_date="2026-04-18")
        assert len(rows) == 1

    def test_ohlcv_values_persisted_correctly(self, paper_db, session_id):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

        c = _make_candle(open=200.5, high=210.0, low=195.0, close=205.5, volume=5000.0)
        record_closed_candles(
            bar_candles=[c],
            session_id=session_id,
            trade_date="2026-04-18",
            feed_source="kite",
            paper_db=paper_db,
            transport="websocket",
        )
        rows = paper_db.get_feed_audit_rows(trade_date="2026-04-18")
        assert len(rows) == 1
        r = rows[0]
        assert r.open == pytest.approx(200.5)
        assert r.high == pytest.approx(210.0)
        assert r.low == pytest.approx(195.0)
        assert r.close == pytest.approx(205.5)
        assert r.volume == pytest.approx(5000.0)
        assert r.feed_source == "kite"
        assert r.transport == "websocket"

    def test_bar_end_stored_as_utc_datetime(self, paper_db, session_id):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import record_closed_candles

        c = _make_candle(bar_end_ist=(9, 25))
        record_closed_candles(
            bar_candles=[c],
            session_id=session_id,
            trade_date="2026-04-18",
            feed_source="replay",
            paper_db=paper_db,
        )
        rows = paper_db.get_feed_audit_rows(trade_date="2026-04-18")
        assert len(rows) == 1
        r = rows[0]
        # bar_end must be a datetime (tz-aware or tz-naive depending on DuckDB driver).
        assert r.bar_end is not None
        assert isinstance(r.bar_end, datetime)


# ---------------------------------------------------------------------------
# _audit_bar_end() helper
# ---------------------------------------------------------------------------


class TestAuditBarEnd:
    def test_replay_uses_bar_end(self):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import _audit_bar_end

        bar_end = _ist_epoch(9, 25)
        bar_start = bar_end - 300
        result = _audit_bar_end(feed_source="replay", bar_start=bar_start, bar_end=bar_end)
        assert result.timestamp() == pytest.approx(bar_end)

    def test_kite_uses_bar_start_plus_interval(self):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import _audit_bar_end

        bar_end = _ist_epoch(9, 25)
        bar_start = bar_end - 300
        result = _audit_bar_end(feed_source="kite", bar_start=bar_start, bar_end=bar_end)
        # kite: bar_start + 300 == bar_end
        assert result.timestamp() == pytest.approx(bar_end)

    def test_replay_case_insensitive(self):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import _audit_bar_end

        bar_end = _ist_epoch(10, 0)
        result = _audit_bar_end(feed_source="REPLAY", bar_start=bar_end - 300, bar_end=bar_end)
        assert result.timestamp() == pytest.approx(bar_end)

    def test_result_is_utc_datetime(self):
        from nse_momentum_lab.services.paper.scripts.paper_feed_audit import _audit_bar_end

        bar_end = _ist_epoch(9, 20)
        result = _audit_bar_end(feed_source="replay", bar_start=bar_end - 300, bar_end=bar_end)
        assert result.tzinfo is not None
        assert result.utcoffset() == timedelta(0)
