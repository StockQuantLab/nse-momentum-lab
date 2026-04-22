from __future__ import annotations

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[3]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from apps.nicegui.pages import paper_ledger_v2  # noqa: E402


class _FakeConsumer:
    def __init__(self, rows: list[dict] | None = None, stale_seconds: float = 0.0) -> None:
        self._rows = rows or []
        self._stale_seconds = stale_seconds

    def execute(self, sql: str, parameters: list | None = None) -> list[dict]:
        assert "paper_sessions" in sql
        return self._rows

    def get_stale_seconds(self) -> float:
        return self._stale_seconds


def test_load_sessions_filters_by_status() -> None:
    consumer = _FakeConsumer(
        rows=[
            {"session_id": "sid-active", "status": "ACTIVE", "trade_date": "2026-04-22"},
            {"session_id": "sid-archived", "status": "COMPLETED", "trade_date": "2026-04-21"},
            {"session_id": "sid-paused", "status": "PAUSED", "trade_date": "2026-04-20"},
        ]
    )

    active = paper_ledger_v2._load_sessions(consumer, paper_ledger_v2.ACTIVE_STATUSES)
    assert len(active) == 2
    assert active[0]["session_id"] == "sid-active"

    archived = paper_ledger_v2._load_sessions(consumer, paper_ledger_v2.ARCHIVED_STATUSES)
    assert len(archived) == 1
    assert archived[0]["session_id"] == "sid-archived"


def test_session_label_format() -> None:
    session = {
        "session_id": "abc123def456",
        "trade_date": "2026-04-22",
        "strategy_name": "thresholdbreakout",
        "status": "ACTIVE",
    }
    label = paper_ledger_v2._session_label(session)
    assert label.startswith("2026-04-22")
    assert "2LYNCH Breakout" in label
    assert "ACTIVE" in label
    assert "abc123def456"[:12] in label


def test_strategy_label() -> None:
    assert paper_ledger_v2._strategy_label("thresholdbreakout") == "2LYNCH Breakout"
    assert paper_ledger_v2._strategy_label("2lynchbreakdown") == "2LYNCH Breakdown"
    assert paper_ledger_v2._strategy_label("episodicpivot") == "EP Pivot"
    assert paper_ledger_v2._strategy_label("unknown") == "unknown"


def test_compute_pnl() -> None:
    positions = [
        {"state": "CLOSED", "pnl": 100.0},
        {"state": "CLOSED", "pnl": -50.0},
        {"state": "OPEN", "pnl": 25.0},
        {"state": "OPEN", "pnl": -10.0},
    ]
    pnl = paper_ledger_v2._compute_pnl(positions)
    assert pnl["realized"] == 50.0
    assert pnl["unrealized"] == 15.0
    assert pnl["total"] == 65.0
