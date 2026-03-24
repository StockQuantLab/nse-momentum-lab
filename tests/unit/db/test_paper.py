from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from nse_momentum_lab.db.paper import (
    alert_session_signals,
    delete_walk_forward_sessions,
    qualify_session_signals,
)


def _scalar_result(*, all_rows=None, one_row=None) -> MagicMock:
    result = MagicMock()
    scalars = MagicMock()
    result.scalars.return_value = scalars
    if all_rows is not None:
        scalars.all.return_value = all_rows
    if one_row is not None:
        scalars.scalar_one_or_none.return_value = one_row
        result.scalar_one_or_none.return_value = one_row
    else:
        scalars.scalar_one_or_none.return_value = None
        result.scalar_one_or_none.return_value = None
    return result


def _signal(signal_id: int, state: str) -> SimpleNamespace:
    return SimpleNamespace(
        signal_id=signal_id,
        session_id="paper-1",
        symbol_id=100 + signal_id,
        asof_date=None,
        strategy_hash="exp-1",
        state=state,
        entry_mode="MARKET",
        planned_entry_date=None,
        initial_stop=None,
        metadata_json={},
        created_at=None,
    )


class TestPaperDBHelpers:
    async def test_qualify_requires_rank_when_max_rank_filter_is_set(self) -> None:
        session = AsyncMock()
        session.add = MagicMock()
        skipped_signal = _signal(1, "NEW")
        qualified_signal = _signal(2, "NEW")
        skipped_pss = SimpleNamespace(rank=None, selection_score=9.5, decision_status="PENDING")
        qualified_pss = SimpleNamespace(rank=1, selection_score=8.5, decision_status="PENDING")
        session.execute.side_effect = [
            _scalar_result(all_rows=[skipped_signal, qualified_signal]),
            _scalar_result(one_row=skipped_pss),
            _scalar_result(one_row=qualified_pss),
        ]

        qualified = await qualify_session_signals(session, "paper-1", max_rank=1)

        assert [row["signal_id"] for row in qualified] == [2]
        assert skipped_signal.state == "NEW"
        assert qualified_signal.state == "QUALIFIED"
        assert skipped_pss.decision_status == "PENDING"
        assert qualified_pss.decision_status == "QUALIFIED"
        session.commit.assert_awaited_once()

    async def test_qualify_requires_score_when_min_score_filter_is_set(self) -> None:
        session = AsyncMock()
        session.add = MagicMock()
        skipped_signal = _signal(1, "NEW")
        qualified_signal = _signal(2, "NEW")
        skipped_pss = SimpleNamespace(rank=1, selection_score=None, decision_status="PENDING")
        qualified_pss = SimpleNamespace(rank=2, selection_score=7.2, decision_status="PENDING")
        session.execute.side_effect = [
            _scalar_result(all_rows=[skipped_signal, qualified_signal]),
            _scalar_result(one_row=skipped_pss),
            _scalar_result(one_row=qualified_pss),
        ]

        qualified = await qualify_session_signals(session, "paper-1", min_score=7.0)

        assert [row["signal_id"] for row in qualified] == [2]
        assert skipped_signal.state == "NEW"
        assert qualified_signal.state == "QUALIFIED"
        assert skipped_pss.decision_status == "PENDING"
        assert qualified_pss.decision_status == "QUALIFIED"
        session.commit.assert_awaited_once()

    async def test_alert_syncs_queue_decision_status(self) -> None:
        session = AsyncMock()
        session.add = MagicMock()
        signal = _signal(11, "QUALIFIED")
        pss = SimpleNamespace(decision_status="QUALIFIED")
        session.execute.side_effect = [
            _scalar_result(all_rows=[signal]),
            _scalar_result(one_row=pss),
        ]

        alerted = await alert_session_signals(session, "paper-1", [11])

        assert [row["signal_id"] for row in alerted] == [11]
        assert signal.state == "ALERTED"
        assert pss.decision_status == "ALERTED"
        session.commit.assert_awaited_once()

    async def test_delete_walk_forward_sessions_removes_sessions_by_filter(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [
            _scalar_result(all_rows=["wf-3", "wf-2", "wf-1"]),
            _scalar_result(),
        ]

        result = await delete_walk_forward_sessions(
            session,
            strategy_name="thresholdbreakout",
            before_date=None,
            after_date=None,
        )

        assert result == {"deleted_count": 3, "session_ids": ["wf-3", "wf-2", "wf-1"]}
        assert session.execute.await_count == 2
        session.commit.assert_awaited_once()
