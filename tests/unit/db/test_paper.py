from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from nse_momentum_lab.db.paper import (
    alert_session_signals,
    delete_walk_forward_session,
    delete_walk_forward_sessions,
    get_walk_forward_session_cleanup_preview,
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


def _walk_forward_session(session_id: str = "wf-1") -> SimpleNamespace:
    return SimpleNamespace(
        session_id=session_id,
        trade_date=None,
        strategy_name="thresholdbreakout",
        experiment_id=None,
        mode="walk_forward",
        status="RUNNING",
        symbols=[],
        strategy_params={},
        risk_config={},
        notes=None,
        created_at=None,
        updated_at=None,
        started_at=None,
        finished_at=None,
        archived_at=None,
    )


def _walk_forward_fold(
    fold_id: int, *, session_id: str = "wf-1", exp_id: str = "exp-1"
) -> SimpleNamespace:
    return SimpleNamespace(
        fold_id=fold_id,
        wf_session_id=session_id,
        fold_index=fold_id,
        train_start=None,
        train_end=None,
        test_start=None,
        test_end=None,
        exp_id=exp_id,
        status="completed",
        total_return_pct=2.5,
        max_drawdown_pct=1.0,
        profit_factor=1.4,
        total_trades=11,
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
        assert session.execute.await_count == 3
        session.commit.assert_awaited_once()

    async def test_get_walk_forward_session_cleanup_preview_includes_folds(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [
            _scalar_result(one_row=_walk_forward_session("wf-1")),
            _scalar_result(all_rows=[_walk_forward_fold(1), _walk_forward_fold(2)]),
        ]

        preview = await get_walk_forward_session_cleanup_preview(session, "wf-1")

        assert preview is not None
        assert preview["session"]["session_id"] == "wf-1"
        assert preview["fold_count"] == 2
        assert [row["fold_id"] for row in preview["folds"]] == [1, 2]

    async def test_delete_walk_forward_session_deletes_parent_row(self) -> None:
        session = AsyncMock()
        session.execute.side_effect = [
            _scalar_result(one_row=_walk_forward_session("wf-1")),
            _scalar_result(all_rows=[_walk_forward_fold(1), _walk_forward_fold(2)]),
            _scalar_result(),
        ]

        result = await delete_walk_forward_session(session, "wf-1")

        assert result == {"deleted_count": 1, "session_ids": ["wf-1"]}
        assert session.execute.await_count == 3
        session.commit.assert_awaited_once()
