from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import Boolean, Integer, cast, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from nse_momentum_lab.db.models import (
    PaperBarCheckpoint,
    PaperFeedState,
    PaperFill,
    PaperOrder,
    PaperOrderEvent,
    PaperPosition,
    PaperSession,
    PaperSessionSignal,
    Signal,
    WalkForwardFold,
)

OPEN_SIGNAL_STATES = {"NEW", "QUALIFIED", "ALERTED", "ENTERED", "MANAGED"}
ACTIVE_SESSION_STATUSES = {"ACTIVE", "RUNNING", "PAUSED", "PLANNING", "STOPPING"}
FINAL_SESSION_STATUSES = {"COMPLETED", "FAILED", "ARCHIVED", "CANCELLED"}


def _now() -> datetime:
    return datetime.now(UTC)


# Backward-compatible alias for older tests and call sites.
_utc_now = _now


def _serialize_paper_session(row: PaperSession) -> dict[str, Any]:
    return {
        "session_id": row.session_id,
        "trade_date": row.trade_date.isoformat() if row.trade_date else None,
        "strategy_name": row.strategy_name,
        "experiment_id": row.experiment_id,
        "mode": row.mode,
        "status": row.status,
        "symbols": list(row.symbols or []),
        "strategy_params": row.strategy_params or {},
        "risk_config": row.risk_config or {},
        "notes": row.notes,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "archived_at": row.archived_at.isoformat() if row.archived_at else None,
    }


def _serialize_paper_session_compact(row: Any) -> dict[str, Any]:
    return {
        "session_id": row.session_id,
        "trade_date": row.trade_date.isoformat() if row.trade_date else None,
        "strategy_name": row.strategy_name,
        "experiment_id": row.experiment_id,
        "mode": row.mode,
        "status": row.status,
        "symbol_count": int(row.symbol_count or 0),
        "strategy_params": row.strategy_params or {},
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _serialize_paper_session_signal(row: PaperSessionSignal) -> dict[str, Any]:
    return {
        "paper_session_signal_id": row.paper_session_signal_id,
        "session_id": row.session_id,
        "signal_id": row.signal_id,
        "symbol_id": row.symbol_id,
        "asof_date": row.asof_date.isoformat() if row.asof_date else None,
        "rank": row.rank,
        "selection_score": float(row.selection_score) if row.selection_score is not None else None,
        "decision_status": row.decision_status,
        "decision_reason": row.decision_reason,
        "metadata_json": row.metadata_json or {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _serialize_paper_order_event(row: PaperOrderEvent) -> dict[str, Any]:
    return {
        "event_id": row.event_id,
        "session_id": row.session_id,
        "order_id": row.order_id,
        "signal_id": row.signal_id,
        "event_type": row.event_type,
        "event_status": row.event_status,
        "broker_order_id": row.broker_order_id,
        "payload_json": row.payload_json or {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _serialize_paper_order(row: PaperOrder) -> dict[str, Any]:
    return {
        "order_id": row.order_id,
        "session_id": row.session_id,
        "broker_order_id": row.broker_order_id,
        "signal_id": row.signal_id,
        "side": row.side,
        "qty": float(row.qty),
        "order_type": row.order_type,
        "limit_price": float(row.limit_price) if row.limit_price is not None else None,
        "status": row.status,
        "broker_status": row.broker_status,
        "broker_payload_json": row.broker_payload_json or {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _serialize_paper_fill(row: PaperFill) -> dict[str, Any]:
    return {
        "fill_id": row.fill_id,
        "session_id": row.session_id,
        "broker_trade_id": row.broker_trade_id,
        "broker_order_id": row.broker_order_id,
        "order_id": row.order_id,
        "fill_time": row.fill_time.isoformat() if row.fill_time else None,
        "fill_price": float(row.fill_price),
        "qty": float(row.qty),
        "fees": float(row.fees) if row.fees is not None else None,
        "slippage_bps": float(row.slippage_bps) if row.slippage_bps is not None else None,
        "broker_payload_json": row.broker_payload_json or {},
    }


def _serialize_paper_feed_state(row: PaperFeedState) -> dict[str, Any]:
    return {
        "session_id": row.session_id,
        "source": row.source,
        "mode": row.mode,
        "status": row.status,
        "is_stale": row.is_stale,
        "subscription_count": row.subscription_count,
        "heartbeat_at": row.heartbeat_at.isoformat() if row.heartbeat_at else None,
        "last_quote_at": row.last_quote_at.isoformat() if row.last_quote_at else None,
        "last_tick_at": row.last_tick_at.isoformat() if row.last_tick_at else None,
        "last_bar_at": row.last_bar_at.isoformat() if row.last_bar_at else None,
        "metadata_json": row.metadata_json or {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _serialize_paper_feed_state_compact(row: Any) -> dict[str, Any]:
    return {
        "source": row.source,
        "mode": row.mode,
        "status": row.status,
        "is_stale": row.is_stale,
        "subscription_count": int(row.subscription_count or 0),
        "token_count": int(row.token_count or 0),
        "last_quote_at": row.last_quote_at.isoformat() if row.last_quote_at else None,
        "last_tick_at": row.last_tick_at.isoformat() if row.last_tick_at else None,
        "heartbeat_at": row.heartbeat_at.isoformat() if row.heartbeat_at else None,
        "observe_only": bool(row.observe_only),
    }


def _serialize_signal(row: Signal) -> dict[str, Any]:
    return {
        "signal_id": row.signal_id,
        "session_id": row.session_id,
        "symbol_id": row.symbol_id,
        "asof_date": row.asof_date.isoformat() if row.asof_date else None,
        "strategy_hash": row.strategy_hash,
        "state": row.state,
        "entry_mode": row.entry_mode,
        "planned_entry_date": row.planned_entry_date.isoformat()
        if row.planned_entry_date
        else None,
        "initial_stop": float(row.initial_stop) if row.initial_stop is not None else None,
        "metadata_json": row.metadata_json or {},
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _serialize_paper_bar_checkpoint(row: PaperBarCheckpoint) -> dict[str, Any]:
    return {
        "checkpoint_id": row.checkpoint_id,
        "session_id": row.session_id,
        "symbol_id": row.symbol_id,
        "bar_interval": row.bar_interval,
        "bar_start": row.bar_start.isoformat() if row.bar_start else None,
        "bar_end": row.bar_end.isoformat() if row.bar_end else None,
        "payload_json": row.payload_json or {},
        "processed": row.processed,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


async def get_paper_session(db_session: AsyncSession, session_id: str) -> PaperSession | None:
    result = await db_session.execute(
        select(PaperSession).where(PaperSession.session_id == session_id)
    )
    return result.scalar_one_or_none()


async def list_paper_sessions(
    db_session: AsyncSession,
    *,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    query = select(PaperSession).order_by(PaperSession.created_at.desc()).limit(limit)
    if status:
        query = query.where(PaperSession.status == status)

    result = await db_session.execute(query)
    sessions = result.scalars().all()
    return [_serialize_paper_session(row) for row in sessions]


async def list_paper_sessions_compact(
    db_session: AsyncSession,
    *,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    query = (
        select(
            PaperSession.session_id,
            PaperSession.trade_date,
            PaperSession.strategy_name,
            PaperSession.experiment_id,
            PaperSession.mode,
            PaperSession.status,
            func.coalesce(func.jsonb_array_length(PaperSession.symbols), 0).label("symbol_count"),
            PaperSession.strategy_params,
            PaperSession.started_at,
            PaperSession.updated_at,
        )
        .order_by(PaperSession.created_at.desc())
        .limit(limit)
    )
    if status:
        query = query.where(PaperSession.status == status)

    result = await db_session.execute(query)
    return [_serialize_paper_session_compact(row) for row in result.all()]


async def get_paper_feed_state(db_session: AsyncSession, session_id: str) -> PaperFeedState | None:
    result = await db_session.execute(
        select(PaperFeedState).where(PaperFeedState.session_id == session_id)
    )
    return result.scalar_one_or_none()


async def upsert_paper_feed_state(
    db_session: AsyncSession,
    *,
    session_id: str,
    source: str,
    mode: str,
    status: str,
    is_stale: bool = False,
    subscription_count: int = 0,
    heartbeat_at: datetime | None = None,
    last_quote_at: datetime | None = None,
    last_tick_at: datetime | None = None,
    last_bar_at: datetime | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> PaperFeedState:
    now = _now()
    row = await get_paper_feed_state(db_session, session_id)
    if row is None:
        row = PaperFeedState(
            session_id=session_id,
            source=source,
            mode=mode,
            status=status,
            is_stale=is_stale,
            subscription_count=subscription_count,
            heartbeat_at=heartbeat_at,
            last_quote_at=last_quote_at,
            last_tick_at=last_tick_at,
            last_bar_at=last_bar_at,
            metadata_json=metadata_json or {},
            created_at=now,
            updated_at=now,
        )
        db_session.add(row)
    else:
        row.source = source
        row.mode = mode
        row.status = status
        row.is_stale = is_stale
        row.subscription_count = subscription_count
        row.heartbeat_at = heartbeat_at
        row.last_quote_at = last_quote_at
        row.last_tick_at = last_tick_at
        row.last_bar_at = last_bar_at
        row.metadata_json = metadata_json or {}
        row.updated_at = now

    await db_session.commit()
    return row


async def touch_paper_feed_state(
    db_session: AsyncSession,
    session_id: str,
    *,
    source: str | None = None,
    mode: str | None = None,
    status: str | None = None,
    is_stale: bool | None = None,
    subscription_count: int | None = None,
    heartbeat_at: datetime | None = None,
    last_quote_at: datetime | None = None,
    last_tick_at: datetime | None = None,
    last_bar_at: datetime | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> PaperFeedState:
    now = _now()
    row = await get_paper_feed_state(db_session, session_id)
    if row is None:
        row = PaperFeedState(
            session_id=session_id,
            source=source or "kite",
            mode=mode or "full",
            status=status or "READY",
            is_stale=is_stale if is_stale is not None else False,
            subscription_count=subscription_count or 0,
            heartbeat_at=heartbeat_at,
            last_quote_at=last_quote_at,
            last_tick_at=last_tick_at,
            last_bar_at=last_bar_at,
            metadata_json=metadata_json or {},
            created_at=now,
            updated_at=now,
        )
        db_session.add(row)
    else:
        if source is not None:
            row.source = source
        if mode is not None:
            row.mode = mode
        if status is not None:
            row.status = status
        if is_stale is not None:
            row.is_stale = is_stale
        if subscription_count is not None:
            row.subscription_count = subscription_count
        if heartbeat_at is not None:
            row.heartbeat_at = heartbeat_at
        if last_quote_at is not None:
            row.last_quote_at = last_quote_at
        if last_tick_at is not None:
            row.last_tick_at = last_tick_at
        if last_bar_at is not None:
            row.last_bar_at = last_bar_at
        if metadata_json is not None:
            row.metadata_json = metadata_json
        row.updated_at = now

    await db_session.commit()
    return row


async def list_paper_session_signals(
    db_session: AsyncSession, session_id: str
) -> list[dict[str, Any]]:
    result = await db_session.execute(
        select(PaperSessionSignal)
        .where(PaperSessionSignal.session_id == session_id)
        .order_by(PaperSessionSignal.rank.asc().nullslast(), PaperSessionSignal.created_at.asc())
    )
    rows = result.scalars().all()
    return [_serialize_paper_session_signal(row) for row in rows]


async def list_session_signals(
    db_session: AsyncSession,
    session_id: str,
    *,
    states: set[str] | None = None,
) -> list[dict[str, Any]]:
    query = (
        select(Signal)
        .where(Signal.session_id == session_id)
        .order_by(Signal.asof_date.asc(), Signal.created_at.asc(), Signal.signal_id.asc())
    )
    if states:
        query = query.where(Signal.state.in_(sorted(states)))
    result = await db_session.execute(query)
    rows = result.scalars().all()
    return [_serialize_signal(row) for row in rows]


async def upsert_signal(
    db_session: AsyncSession,
    *,
    session_id: str,
    symbol_id: int,
    asof_date: date,
    strategy_hash: str,
    state: str,
    entry_mode: str,
    planned_entry_date: date | None = None,
    initial_stop: float | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> Signal:
    result = await db_session.execute(
        select(Signal).where(
            Signal.session_id == session_id,
            Signal.symbol_id == symbol_id,
            Signal.asof_date == asof_date,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        row = Signal(
            session_id=session_id,
            symbol_id=symbol_id,
            asof_date=asof_date,
            strategy_hash=strategy_hash,
            state=state,
            entry_mode=entry_mode,
            planned_entry_date=planned_entry_date,
            initial_stop=initial_stop,
            metadata_json=metadata_json or {},
            created_at=_now(),
        )
        db_session.add(row)
    else:
        row.strategy_hash = strategy_hash
        row.state = state
        row.entry_mode = entry_mode
        row.planned_entry_date = planned_entry_date
        row.initial_stop = initial_stop
        row.metadata_json = metadata_json or {}

    await db_session.commit()
    await db_session.refresh(row)
    return row


async def reset_session_signal_queue(db_session: AsyncSession, session_id: str) -> None:
    await db_session.execute(
        delete(PaperSessionSignal).where(PaperSessionSignal.session_id == session_id)
    )
    await db_session.execute(delete(Signal).where(Signal.session_id == session_id))
    await db_session.commit()


async def upsert_paper_session_signal(
    db_session: AsyncSession,
    *,
    session_id: str,
    signal_id: int,
    symbol_id: int,
    asof_date: date,
    decision_status: str,
    rank: int | None = None,
    selection_score: float | None = None,
    decision_reason: str | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> PaperSessionSignal:
    now = _now()
    result = await db_session.execute(
        select(PaperSessionSignal).where(
            PaperSessionSignal.session_id == session_id,
            PaperSessionSignal.signal_id == signal_id,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        row = PaperSessionSignal(
            session_id=session_id,
            signal_id=signal_id,
            symbol_id=symbol_id,
            asof_date=asof_date,
            rank=rank,
            selection_score=selection_score,
            decision_status=decision_status,
            decision_reason=decision_reason,
            metadata_json=metadata_json or {},
            created_at=now,
        )
        db_session.add(row)
    else:
        row.symbol_id = symbol_id
        row.asof_date = asof_date
        row.rank = rank
        row.selection_score = selection_score
        row.decision_status = decision_status
        row.decision_reason = decision_reason
        row.metadata_json = metadata_json or {}

    await db_session.commit()
    await db_session.refresh(row)
    return row


async def sync_paper_session_signals_from_signals(
    db_session: AsyncSession,
    session_id: str,
    *,
    decision_status: str = "PENDING",
) -> list[PaperSessionSignal]:
    result = await db_session.execute(
        select(Signal).where(Signal.session_id == session_id).order_by(Signal.created_at.asc())
    )
    rows = result.scalars().all()
    synced: list[PaperSessionSignal] = []
    for idx, row in enumerate(rows, start=1):
        synced.append(
            await upsert_paper_session_signal(
                db_session,
                session_id=session_id,
                signal_id=row.signal_id,
                symbol_id=row.symbol_id,
                asof_date=row.asof_date,
                decision_status=decision_status,
                rank=idx,
                selection_score=None,
                decision_reason=row.metadata_json.get("decision_reason"),
                metadata_json=row.metadata_json or {},
            )
        )
    return synced


async def list_paper_order_events(
    db_session: AsyncSession, session_id: str, *, limit: int = 200
) -> list[dict[str, Any]]:
    result = await db_session.execute(
        select(PaperOrderEvent)
        .where(PaperOrderEvent.session_id == session_id)
        .order_by(PaperOrderEvent.created_at.desc())
        .limit(limit)
    )
    rows = result.scalars().all()
    return [_serialize_paper_order_event(row) for row in rows]


async def list_paper_orders(
    db_session: AsyncSession, session_id: str, *, limit: int = 200
) -> list[dict[str, Any]]:
    result = await db_session.execute(
        select(PaperOrder)
        .where(PaperOrder.session_id == session_id)
        .order_by(PaperOrder.created_at.desc())
        .limit(limit)
    )
    rows = result.scalars().all()
    return [_serialize_paper_order(row) for row in rows]


async def get_paper_order_by_broker_order_id(
    db_session: AsyncSession, broker_order_id: str
) -> PaperOrder | None:
    result = await db_session.execute(
        select(PaperOrder).where(PaperOrder.broker_order_id == broker_order_id)
    )
    return result.scalar_one_or_none()


async def upsert_paper_order(
    db_session: AsyncSession,
    *,
    session_id: str,
    signal_id: int,
    side: str,
    qty: float,
    order_type: str,
    limit_price: float | None = None,
    status: str = "PENDING",
    broker_order_id: str | None = None,
    broker_status: str | None = None,
    broker_payload_json: dict[str, Any] | None = None,
) -> PaperOrder:
    row: PaperOrder | None = None
    if broker_order_id:
        row = await get_paper_order_by_broker_order_id(db_session, broker_order_id)
    if row is None:
        result = await db_session.execute(
            select(PaperOrder).where(
                PaperOrder.session_id == session_id,
                PaperOrder.signal_id == signal_id,
                PaperOrder.side == side,
            )
        )
        row = result.scalar_one_or_none()

    now = _now()
    if row is None:
        row = PaperOrder(
            session_id=session_id,
            broker_order_id=broker_order_id,
            signal_id=signal_id,
            side=side,
            qty=qty,
            order_type=order_type,
            limit_price=limit_price,
            status=status,
            broker_status=broker_status,
            broker_payload_json=broker_payload_json or {},
            created_at=now,
        )
        db_session.add(row)
    else:
        row.session_id = session_id
        row.broker_order_id = broker_order_id or row.broker_order_id
        row.signal_id = signal_id
        row.side = side
        row.qty = qty
        row.order_type = order_type
        row.limit_price = limit_price
        row.status = status
        row.broker_status = broker_status
        row.broker_payload_json = broker_payload_json or row.broker_payload_json or {}

    await db_session.commit()
    await db_session.refresh(row)
    return row


async def update_paper_order_broker_state(
    db_session: AsyncSession,
    *,
    broker_order_id: str,
    broker_status: str,
    payload_json: dict[str, Any] | None = None,
) -> PaperOrder | None:
    row = await get_paper_order_by_broker_order_id(db_session, broker_order_id)
    if row is None:
        return None
    row.broker_status = broker_status
    row.broker_payload_json = payload_json or row.broker_payload_json or {}
    row.status = broker_status
    await db_session.commit()
    await db_session.refresh(row)
    return row


async def list_paper_fills(
    db_session: AsyncSession, session_id: str, *, limit: int = 200
) -> list[dict[str, Any]]:
    result = await db_session.execute(
        select(PaperFill)
        .where(PaperFill.session_id == session_id)
        .order_by(PaperFill.fill_time.desc())
        .limit(limit)
    )
    rows = result.scalars().all()
    return [_serialize_paper_fill(row) for row in rows]


async def upsert_paper_fill(
    db_session: AsyncSession,
    *,
    session_id: str,
    order_id: int,
    fill_time: datetime,
    fill_price: float,
    qty: float,
    fees: float | None = None,
    slippage_bps: float | None = None,
    broker_trade_id: str | None = None,
    broker_order_id: str | None = None,
    broker_payload_json: dict[str, Any] | None = None,
) -> PaperFill:
    row: PaperFill | None = None
    if broker_trade_id:
        result = await db_session.execute(
            select(PaperFill).where(PaperFill.broker_trade_id == broker_trade_id)
        )
        row = result.scalar_one_or_none()
    if row is None:
        row = PaperFill(
            session_id=session_id,
            broker_trade_id=broker_trade_id,
            broker_order_id=broker_order_id,
            order_id=order_id,
            fill_time=fill_time,
            fill_price=fill_price,
            qty=qty,
            fees=fees,
            slippage_bps=slippage_bps,
            broker_payload_json=broker_payload_json or {},
        )
        db_session.add(row)
    else:
        row.session_id = session_id
        row.broker_order_id = broker_order_id or row.broker_order_id
        row.order_id = order_id
        row.fill_time = fill_time
        row.fill_price = fill_price
        row.qty = qty
        row.fees = fees
        row.slippage_bps = slippage_bps
        row.broker_payload_json = broker_payload_json or row.broker_payload_json or {}
    await db_session.commit()
    await db_session.refresh(row)
    return row


async def upsert_paper_order_event(
    db_session: AsyncSession,
    *,
    session_id: str,
    event_type: str,
    event_status: str,
    order_id: int | None = None,
    signal_id: int | None = None,
    broker_order_id: str | None = None,
    payload_json: dict[str, Any] | None = None,
) -> PaperOrderEvent:
    row = PaperOrderEvent(
        session_id=session_id,
        order_id=order_id,
        signal_id=signal_id,
        event_type=event_type,
        event_status=event_status,
        broker_order_id=broker_order_id,
        payload_json=payload_json or {},
        created_at=_now(),
    )
    db_session.add(row)
    await db_session.commit()
    await db_session.refresh(row)
    return row


async def list_paper_bar_checkpoints(
    db_session: AsyncSession, session_id: str, *, limit: int = 200
) -> list[dict[str, Any]]:
    result = await db_session.execute(
        select(PaperBarCheckpoint)
        .where(PaperBarCheckpoint.session_id == session_id)
        .order_by(PaperBarCheckpoint.bar_start.desc())
        .limit(limit)
    )
    rows = result.scalars().all()
    return [_serialize_paper_bar_checkpoint(row) for row in rows]


async def upsert_paper_bar_checkpoint(
    db_session: AsyncSession,
    *,
    session_id: str,
    symbol_id: int,
    bar_interval: str,
    bar_start: datetime,
    bar_end: datetime | None = None,
    payload_json: dict[str, Any] | None = None,
    processed: bool = False,
) -> PaperBarCheckpoint:
    result = await db_session.execute(
        select(PaperBarCheckpoint).where(
            PaperBarCheckpoint.session_id == session_id,
            PaperBarCheckpoint.symbol_id == symbol_id,
            PaperBarCheckpoint.bar_interval == bar_interval,
            PaperBarCheckpoint.bar_start == bar_start,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        row = PaperBarCheckpoint(
            session_id=session_id,
            symbol_id=symbol_id,
            bar_interval=bar_interval,
            bar_start=bar_start,
            bar_end=bar_end,
            payload_json=payload_json or {},
            processed=processed,
            created_at=_now(),
            updated_at=_now(),
        )
        db_session.add(row)
    else:
        row.bar_end = bar_end
        row.payload_json = payload_json or {}
        row.processed = processed
        row.updated_at = _now()

    await db_session.commit()
    await db_session.refresh(row)
    return row


async def create_or_update_paper_session(
    db_session: AsyncSession,
    *,
    session_id: str,
    trade_date: date | None,
    strategy_name: str,
    mode: str,
    status: str,
    experiment_id: str | None = None,
    symbols: list[str] | None = None,
    strategy_params: dict[str, Any] | None = None,
    risk_config: dict[str, Any] | None = None,
    notes: str | None = None,
) -> PaperSession:
    now = _now()
    existing = await get_paper_session(db_session, session_id)
    if existing is None:
        existing = PaperSession(
            session_id=session_id,
            trade_date=trade_date,
            strategy_name=strategy_name,
            experiment_id=experiment_id,
            mode=mode,
            status=status,
            symbols=symbols or [],
            strategy_params=strategy_params or {},
            risk_config=risk_config or {},
            notes=notes,
            created_at=now,
            updated_at=now,
            started_at=now if status in ACTIVE_SESSION_STATUSES else None,
            finished_at=now if status in FINAL_SESSION_STATUSES else None,
            archived_at=now if status == "ARCHIVED" else None,
        )
        db_session.add(existing)
    else:
        existing.trade_date = trade_date
        existing.strategy_name = strategy_name
        existing.experiment_id = experiment_id
        existing.mode = mode
        existing.status = status
        existing.symbols = symbols or []
        existing.strategy_params = strategy_params or {}
        existing.risk_config = risk_config or {}
        existing.notes = notes
        existing.updated_at = now
        if status in ACTIVE_SESSION_STATUSES and existing.started_at is None:
            existing.started_at = now
        if status in FINAL_SESSION_STATUSES and existing.finished_at is None:
            existing.finished_at = now
        if status == "ARCHIVED" and existing.archived_at is None:
            existing.archived_at = now

    await db_session.commit()
    await db_session.refresh(existing)
    return existing


async def update_paper_session(
    db_session: AsyncSession,
    *,
    session_id: str,
    symbols: list[str] | None = None,
    strategy_params: dict[str, Any] | None = None,
    risk_config: dict[str, Any] | None = None,
    notes: str | None = None,
) -> PaperSession | None:
    row = await get_paper_session(db_session, session_id)
    if row is None:
        return None

    row.updated_at = _now()
    if symbols is not None:
        row.symbols = symbols
    if strategy_params is not None:
        row.strategy_params = strategy_params
    if risk_config is not None:
        row.risk_config = risk_config
    if notes is not None:
        row.notes = notes

    await db_session.commit()
    await db_session.refresh(row)
    return row


async def set_paper_session_status(
    db_session: AsyncSession,
    *,
    session_id: str,
    status: str,
    notes: str | None = None,
) -> PaperSession | None:
    row = await get_paper_session(db_session, session_id)
    if row is None:
        return None

    now = _now()
    row.status = status
    row.notes = notes if notes is not None else row.notes
    row.updated_at = now
    if status in ACTIVE_SESSION_STATUSES and row.started_at is None:
        row.started_at = now
    if status in FINAL_SESSION_STATUSES and row.finished_at is None:
        row.finished_at = now
    if status == "ARCHIVED" and row.archived_at is None:
        row.archived_at = now

    await db_session.commit()
    await db_session.refresh(row)
    return row


async def get_paper_session_summary(
    db_session: AsyncSession, session_id: str
) -> dict[str, Any] | None:
    row = await get_paper_session(db_session, session_id)
    if row is None:
        return None

    counts_row = (
        await db_session.execute(
            select(
                select(func.count(Signal.signal_id))
                .where(Signal.session_id == session_id)
                .scalar_subquery()
                .label("signal_count"),
                select(func.count(Signal.signal_id))
                .where(
                    Signal.session_id == session_id,
                    Signal.state.in_(sorted(OPEN_SIGNAL_STATES)),
                )
                .scalar_subquery()
                .label("open_signal_count"),
                select(func.count(PaperPosition.position_id))
                .where(
                    PaperPosition.session_id == session_id,
                    PaperPosition.closed_at.is_(None),
                )
                .scalar_subquery()
                .label("open_position_count"),
                select(func.count(PaperOrder.order_id))
                .where(PaperOrder.session_id == session_id)
                .scalar_subquery()
                .label("order_count"),
                select(func.count(PaperFill.fill_id))
                .where(PaperFill.session_id == session_id)
                .scalar_subquery()
                .label("fill_count"),
                select(func.count(PaperSessionSignal.paper_session_signal_id))
                .where(PaperSessionSignal.session_id == session_id)
                .scalar_subquery()
                .label("queue_count"),
            )
        )
    ).one()
    feed_state = await get_paper_feed_state(db_session, session_id)

    return {
        "session": _serialize_paper_session(row),
        "counts": {
            "signals": int(counts_row.signal_count or 0),
            "open_signals": int(counts_row.open_signal_count or 0),
            "open_positions": int(counts_row.open_position_count or 0),
            "orders": int(counts_row.order_count or 0),
            "fills": int(counts_row.fill_count or 0),
            "queue_signals": int(counts_row.queue_count or 0),
        },
        "feed_state": _serialize_paper_feed_state(feed_state) if feed_state else None,
    }


async def get_paper_session_summary_compact(
    db_session: AsyncSession, session_id: str
) -> dict[str, Any] | None:
    session_row = (
        await db_session.execute(
            select(
                PaperSession.session_id,
                PaperSession.trade_date,
                PaperSession.strategy_name,
                PaperSession.experiment_id,
                PaperSession.mode,
                PaperSession.status,
                func.coalesce(func.jsonb_array_length(PaperSession.symbols), 0).label(
                    "symbol_count"
                ),
                PaperSession.strategy_params,
                PaperSession.started_at,
                PaperSession.updated_at,
            ).where(PaperSession.session_id == session_id)
        )
    ).one_or_none()
    if session_row is None:
        return None

    counts_row = (
        await db_session.execute(
            select(
                select(func.count(Signal.signal_id))
                .where(Signal.session_id == session_id)
                .scalar_subquery()
                .label("signal_count"),
                select(func.count(Signal.signal_id))
                .where(
                    Signal.session_id == session_id,
                    Signal.state.in_(sorted(OPEN_SIGNAL_STATES)),
                )
                .scalar_subquery()
                .label("open_signal_count"),
                select(func.count(PaperPosition.position_id))
                .where(
                    PaperPosition.session_id == session_id,
                    PaperPosition.closed_at.is_(None),
                )
                .scalar_subquery()
                .label("open_position_count"),
                select(func.count(PaperOrder.order_id))
                .where(PaperOrder.session_id == session_id)
                .scalar_subquery()
                .label("order_count"),
                select(func.count(PaperFill.fill_id))
                .where(PaperFill.session_id == session_id)
                .scalar_subquery()
                .label("fill_count"),
                select(func.count(PaperSessionSignal.paper_session_signal_id))
                .where(PaperSessionSignal.session_id == session_id)
                .scalar_subquery()
                .label("queue_count"),
            )
        )
    ).one()
    feed_row = (
        await db_session.execute(
            select(
                PaperFeedState.source,
                PaperFeedState.mode,
                PaperFeedState.status,
                PaperFeedState.is_stale,
                PaperFeedState.subscription_count,
                PaperFeedState.last_quote_at,
                PaperFeedState.last_tick_at,
                PaperFeedState.heartbeat_at,
                func.coalesce(
                    cast(PaperFeedState.metadata_json["token_count"].astext, Integer),
                    PaperFeedState.subscription_count,
                    0,
                ).label("token_count"),
                func.coalesce(
                    cast(PaperFeedState.metadata_json["observe_only"].astext, Boolean),
                    False,
                ).label("observe_only"),
            ).where(PaperFeedState.session_id == session_id)
        )
    ).one_or_none()

    return {
        "session": _serialize_paper_session_compact(session_row),
        "counts": {
            "signals": int(counts_row.signal_count or 0),
            "open_signals": int(counts_row.open_signal_count or 0),
            "open_positions": int(counts_row.open_position_count or 0),
            "orders": int(counts_row.order_count or 0),
            "fills": int(counts_row.fill_count or 0),
            "queue_signals": int(counts_row.queue_count or 0),
        },
        "feed_state": _serialize_paper_feed_state_compact(feed_row) if feed_row else None,
    }


# ---------------------------------------------------------------------------
# WalkForwardFold helpers
# ---------------------------------------------------------------------------


def _serialize_walk_forward_fold(row: WalkForwardFold) -> dict[str, Any]:
    return {
        "fold_id": row.fold_id,
        "wf_session_id": row.wf_session_id,
        "fold_index": row.fold_index,
        "train_start": row.train_start.isoformat() if row.train_start else None,
        "train_end": row.train_end.isoformat() if row.train_end else None,
        "test_start": row.test_start.isoformat() if row.test_start else None,
        "test_end": row.test_end.isoformat() if row.test_end else None,
        "exp_id": row.exp_id,
        "status": row.status,
        "total_return_pct": float(row.total_return_pct)
        if row.total_return_pct is not None
        else None,
        "max_drawdown_pct": float(row.max_drawdown_pct)
        if row.max_drawdown_pct is not None
        else None,
        "profit_factor": float(row.profit_factor) if row.profit_factor is not None else None,
        "total_trades": row.total_trades,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


async def insert_walk_forward_fold(
    db_session: AsyncSession,
    *,
    wf_session_id: str,
    fold_index: int,
    train_start: date,
    train_end: date,
    test_start: date,
    test_end: date,
    exp_id: str | None = None,
    status: str | None = None,
    total_return_pct: float | None = None,
    max_drawdown_pct: float | None = None,
    profit_factor: float | None = None,
    total_trades: int | None = None,
) -> WalkForwardFold:
    row = WalkForwardFold(
        wf_session_id=wf_session_id,
        fold_index=fold_index,
        train_start=train_start,
        train_end=train_end,
        test_start=test_start,
        test_end=test_end,
        exp_id=exp_id,
        status=status,
        total_return_pct=total_return_pct,
        max_drawdown_pct=max_drawdown_pct,
        profit_factor=profit_factor,
        total_trades=total_trades,
        created_at=_now(),
    )
    db_session.add(row)
    await db_session.flush()
    return row


async def list_walk_forward_folds(
    db_session: AsyncSession,
    wf_session_id: str,
) -> list[dict[str, Any]]:
    result = await db_session.execute(
        select(WalkForwardFold)
        .where(WalkForwardFold.wf_session_id == wf_session_id)
        .order_by(WalkForwardFold.fold_index.asc())
    )
    rows = result.scalars().all()
    return [_serialize_walk_forward_fold(row) for row in rows]


async def reset_walk_forward_folds(
    db_session: AsyncSession,
    wf_session_id: str,
) -> None:
    await db_session.execute(
        delete(WalkForwardFold).where(WalkForwardFold.wf_session_id == wf_session_id)
    )
    await db_session.flush()


async def get_walk_forward_session_cleanup_preview(
    db_session: AsyncSession,
    session_id: str,
) -> dict[str, Any] | None:
    """Return the walk-forward parent row and folds that would be removed."""
    row = await get_paper_session(db_session, session_id)
    if row is None or row.mode != "walk_forward":
        return None
    folds = await list_walk_forward_folds(db_session, session_id)
    return {
        "session": _serialize_paper_session(row),
        "folds": folds,
        "fold_count": len(folds),
    }


async def delete_walk_forward_session(
    db_session: AsyncSession,
    session_id: str,
) -> dict[str, Any]:
    """Delete one walk-forward parent session and let FK cascade remove the folds."""
    preview = await get_walk_forward_session_cleanup_preview(db_session, session_id)
    if preview is None:
        return {"deleted_count": 0, "session_ids": []}

    await db_session.execute(
        delete(PaperSession).where(
            PaperSession.session_id == session_id,
            PaperSession.mode == "walk_forward",
        )
    )
    await db_session.commit()
    return {"deleted_count": 1, "session_ids": [session_id]}


async def delete_walk_forward_sessions_by_ids(
    db_session: AsyncSession,
    session_ids: list[str],
) -> dict[str, Any]:
    """Delete multiple walk-forward parent sessions and cascade their folds."""
    unique_ids = [session_id for session_id in dict.fromkeys(session_ids) if session_id]
    if not unique_ids:
        return {"deleted_count": 0, "session_ids": []}

    result = await db_session.execute(
        select(PaperSession.session_id).where(
            PaperSession.session_id.in_(unique_ids),
            PaperSession.mode == "walk_forward",
        )
    )
    deleted_ids = [str(session_id) for session_id in result.scalars().all()]
    if not deleted_ids:
        return {"deleted_count": 0, "session_ids": []}

    await db_session.execute(
        delete(PaperSession).where(
            PaperSession.session_id.in_(deleted_ids),
            PaperSession.mode == "walk_forward",
        )
    )
    await db_session.commit()
    return {"deleted_count": len(deleted_ids), "session_ids": deleted_ids}


async def get_latest_passed_walk_forward(
    db_session: AsyncSession,
    strategy_name: str,
) -> dict[str, Any] | None:
    """Return the most recent COMPLETED walk_forward session for a strategy."""
    sessions = await list_passed_walk_forward_sessions(db_session, strategy_name, limit=1)
    return sessions[0] if sessions else None


async def list_passed_walk_forward_sessions(
    db_session: AsyncSession,
    strategy_name: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return recent COMPLETED walk_forward sessions for a strategy."""
    result = await db_session.execute(
        select(PaperSession)
        .where(
            PaperSession.strategy_name == strategy_name,
            PaperSession.mode == "walk_forward",
            PaperSession.status == "COMPLETED",
        )
        .order_by(PaperSession.created_at.desc())
        .limit(limit)
    )
    rows = result.scalars().all()
    return [_serialize_paper_session(row) for row in rows]


async def delete_walk_forward_sessions(
    db_session: AsyncSession,
    *,
    strategy_name: str | None = None,
    before_date: date | None = None,
    after_date: date | None = None,
) -> dict[str, Any]:
    """Delete walk-forward sessions and return a summary of the removed rows."""
    query = select(PaperSession.session_id).where(PaperSession.mode == "walk_forward")
    if strategy_name:
        query = query.where(PaperSession.strategy_name == strategy_name)
    if before_date is not None:
        query = query.where(PaperSession.trade_date < before_date)
    if after_date is not None:
        query = query.where(PaperSession.trade_date >= after_date)

    result = await db_session.execute(query.order_by(PaperSession.created_at.desc()))
    session_ids = [str(session_id) for session_id in result.scalars().all()]
    return await delete_walk_forward_sessions_by_ids(db_session, session_ids)


# ---------------------------------------------------------------------------
# Flatten liquidation
# ---------------------------------------------------------------------------


async def flatten_open_positions(
    db_session: AsyncSession,
    session_id: str,
    *,
    exit_note: str = "FLATTEN",
) -> list[dict[str, Any]]:
    """Close all open positions at last-mark price (or avg_entry as a zero-PnL fallback)."""
    result = await db_session.execute(
        select(PaperPosition).where(
            PaperPosition.session_id == session_id,
            PaperPosition.closed_at.is_(None),
        )
    )
    open_positions = result.scalars().all()
    closed: list[dict[str, Any]] = []
    now = _now()

    for position in open_positions:
        metadata = dict(position.metadata_json or {})
        signal_id: int | None = metadata.get("signal_id")
        exit_price = float(metadata.get("last_mark_price") or position.avg_entry)
        pnl = (exit_price - float(position.avg_entry)) * float(position.qty)

        await db_session.execute(
            update(PaperPosition)
            .where(PaperPosition.position_id == position.position_id)
            .values(closed_at=now, avg_exit=exit_price, pnl=pnl, state="EXITED")
        )

        order = PaperOrder(
            session_id=session_id,
            broker_order_id=None,
            signal_id=signal_id,
            side="SELL",
            qty=position.qty,
            order_type="MARKET",
            limit_price=None,
            status="FILLED",
            broker_status=None,
            broker_payload_json={},
            created_at=now,
        )
        db_session.add(order)
        await db_session.flush()

        fill = PaperFill(
            session_id=session_id,
            order_id=order.order_id,
            fill_time=now,
            fill_price=exit_price,
            qty=position.qty,
            fees=round(exit_price * float(position.qty) * 0.001, 4),
            slippage_bps=0,
        )
        db_session.add(fill)
        await db_session.flush()

        if signal_id is not None:
            await db_session.execute(
                update(Signal).where(Signal.signal_id == signal_id).values(state="EXITED")
            )

        event = PaperOrderEvent(
            session_id=session_id,
            order_id=order.order_id,
            signal_id=signal_id,
            event_type="POSITION_FLATTENED",
            event_status="FILLED",
            payload_json={
                "exit_price": exit_price,
                "pnl": pnl,
                "note": exit_note,
                "position_id": position.position_id,
            },
            created_at=now,
        )
        db_session.add(event)
        closed.append(
            {
                "position_id": position.position_id,
                "symbol_id": position.symbol_id,
                "exit_price": exit_price,
                "pnl": pnl,
            }
        )

    await db_session.commit()
    return closed


# ---------------------------------------------------------------------------
# Signal state transitions: QUALIFY / ALERT
# ---------------------------------------------------------------------------


async def qualify_session_signals(
    db_session: AsyncSession,
    session_id: str,
    *,
    max_rank: int | None = None,
    min_score: float | None = None,
) -> list[dict[str, Any]]:
    """Promote top-ranked NEW signals to QUALIFIED and write audit events."""
    result = await db_session.execute(
        select(Signal).where(
            Signal.session_id == session_id,
            Signal.state == "NEW",
        )
    )
    signals = result.scalars().all()

    qualified: list[dict[str, Any]] = []
    for signal in signals:
        pss_result = await db_session.execute(
            select(PaperSessionSignal).where(
                PaperSessionSignal.session_id == session_id,
                PaperSessionSignal.signal_id == signal.signal_id,
            )
        )
        pss = pss_result.scalar_one_or_none()
        rank = pss.rank if pss else None
        score = float(pss.selection_score) if pss and pss.selection_score is not None else None

        if max_rank is not None and (rank is None or rank > max_rank):
            continue
        if min_score is not None and (score is None or score < min_score):
            continue

        signal.state = "QUALIFIED"
        if pss is not None:
            pss.decision_status = "QUALIFIED"
        db_session.add(
            PaperOrderEvent(
                session_id=session_id,
                signal_id=signal.signal_id,
                event_type="SIGNAL_QUALIFIED",
                event_status="QUALIFIED",
                payload_json={"rank": rank, "selection_score": score},
                created_at=_now(),
            )
        )
        qualified.append(_serialize_signal(signal))

    await db_session.commit()
    return qualified


async def alert_session_signals(
    db_session: AsyncSession,
    session_id: str,
    signal_ids: list[int],
) -> list[dict[str, Any]]:
    """Promote specific QUALIFIED signals to ALERTED."""
    result = await db_session.execute(
        select(Signal).where(
            Signal.session_id == session_id,
            Signal.state == "QUALIFIED",
            Signal.signal_id.in_(signal_ids),
        )
    )
    signals = result.scalars().all()

    alerted: list[dict[str, Any]] = []
    for signal in signals:
        signal.state = "ALERTED"
        pss_result = await db_session.execute(
            select(PaperSessionSignal).where(
                PaperSessionSignal.session_id == session_id,
                PaperSessionSignal.signal_id == signal.signal_id,
            )
        )
        pss = pss_result.scalar_one_or_none()
        if pss is not None:
            pss.decision_status = "ALERTED"
        db_session.add(
            PaperOrderEvent(
                session_id=session_id,
                signal_id=signal.signal_id,
                event_type="SIGNAL_ALERTED",
                event_status="ALERTED",
                payload_json={},
                created_at=_now(),
            )
        )
        alerted.append(_serialize_signal(signal))

    await db_session.commit()
    return alerted


# ---------------------------------------------------------------------------
# Session cleanup and archive
# ---------------------------------------------------------------------------


async def list_stale_sessions(
    db_session: AsyncSession,
    *,
    mode: str | None = None,
    max_age_hours: int = 48,
    exclude_recent: int = 1,
) -> list[dict[str, Any]]:
    """Find sessions that appear stale and should be cleaned up.

    A session is considered stale if:
    - Its status is in ACTIVE_SESSION_STATUSES
    - It was created more than *max_age_hours* ago
    - It is not the most recent session of its mode (reserved by *exclude_recent*)

    Returns serialized session dicts.
    """
    cutoff = _now() - timedelta(hours=max_age_hours)

    query = (
        select(PaperSession)
        .where(
            PaperSession.status.in_(ACTIVE_SESSION_STATUSES),
            PaperSession.created_at < cutoff,
        )
        .order_by(PaperSession.created_at.desc())
    )
    if mode:
        query = query.where(PaperSession.mode == mode)

    result = await db_session.execute(query)
    all_stale = result.scalars().all()

    # Protect the N most recent sessions per mode from accidental cleanup
    if exclude_recent > 0 and all_stale:
        protected_ids: set[str] = set()
        protect_query = (
            select(PaperSession.session_id)
            .order_by(PaperSession.created_at.desc())
            .limit(exclude_recent)
        )
        if mode:
            protect_query = protect_query.where(PaperSession.mode == mode)
        protect_result = await db_session.execute(protect_query)
        protected_ids = {row[0] for row in protect_result.fetchall()}

        all_stale = [s for s in all_stale if s.session_id not in protected_ids]

    return [_serialize_paper_session(s) for s in all_stale]


async def archive_sessions(
    db_session: AsyncSession,
    session_ids: list[str],
) -> dict[str, Any]:
    """Archive a list of sessions by setting status to ARCHIVED.

    Returns a summary with counts of archived and not-found sessions.
    """
    archived = 0
    not_found = 0
    now = _now()

    for session_id in session_ids:
        row = await get_paper_session(db_session, session_id)
        if row is None:
            not_found += 1
            continue
        row.status = "ARCHIVED"
        row.finished_at = row.finished_at or now
        row.archived_at = now
        row.updated_at = now
        archived += 1

    await db_session.commit()
    return {"archived": archived, "not_found": not_found}


async def get_active_session(
    db_session: AsyncSession,
    *,
    mode: str | None = None,
) -> dict[str, Any] | None:
    """Return the most recent active (non-archived, non-final) session.

    Useful for operators to identify the current live session at a glance.
    """
    query = (
        select(PaperSession)
        .where(PaperSession.status.in_(ACTIVE_SESSION_STATUSES))
        .order_by(PaperSession.created_at.desc())
        .limit(1)
    )
    if mode:
        query = query.where(PaperSession.mode == mode)

    result = await db_session.execute(query)
    row = result.scalar_one_or_none()
    return _serialize_paper_session(row) if row else None
