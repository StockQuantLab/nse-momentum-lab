"""Paper Ledger page - Session-aware paper trading operations view."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

_apps_root = Path(__file__).resolve().parent.parent
_project_root = _apps_root.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))
if str(_apps_root) not in sys.path:
    sys.path.insert(0, str(_apps_root))

from nicegui import ui

from apps.nicegui.components import (
    COLORS,
    THEME,
    divider,
    empty_state,
    info_box,
    kpi_grid,
    page_layout,
    paginated_table,
)
from apps.nicegui.state import (
    aget_paper_positions,
    aget_paper_session_events,
    aget_paper_session_fills,
    aget_paper_session_orders,
    aget_paper_session_signals,
    aget_paper_session_summary,
    aget_paper_sessions,
    aget_walk_forward_folds,
)


def _fmt_float(value: Any, digits: int = 2) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_ts(value: Any) -> str:
    if not value:
        return "-"
    return str(value).replace("T", " ")[:19]


def _status_color(status: str | None) -> str:
    normalized = str(status or "").upper()
    if normalized in {"ACTIVE", "RUNNING", "CONNECTED", "READY", "COMPLETE", "COMPLETED", "PASS"}:
        return COLORS["success"]
    if normalized in {"PAUSED", "STOPPING", "PLANNING", "CONNECTING"}:
        return COLORS["warning"]
    if normalized in {"FAILED", "ERROR", "DISCONNECTED", "ARCHIVED", "CANCELLED"}:
        return COLORS["error"]
    return COLORS["info"]


def _session_label(session: dict[str, Any]) -> str:
    trade_date = session.get("trade_date") or "na"
    strategy = session.get("strategy_name") or "strategy?"
    status = session.get("status") or "UNKNOWN"
    mode = session.get("mode") or "?"
    return f"{trade_date} | {strategy} | {mode} | {status} | {session['session_id'][:18]}"


def _queue_rows(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in signals:
        metadata = row.get("metadata_json") or {}
        rows.append(
            {
                "rank": row.get("rank"),
                "symbol": metadata.get("symbol") or f"ID {row.get('symbol_id')}",
                "decision_status": row.get("decision_status"),
                "decision_reason": row.get("decision_reason")
                or metadata.get("backtest_reason")
                or "-",
                "selection_score": _fmt_float(row.get("selection_score"), 3),
                "backtest_status": metadata.get("backtest_status") or "-",
                "entry_price": _fmt_float(metadata.get("entry_price")),
                "entry_time": str(metadata.get("entry_time") or "-"),
                "signal_id": row.get("signal_id"),
            }
        )
    return rows


def _order_rows(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "order_id": row.get("order_id"),
            "signal_id": row.get("signal_id"),
            "side": row.get("side"),
            "qty": _fmt_float(row.get("qty")),
            "order_type": row.get("order_type"),
            "status": row.get("status"),
            "broker_status": row.get("broker_status") or "-",
            "broker_order_id": row.get("broker_order_id") or "-",
            "created_at": _fmt_ts(row.get("created_at")),
        }
        for row in orders
    ]


def _fill_rows(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "fill_id": row.get("fill_id"),
            "order_id": row.get("order_id"),
            "qty": _fmt_float(row.get("qty")),
            "fill_price": _fmt_float(row.get("fill_price")),
            "fees": _fmt_float(row.get("fees")),
            "slippage_bps": _fmt_float(row.get("slippage_bps")),
            "broker_trade_id": row.get("broker_trade_id") or "-",
            "fill_time": _fmt_ts(row.get("fill_time")),
        }
        for row in fills
    ]


def _position_rows(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "position_id": row.get("position_id"),
            "symbol": row.get("symbol") or f"ID {row.get('symbol_id')}",
            "symbol_id": row.get("symbol_id"),
            "qty": _fmt_float(row.get("qty")),
            "avg_entry": _fmt_float(row.get("avg_entry")),
            "avg_exit": _fmt_float(row.get("avg_exit")),
            "pnl": _fmt_float(row.get("pnl")),
            "market_price": _fmt_float(row.get("market_price")),
            "unrealized_pnl": _fmt_float(row.get("unrealized_pnl")),
            "state": row.get("state"),
            "opened_at": _fmt_ts(row.get("opened_at")),
            "closed_at": _fmt_ts(row.get("closed_at")),
        }
        for row in positions
    ]


def _walk_forward_rows(strategy_params: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not strategy_params:
        return []
    walk_forward = strategy_params.get("walk_forward")
    if not isinstance(walk_forward, dict):
        return []
    folds = walk_forward.get("folds")
    if not isinstance(folds, list):
        return []
    rows: list[dict[str, Any]] = []
    for idx, fold in enumerate(folds, start=1):
        if not isinstance(fold, dict):
            continue
        rows.append(
            {
                "fold": idx,
                "train": f"{fold.get('train_start') or '-'} -> {fold.get('train_end') or '-'}",
                "test": f"{fold.get('test_start') or '-'} -> {fold.get('test_end') or '-'}",
                "status": fold.get("status") or "-",
                "return_pct": _fmt_float(fold.get("total_return_pct")),
                "drawdown_pct": _fmt_float(fold.get("max_drawdown_pct")),
                "trades": fold.get("total_trades") or 0,
                "exp_id": str(fold.get("exp_id") or "")[:12],
            }
        )
    return rows


def _walk_forward_rows_from_db(db_folds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Format DB-sourced WalkForwardFold rows for the paginated table."""
    return [
        {
            "fold": fold.get("fold_index"),
            "train": f"{fold.get('train_start') or '-'} -> {fold.get('train_end') or '-'}",
            "test": f"{fold.get('test_start') or '-'} -> {fold.get('test_end') or '-'}",
            "status": fold.get("status") or "-",
            "return_pct": _fmt_float(fold.get("total_return_pct")),
            "drawdown_pct": _fmt_float(fold.get("max_drawdown_pct")),
            "trades": fold.get("total_trades") or 0,
            "exp_id": str(fold.get("exp_id") or "")[:12],
        }
        for fold in db_folds
    ]


def _event_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "event_id": row.get("event_id"),
            "event_type": row.get("event_type"),
            "event_status": row.get("event_status"),
            "order_id": row.get("order_id"),
            "signal_id": row.get("signal_id"),
            "broker_order_id": row.get("broker_order_id") or "-",
            "created_at": _fmt_ts(row.get("created_at")),
        }
        for row in events
    ]


async def paper_ledger_page() -> None:
    """Render the paper trading ledger page."""
    with page_layout("Paper Ledger", "receipt_long"):
        with ui.column().classes("kpi-card p-6 mb-6"):
            ui.label("Paper Session Ledger").classes("text-xl font-semibold mb-2").style(
                f"color: {THEME['text_primary']};"
            )
            ui.label(
                "This page reads live paper-session state from PostgreSQL so Monday testing can be "
                "observed without leaving the dashboard."
            ).classes("mb-3").style(f"color: {THEME['text_secondary']};")
            ui.label(
                "Walk-forward remains the promotion gate. Then load a session from an approved "
                "experiment/date, execute replay or live once, and monitor feed, queue, orders, fills, and positions here."
            ).style(f"color: {THEME['text_muted']};")

        try:
            sessions = await aget_paper_sessions(limit=50)
        except Exception as exc:
            info_box(
                f"Could not load paper sessions. Ensure Doppler-backed database settings are available. {exc}",
                color="red",
            )
            return

        if not sessions:
            empty_state(
                "No paper sessions yet",
                "Create a paper session with nseml-paper replay-day/live first, then return here to monitor it.",
                icon="receipt_long",
            )
            return

        ordered_sessions = sorted(
            sessions,
            key=lambda row: (
                str(row.get("status") or "") not in {"ACTIVE", "RUNNING", "PAUSED", "PLANNING"},
                str(row.get("updated_at") or ""),
            ),
            reverse=True,
        )
        option_map = {
            _session_label(session): session["session_id"] for session in ordered_sessions
        }
        current = {
            "label": next(iter(option_map)),
            "session_id": ordered_sessions[0]["session_id"],
            "auto_refresh": False,
        }

        async def handle_session_change(event) -> None:
            current["label"] = str(event.value)
            current["session_id"] = option_map[current["label"]]
            await render_session(current["session_id"])

        with ui.row().classes("items-center gap-3 mb-6"):
            ui.select(
                options=list(option_map.keys()),
                value=current["label"],
                label="Paper Session",
                on_change=handle_session_change,
            ).classes("min-w-[420px]")
            ui.button(
                "Refresh",
                icon="refresh",
                on_click=lambda: asyncio.create_task(render_session(current["session_id"])),
            ).props("outline")
            ui.label("Auto-refresh every 30s when active").classes("text-sm").style(
                f"color: {THEME['text_muted']};"
            )

        commands_card = ui.column().classes("kpi-card p-5 mb-6")
        wf_card = ui.column().classes("kpi-card p-5 mb-6")
        content = ui.column().classes("w-full")

        def render_dynamic_panels(
            session: dict[str, Any], db_folds: list[dict[str, Any]] | None = None
        ) -> None:
            strategy = str(session.get("strategy_name") or "indian_2lynch")
            trade_date = str(session.get("trade_date") or "<TRADE_DATE>")
            exp_id = str(session.get("experiment_id") or "<EXP_ID>")

            commands_card.clear()
            with commands_card:
                ui.label("Commands").classes("text-lg font-semibold mb-3").style(
                    f"color: {THEME['text_primary']};"
                )
                for command in [
                    (
                        "doppler run -- uv run nseml-paper walk-forward "
                        f"--strategy {strategy} --start-date 2025-04-01 --end-date 2026-03-09"
                    ),
                    (
                        "doppler run -- uv run nseml-paper replay-day "
                        f"--trade-date {trade_date} --experiment-id {exp_id} --execute"
                    ),
                    (
                        "doppler run -- uv run nseml-paper live "
                        f"--trade-date {trade_date} --experiment-id {exp_id} --execute"
                    ),
                    (
                        "doppler run -- uv run nseml-paper stream "
                        f"--trade-date {trade_date} --experiment-id {exp_id}"
                    ),
                ]:
                    ui.label(command).classes("font-mono text-sm px-3 py-2 rounded mb-2").style(
                        f"background: {THEME['surface_hover']}; border: 1px solid {THEME['surface_border']}; color: {THEME['text_primary']}; border-radius: 6px;"
                    )

            wf_card.clear()
            with wf_card:
                ui.label("Walk-Forward").classes("text-lg font-semibold mb-3").style(
                    f"color: {THEME['text_primary']};"
                )
                strategy_params = session.get("strategy_params") or {}
                walk_forward = strategy_params.get("walk_forward")
                if not isinstance(walk_forward, dict):
                    empty_state(
                        "No walk-forward report",
                        "This session has no persisted walk-forward summary yet.",
                        icon="query_stats",
                    )
                    return

                decision = walk_forward.get("decision") or {}
                summary = walk_forward.get("summary") or {}
                kpi_grid(
                    [
                        dict(
                            title="Decision",
                            value=str(decision.get("status") or "-"),
                            subtitle=str(decision.get("reason") or "-"),
                            icon="flag",
                            color=_status_color(decision.get("status")),
                        ),
                        dict(
                            title="Folds",
                            value=int(summary.get("folds_total") or 0),
                            subtitle=f"Completed {int(summary.get('folds_completed') or 0)}",
                            icon="view_week",
                            color=COLORS["info"],
                        ),
                        dict(
                            title="Avg Return",
                            value=_fmt_float(summary.get("avg_return_pct")),
                            subtitle=f"Median {_fmt_float(summary.get('median_return_pct'))}%",
                            icon="trending_up",
                            color=COLORS["success"],
                        ),
                        dict(
                            title="Worst DD",
                            value=_fmt_float(summary.get("worst_drawdown_pct")),
                            subtitle=f"Profitable {summary.get('folds_profitable') or 0}",
                            icon="trending_down",
                            color=COLORS["warning"],
                        ),
                    ],
                    columns=4,
                )

                wf_rows = (
                    _walk_forward_rows_from_db(db_folds)
                    if db_folds
                    else _walk_forward_rows(strategy_params)
                )
                if wf_rows:
                    paginated_table(
                        rows=wf_rows,
                        columns=[
                            {"name": "fold", "label": "Fold", "field": "fold"},
                            {"name": "train", "label": "Train", "field": "train"},
                            {"name": "test", "label": "Test", "field": "test"},
                            {"name": "status", "label": "Status", "field": "status"},
                            {"name": "return_pct", "label": "Return", "field": "return_pct"},
                            {"name": "drawdown_pct", "label": "Drawdown", "field": "drawdown_pct"},
                            {"name": "trades", "label": "Trades", "field": "trades"},
                            {"name": "exp_id", "label": "Exp", "field": "exp_id"},
                        ],
                        page_size=8,
                    )
                else:
                    empty_state(
                        "No fold rows",
                        "The persisted walk-forward payload does not contain per-fold results.",
                        icon="table_rows",
                    )

        async def render_session(session_id: str) -> None:
            content.clear()
            with content:
                try:
                    summary, signals, orders, fills, events, positions, db_folds = (
                        await asyncio.gather(
                            aget_paper_session_summary(session_id),
                            aget_paper_session_signals(session_id),
                            aget_paper_session_orders(session_id, limit=100),
                            aget_paper_session_fills(session_id, limit=100),
                            aget_paper_session_events(session_id, limit=100),
                            aget_paper_positions(session_id, open_only=False),
                            aget_walk_forward_folds(session_id),
                        )
                    )
                except Exception as exc:
                    info_box(f"Could not load session data for {session_id}: {exc}", color="red")
                    return

                if not summary:
                    empty_state(
                        "Paper session not found",
                        f"Session {session_id} no longer exists.",
                        icon="error",
                    )
                    return

                session = summary["session"]
                counts = summary.get("counts", {})
                feed_state = summary.get("feed_state") or {}
                session_color = _status_color(session.get("status"))
                feed_color = _status_color(feed_state.get("status"))
                current["auto_refresh"] = str(session.get("status") or "").upper() in {
                    "ACTIVE",
                    "RUNNING",
                }
                render_dynamic_panels(session, db_folds=db_folds)

                realized_pnl = sum(
                    float(row.get("pnl") or 0.0)
                    for row in positions
                    if row.get("pnl") is not None
                )
                unrealized_pnl = sum(
                    float(row.get("unrealized_pnl") or 0.0)
                    for row in positions
                    if row.get("unrealized_pnl") is not None
                )
                total_pnl = realized_pnl + unrealized_pnl

                kpi_grid(
                    [
                        dict(
                            title="Session Status",
                            value=str(session.get("status", "-")),
                            subtitle=session.get("mode") or "-",
                            icon="schedule",
                            color=session_color,
                        ),
                        dict(
                            title="Queue Signals",
                            value=int(counts.get("queue_signals", 0)),
                            subtitle=f"Open signals {int(counts.get('open_signals', 0))}",
                            icon="queue",
                            color=COLORS["info"],
                        ),
                        dict(
                            title="Open Positions",
                            value=int(counts.get("open_positions", 0)),
                            subtitle=f"Orders {int(counts.get('orders', 0))}",
                            icon="work",
                            color=COLORS["warning"],
                        ),
                        dict(
                            title="Fills",
                            value=int(counts.get("fills", 0)),
                            subtitle=f"Session signals {int(counts.get('signals', 0))}",
                            icon="done_all",
                            color=COLORS["success"],
                        ),
                        dict(
                            title="Feed",
                            value=str(feed_state.get("status", "UNAVAILABLE")),
                            subtitle=f"{feed_state.get('source', '-')}/{feed_state.get('mode', '-')}",
                            icon="sensors",
                            color=feed_color,
                        ),
                    ],
                    columns=5,
                )

                kpi_grid(
                    [
                        dict(
                            title="Realized P&L",
                            value=_fmt_float(realized_pnl),
                            subtitle="Closed positions",
                            icon="account_balance_wallet",
                            color=COLORS["success"] if realized_pnl >= 0 else COLORS["error"],
                        ),
                        dict(
                            title="Unrealized P&L",
                            value=_fmt_float(unrealized_pnl),
                            subtitle="Open positions MTM",
                            icon="show_chart",
                            color=COLORS["warning"] if unrealized_pnl >= 0 else COLORS["error"],
                        ),
                        dict(
                            title="Total P&L",
                            value=_fmt_float(total_pnl),
                            subtitle="Realized + unrealized",
                            icon="query_stats",
                            color=COLORS["info"] if total_pnl >= 0 else COLORS["error"],
                        ),
                    ],
                    columns=3,
                )

                with ui.grid(columns=2).classes("w-full gap-4 mb-6"):
                    with ui.column().classes("kpi-card p-5 h-full"):
                        ui.label("Session").classes("text-lg font-semibold mb-3").style(
                            f"color: {THEME['text_primary']};"
                        )
                        for label, value in [
                            ("Session ID", session.get("session_id")),
                            ("Trade Date", session.get("trade_date")),
                            ("Strategy", session.get("strategy_name")),
                            ("Experiment", session.get("experiment_id") or "-"),
                            ("Updated", _fmt_ts(session.get("updated_at"))),
                        ]:
                            with ui.row().classes("justify-between w-full gap-4"):
                                ui.label(label).style(f"color: {THEME['text_secondary']};")
                                ui.label(str(value or "-")).classes("font-mono").style(
                                    f"color: {THEME['text_primary']};"
                                )

                    with ui.column().classes("kpi-card p-5 h-full"):
                        ui.label("Feed State").classes("text-lg font-semibold mb-3").style(
                            f"color: {THEME['text_primary']};"
                        )
                        for label, value in [
                            ("Source", feed_state.get("source")),
                            ("Mode", feed_state.get("mode")),
                            ("Status", feed_state.get("status")),
                            ("Subscriptions", feed_state.get("subscription_count")),
                            ("Heartbeat", _fmt_ts(feed_state.get("heartbeat_at"))),
                            ("Last Tick", _fmt_ts(feed_state.get("last_tick_at"))),
                        ]:
                            with ui.row().classes("justify-between w-full gap-4"):
                                ui.label(label).style(f"color: {THEME['text_secondary']};")
                                ui.label(str(value or "-")).classes("font-mono").style(
                                    f"color: {THEME['text_primary']};"
                                )

                divider()

                ui.label("Queue").classes("text-lg font-semibold mb-3").style(
                    f"color: {THEME['text_primary']};"
                )
                queue_rows = _queue_rows(signals)
                if queue_rows:
                    paginated_table(
                        rows=queue_rows,
                        columns=[
                            {"name": "rank", "label": "Rank", "field": "rank"},
                            {"name": "symbol", "label": "Symbol", "field": "symbol"},
                            {
                                "name": "decision_status",
                                "label": "Decision",
                                "field": "decision_status",
                            },
                            {
                                "name": "decision_reason",
                                "label": "Reason",
                                "field": "decision_reason",
                            },
                            {
                                "name": "selection_score",
                                "label": "Score",
                                "field": "selection_score",
                            },
                            {
                                "name": "backtest_status",
                                "label": "Backtest",
                                "field": "backtest_status",
                            },
                            {"name": "entry_price", "label": "Entry Px", "field": "entry_price"},
                            {"name": "entry_time", "label": "Entry Time", "field": "entry_time"},
                        ],
                        page_size=12,
                    )
                else:
                    empty_state(
                        "No queue rows",
                        "This session has no advisory queue yet.",
                        icon="queue",
                    )

                divider()

                with ui.grid(columns=2).classes("w-full gap-4"):
                    with ui.column().classes("w-full"):
                        ui.label("Positions").classes("text-lg font-semibold mb-3").style(
                            f"color: {THEME['text_primary']};"
                        )
                        position_rows = _position_rows(positions)
                        if position_rows:
                            paginated_table(
                                rows=position_rows,
                                columns=[
                                    {"name": "position_id", "label": "ID", "field": "position_id"},
                                    {
                                        "name": "symbol",
                                        "label": "Symbol",
                                        "field": "symbol",
                                    },
                                    {"name": "symbol_id", "label": "Symbol ID", "field": "symbol_id"},
                                    {"name": "qty", "label": "Qty", "field": "qty"},
                                    {
                                        "name": "avg_entry",
                                        "label": "Avg Entry",
                                        "field": "avg_entry",
                                    },
                                    {"name": "avg_exit", "label": "Avg Exit", "field": "avg_exit"},
                                    {"name": "pnl", "label": "PnL", "field": "pnl"},
                                    {
                                        "name": "market_price",
                                        "label": "Mkt Px",
                                        "field": "market_price",
                                    },
                                    {
                                        "name": "unrealized_pnl",
                                        "label": "Unrlzd PnL",
                                        "field": "unrealized_pnl",
                                    },
                                    {"name": "state", "label": "State", "field": "state"},
                                ],
                                page_size=8,
                            )
                        else:
                            empty_state(
                                "No positions",
                                "This session has not opened any positions yet.",
                                icon="work_off",
                            )

                    with ui.column().classes("w-full"):
                        ui.label("Orders").classes("text-lg font-semibold mb-3").style(
                            f"color: {THEME['text_primary']};"
                        )
                        order_rows = _order_rows(orders)
                        if order_rows:
                            paginated_table(
                                rows=order_rows,
                                columns=[
                                    {"name": "order_id", "label": "Order", "field": "order_id"},
                                    {"name": "signal_id", "label": "Signal", "field": "signal_id"},
                                    {"name": "side", "label": "Side", "field": "side"},
                                    {"name": "qty", "label": "Qty", "field": "qty"},
                                    {"name": "order_type", "label": "Type", "field": "order_type"},
                                    {"name": "status", "label": "Status", "field": "status"},
                                    {
                                        "name": "broker_status",
                                        "label": "Broker",
                                        "field": "broker_status",
                                    },
                                    {
                                        "name": "created_at",
                                        "label": "Created",
                                        "field": "created_at",
                                    },
                                ],
                                page_size=8,
                            )
                        else:
                            empty_state(
                                "No orders",
                                "Orders will appear here after replay/live execution.",
                                icon="shopping_cart",
                            )

                divider()

                with ui.grid(columns=2).classes("w-full gap-4"):
                    with ui.column().classes("w-full"):
                        ui.label("Fills").classes("text-lg font-semibold mb-3").style(
                            f"color: {THEME['text_primary']};"
                        )
                        fill_rows = _fill_rows(fills)
                        if fill_rows:
                            paginated_table(
                                rows=fill_rows,
                                columns=[
                                    {"name": "fill_id", "label": "Fill", "field": "fill_id"},
                                    {"name": "order_id", "label": "Order", "field": "order_id"},
                                    {"name": "qty", "label": "Qty", "field": "qty"},
                                    {"name": "fill_price", "label": "Price", "field": "fill_price"},
                                    {"name": "fees", "label": "Fees", "field": "fees"},
                                    {
                                        "name": "slippage_bps",
                                        "label": "Slip bps",
                                        "field": "slippage_bps",
                                    },
                                    {"name": "fill_time", "label": "Time", "field": "fill_time"},
                                ],
                                page_size=8,
                            )
                        else:
                            empty_state(
                                "No fills",
                                "Fills will appear after orders are processed.",
                                icon="done_outline",
                            )

                    with ui.column().classes("w-full"):
                        ui.label("Recent Events").classes("text-lg font-semibold mb-3").style(
                            f"color: {THEME['text_primary']};"
                        )
                        event_rows = _event_rows(events)
                        if event_rows:
                            paginated_table(
                                rows=event_rows,
                                columns=[
                                    {"name": "event_id", "label": "Event", "field": "event_id"},
                                    {"name": "event_type", "label": "Type", "field": "event_type"},
                                    {
                                        "name": "event_status",
                                        "label": "Status",
                                        "field": "event_status",
                                    },
                                    {"name": "order_id", "label": "Order", "field": "order_id"},
                                    {"name": "signal_id", "label": "Signal", "field": "signal_id"},
                                    {
                                        "name": "created_at",
                                        "label": "Created",
                                        "field": "created_at",
                                    },
                                ],
                                page_size=8,
                            )
                        else:
                            empty_state(
                                "No events",
                                "Feed and broker events will show up here.",
                                icon="event_note",
                            )

        async def auto_refresh() -> None:
            if not current["auto_refresh"]:
                return
            await render_session(current["session_id"])

        await render_session(current["session_id"])
        ui.timer(30.0, lambda: asyncio.create_task(auto_refresh()))
