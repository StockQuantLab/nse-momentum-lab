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
    color_error,
    color_info,
    color_success,
    color_warning,
    divider,
    empty_state,
    info_box,
    kpi_grid,
    page_layout,
    paginated_table,
    SPACE_GRID_DEFAULT,
    SPACE_MD,
    SPACE_SECTION,
    SPACE_SM,
    SPACE_XS,
    theme_surface_border,
    theme_surface_hover,
    theme_text_muted,
    theme_text_primary,
    theme_text_secondary,
)
from apps.nicegui.state import (
    aget_paper_positions,
    aget_paper_session_events,
    aget_paper_session_fills,
    aget_paper_session_orders,
    aget_paper_session_signals,
    aget_paper_session_summary,
    aget_paper_sessions,
)


def _fmt_float(value: Any, digits: int = 2) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):,.{digits}f}"
    except TypeError, ValueError:
        return str(value)


def _fmt_ts(value: Any) -> str:
    if not value:
        return "-"
    return str(value).replace("T", " ")[:19]


def _status_color(status: str | None) -> str:
    normalized = str(status or "").upper()
    if normalized in {"ACTIVE", "RUNNING", "CONNECTED", "READY", "COMPLETE", "COMPLETED", "PASS"}:
        return color_success()
    if normalized in {"PAUSED", "STOPPING", "PLANNING", "CONNECTING"}:
        return color_warning()
    if normalized in {"FAILED", "ERROR", "DISCONNECTED", "ARCHIVED", "CANCELLED"}:
        return color_error()
    return color_info()


def _title_case_words(value: str) -> str:
    return " ".join(part.capitalize() for part in value.replace("_", " ").split())


def _mode_label(mode: Any) -> str:
    normalized = str(mode or "").strip().lower()
    labels = {
        "walk_forward": "Walk-Forward Check",
        "replay": "Replay Session",
        "live": "Live Session",
        "stream": "Live Feed",
        "planning": "Planning",
    }
    return labels.get(normalized, _title_case_words(normalized) if normalized else "-")


def _status_label(status: Any) -> str:
    normalized = str(status or "").strip().lower()
    labels = {
        "failed": "Needs Review",
        "pass": "Passed",
        "completed": "Completed",
        "running": "Running",
        "active": "Active",
        "paused": "Paused",
        "ready": "Ready",
        "error": "Error",
        "unavailable": "Not Running",
    }
    return labels.get(normalized, _title_case_words(normalized) if normalized else "-")


def _feed_label(feed_state: dict[str, Any]) -> tuple[str, str]:
    status = str(feed_state.get("status") or "").strip()
    source = str(feed_state.get("source") or "").strip()
    mode = str(feed_state.get("mode") or "").strip()
    if not status and not source and not mode:
        return ("Not Running", "No live or replay feed is attached to this session")
    status_label = _status_label(status)
    subtitle_bits = [
        bit for bit in [_title_case_words(source), _mode_label(mode)] if bit and bit != "-"
    ]
    subtitle = " / ".join(subtitle_bits) if subtitle_bits else "Feed details available"
    return (status_label, subtitle)


def _decision_status_label(status: Any) -> str:
    normalized = str(status or "").strip().upper()
    labels = {
        "PASS": "Ready for Promotion",
        "FAIL": "Needs Review",
    }
    return labels.get(normalized, _title_case_words(normalized.lower()) if normalized else "-")


def _decision_reason_label(reason: Any) -> str:
    normalized = str(reason or "").strip()
    if not normalized:
        return "-"
    if normalized == "all_thresholds_met":
        return "All walk-forward checks passed"
    reason_map = {
        "no_folds": "No test windows were generated",
        "incomplete_folds": "One or more test windows did not finish",
        "non_positive_average_return": "Average return was not positive",
        "insufficient_profitable_folds": "Fewer than half of test windows were profitable",
        "excessive_drawdown": "Drawdown was above the allowed limit",
    }
    parts = [
        reason_map.get(part.strip(), _title_case_words(part.strip()))
        for part in normalized.split(",")
    ]
    return "; ".join(part for part in parts if part)


def _session_summary_text(
    session: dict[str, Any], counts: dict[str, Any], feed_state: dict[str, Any]
) -> str:
    mode = str(session.get("mode") or "").strip().lower()
    if mode == "replay":
        return (
            "This is a replay session. It re-runs a past trading day using local market data so "
            "you can observe the paper-trading flow without connecting to live prices."
        )
    if mode == "live":
        return (
            "This is a live paper-trading session. Watch the feed, trade watchlist, orders, fills, "
            "and open positions here while the session is running."
        )
    queue_signals = int(counts.get("queue_signals", 0) or 0)
    feed_value, _ = _feed_label(feed_state)
    return (
        f"This session is currently shown as {_mode_label(mode)}. "
        f"Feed: {feed_value}. Watchlist items: {queue_signals}."
    )


def _strategy_label(strategy_name: Any, strategy_params: dict[str, Any] | None = None) -> str:
    raw_strategy = str(strategy_name or "strategy?")
    normalized = raw_strategy.strip().lower().replace("_", "")
    labels = {
        "2lynchbreakout": "2LYNCH Breakout",
        "thresholdbreakout": "2LYNCH Breakout",
        "2lynchbreakdown": "2LYNCH Breakdown",
        "thresholdbreakdown": "2LYNCH Breakdown",
    }
    strategy = labels.get(normalized, _title_case_words(raw_strategy))
    params = strategy_params or {}
    threshold = params.get("breakout_threshold")
    if threshold is None:
        return strategy
    try:
        pct = round(float(threshold) * 100)
    except TypeError, ValueError:
        return strategy
    return f"{strategy} {pct}%"


def _session_label(session: dict[str, Any]) -> str:
    trade_date = session.get("trade_date") or "na"
    strategy = _strategy_label(session.get("strategy_name"), session.get("strategy_params") or {})
    status = _status_label(session.get("status"))
    mode = _mode_label(session.get("mode"))
    session_id = str(session.get("session_id") or "-")
    return f"{trade_date} | {strategy} | {mode} | {status} | {session_id}"


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
                "status": _title_case_words(str(fold.get("status") or "-")),
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
            "status": _title_case_words(str(fold.get("status") or "-")),
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
        with ui.column().classes(f"kpi-card p-6 {SPACE_SECTION}"):
            ui.label("Paper Session Ledger").classes(f"text-xl font-semibold {SPACE_SM}").style(
                f"color: {theme_text_primary()};"
            )
            ui.label(
                "This page reads live paper-session state from PostgreSQL so replay and live "
                "sessions can be observed without leaving the dashboard."
            ).classes(SPACE_MD).style(f"color: {theme_text_secondary()};")
            ui.label(
                "Use the separate /walk_forward page for validation history and reruns. "
                "Load an approved experiment/date here, execute replay or live once, and "
                "monitor feed, queue, orders, fills, and positions."
            ).style(f"color: {theme_text_muted()};")

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

        paper_sessions = [
            session
            for session in sessions
            if str(session.get("mode") or "").strip().lower() != "walk_forward"
        ]
        if not paper_sessions:
            empty_state(
                "No paper sessions yet",
                "Create a replay or live paper session first. Walk-forward checks now live on the separate validation page.",
                icon="receipt_long",
            )
            return

        ordered_sessions = sorted(
            paper_sessions,
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

        with ui.row().classes(f"items-center {SPACE_MD} {SPACE_SECTION} w-full"):
            ui.select(
                options=list(option_map.keys()),
                value=current["label"],
                label="Paper Session",
                on_change=handle_session_change,
            ).classes("flex-1 min-w-[860px]")
            ui.button(
                "Refresh",
                icon="refresh",
                on_click=lambda: asyncio.create_task(render_session(current["session_id"])),
            ).props("outline")
            ui.label("Auto-refresh every 30s when active").classes("text-sm").style(
                f"color: {theme_text_muted()};"
            )

        commands_card = ui.column().classes(f"kpi-card p-5 {SPACE_SECTION}")
        content = ui.column().classes("w-full")

        def render_dynamic_panels(session: dict[str, Any]) -> None:
            strategy_params = session.get("strategy_params") or {}
            strategy = str(session.get("strategy_name") or "thresholdbreakout")
            strategy_display = _strategy_label(strategy, strategy_params)
            trade_date = str(session.get("trade_date") or "<TRADE_DATE>")
            exp_id = str(session.get("experiment_id") or "<EXP_ID>")
            commands_card.clear()
            with commands_card:
                with ui.expansion("Commands", icon="terminal").classes("w-full"):
                    ui.label(
                        f"Suggested commands for the selected session ({strategy_display})."
                    ).classes(f"text-sm {SPACE_MD}").style(f"color: {theme_text_secondary()};")
                    for command in [
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
                        ui.label(command).classes(
                            f"font-mono text-sm px-3 py-2 rounded {SPACE_XS}"
                        ).style(
                            f"background: {theme_surface_hover()}; border: 1px solid {theme_surface_border()}; color: {theme_text_primary()}; border-radius: 6px;"
                        )

        async def render_session(session_id: str) -> None:
            content.clear()
            with content:
                try:
                    (
                        summary,
                        signals,
                        orders,
                        fills,
                        events,
                        positions,
                    ) = await asyncio.gather(
                        aget_paper_session_summary(session_id),
                        aget_paper_session_signals(session_id),
                        aget_paper_session_orders(session_id, limit=100),
                        aget_paper_session_fills(session_id, limit=100),
                        aget_paper_session_events(session_id, limit=100),
                        aget_paper_positions(session_id, open_only=False),
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
                feed_value, feed_subtitle = _feed_label(feed_state)
                session_color = _status_color(session.get("status"))
                feed_color = _status_color(feed_state.get("status"))
                current["auto_refresh"] = str(session.get("status") or "").upper() in {
                    "ACTIVE",
                    "RUNNING",
                }
                render_dynamic_panels(session)

                realized_pnl = sum(
                    float(row.get("pnl") or 0.0) for row in positions if row.get("pnl") is not None
                )
                unrealized_pnl = sum(
                    float(row.get("unrealized_pnl") or 0.0)
                    for row in positions
                    if row.get("unrealized_pnl") is not None
                )
                total_pnl = realized_pnl + unrealized_pnl

                with ui.column().classes(f"kpi-card p-5 {SPACE_SECTION}"):
                    ui.label("What This Session Means").classes(
                        f"text-lg font-semibold {SPACE_SM}"
                    ).style(f"color: {theme_text_primary()};")
                    ui.label(_session_summary_text(session, counts, feed_state)).classes(
                        "leading-6"
                    ).style(f"color: {theme_text_secondary()};")

                kpi_grid(
                    [
                        dict(
                            title="Session Status",
                            value=_status_label(session.get("status")),
                            subtitle=_mode_label(session.get("mode")),
                            icon="schedule",
                            color=session_color,
                        ),
                        dict(
                            title="Queued Signals",
                            value=int(counts.get("queue_signals", 0)),
                            subtitle=f"Open signals: {int(counts.get('open_signals', 0))}",
                            icon="queue",
                            color=color_info(),
                        ),
                        dict(
                            title="Open Positions",
                            value=int(counts.get("open_positions", 0)),
                            subtitle=f"Orders: {int(counts.get('orders', 0))}",
                            icon="work",
                            color=color_warning(),
                        ),
                        dict(
                            title="Fills",
                            value=int(counts.get("fills", 0)),
                            subtitle=f"Signals tracked: {int(counts.get('signals', 0))}",
                            icon="done_all",
                            color=color_success(),
                        ),
                        dict(
                            title="Feed",
                            value=feed_value,
                            subtitle=feed_subtitle,
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
                            color=color_success() if realized_pnl >= 0 else color_error(),
                        ),
                        dict(
                            title="Unrealized P&L",
                            value=_fmt_float(unrealized_pnl),
                            subtitle="Open positions MTM",
                            icon="show_chart",
                            color=color_warning() if unrealized_pnl >= 0 else color_error(),
                        ),
                        dict(
                            title="Total P&L",
                            value=_fmt_float(total_pnl),
                            subtitle="Realized + unrealized",
                            icon="query_stats",
                            color=color_info() if total_pnl >= 0 else color_error(),
                        ),
                    ],
                    columns=3,
                )

                with ui.expansion("Session Details", icon="info").classes(
                    f"kpi-card p-5 {SPACE_SECTION} w-full"
                ):
                    with ui.grid(columns=2).classes(f"w-full {SPACE_GRID_DEFAULT}"):
                        with ui.column().classes("h-full"):
                            ui.label("Session").classes(f"text-lg font-semibold {SPACE_MD}").style(
                                f"color: {theme_text_primary()};"
                            )
                            for label, value in [
                                ("Internal Session ID", session.get("session_id")),
                                ("Trade Date", session.get("trade_date")),
                                (
                                    "Strategy",
                                    _strategy_label(
                                        session.get("strategy_name"),
                                        session.get("strategy_params") or {},
                                    ),
                                ),
                                ("Session Type", _mode_label(session.get("mode"))),
                                ("Experiment ID", session.get("experiment_id") or "-"),
                                ("Updated", _fmt_ts(session.get("updated_at"))),
                            ]:
                                with ui.row().classes(
                                    f"justify-between w-full {SPACE_GRID_DEFAULT}"
                                ):
                                    ui.label(label).style(f"color: {theme_text_secondary()};")
                                    ui.label(str(value or "-")).classes("font-mono").style(
                                        f"color: {theme_text_primary()};"
                                    )

                        with ui.column().classes("h-full"):
                            ui.label("Feed Details").classes(
                                f"text-lg font-semibold {SPACE_MD}"
                            ).style(f"color: {theme_text_primary()};")
                            for label, value in [
                                (
                                    "Source",
                                    _title_case_words(str(feed_state.get("source") or "")) or "-",
                                ),
                                ("Mode", _mode_label(feed_state.get("mode"))),
                                ("Status", _status_label(feed_state.get("status"))),
                                ("Subscriptions", feed_state.get("subscription_count")),
                                ("Heartbeat", _fmt_ts(feed_state.get("heartbeat_at"))),
                                ("Last Tick", _fmt_ts(feed_state.get("last_tick_at"))),
                            ]:
                                with ui.row().classes(
                                    f"justify-between w-full {SPACE_GRID_DEFAULT}"
                                ):
                                    ui.label(label).style(f"color: {theme_text_secondary()};")
                                    ui.label(str(value or "-")).classes("font-mono").style(
                                        f"color: {theme_text_primary()};"
                                    )

                divider()

                ui.label("Trade Watchlist").classes(f"text-lg font-semibold {SPACE_MD}").style(
                    f"color: {theme_text_primary()};"
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
                        "No watchlist items",
                        "This session does not currently have any signals waiting for review.",
                        icon="queue",
                    )

                divider()

                with ui.grid(columns=2).classes(f"w-full {SPACE_GRID_DEFAULT}"):
                    with ui.column().classes("w-full"):
                        ui.label("Positions").classes(f"text-lg font-semibold {SPACE_MD}").style(
                            f"color: {theme_text_primary()};"
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
                                    {
                                        "name": "symbol_id",
                                        "label": "Symbol ID",
                                        "field": "symbol_id",
                                    },
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
                        ui.label("Orders").classes(f"text-lg font-semibold {SPACE_MD}").style(
                            f"color: {theme_text_primary()};"
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

                with ui.grid(columns=2).classes(f"w-full {SPACE_GRID_DEFAULT}"):
                    with ui.column().classes("w-full"):
                        ui.label("Fills").classes(f"text-lg font-semibold {SPACE_MD}").style(
                            f"color: {theme_text_primary()};"
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
                        ui.label("Recent Activity").classes(
                            f"text-lg font-semibold {SPACE_MD}"
                        ).style(f"color: {theme_text_primary()};")
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
                                "No recent activity",
                                "Feed and broker updates will show up here.",
                                icon="event_note",
                            )

        async def auto_refresh() -> None:
            if not current["auto_refresh"]:
                return
            await render_session(current["session_id"])

        await render_session(current["session_id"])
        ui.timer(30.0, lambda: asyncio.create_task(auto_refresh()))
