"""Paper Ledger v2 page — DuckDB-backed paper trading dashboard.

Reads from the paper_dashboard.duckdb replica so the live writer is never blocked.
Shows active sessions with positions/orders/fills/signals and archived sessions
with risk metrics, trade ledger, and equity curve.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
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

import plotly.graph_objects as go
from nicegui import ui

from apps.nicegui.components import (
    apply_chart_theme,
    color_error,
    color_gray,
    color_info,
    color_primary,
    color_success,
    color_warning,
    empty_state,
    hex_to_rgba,
    kpi_grid,
    page_layout,
    paginated_table,
    safe_timer,
    SPACE_LG,
    SPACE_MD,
    theme_text_muted,
    theme_text_primary,
)
from nse_momentum_lab.db.versioned_replica_consumer import VersionedReplicaConsumer

ACTIVE_STATUSES = {"PLANNED", "ACTIVE", "PAUSED", "RUNNING", "STOPPING"}
ARCHIVED_STATUSES = {"COMPLETED", "FAILED", "ARCHIVED", "CANCELLED"}


def _get_consumer() -> VersionedReplicaConsumer:
    return VersionedReplicaConsumer(
        replica_dir=_project_root / "data" / "paper_replica",
        prefix="paper_replica",
        fallback_path=_project_root / "data" / "paper_dashboard.duckdb",
    )


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except TypeError, ValueError:
        return None


def _fmt(value: Any, digits: int = 2) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):,.{digits}f}"
    except TypeError, ValueError:
        return str(value)


def _strategy_label(strategy: str | None) -> str:
    labels = {
        "2lynchbreakout": "2LYNCH Breakout",
        "thresholdbreakout": "2LYNCH Breakout",
        "2lynchbreakdown": "2LYNCH Breakdown",
        "episodicpivot": "EP Pivot",
    }
    raw = str(strategy or "").strip().lower()
    return labels.get(raw, str(strategy or "-"))


def _direction_cell(direction: str) -> str:
    return str(direction or "-").upper()


def _fmt_qty(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{int(value):,}"
    except TypeError, ValueError:
        return str(value)


def _pnl_color(value: float | None) -> str:
    if value is None:
        return color_gray()
    return color_success() if value >= 0 else color_error()


def _parse_metadata(raw: Any) -> dict:
    if not raw or raw == "{}":
        return {}
    try:
        if isinstance(raw, dict):
            return raw
        return json.loads(str(raw)) if isinstance(raw, str) else {}
    except TypeError, ValueError:
        return {}


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


def _load_sessions(consumer: VersionedReplicaConsumer, statuses: set[str]) -> list[dict]:
    rows = consumer.execute("SELECT * FROM paper_sessions ORDER BY updated_at DESC")
    if not rows:
        return []
    return [r for r in rows if str(r.get("status", "")).upper() in statuses]


def _load_positions(consumer: VersionedReplicaConsumer, session_id: str) -> list[dict]:
    return (
        consumer.execute(
            "SELECT * FROM paper_positions WHERE session_id = ? ORDER BY opened_at DESC",
            [session_id],
        )
        or []
    )


def _load_orders(consumer: VersionedReplicaConsumer, session_id: str) -> list[dict]:
    return (
        consumer.execute(
            "SELECT * FROM paper_orders WHERE session_id = ? ORDER BY created_at DESC",
            [session_id],
        )
        or []
    )


def _load_fills(consumer: VersionedReplicaConsumer, session_id: str) -> list[dict]:
    return (
        consumer.execute(
            "SELECT * FROM paper_fills WHERE session_id = ? ORDER BY fill_time DESC",
            [session_id],
        )
        or []
    )


def _load_alerts(
    consumer: VersionedReplicaConsumer, session_id: str, limit: int = 50
) -> list[dict]:
    return (
        consumer.execute(
            "SELECT * FROM alert_log WHERE session_id = ? ORDER BY sent_at DESC LIMIT ?",
            [session_id, limit],
        )
        or []
    )


def _load_signals(consumer: VersionedReplicaConsumer, session_id: str) -> list[dict]:
    return (
        consumer.execute(
            """SELECT s.signal_id, s.symbol, s.asof_date, s.state, s.entry_mode,
                  s.initial_stop, ss.rank, ss.selection_score,
                  ss.decision_status, ss.decision_reason
           FROM paper_signals s
           LEFT JOIN paper_session_signals ss
             ON s.signal_id = ss.signal_id AND ss.session_id = ?
           WHERE s.session_id = ?
           ORDER BY s.asof_date DESC, ss.rank ASC""",
            [session_id, session_id],
        )
        or []
    )


def _load_feed_state(consumer: VersionedReplicaConsumer, session_id: str) -> dict:
    rows = consumer.execute("SELECT * FROM paper_feed_state WHERE session_id = ?", [session_id])
    return rows[0] if rows else {}


def _compute_pnl(positions: list[dict]) -> dict[str, float]:
    realized = unrealized = 0.0
    for p in positions:
        state = str(p.get("state", "")).upper()
        if state == "CLOSED":
            pnl = float(p.get("pnl", 0) or 0)
            realized += pnl
        elif state == "OPEN":
            md = _parse_metadata(p.get("metadata_json"))
            avg_entry = _to_float(p.get("avg_entry"))
            qty = _to_float(p.get("qty"))
            mark = _to_float(md.get("last_mark_price"))
            direction = str(p.get("direction", "LONG")).upper()
            if avg_entry is not None and qty is not None and mark is not None:
                if direction == "SHORT":
                    unrealized += (avg_entry - mark) * qty
                else:
                    unrealized += (mark - avg_entry) * qty
            else:
                unrealized += float(p.get("pnl", 0) or 0)
    return {"realized": realized, "unrealized": unrealized, "total": realized + unrealized}


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------


def _position_rows(positions: list[dict]) -> list[dict]:
    rows = []
    for p in positions:
        md = _parse_metadata(p.get("metadata_json"))
        rows.append(
            {
                "symbol": p.get("symbol", "-"),
                "direction": _direction_cell(p.get("direction", "-")),
                "state": p.get("state", "-"),
                "qty": _fmt_qty(p.get("qty")),
                "avg_entry": _fmt(p.get("avg_entry")),
                "avg_exit": _fmt(p.get("avg_exit")),
                "current_price": _fmt(md.get("last_mark_price")),
                "pnl": _fmt(p.get("pnl")),
                "initial_sl": _fmt(md.get("initial_sl")),
                "target": _fmt(md.get("target")),
                "exit_reason": md.get("exit_reason", "-"),
                "opened_at": str(p.get("opened_at", "-"))[:19],
                "closed_at": str(p.get("closed_at", "-"))[:19] if p.get("closed_at") else "-",
            }
        )
    return rows


def _order_rows(orders: list[dict]) -> list[dict]:
    return [
        {
            "order_id": str(o.get("order_id", "-"))[:12],
            "symbol": o.get("symbol", "-"),
            "side": o.get("side", "-"),
            "status": o.get("status", "-"),
            "order_type": o.get("order_type", "-"),
            "qty": _fmt_qty(o.get("qty")),
            "limit_price": _fmt(o.get("limit_price")),
            "created_at": str(o.get("created_at", "-"))[:19],
        }
        for o in orders
    ]


def _fill_rows(fills: list[dict]) -> list[dict]:
    return [
        {
            "symbol": f.get("symbol", "-"),
            "side": f.get("side", "-"),
            "qty": _fmt_qty(f.get("qty")),
            "price": _fmt(f.get("fill_price")),
            "pnl": _fmt(f.get("pnl")),
            "slippage": _fmt(f.get("slippage_bps")),
            "fees": _fmt(f.get("fees")),
            "fill_time": str(f.get("fill_time", "-"))[:19],
        }
        for f in fills
    ]


def _signal_rows(signals: list[dict]) -> list[dict]:
    return [
        {
            "symbol": s.get("symbol", "-"),
            "asof_date": str(s.get("asof_date", "-")),
            "state": s.get("state", "-"),
            "entry_mode": str(s.get("entry_mode", "-")).upper(),
            "initial_stop": _fmt(s.get("initial_stop")),
            "rank": s.get("rank", "-"),
            "score": _fmt(s.get("selection_score")),
            "decision": s.get("decision_status", "-"),
        }
        for s in signals
    ]


def _alert_rows(alerts: list[dict]) -> list[dict]:
    return [
        {
            "type": a.get("alert_type", "-"),
            "level": a.get("alert_level", "-"),
            "status": a.get("status", "-"),
            "channel": a.get("channel", "-"),
            "subject": str(a.get("subject", a.get("payload", "-")))[:80],
            "sent_at": str(a.get("sent_at", "-"))[:19],
        }
        for a in alerts
    ]


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------


async def paper_ledger_v2_page() -> None:
    """Paper Ledger v2 page — reads from DuckDB replica."""
    with page_layout("Paper Ledger", "receipt_long"):
        consumer = _get_consumer()

        # Staleness indicator
        stale_sec = consumer.get_stale_seconds()
        if stale_sec is not None and stale_sec > 60:
            ui.label(f"Replica is {stale_sec:.0f}s behind live").classes(
                f"text-sm {theme_text_muted()}"
            )

        # Main tabs: Active / Archived
        with ui.tabs().classes("w-full") as tabs:
            tab_active = ui.tab("Active Sessions")
            tab_archived = ui.tab("Archived Sessions")

        with ui.tab_panels(tabs, value=tab_active).classes("w-full"):
            # ═══════════════════════════════════════════════════════════
            # ACTIVE SESSIONS TAB
            # ═══════════════════════════════════════════════════════════
            with ui.tab_panel(tab_active):
                active_sessions = _load_sessions(consumer, ACTIVE_STATUSES)
                if not active_sessions:
                    empty_state(
                        "No Active Sessions",
                        "Create one with nseml-paper prepare",
                        icon="receipt_long",
                    )
                else:
                    session_options = {
                        s.get("session_id", ""): _session_label(s) for s in active_sessions
                    }
                    active_sid = active_sessions[0].get("session_id", "")

                    select = ui.select(
                        options=session_options,
                        value=active_sid,
                        with_input=True,
                        label="Session",
                    ).classes("w-full")

                    kpi_container = ui.column().classes("w-full")
                    content = ui.column().classes("w-full")

                    auto_refresh = ui.checkbox("Auto-refresh (30s)", value=True)

                    def render_active() -> None:
                        sid = select.value
                        session = None
                        fresh_sessions = _load_sessions(consumer, ACTIVE_STATUSES)
                        if not fresh_sessions:
                            return

                        fresh_options = {
                            s.get("session_id", ""): _session_label(s) for s in fresh_sessions
                        }
                        if select.options != fresh_options:
                            select.options = fresh_options
                            if sid not in fresh_options:
                                sid = fresh_sessions[0].get("session_id", "")
                                select.value = sid
                            select.update()

                        if not sid:
                            return

                        for s in fresh_sessions:
                            if s.get("session_id") == sid:
                                session = s
                                break
                        if session is None:
                            return

                        positions = _load_positions(consumer, sid)
                        pnl = _compute_pnl(positions)
                        fills = _load_fills(consumer, sid)
                        orders = _load_orders(consumer, sid)
                        alerts = _load_alerts(consumer, sid)
                        signals = _load_signals(consumer, sid)
                        feed = _load_feed_state(consumer, sid)

                        open_count = sum(
                            1 for p in positions if str(p.get("state", "")).upper() == "OPEN"
                        )
                        closed_count = sum(
                            1 for p in positions if str(p.get("state", "")).upper() == "CLOSED"
                        )

                        feed_status = str(feed.get("status", "-")).upper()
                        feed_stale = feed.get("is_stale", False)
                        feed_healthy = feed_status == "CONNECTED" and not feed_stale

                        kpi_container.clear()
                        content.clear()

                        with kpi_container:
                            # Metadata chips
                            with ui.row().classes("items-center gap-2 mb-4 flex-wrap"):
                                ui.badge(
                                    f"ID: {str(session.get('session_id', '-'))[:12]}",
                                    color=color_gray(),
                                ).props("outline")
                                ui.badge(
                                    _strategy_label(session.get("strategy_name")),
                                    color=color_info(),
                                ).props("outline")
                                ui.badge(
                                    f"Mode: {session.get('mode', '-')}",
                                    color=color_primary(),
                                ).props("outline")
                                ui.badge(
                                    f"Feed: {feed.get('source', '-')}",
                                    color=color_warning(),
                                ).props("outline")
                                last_candle = feed.get("last_bar_at")
                                if last_candle:
                                    ui.badge(
                                        f"Last: {str(last_candle)[:19]}",
                                        color=color_gray(),
                                    ).props("outline")

                            # 8 KPI cards
                            kpi_grid(
                                [
                                    dict(
                                        title="Status",
                                        value=str(session.get("status", "-")).upper(),
                                        icon="info",
                                        color=color_success()
                                        if str(session.get("status", "")).upper() == "ACTIVE"
                                        else color_warning(),
                                    ),
                                    dict(
                                        title="Feed Health",
                                        value="Healthy" if feed_healthy else feed_status,
                                        subtitle="Stale" if feed_stale else None,
                                        icon="wifi",
                                        color=color_success() if feed_healthy else color_error(),
                                    ),
                                    dict(
                                        title="Open Positions",
                                        value=str(open_count),
                                        icon="open_in_new",
                                        color=color_primary(),
                                    ),
                                    dict(
                                        title="Closed Positions",
                                        value=str(closed_count),
                                        icon="done_all",
                                        color=color_info(),
                                    ),
                                    dict(
                                        title="Realized P&L",
                                        value=_fmt(pnl["realized"]),
                                        icon="account_balance_wallet",
                                        color=_pnl_color(pnl["realized"]),
                                    ),
                                    dict(
                                        title="Unrealized P&L",
                                        value=_fmt(pnl["unrealized"]),
                                        icon="trending_up",
                                        color=_pnl_color(pnl["unrealized"]),
                                    ),
                                    dict(
                                        title="Total P&L",
                                        value=_fmt(pnl["total"]),
                                        icon="payments",
                                        color=_pnl_color(pnl["total"]),
                                    ),
                                    dict(
                                        title="Fills",
                                        value=str(len(fills)),
                                        icon="receipt",
                                        color=color_gray(),
                                    ),
                                ],
                                columns=4,
                            )

                        with content:
                            # Positions (13 cols)
                            ui.label("Positions").classes(
                                f"text-lg font-semibold {SPACE_MD}"
                            ).style(f"color: {theme_text_primary()};")
                            pos_cols = [
                                {
                                    "name": "symbol",
                                    "label": "Symbol",
                                    "field": "symbol",
                                    "sortable": True,
                                },
                                {"name": "direction", "label": "Dir", "field": "direction"},
                                {
                                    "name": "state",
                                    "label": "State",
                                    "field": "state",
                                    "sortable": True,
                                },
                                {"name": "qty", "label": "Qty", "field": "qty", "sortable": True},
                                {
                                    "name": "avg_entry",
                                    "label": "Entry",
                                    "field": "avg_entry",
                                    "sortable": True,
                                },
                                {
                                    "name": "current_price",
                                    "label": "Current",
                                    "field": "current_price",
                                    "sortable": True,
                                },
                                {
                                    "name": "avg_exit",
                                    "label": "Exit",
                                    "field": "avg_exit",
                                    "sortable": True,
                                },
                                {"name": "pnl", "label": "P&L", "field": "pnl", "sortable": True},
                                {"name": "initial_sl", "label": "Stop", "field": "initial_sl"},
                                {"name": "target", "label": "Target", "field": "target"},
                                {
                                    "name": "exit_reason",
                                    "label": "Exit Reason",
                                    "field": "exit_reason",
                                },
                                {
                                    "name": "opened_at",
                                    "label": "Opened",
                                    "field": "opened_at",
                                    "sortable": True,
                                },
                                {
                                    "name": "closed_at",
                                    "label": "Closed",
                                    "field": "closed_at",
                                    "sortable": True,
                                },
                            ]
                            paginated_table(_position_rows(positions), pos_cols, page_size=10)

                            ui.space().classes(SPACE_LG)

                            # Orders table
                            if orders:
                                ui.label("Orders").classes(
                                    f"text-lg font-semibold {SPACE_MD}"
                                ).style(f"color: {theme_text_primary()};")
                                ord_cols = [
                                    {"name": "order_id", "label": "Order ID", "field": "order_id"},
                                    {
                                        "name": "symbol",
                                        "label": "Symbol",
                                        "field": "symbol",
                                        "sortable": True,
                                    },
                                    {"name": "side", "label": "Side", "field": "side"},
                                    {"name": "status", "label": "Status", "field": "status"},
                                    {"name": "order_type", "label": "Type", "field": "order_type"},
                                    {"name": "qty", "label": "Qty", "field": "qty"},
                                    {
                                        "name": "limit_price",
                                        "label": "Limit",
                                        "field": "limit_price",
                                    },
                                    {
                                        "name": "created_at",
                                        "label": "Created",
                                        "field": "created_at",
                                        "sortable": True,
                                    },
                                ]
                                paginated_table(_order_rows(orders), ord_cols, page_size=10)
                                ui.space().classes(SPACE_LG)

                            # Fills table
                            ui.label("Fills").classes(f"text-lg font-semibold {SPACE_MD}").style(
                                f"color: {theme_text_primary()};"
                            )
                            fill_cols = [
                                {
                                    "name": "symbol",
                                    "label": "Symbol",
                                    "field": "symbol",
                                    "sortable": True,
                                },
                                {"name": "side", "label": "Side", "field": "side"},
                                {"name": "qty", "label": "Qty", "field": "qty"},
                                {
                                    "name": "price",
                                    "label": "Price",
                                    "field": "price",
                                    "sortable": True,
                                },
                                {"name": "pnl", "label": "P&L", "field": "pnl", "sortable": True},
                                {"name": "slippage", "label": "Slip (bps)", "field": "slippage"},
                                {"name": "fees", "label": "Fees", "field": "fees"},
                                {
                                    "name": "fill_time",
                                    "label": "Time",
                                    "field": "fill_time",
                                    "sortable": True,
                                },
                            ]
                            paginated_table(_fill_rows(fills), fill_cols, page_size=10)

                            # Signals table
                            if signals:
                                ui.space().classes(SPACE_LG)
                                ui.label("Signals").classes(
                                    f"text-lg font-semibold {SPACE_MD}"
                                ).style(f"color: {theme_text_primary()};")
                                sig_cols = [
                                    {
                                        "name": "symbol",
                                        "label": "Symbol",
                                        "field": "symbol",
                                        "sortable": True,
                                    },
                                    {
                                        "name": "asof_date",
                                        "label": "Date",
                                        "field": "asof_date",
                                        "sortable": True,
                                    },
                                    {"name": "state", "label": "State", "field": "state"},
                                    {"name": "entry_mode", "label": "Mode", "field": "entry_mode"},
                                    {
                                        "name": "initial_stop",
                                        "label": "Stop",
                                        "field": "initial_stop",
                                    },
                                    {
                                        "name": "rank",
                                        "label": "Rank",
                                        "field": "rank",
                                        "sortable": True,
                                    },
                                    {
                                        "name": "score",
                                        "label": "Score",
                                        "field": "score",
                                        "sortable": True,
                                    },
                                    {"name": "decision", "label": "Decision", "field": "decision"},
                                ]
                                paginated_table(_signal_rows(signals), sig_cols, page_size=10)

                            # Alerts table
                            if alerts:
                                ui.space().classes(SPACE_LG)
                                ui.label("Alerts").classes(
                                    f"text-lg font-semibold {SPACE_MD}"
                                ).style(f"color: {theme_text_primary()};")
                                alert_cols = [
                                    {
                                        "name": "type",
                                        "label": "Type",
                                        "field": "type",
                                        "sortable": True,
                                    },
                                    {"name": "level", "label": "Level", "field": "level"},
                                    {"name": "status", "label": "Status", "field": "status"},
                                    {"name": "channel", "label": "Channel", "field": "channel"},
                                    {"name": "subject", "label": "Subject", "field": "subject"},
                                    {
                                        "name": "sent_at",
                                        "label": "Time",
                                        "field": "sent_at",
                                        "sortable": True,
                                    },
                                ]
                                paginated_table(_alert_rows(alerts), alert_cols, page_size=10)

                    select.on_value_change(lambda: render_active())
                    render_active()

                    # Auto-refresh timer with toggle
                    def _refresh_if_enabled() -> None:
                        if auto_refresh.value:
                            render_active()

                    safe_timer(30, _refresh_if_enabled, once=False)

            # ═══════════════════════════════════════════════════════════
            # ARCHIVED SESSIONS TAB
            # ═══════════════════════════════════════════════════════════
            with ui.tab_panel(tab_archived):
                archived_sessions = _load_sessions(consumer, ARCHIVED_STATUSES)
                if not archived_sessions:
                    empty_state(
                        "No Archived Sessions",
                        "Completed or stopped sessions will appear here.",
                        icon="archive",
                    )
                else:
                    arch_options = {
                        s.get("session_id", ""): _session_label(s) for s in archived_sessions
                    }
                    arch_sid = archived_sessions[0].get("session_id", "")

                    arch_select = ui.select(
                        options=arch_options,
                        value=arch_sid,
                        with_input=True,
                        label="Archived Session",
                    ).classes("w-full")

                    arch_kpi = ui.column().classes("w-full")
                    arch_content = ui.column().classes("w-full")

                    def render_archived() -> None:
                        sid = arch_select.value
                        fresh_archived = _load_sessions(consumer, ARCHIVED_STATUSES)
                        if not fresh_archived:
                            return

                        fresh_arch_options = {
                            s.get("session_id", ""): _session_label(s) for s in fresh_archived
                        }
                        if arch_select.options != fresh_arch_options:
                            arch_select.options = fresh_arch_options
                            if sid not in fresh_arch_options:
                                sid = fresh_archived[0].get("session_id", "")
                                arch_select.value = sid
                            arch_select.update()

                        if not sid:
                            return

                        session = None
                        for s in fresh_archived:
                            if s.get("session_id") == sid:
                                session = s
                                break
                        if session is None:
                            return

                        positions = _load_positions(consumer, sid)
                        closed_positions = [
                            p for p in positions if str(p.get("state", "")).upper() == "CLOSED"
                        ]

                        total_trades = len(closed_positions)
                        winners = [p for p in closed_positions if float(p.get("pnl", 0) or 0) > 0]
                        losers = [p for p in closed_positions if float(p.get("pnl", 0) or 0) <= 0]
                        win_rate = len(winners) / max(total_trades, 1) * 100
                        total_pnl = sum(float(p.get("pnl", 0) or 0) for p in closed_positions)
                        gross_profit = sum(float(p.get("pnl", 0) or 0) for p in winners)
                        gross_loss = abs(sum(float(p.get("pnl", 0) or 0) for p in losers))
                        profit_factor = gross_profit / max(gross_loss, 1)

                        # Max drawdown from cumulative P/L
                        cum_pnl = 0.0
                        peak = 0.0
                        max_dd = 0.0
                        for p in sorted(closed_positions, key=lambda x: x.get("closed_at", "")):
                            cum_pnl += float(p.get("pnl", 0) or 0)
                            if cum_pnl > peak:
                                peak = cum_pnl
                            dd = peak - cum_pnl
                            if dd > max_dd:
                                max_dd = dd

                        risk_config = _parse_metadata(session.get("risk_config"))
                        portfolio_base = float(risk_config.get("portfolio_base", 1_000_000))
                        final_equity = portfolio_base + total_pnl
                        annual_return = total_pnl / portfolio_base * 100
                        max_dd_pct = max_dd / portfolio_base * 100
                        calmar = annual_return / max(max_dd_pct, 0.01)

                        arch_kpi.clear()
                        arch_content.clear()

                        with arch_kpi:
                            kpi_grid(
                                [
                                    dict(
                                        title="Status",
                                        value=str(session.get("status", "-")).upper(),
                                        icon="info",
                                        color=color_gray(),
                                    ),
                                    dict(
                                        title="Win Rate",
                                        value=f"{win_rate:.1f}%",
                                        icon="target",
                                        color=color_success() if win_rate >= 50 else color_error(),
                                    ),
                                    dict(
                                        title="Total P&L",
                                        value=_fmt(total_pnl),
                                        icon="payments",
                                        color=_pnl_color(total_pnl),
                                    ),
                                    dict(
                                        title="Profit Factor",
                                        value=f"{profit_factor:.2f}",
                                        icon="analytics",
                                        color=color_success()
                                        if profit_factor >= 1.5
                                        else color_warning(),
                                    ),
                                    dict(
                                        title="Max Drawdown",
                                        value=_fmt(max_dd),
                                        icon="trending_down",
                                        color=color_error(),
                                    ),
                                    dict(
                                        title="Calmar",
                                        value=f"{calmar:.1f}",
                                        icon="speed",
                                        color=color_success() if calmar >= 2 else color_warning(),
                                    ),
                                    dict(
                                        title="Trades",
                                        value=str(total_trades),
                                        icon="swap_horiz",
                                        color=color_info(),
                                    ),
                                    dict(
                                        title="Portfolio Base",
                                        value=f"₹{portfolio_base:,.0f}",
                                        icon="account_balance",
                                        color=color_gray(),
                                    ),
                                    dict(
                                        title="Final Equity",
                                        value=f"₹{final_equity:,.0f}",
                                        icon="savings",
                                        color=_pnl_color(total_pnl),
                                    ),
                                ],
                                columns=3,
                            )

                        with arch_content:
                            # Equity curve
                            if closed_positions:
                                sorted_closed = sorted(
                                    closed_positions, key=lambda x: x.get("closed_at", "")
                                )
                                cum = 0.0
                                equity_dates = []
                                equity_vals = []
                                for p in sorted_closed:
                                    cum += float(p.get("pnl", 0) or 0)
                                    equity_dates.append(str(p.get("closed_at", ""))[:19])
                                    equity_vals.append(cum)

                                fig = go.Figure()
                                fig.add_trace(
                                    go.Scatter(
                                        x=equity_dates,
                                        y=equity_vals,
                                        fill="tozeroy",
                                        line=dict(color=color_primary(), width=2),
                                        fillpattern=dict(
                                            shape="/",
                                            fgcolor=hex_to_rgba(color_primary(), 0.2),
                                        ),
                                    )
                                )
                                fig.update_layout(
                                    title="Cumulative P/L",
                                    height=300,
                                    margin=dict(l=60, r=20, t=40, b=40),
                                    xaxis_title="",
                                    yaxis_title="P/L (₹)",
                                )
                                apply_chart_theme(fig)
                                ui.plotly(fig).classes("w-full")

                                ui.space().classes(SPACE_LG)

                            # Trade ledger
                            ui.label("Trade Ledger").classes(
                                f"text-lg font-semibold {SPACE_MD}"
                            ).style(f"color: {theme_text_primary()};")

                            ledger_rows = []
                            for p in sorted_closed:
                                entry = _to_float(p.get("avg_entry"))
                                exit_p = _to_float(p.get("avg_exit"))
                                pnl_val = float(p.get("pnl", 0) or 0)
                                pnl_pct = (
                                    pnl_val / (entry * int(p.get("qty", 1))) * 100
                                    if entry and entry > 0
                                    else 0
                                )
                                md = _parse_metadata(p.get("metadata_json"))
                                opened = str(p.get("opened_at", "-"))[:19]
                                closed = str(p.get("closed_at", "-"))[:19]
                                hold_time = "-"
                                if opened != "-" and closed != "-":
                                    try:
                                        ot = datetime.fromisoformat(opened)
                                        ct = datetime.fromisoformat(closed)
                                        delta = ct - ot
                                        hours = delta.total_seconds() / 3600
                                        hold_time = (
                                            f"{hours:.1f}h" if hours < 48 else f"{hours / 24:.1f}d"
                                        )
                                    except ValueError:
                                        pass

                                ledger_rows.append(
                                    {
                                        "symbol": p.get("symbol", "-"),
                                        "direction": _direction_cell(p.get("direction", "-")),
                                        "qty": p.get("qty", 0),
                                        "entry": _fmt(entry),
                                        "exit": _fmt(exit_p),
                                        "pnl": _fmt(pnl_val),
                                        "pnl_pct": f"{pnl_pct:.2f}%",
                                        "hold_time": hold_time,
                                        "exit_reason": md.get("exit_reason", "-"),
                                        "opened": opened,
                                        "closed": closed,
                                    }
                                )

                            ledger_cols = [
                                {
                                    "name": "symbol",
                                    "label": "Symbol",
                                    "field": "symbol",
                                    "sortable": True,
                                },
                                {"name": "direction", "label": "Dir", "field": "direction"},
                                {"name": "qty", "label": "Qty", "field": "qty"},
                                {
                                    "name": "entry",
                                    "label": "Entry",
                                    "field": "entry",
                                    "sortable": True,
                                },
                                {
                                    "name": "exit",
                                    "label": "Exit",
                                    "field": "exit",
                                    "sortable": True,
                                },
                                {"name": "pnl", "label": "P&L", "field": "pnl", "sortable": True},
                                {
                                    "name": "pnl_pct",
                                    "label": "P&L %",
                                    "field": "pnl_pct",
                                    "sortable": True,
                                },
                                {"name": "hold_time", "label": "Hold", "field": "hold_time"},
                                {
                                    "name": "exit_reason",
                                    "label": "Exit Reason",
                                    "field": "exit_reason",
                                },
                                {
                                    "name": "opened",
                                    "label": "Opened",
                                    "field": "opened",
                                    "sortable": True,
                                },
                                {
                                    "name": "closed",
                                    "label": "Closed",
                                    "field": "closed",
                                    "sortable": True,
                                },
                            ]
                            paginated_table(ledger_rows, ledger_cols, page_size=15)

                    arch_select.on_value_change(lambda: render_archived())
                    render_archived()


def _session_label(session: dict[str, Any]) -> str:
    trade_date = session.get("trade_date", "na")
    strategy = _strategy_label(session.get("strategy_name"))
    status = str(session.get("status", "?")).upper()
    sid = str(session.get("session_id", "-"))[:12]
    return f"{trade_date} | {strategy} | {status} | {sid}"
