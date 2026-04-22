"""Scans page - Momentum signal scans from DuckDB feat_daily_core."""

from __future__ import annotations

import sys
from pathlib import Path

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
    color_info,
    color_primary,
    color_success,
    color_warning,
    divider,
    empty_state,
    info_box,
    kpi_grid,
    paginated_table,
    page_layout,
    SPACE_LG,
    SPACE_MD,
    SPACE_XL,
    theme_text_primary,
    theme_text_secondary,
)
from apps.nicegui.state import get_db


def _num(value: object, decimals: int = 2) -> str:
    if value is None or value == "":
        return "-"
    try:
        return f"{float(value):,.{decimals}f}"
    except TypeError, ValueError:
        return "-"


def _filters_badge(n: bool, two: bool, y: bool, c: bool, l_filter: bool) -> str:
    """Build a compact HTML badge showing which filters passed."""
    parts = []
    colors = {
        "N": (color_success(), n),
        "2": (color_primary(), two),
        "Y": (color_info(), y),
        "C": (color_warning(), c),
        "L": (color_error(), l_filter),
    }
    for label, (clr, passed) in colors.items():
        if passed is True or (isinstance(passed, (int, float)) and passed):
            parts.append(
                f'<span style="background:{clr};color:#fff;padding:1px 5px;'
                f'border-radius:3px;font-size:10px;margin-right:2px">{label}</span>'
            )
    return "".join(parts) if parts else "-"


def _load_latest_scans() -> dict:
    """Load breakout/breakdown candidates from feat_daily_core."""
    db = get_db()
    latest_rows = db.execute("SELECT MAX(trading_date) as d FROM feat_daily_core")
    if not latest_rows or not latest_rows[0].get("d"):
        return {}
    latest_date = latest_rows[0]["d"]

    # Breakout candidates (gap >= 2%)
    breakout_rows = (
        db.execute(
            """SELECT symbol, trading_date, gap_pct, close, volume, atr_pct,
                  close_pos_in_range, filter_n, filter_2, filter_y, filter_c, filter_l
           FROM feat_daily_core
           WHERE trading_date = ? AND gap_pct >= 0.02
           ORDER BY gap_pct DESC""",
            [latest_date],
        )
        or []
    )

    # Breakdown candidates (gap <= -2%)
    breakdown_rows = (
        db.execute(
            """SELECT symbol, trading_date, gap_pct, close, volume, atr_pct,
                  close_pos_in_range, filter_n, filter_2, filter_y, filter_c, filter_l
           FROM feat_daily_core
           WHERE trading_date = ? AND gap_pct <= -0.02
           ORDER BY gap_pct ASC""",
            [latest_date],
        )
        or []
    )

    # 4%+ movers for KPIs
    breakout_4 = db.execute(
        """SELECT COUNT(*) as cnt FROM feat_daily_core
           WHERE trading_date = ? AND gap_pct >= 0.04""",
        [latest_date],
    )
    breakdown_4 = db.execute(
        """SELECT COUNT(*) as cnt FROM feat_daily_core
           WHERE trading_date = ? AND gap_pct <= -0.04""",
        [latest_date],
    )

    # 2LYNCH pass rate
    lynch_rows = db.execute(
        """SELECT
             COUNT(*) as total,
             COUNT(*) FILTER (WHERE filter_2 = true) as passed
           FROM feat_daily_core
           WHERE trading_date = ?""",
        [latest_date],
    )
    lynch = lynch_rows[0] if lynch_rows else {}
    total = int(lynch.get("total", 0) or 0)
    passed = int(lynch.get("passed", 0) or 0)
    pass_rate = (passed / total * 100) if total > 0 else 0

    return {
        "latest_date": str(latest_date),
        "breakout": breakout_rows,
        "breakdown": breakdown_rows,
        "breakout_4_count": int(breakout_4[0]["cnt"] if breakout_4 else 0),
        "breakdown_4_count": int(breakdown_4[0]["cnt"] if breakdown_4 else 0),
        "lynch_total": total,
        "lynch_pass_rate": pass_rate,
    }


def _load_scan_history() -> list[dict]:
    """Load 30-day scan history from feat_daily_core."""
    db = get_db()
    return (
        db.execute(
            """SELECT trading_date,
               COUNT(*) FILTER (WHERE gap_pct >= 0.02) as breakout_count,
               COUNT(*) FILTER (WHERE gap_pct <= -0.02) as breakdown_count,
               COUNT(*) FILTER (WHERE gap_pct >= 0.04) as breakout_4,
               COUNT(*) FILTER (WHERE gap_pct <= -0.04) as breakdown_4,
               COUNT(*) FILTER (WHERE filter_2 = true) * 100.0 / COUNT(*) as pass_rate,
               AVG(gap_pct) as avg_gap,
               AVG(atr_pct) as avg_atr
           FROM feat_daily_core
           WHERE trading_date >= (
               SELECT MAX(trading_date) - INTERVAL '30 days' FROM feat_daily_core
           )
           GROUP BY trading_date
           ORDER BY trading_date DESC"""
        )
        or []
    )


def _breakout_rows(data: list[dict]) -> list[dict]:
    rows = []
    for r in data:
        rows.append(
            {
                "symbol": r.get("symbol", "-"),
                "gap_pct": _num(r.get("gap_pct")),
                "close": _num(r.get("close")),
                "volume": f"{int(r.get('volume', 0) or 0):,}",
                "atr_pct": _num(r.get("atr_pct")),
                "cpr": _num(r.get("close_pos_in_range")),
                "filters": _filters_badge(
                    r.get("filter_n"),
                    r.get("filter_2"),
                    r.get("filter_y"),
                    r.get("filter_c"),
                    r.get("filter_l"),
                ),
            }
        )
    return rows


def _signal_cols() -> list[dict]:
    return [
        {"name": "symbol", "label": "Symbol", "field": "symbol", "sortable": True},
        {"name": "gap_pct", "label": "Gap %", "field": "gap_pct", "sortable": True},
        {"name": "close", "label": "Close", "field": "close", "sortable": True},
        {"name": "volume", "label": "Volume", "field": "volume", "sortable": True},
        {"name": "atr_pct", "label": "ATR %", "field": "atr_pct", "sortable": True},
        {
            "name": "cpr",
            "label": "Close Pos",
            "field": "cpr",
            "sortable": True,
        },
        {"name": "filters", "label": "Filters", "field": "filters"},
    ]


async def scans_page() -> None:
    """Render the momentum scans page with DuckDB data."""
    with page_layout("Scans", "radar"):
        info_box(
            "Daily breakout and breakdown candidates based on gap thresholds and 2LYNCH "
            "filter rules. Research signals, not trade recommendations."
        )

        scans = _load_latest_scans()
        if not scans:
            empty_state(
                "No Feature Data",
                "Run nseml-build-features to populate feat_daily_core.",
                icon="radar",
            )
            return

        # KPI cards
        kpi_grid(
            [
                dict(
                    title="Breakout Candidates",
                    value=str(scans["breakout_4_count"]),
                    subtitle=f"{len(scans['breakout'])} with 2%+ gap",
                    icon="trending_up",
                    color=color_success(),
                ),
                dict(
                    title="Breakdown Candidates",
                    value=str(scans["breakdown_4_count"]),
                    subtitle=f"{len(scans['breakdown'])} with -2%+ gap",
                    icon="trending_down",
                    color=color_error(),
                ),
                dict(
                    title="2LYNCH Pass Rate",
                    value=f"{scans['lynch_pass_rate']:.1f}%",
                    subtitle=f"{scans['lynch_total']} symbols scanned",
                    icon="filter_alt",
                    color=color_primary(),
                ),
                dict(
                    title="Scan Date",
                    value=scans["latest_date"],
                    subtitle="Latest trading date",
                    icon="today",
                    color=color_info(),
                ),
            ],
            columns=4,
        )

        divider()

        # Tabs: Today's Signals / Signal History
        with ui.tabs().classes("w-full") as tabs:
            tab_today = ui.tab("Today's Signals")
            tab_history = ui.tab("Signal History")

        with ui.tab_panels(tabs, value=tab_today).classes("w-full"):
            # ---- Today's Signals ----
            with ui.tab_panel(tab_today):
                # Breakout signals
                ui.label("Breakout Candidates (2%+ Gap)").classes(
                    f"text-lg font-semibold {SPACE_MD}"
                ).style(f"color: {theme_text_primary()};")
                breakout_data = scans.get("breakout", [])
                if breakout_data:
                    paginated_table(
                        _breakout_rows(breakout_data),
                        _signal_cols(),
                        page_size=15,
                    )
                else:
                    ui.label("No breakout candidates today.").classes("text-sm").style(
                        f"color: {theme_text_secondary()};"
                    )

                ui.space().classes(SPACE_XL)

                # Breakdown signals
                ui.label("Breakdown Candidates (-2%+ Gap)").classes(
                    f"text-lg font-semibold {SPACE_MD}"
                ).style(f"color: {theme_text_primary()};")
                breakdown_data = scans.get("breakdown", [])
                if breakdown_data:
                    paginated_table(
                        _breakout_rows(breakdown_data),
                        _signal_cols(),
                        page_size=15,
                    )
                else:
                    ui.label("No breakdown candidates today.").classes("text-sm").style(
                        f"color: {theme_text_secondary()};"
                    )

            # ---- Signal History ----
            with ui.tab_panel(tab_history):
                history = _load_scan_history()
                if not history:
                    ui.label("No historical scan data available.").classes("text-sm").style(
                        f"color: {theme_text_secondary()};"
                    )
                    return

                # Trend chart
                dates = [str(r.get("trading_date", "")) for r in reversed(history)]
                breakout_counts = [int(r.get("breakout_count", 0) or 0) for r in reversed(history)]
                breakdown_counts = [
                    int(r.get("breakdown_count", 0) or 0) for r in reversed(history)
                ]

                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=dates,
                        y=breakout_counts,
                        name="Breakouts (2%+)",
                        fill="tozeroy",
                        line=dict(color=color_success(), width=2),
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=dates,
                        y=breakdown_counts,
                        name="Breakdowns (-2%+)",
                        fill="tozeroy",
                        line=dict(color=color_error(), width=2),
                    )
                )
                fig.update_layout(
                    height=300,
                    margin=dict(l=50, r=20, t=20, b=40),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                apply_chart_theme(fig)
                ui.plotly(fig).classes("w-full")

                ui.space().classes(SPACE_LG)

                # History table
                table_rows = []
                for r in history:
                    table_rows.append(
                        {
                            "date": str(r.get("trading_date", "-")),
                            "breakouts": int(r.get("breakout_count", 0) or 0),
                            "breakdowns": int(r.get("breakdown_count", 0) or 0),
                            "b4": int(r.get("breakout_4", 0) or 0),
                            "bd4": int(r.get("breakdown_4", 0) or 0),
                            "pass_rate": _num(r.get("pass_rate"), 1) + "%",
                            "avg_gap": _num(r.get("avg_gap")),
                            "avg_atr": _num(r.get("avg_atr")),
                        }
                    )
                cols = [
                    {"name": "date", "label": "Date", "field": "date", "sortable": True},
                    {
                        "name": "breakouts",
                        "label": "2%+ Up",
                        "field": "breakouts",
                        "sortable": True,
                    },
                    {
                        "name": "breakdowns",
                        "label": "2%+ Down",
                        "field": "breakdowns",
                        "sortable": True,
                    },
                    {
                        "name": "b4",
                        "label": "4% Up",
                        "field": "b4",
                        "sortable": True,
                    },
                    {
                        "name": "bd4",
                        "label": "4% Down",
                        "field": "bd4",
                        "sortable": True,
                    },
                    {
                        "name": "pass_rate",
                        "label": "2LYNCH %",
                        "field": "pass_rate",
                        "sortable": True,
                    },
                    {
                        "name": "avg_gap",
                        "label": "Avg Gap %",
                        "field": "avg_gap",
                        "sortable": True,
                    },
                    {
                        "name": "avg_atr",
                        "label": "Avg ATR %",
                        "field": "avg_atr",
                        "sortable": True,
                    },
                ]
                paginated_table(table_rows, cols)
