"""Daily Summary page - Market breadth and regime overview with real data."""

from __future__ import annotations

import math
import sys
from datetime import date, datetime
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
    hex_to_rgba,
    kpi_grid,
    paginated_table,
    page_layout,
    SPACE_LG,
    SPACE_XL,
    theme_text_primary,
    theme_text_secondary,
)
from apps.nicegui.state import aget_market_monitor_history, aget_market_monitor_latest


def _num(value: object, decimals: int = 1) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    try:
        return f"{float(value):,.{decimals}f}"
    except TypeError, ValueError:
        return "-"


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _regime_color(regime: str | None) -> str:
    r = (regime or "").lower()
    if "bull" in r:
        return color_success()
    if "bear" in r:
        return color_error()
    return color_warning()


def _regime_label(regime: str | None) -> str:
    return str(regime or "-").replace("_", " ").title()


def _breadth_label(pct: float | None) -> str:
    if pct is None or (isinstance(pct, float) and math.isnan(pct)):
        return ""
    if pct > 70:
        return "Overbought"
    if pct < 30:
        return "Oversold"
    return ""


async def daily_summary_page() -> None:
    """Render the daily summary page with real market breadth data."""
    with page_layout("Daily Summary", "today"):
        latest = await aget_market_monitor_latest()
        history = await aget_market_monitor_history(days=90)

        if latest is None or latest.is_empty():
            empty_state(
                "No Market Data",
                "Run nseml-market-monitor to populate breadth data.",
                icon="today",
            )
            return

        latest = latest.to_dicts()[0]

        trading_date = _coerce_date(latest.get("trading_date"))
        date_str = trading_date.isoformat() if trading_date else date.today().isoformat()

        # Convert history to list of dicts for iteration
        history_dicts = history.to_dicts() if history is not None and not history.is_empty() else []

        ui.label(f"Market Summary for {date_str}").classes(
            f"text-xl font-semibold {SPACE_XL}"
        ).style(f"color: {theme_text_primary()};")

        # Regime status row
        primary_regime = latest.get("primary_regime")
        tactical_regime = latest.get("tactical_regime")
        posture = latest.get("posture_label")
        aggression = latest.get("aggression_score")

        regime_color = _regime_color(primary_regime)
        regime_text = _regime_label(primary_regime)

        with ui.row().classes("items-center gap-4 mb-6"):
            ui.label("Regime:").classes("text-sm font-medium").style(
                f"color: {theme_text_secondary()};"
            )
            ui.badge(regime_text, color=regime_color).props("outline")
            if tactical_regime:
                ui.badge(_regime_label(tactical_regime), color=color_info()).props("outline")
            if posture:
                ui.label(f"Posture: {_regime_label(posture)}").classes("text-sm").style(
                    f"color: {theme_text_secondary()};"
                )

        # 8 KPI cards
        up_4 = latest.get("up_4pct_count", 0) or 0
        down_4 = latest.get("down_4pct_count", 0) or 0
        up_4_pct = latest.get("up_4pct_pct")
        down_4_pct = latest.get("down_4pct_pct")
        ratio_5d = latest.get("ratio_5d")
        pct_ma40 = latest.get("pct_above_ma40")
        pct_ma20 = latest.get("pct_above_ma20")
        universe = latest.get("universe_size", 0) or 0

        is_live = trading_date == date.today() if trading_date else False
        status_text = "Live" if is_live else f"Last: {date_str}"
        status_color = color_success() if is_live else color_warning()

        kpi_grid(
            [
                dict(
                    title="Data Status",
                    value=status_text,
                    subtitle="Market data freshness",
                    icon="check_circle",
                    color=status_color,
                ),
                dict(
                    title="Universe Size",
                    value=f"{int(universe):,}",
                    subtitle="Tradeable symbols",
                    icon="bar_chart",
                    color=color_info(),
                ),
                dict(
                    title="4% Up",
                    value=str(int(up_4)),
                    subtitle=f"{_num(up_4_pct)}% of universe",
                    icon="trending_up",
                    color=color_success(),
                ),
                dict(
                    title="4% Down",
                    value=str(int(down_4)),
                    subtitle=f"{_num(down_4_pct)}% of universe",
                    icon="trending_down",
                    color=color_error(),
                ),
                dict(
                    title="5D Breadth Ratio",
                    value=_num(ratio_5d, 2),
                    subtitle="Advancers / Decliners",
                    icon="compare_arrows",
                    color=color_success() if (ratio_5d or 0) > 1 else color_error(),
                ),
                dict(
                    title="MA40 Breadth",
                    value=f"{_num(pct_ma40)}%",
                    subtitle=_breadth_label(pct_ma40),
                    icon="show_chart",
                    color=color_info(),
                ),
                dict(
                    title="MA20 Breadth",
                    value=f"{_num(pct_ma20)}%",
                    subtitle=_breadth_label(pct_ma20),
                    icon="speed",
                    color=color_primary(),
                ),
                dict(
                    title="Aggression Score",
                    value=_num(aggression, 0),
                    subtitle="Momentum strength (0-100)",
                    icon="bolt",
                    color=color_warning() if (aggression or 0) < 50 else color_success(),
                ),
            ],
            columns=4,
        )

        divider()

        # Breadth trend chart (last 90 days)
        if len(history_dicts) > 1:
            ui.label("Market Breadth Trend (90 Days)").classes(
                f"text-lg font-semibold {SPACE_LG}"
            ).style(f"color: {theme_text_primary()};")

            dates = [str(r.get("trading_date", "")) for r in history_dicts]
            up_pct = [float(r.get("up_4pct_pct", 0) or 0) for r in history_dicts]
            down_pct = [float(r.get("down_4pct_pct", 0) or 0) for r in history_dicts]
            ma40_pct = [float(r.get("pct_above_ma40", 0) or 0) for r in history_dicts]
            ma20_pct = [float(r.get("pct_above_ma20", 0) or 0) for r in history_dicts]

            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=up_pct,
                    name="4% Up %",
                    fill="tozeroy",
                    line=dict(color=color_success(), width=1.5),
                    fillpattern=dict(shape="/", fgcolor=hex_to_rgba(color_success(), 0.3)),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=down_pct,
                    name="4% Down %",
                    fill="tozeroy",
                    line=dict(color=color_error(), width=1.5),
                    fillpattern=dict(shape="\\", fgcolor=hex_to_rgba(color_error(), 0.3)),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=ma40_pct,
                    name="MA40 Breadth %",
                    line=dict(color=color_info(), width=2),
                    yaxis="y2",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=ma20_pct,
                    name="MA20 Breadth %",
                    line=dict(color=color_primary(), width=2, dash="dot"),
                    yaxis="y2",
                )
            )
            fig.update_layout(
                yaxis=dict(title="4% Move %"),
                yaxis2=dict(title="Breadth %", overlaying="y", side="right", range=[0, 100]),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                height=350,
                margin=dict(l=50, r=50, t=30, b=40),
            )
            apply_chart_theme(fig)
            ui.plotly(fig).classes("w-full")

            divider()

        # Breadth history table (last 30 days)
        hist_30 = history_dicts[:30]
        if hist_30:
            ui.label("Breadth History (30 Days)").classes(
                f"text-lg font-semibold {SPACE_LG}"
            ).style(f"color: {theme_text_primary()};")

            table_rows = []
            for r in hist_30:
                td = _coerce_date(r.get("trading_date"))
                regime = r.get("primary_regime", "-")
                table_rows.append(
                    {
                        "date": td.isoformat() if td else "-",
                        "regime": _regime_label(regime),
                        "up_4pct": int(r.get("up_4pct_count", 0) or 0),
                        "down_4pct": int(r.get("down_4pct_count", 0) or 0),
                        "ratio_5d": _num(r.get("ratio_5d"), 2),
                        "ma40": f"{_num(r.get('pct_above_ma40'))}%",
                        "ma20": f"{_num(r.get('pct_above_ma20'))}%",
                        "aggression": _num(r.get("aggression_score"), 0),
                    }
                )

            cols = [
                {"name": "date", "label": "Date", "field": "date", "sortable": True},
                {
                    "name": "regime",
                    "label": "Regime",
                    "field": "regime",
                    "sortable": True,
                },
                {
                    "name": "up_4pct",
                    "label": "4% Up",
                    "field": "up_4pct",
                    "sortable": True,
                },
                {
                    "name": "down_4pct",
                    "label": "4% Down",
                    "field": "down_4pct",
                    "sortable": True,
                },
                {
                    "name": "ratio_5d",
                    "label": "5D Ratio",
                    "field": "ratio_5d",
                    "sortable": True,
                },
                {
                    "name": "ma40",
                    "label": "MA40 %",
                    "field": "ma40",
                    "sortable": True,
                },
                {
                    "name": "ma20",
                    "label": "MA20 %",
                    "field": "ma20",
                    "sortable": True,
                },
                {
                    "name": "aggression",
                    "label": "Aggression",
                    "field": "aggression",
                    "sortable": True,
                },
            ]
            paginated_table(table_rows, cols)
