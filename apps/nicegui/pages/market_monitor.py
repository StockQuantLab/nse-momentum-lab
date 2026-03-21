"""Market Monitor page - Stockbee-inspired NSE breadth regime."""

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

import polars as pl
import plotly.graph_objects as go
from nicegui import ui

from apps.nicegui.components import (
    COLORS,
    THEME,
    divider,
    empty_state,
    kpi_grid,
    page_header,
    page_layout,
)
from apps.nicegui.state import aget_market_monitor_all, aget_market_monitor_latest


# ============================================================================
# CONSTANTS
# ============================================================================

T2108_OVERBOUGHT = 70
T2108_OVERSOLD = 30
RATIO_10D_BULLISH = 1.5
RATIO_10D_BEARISH = 0.67


def _safe_value(record: dict, key: str, default: str = "-") -> str:
    value = record.get(key, default)
    if _is_missing(value):
        return default
    return str(value)


def _is_missing(value: object) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _num(value: object, decimals: int = 0) -> str:
    if _is_missing(value):
        return "-"
    try:
        num = float(value)
    except TypeError, ValueError:
        return "-"
    if decimals == 0:
        return f"{round(num):,}"
    return f"{num:,.{decimals}f}"


def _regime_color(label: str) -> str:
    value = (label or "").lower()
    if any(token in value for token in ["bull", "long", "aggressive"]):
        return COLORS["success"]
    if any(token in value for token in ["bear", "short", "defensive"]):
        return COLORS["error"]
    return COLORS["warning"]


def _latest_cards(latest: dict) -> list[dict]:
    return [
        dict(
            title="Primary Regime",
            value=_safe_value(latest, "primary_regime").replace("_", " ").title(),
            icon="insights",
            color=_regime_color(_safe_value(latest, "primary_regime")),
        ),
        dict(
            title="Tactical Regime",
            value=_safe_value(latest, "tactical_regime").replace("_", " ").title(),
            icon="timeline",
            color=_regime_color(_safe_value(latest, "tactical_regime")),
        ),
        dict(
            title="Posture",
            value=_safe_value(latest, "posture_label").title(),
            icon="target",
            color=_regime_color(_safe_value(latest, "posture_label")),
        ),
        dict(
            title="Aggression",
            value=_num(latest.get("aggression_score"), decimals=1),
            icon="speed",
            color=COLORS["info"],
        ),
        dict(
            title="Universe",
            value=_num(latest.get("universe_size"), decimals=0),
            icon="groups",
            color=COLORS["primary"],
        ),
        dict(
            title="Last Update",
            value=_safe_value(latest, "trading_date"),
            icon="event",
            color=COLORS["gray"],
        ),
    ]


# Cell color helpers
def _color_for_ratio(value: float) -> str:
    if _is_missing(value):
        return "cell-neutral"
    if value >= 1.5:
        return "cell-strong-bullish"
    elif value >= 1.0:
        return "cell-bullish"
    elif value <= 0.5:
        return "cell-strong-bearish"
    elif value <= 0.67:
        return "cell-bearish"
    return "cell-neutral"


def _color_for_4pct_up(value: int) -> str:
    if _is_missing(value):
        return "cell-neutral"
    if value >= 150:
        return "cell-strong-bullish"
    elif value >= 100:
        return "cell-bullish"
    elif value >= 50:
        return "cell-neutral"
    return "cell-weak"


def _color_for_4pct_down(value: int) -> str:
    if _is_missing(value):
        return "cell-neutral"
    if value >= 150:
        return "cell-strong-bearish"
    elif value >= 100:
        return "cell-bearish"
    elif value >= 50:
        return "cell-neutral"
    return "cell-weak"


def _color_for_t2108(value: float) -> str:
    if _is_missing(value):
        return "cell-neutral"
    if value >= 70:
        return "cell-overbought"
    elif value <= 30:
        return "cell-oversold"
    elif value >= 55:
        return "cell-bullish"
    elif value <= 45:
        return "cell-bearish"
    return "cell-neutral"


def _color_for_ma20(value: float) -> str:
    if _is_missing(value):
        return "cell-neutral"
    if value >= 65:
        return "cell-bullish"
    elif value <= 35:
        return "cell-bearish"
    return "cell-neutral"


def _color_for_25q(up: int, down: int) -> str:
    if _is_missing(up) or _is_missing(down):
        return "cell-neutral"
    if up >= 300:
        return "cell-strong-bullish"
    elif up >= 150:
        return "cell-bullish"
    elif down >= 200:
        return "cell-capitulation"
    elif down >= 100:
        return "cell-bearish"
    return "cell-neutral"


def _color_for_pair(up: int, down: int) -> str:
    if _is_missing(up) or _is_missing(down):
        return "cell-neutral"
    if up > down * 1.1:
        return "cell-bullish"
    if down > up * 1.1:
        return "cell-bearish"
    return "cell-neutral"


def _rgba(hex_color: str, alpha: float) -> str:
    """Convert a theme hex color to a Plotly-safe rgba string."""
    color = hex_color.strip().lstrip("#")
    if len(color) == 3:
        color = "".join(ch * 2 for ch in color)
    if len(color) != 6:
        return hex_color
    red = int(color[0:2], 16)
    green = int(color[2:4], 16)
    blue = int(color[4:6], 16)
    return f"rgba({red}, {green}, {blue}, {alpha})"


def _year_table_row(row: dict[str, object]) -> dict:
    """Normalize a market-monitor row for the year table."""
    trading_date = _coerce_date(row.get("trading_date"))
    date_key = trading_date.strftime("%Y-%m-%d") if trading_date is not None else ""

    def _int(key: str) -> int:
        value = row.get(key, 0)
        return int(value) if not _is_missing(value) else 0

    def _flt(key: str) -> float:
        value = row.get(key, 0)
        return float(value) if not _is_missing(value) else 0.0

    regime = str(row.get("primary_regime", "")).lower()
    if "bull" in regime:
        badge = "bullish-badge"
    elif "bear" in regime:
        badge = "bearish-badge"
    else:
        badge = "neutral-badge"

    # Pre-compute values to avoid repeated row lookups
    up_4 = _int("up_4pct_count")
    dn_4 = _int("down_4pct_count")
    r5d = _flt("ratio_5d")
    r10d = _flt("ratio_10d")
    t2108 = _flt("t2108_equivalent_pct")
    up_25q = _int("up_25q_count")
    dn_25q = _int("down_25q_count")
    up_25m = _int("up_25m_count")
    dn_25m = _int("down_25m_count")
    up_50m = _int("up_50m_count")
    dn_50m = _int("down_50m_count")
    up_13_34 = _int("up_13_34_count")
    dn_13_34 = _int("down_13_34_count")
    ma20 = _flt("pct_above_ma20")

    return {
        "weekday": row.get("weekday", ""),
        "date_display": row.get("date_display", ""),
        "primary_display": row.get("primary_display", ""),
        "date_key": date_key,
        "regime_badge_class": badge,
        "up_4pct_val": up_4,
        "up_4pct_display": f"{up_4}",
        "down_4pct_val": dn_4,
        "down_4pct_display": f"{dn_4}",
        "up_4pct_class": _color_for_4pct_up(up_4),
        "down_4pct_class": _color_for_4pct_down(dn_4),
        "ratio_5d_val": r5d,
        "ratio_5d_display": f"{r5d:.2f}",
        "ratio_5d_class": _color_for_ratio(r5d),
        "ratio_10d_val": r10d,
        "ratio_10d_display": f"{r10d:.2f}",
        "ratio_10d_class": _color_for_ratio(r10d),
        "t2108_val": t2108,
        "t2108_display": f"{t2108:.0f}%",
        "t2108_class": _color_for_t2108(t2108),
        "up_25q_val": up_25q,
        "up_25q_display": f"{up_25q}",
        "down_25q_val": dn_25q,
        "down_25q_display": f"{dn_25q}",
        "up_25q_class": _color_for_25q(up_25q, dn_25q),
        "up_25m_val": up_25m,
        "up_25m_display": f"{up_25m}",
        "down_25m_val": dn_25m,
        "down_25m_display": f"{dn_25m}",
        "up_25m_class": _color_for_pair(up_25m, dn_25m),
        "up_50m_val": up_50m,
        "up_50m_display": f"{up_50m}",
        "down_50m_val": dn_50m,
        "down_50m_display": f"{dn_50m}",
        "up_50m_class": _color_for_pair(up_50m, dn_50m),
        "up_13_34_val": up_13_34,
        "up_13_34_display": f"{up_13_34}",
        "down_13_34_val": dn_13_34,
        "down_13_34_display": f"{dn_13_34}",
        "up_13_34_class": _color_for_pair(up_13_34, dn_13_34),
        "ma20_val": ma20,
        "ma20_display": f"{ma20:.0f}%",
        "ma20_class": _color_for_ma20(ma20),
    }


def _year_table_rows(frame: pl.DataFrame) -> list[dict]:
    """Convert a dataframe into table-ready rows."""
    return [_year_table_row(row) for row in frame.iter_rows(named=True)]


# ============================================================================
# MAIN PAGE
# ============================================================================


async def market_monitor_page() -> None:
    """Render the Market Monitor page."""
    with page_layout("Market Monitor", "monitoring"):
        page_header(
            "Market Monitor",
            "NSE breadth regime tracking - Stockbee-inspired for Indian markets.",
        )

        # Fetch all data
        latest_df = await aget_market_monitor_latest()
        history_df = await aget_market_monitor_all()

        if latest_df.is_empty():
            empty_state(
                "No market monitor data yet",
                "Run <code>doppler run -- uv run nseml-market-monitor</code> to build the monitor table.",
                icon="monitoring",
            )
            return

        latest = latest_df.to_dicts()[0]

        # Extract values for cards
        up_4pct = (
            latest.get("up_4pct_count", 0) if not _is_missing(latest.get("up_4pct_count")) else 0
        )
        down_4pct = (
            latest.get("down_4pct_count", 0)
            if not _is_missing(latest.get("down_4pct_count"))
            else 0
        )
        ratio_5d = latest.get("ratio_5d", 0) if not _is_missing(latest.get("ratio_5d")) else 0
        t2108 = (
            latest.get("t2108_equivalent_pct", 0)
            if not _is_missing(latest.get("t2108_equivalent_pct"))
            else 0
        )
        ma20_breadth = (
            latest.get("pct_above_ma20", 0) if not _is_missing(latest.get("pct_above_ma20")) else 0
        )
        success_bg = _rgba(COLORS["success"], 0.25)
        success_border = _rgba(COLORS["success"], 0.3)
        success_fill = _rgba(COLORS["success"], 0.1)
        success_fill_soft = _rgba(COLORS["success"], 0.06)
        error_bg = _rgba(COLORS["error"], 0.25)
        error_border = _rgba(COLORS["error"], 0.3)
        error_fill = _rgba(COLORS["error"], 0.1)
        error_fill_soft = _rgba(COLORS["error"], 0.06)
        warning_bg = _rgba(COLORS["warning"], 0.25)
        warning_border = _rgba(COLORS["warning"], 0.3)
        info_bg = _rgba(COLORS["info"], 0.25)

        # ========================================================================
        # MARKET HEALTH DASHBOARD - Enhanced Cards with Visual Indicators
        # ========================================================================
        with (
            ui.card()
            .classes("w-full p-5 mb-6")
            .style(
                f"background: linear-gradient(145deg, {THEME['surface']}, {THEME['surface_hover']}40); border: 1px solid {THEME['surface_border']}; border-radius: 16px;"
            )
        ):
            ui.label("MARKET HEALTH DASHBOARD").classes(
                "text-sm font-bold tracking-wider mb-2"
            ).style(
                f"color: {THEME['text_primary']}; letter-spacing: 0.15em; text-transform: uppercase;"
            )
            ui.label("Latest daily breadth snapshot for the most recent trading date.").classes(
                "text-xs mb-4"
            ).style(f"color: {THEME['text_muted']};")
            kpi_grid(
                [
                    dict(
                        title="4% Moves Today",
                        value=f"{int(up_4pct)}↑ / {int(down_4pct)}↓",
                        subtitle=(
                            "Stocks with ≥4% daily move • "
                            + (
                                "STRONG"
                                if up_4pct >= 100
                                else ("WEAK" if down_4pct >= 100 else "NORMAL")
                            )
                        ),
                        icon="show_chart",
                        color=COLORS["success"]
                        if up_4pct >= down_4pct
                        else (COLORS["error"] if down_4pct > up_4pct else COLORS["warning"]),
                    ),
                    dict(
                        title="5-Day Breadth Ratio",
                        value=f"{ratio_5d:.2f}",
                        subtitle="Up ÷ Down (5D rolling) • "
                        + (
                            "BULLISH"
                            if ratio_5d >= 1.5
                            else ("BEARISH" if ratio_5d <= 0.67 else "NEUTRAL")
                        ),
                        icon="timeline",
                        color=COLORS["info"],
                    ),
                    dict(
                        title="MA40 Breadth",
                        value=f"{t2108:.0f}%",
                        subtitle="% stocks above 40-day MA • "
                        + (
                            "OVERSOLD"
                            if t2108 <= 30
                            else ("OVERBOUGHT" if t2108 >= 70 else "NEUTRAL")
                        ),
                        icon="insights",
                        color=COLORS["primary"],
                    ),
                    dict(
                        title="MA20 Breadth",
                        value=f"{ma20_breadth:.0f}%",
                        subtitle="% stocks above 20-day MA • "
                        + (
                            "UPTREND"
                            if ma20_breadth >= 60
                            else ("DOWNTREND" if ma20_breadth <= 40 else "MIXED")
                        ),
                        icon="moving",
                        color=COLORS["warning"],
                    ),
                ],
                columns=4,
            )

        divider()

        # ========================================================================
        # REGIME STATUS
        # ========================================================================
        with (
            ui.card()
            .classes("w-full p-5 mb-6")
            .style(
                f"background: linear-gradient(145deg, {THEME['surface']}, {THEME['surface_hover']}35); border: 1px solid {THEME['surface_border']}; border-left: 4px solid {COLORS['info']}; border-radius: 16px;"
            )
        ):
            ui.label("REGIME STATUS").classes("text-sm font-bold tracking-wider mb-2").style(
                f"color: {THEME['text_primary']}; letter-spacing: 0.1em; text-transform: uppercase;"
            )
            ui.label("Derived interpretation of the raw breadth snapshot above.").classes(
                "text-xs mb-4"
            ).style(f"color: {THEME['text_muted']};")
            ui.label(f"{_safe_value(latest, 'trading_date')} Market State").classes(
                "text-2xl font-bold mb-1"
            ).style(f"color: {THEME['text_primary']};")
            ui.label("Current regime classification for tactical trading decisions").classes(
                "text-sm mb-4"
            ).style(f"color: {THEME['text_secondary']};")
            kpi_grid(_latest_cards(latest), columns=6)

        divider()

        # ========================================================================
        # TABBED YEAR-BASED TABLE (Stockbee-style)
        # ========================================================================
        with (
            ui.card()
            .classes("w-full p-5")
            .style(
                f"background: linear-gradient(145deg, {THEME['surface']}, {THEME['surface_hover']}35); border: 1px solid {THEME['surface_border']}; border-radius: 16px;"
            )
        ):
            ui.label("MARKET BREADTH HISTORY").classes(
                "text-sm font-bold tracking-wider mb-2"
            ).style(f"color: {THEME['text_primary']}; letter-spacing: 0.1em;")
            ui.label(
                "Select a year tab to inspect the daily breadth table. The table stays compact, while the definitions below explain each short column name."
            ).classes("text-xs mb-4").style(f"color: {THEME['text_muted']};")
            ui.label(
                "Bullish = favorable breadth | Bearish = weak breadth | Capitulation = extreme weakness / watch zone"
            ).classes("text-xs mb-1").style(f"color: {THEME['text_secondary']};")
            ui.label(
                "4%↑ / 4%↓ = stocks moving at least +/-4% today | 5D BR / 10D BR = up-vs-down breadth ratio over 5 / 10 days | MA40 = percent of stocks above 40-day moving average | 25Q / 25M / 50M / 13/34 = Stockbee-style move buckets for quarterly, monthly, and medium-term breadth"
            ).classes("text-xs mb-4").style(f"color: {THEME['text_secondary']};")

            # Prepare data
            numeric_count_cols = [
                "up_4pct_count",
                "down_4pct_count",
                "up_25q_count",
                "down_25q_count",
                "up_25m_count",
                "down_25m_count",
                "up_50m_count",
                "down_50m_count",
                "up_13_34_count",
                "down_13_34_count",
            ]
            df = history_df.with_columns(
                [
                    pl.col("trading_date").cast(pl.Date, strict=False),
                    pl.col("trading_date").cast(pl.Date, strict=False).dt.year().alias("year"),
                    pl.col("trading_date")
                    .cast(pl.Date, strict=False)
                    .dt.strftime("%b %d")
                    .alias("date_display"),
                    pl.col("trading_date")
                    .cast(pl.Date, strict=False)
                    .dt.strftime("%a")
                    .alias("weekday"),
                    pl.col("primary_regime")
                    .cast(pl.Utf8, strict=False)
                    .str.replace_all("_", " ")
                    .str.to_titlecase()
                    .alias("primary_display"),
                    *[
                        pl.col(col).fill_null(0).cast(pl.Int64, strict=False).alias(col)
                        for col in numeric_count_cols
                    ],
                ]
            )

            years = df.get_column("year").unique().sort(descending=True).to_list()

            history_styles_html = f"""
        <style>
            .mm-history-card {{
                background: linear-gradient(145deg, {THEME["surface"]}, {THEME["surface_hover"]}40);
                border: 1px solid {THEME["surface_border"]};
                border-radius: 16px;
                box-shadow: 0 2px 10px rgba(0, 0, 0, 0.18);
                overflow: hidden;
            }}
            .mm-history-table .q-table__container,
            .mm-history-table .q-table__card {{
                background: transparent;
                border-radius: 14px;
            }}
            .mm-history-table .q-table__top,
            .mm-history-table .q-table__bottom {{
                background: transparent;
            }}
            .mm-history-table thead th {{
                background: {THEME["surface"]};
                color: {THEME["text_secondary"]};
                font-size: 9px;
                font-weight: 800;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                border-bottom: 1px solid {THEME["surface_border"]};
                white-space: nowrap;
            }}
            .mm-history-table tbody tr:hover {{
                background: rgba(34, 255, 136, 0.06) !important;
            }}
            .mm-history-table .mm-weekday {{
                display: block;
                color: {THEME["text_muted"]};
                font-size: 9px;
                text-transform: uppercase;
                letter-spacing: 0.05em;
                line-height: 1.1;
            }}
            .mm-history-table .mm-date-text {{
                display: block;
                color: {THEME["text_primary"]};
                font-weight: 700;
                line-height: 1.2;
            }}
            .mm-history-table .mm-regime-badge {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: 4px 10px;
                border-radius: 999px;
                font-size: 9px;
                font-weight: 800;
                text-transform: uppercase;
                letter-spacing: 0.05em;
                white-space: nowrap;
            }}
            .bullish-badge {{ background: {success_bg}; color: {COLORS["success"]}; border: 1px solid {success_border}; }}
            .bearish-badge {{ background: {error_bg}; color: {COLORS["error"]}; border: 1px solid {error_border}; }}
            .neutral-badge {{ background: {warning_bg}; color: {COLORS["warning"]}; border: 1px solid {warning_border}; }}
            .cell-strong-bullish {{ background: linear-gradient(90deg, {success_fill}, transparent); color: {COLORS["success"]}; font-weight: 700; }}
            .cell-bullish {{ background: linear-gradient(90deg, {success_fill_soft}, transparent); color: {COLORS["success"]}; }}
            .cell-strong-bearish {{ background: linear-gradient(90deg, {error_fill}, transparent); color: {COLORS["error"]}; font-weight: 700; }}
            .cell-bearish {{ background: linear-gradient(90deg, {error_fill_soft}, transparent); color: {COLORS["error"]}; }}
            .cell-oversold {{ background: linear-gradient(90deg, {success_bg}, transparent); color: {COLORS["success"]}; font-weight: 800; border-left: 3px solid {COLORS["success"]}; }}
            .cell-overbought {{ background: linear-gradient(90deg, {error_bg}, transparent); color: {COLORS["error"]}; font-weight: 800; border-left: 3px solid {COLORS["error"]}; }}
            .cell-capitulation {{ background: linear-gradient(90deg, {info_bg}, transparent); color: {COLORS["info"]}; font-weight: 700; border-left: 3px solid {COLORS["info"]}; }}
            .cell-neutral {{ color: {THEME["text_primary"]}; }}
            .cell-weak {{ color: {THEME["text_muted"]}; }}
            .mm-history-legend {{
                display: flex;
                align-items: center;
                justify-content: center;
                flex-wrap: wrap;
                gap: 16px;
                padding: 12px 0 0;
                font-size: 12px;
                color: {THEME["text_muted"]};
            }}
            .mm-legend-chip {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
            }}
            .mm-legend-dot {{
                width: 10px;
                height: 10px;
                border-radius: 999px;
                display: inline-block;
            }}
            .mm-column-guide {{
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 8px 16px;
                margin-top: 12px;
                color: {THEME["text_muted"]};
                font-size: 12px;
            }}
            .mm-column-guide code {{
                color: {THEME["text_primary"]};
                font-weight: 700;
            }}
        </style>
        """
            ui.add_head_html(history_styles_html)

            history_columns = [
                {"name": "date_display", "label": "Date", "field": "date_display"},
                {"name": "primary_display", "label": "Regime", "field": "primary_display"},
                {"name": "up_4pct_display", "label": "4%↑", "field": "up_4pct_display"},
                {"name": "down_4pct_display", "label": "4%↓", "field": "down_4pct_display"},
                {"name": "ratio_5d_display", "label": "5D BR", "field": "ratio_5d_display"},
                {"name": "ratio_10d_display", "label": "10D BR", "field": "ratio_10d_display"},
                {"name": "t2108_display", "label": "MA40", "field": "t2108_display"},
                {"name": "up_25q_display", "label": "25Q", "field": "up_25q_display"},
                {"name": "up_25m_display", "label": "25M", "field": "up_25m_display"},
                {"name": "up_50m_display", "label": "50M", "field": "up_50m_display"},
                {"name": "up_13_34_display", "label": "13/34", "field": "up_13_34_display"},
                {"name": "ma20_display", "label": "MA20", "field": "ma20_display"},
            ]

            def render_history_table(frame: pl.DataFrame) -> None:
                rows = _year_table_rows(frame.sort("trading_date", descending=True))
                table = (
                    ui.table(
                        columns=history_columns,
                        rows=rows,
                        row_key="date_key",
                        pagination={"rowsPerPage": 20, "rowsPerPage_options": [10, 20, 50, 100]},
                    )
                    .classes("w-full mm-history-table")
                    .props("flat bordered dense")
                )

                table.add_slot(
                    "body-cell-date_display",
                    """
                    <q-td :props="props" class="text-left">
                        <span class="mm-weekday">{{ props.row.weekday }}</span>
                        <span class="mm-date-text">{{ props.row.date_display }}</span>
                    </q-td>
                    """,
                )
                table.add_slot(
                    "body-cell-primary_display",
                    """
                    <q-td :props="props" class="text-center">
                        <span :class="['mm-regime-badge', props.row.regime_badge_class]">
                            {{ props.row.primary_display }}
                        </span>
                    </q-td>
                    """,
                )
                for column in [
                    ("up_4pct_display", "up_4pct_class"),
                    ("down_4pct_display", "down_4pct_class"),
                    ("ratio_5d_display", "ratio_5d_class"),
                    ("ratio_10d_display", "ratio_10d_class"),
                    ("t2108_display", "t2108_class"),
                    ("up_25q_display", "up_25q_class"),
                    ("up_25m_display", "up_25m_class"),
                    ("up_50m_display", "up_50m_class"),
                    ("up_13_34_display", "up_13_34_class"),
                    ("ma20_display", "ma20_class"),
                ]:
                    field_name, class_name = column
                    table.add_slot(
                        f"body-cell-{field_name}",
                        (
                            '<q-td :props="props" :class="props.row.'
                            + class_name
                            + '">'
                            + "{{ props.row."
                            + field_name
                            + " }}"
                            + "</q-td>"
                        ),
                    )

            tabs = ui.tabs().classes("w-full mt-1")
            with tabs:
                tab_all = ui.tab("All Years")
                year_tabs = {
                    year: ui.tab(f"{year} ({df.filter(pl.col('year') == year).height})")
                    for year in years
                }

            with ui.tab_panels(tabs, value=tab_all).classes("w-full"):
                with ui.tab_panel(tab_all):
                    with ui.expansion(
                        f"ALL YEARS ({df.height} days)", icon="calendar_month", value=True
                    ).classes("w-full"):
                        render_history_table(df)

                for year in years:
                    year_df = df.filter(pl.col("year") == year).sort(
                        "trading_date", descending=True
                    )
                    year_count = year_df.height
                    with ui.tab_panel(year_tabs[year]):
                        with ui.expansion(
                            f"{year} DATA ({year_count} days)", icon="event", value=True
                        ).classes("w-full"):
                            render_history_table(year_df)

        divider()

        # ========================================================================
        # CHARTS - Enhanced Section
        # ========================================================================
        with ui.expansion("BREADTH TRENDS (90 Days)", icon="timeline", value=False).classes(
            "w-full"
        ):
            with ui.column().classes("w-full gap-4 pt-3"):
                with ui.row().classes("w-full gap-4"):
                    _create_4pct_chart(history_df)
                    _create_ratio_chart(history_df)
                with ui.row().classes("w-full gap-4"):
                    _create_t2108_chart(history_df)
                    _create_ma_breadth_chart(history_df)


# ============================================================================
# CHART FUNCTIONS
# ============================================================================


def _create_chart_layout(title: str, height: int = 240) -> dict:
    grid_color = _rgba(THEME["surface_border"], 0.35)
    return {
        "title": {
            "text": title,
            "font": {"size": 12, "color": THEME["text_primary"], "family": "system-ui, sans-serif"},
            "x": 0.02,
            "xanchor": "left",
        },
        "height": height,
        "margin": dict(l=40, r=20, t=40, b=35),
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": _rgba(THEME["surface_hover"], 0.12),
        "font": dict(color=THEME["text_primary"], size=10),
        "legend": dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=9, family="system-ui, sans-serif"),
            bgcolor="rgba(0,0,0,0)",
        ),
        "xaxis": dict(
            showgrid=True,
            gridcolor=grid_color,
            linewidth=1,
            title="",
            showline=True,
            linecolor=THEME["surface_border"],
            tickfont=dict(size=9, color=THEME["text_muted"]),
        ),
        "yaxis": dict(
            showgrid=True,
            gridcolor=grid_color,
            linewidth=1,
            title="",
            showline=True,
            linecolor=THEME["surface_border"],
            tickfont=dict(size=9, color=THEME["text_muted"]),
        ),
    }


def _create_4pct_chart(df: pl.DataFrame) -> None:
    df_sorted = df.sort("trading_date").tail(90)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_sorted.get_column("trading_date").to_list(),
            y=df_sorted.get_column("up_4pct_count").to_list(),
            mode="lines+markers",
            name="4% Up",
            line=dict(color=COLORS["success"], width=2.5, shape="spline"),
            marker=dict(
                size=5, color=COLORS["success"], line=dict(width=1, color=THEME["surface"])
            ),
            fill="tozeroy",
            fillcolor=_rgba(COLORS["success"], 0.1),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_sorted.get_column("trading_date").to_list(),
            y=df_sorted.get_column("down_4pct_count").to_list(),
            mode="lines+markers",
            name="4% Down",
            line=dict(color=COLORS["error"], width=2.5, shape="spline"),
            marker=dict(size=5, color=COLORS["error"], line=dict(width=1, color=THEME["surface"])),
        )
    )
    fig.add_hline(
        y=100, line_dash="dash", line_color=COLORS["success"], opacity=0.5, annotation_text="Strong"
    )
    fig.update_layout(**_create_chart_layout("4% Move Participation"))
    ui.plotly(fig).classes("w-full rounded-lg overflow-hidden")


def _create_ratio_chart(df: pl.DataFrame) -> None:
    df_sorted = df.sort("trading_date").tail(90)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_sorted.get_column("trading_date").to_list(),
            y=df_sorted.get_column("ratio_5d").to_list(),
            mode="lines+markers",
            name="5D Ratio",
            line=dict(color=COLORS["info"], width=2, shape="spline"),
            marker=dict(size=4, color=COLORS["info"], line=dict(width=1, color=THEME["surface"])),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_sorted.get_column("trading_date").to_list(),
            y=df_sorted.get_column("ratio_10d").to_list(),
            mode="lines+markers",
            name="10D Ratio",
            line=dict(color=COLORS["primary"], width=3, shape="spline"),
            marker=dict(
                size=5, color=COLORS["primary"], line=dict(width=1, color=THEME["surface"])
            ),
        )
    )
    fig.add_hline(y=1.0, line_dash="dot", line_color=THEME["text_muted"], opacity=0.5)
    fig.add_hline(
        y=RATIO_10D_BULLISH,
        line_dash="dash",
        line_color=COLORS["success"],
        opacity=0.6,
        annotation_text="Bullish",
    )
    fig.add_hline(
        y=RATIO_10D_BEARISH,
        line_dash="dash",
        line_color=COLORS["error"],
        opacity=0.6,
        annotation_text="Bearish",
    )
    fig.update_layout(**_create_chart_layout("Breadth Ratios"))
    ui.plotly(fig).classes("w-full rounded-lg overflow-hidden")


def _create_t2108_chart(df: pl.DataFrame) -> None:
    df_sorted = df.sort("trading_date").tail(90)
    fig = go.Figure()

    # Add gradient zones
    fig.add_hrect(
        y0=T2108_OVERBOUGHT,
        y1=100,
        fillcolor=_rgba(COLORS["error"], 0.1),
        layer="below",
        line_width=0,
    )
    fig.add_hrect(
        y0=0,
        y1=T2108_OVERSOLD,
        fillcolor=_rgba(COLORS["success"], 0.1),
        layer="below",
        line_width=0,
    )

    fig.add_trace(
        go.Scatter(
            x=df_sorted.get_column("trading_date").to_list(),
            y=df_sorted.get_column("t2108_equivalent_pct").to_list(),
            mode="lines+markers",
            name="Above MA40",
            line=dict(color=COLORS["primary"], width=2.5, shape="spline"),
            marker=dict(
                size=5, color=COLORS["primary"], line=dict(width=1, color=THEME["surface"])
            ),
            fill="tozeroy",
            fillcolor=_rgba(COLORS["primary"], 0.15),
        )
    )
    fig.add_hline(
        y=T2108_OVERSOLD,
        line_dash="dash",
        line_color=COLORS["success"],
        opacity=0.7,
        annotation_text="Oversold",
    )
    fig.add_hline(
        y=T2108_OVERBOUGHT,
        line_dash="dash",
        line_color=COLORS["error"],
        opacity=0.7,
        annotation_text="Overbought",
    )
    fig.update_layout(
        **_create_chart_layout("MA40 Breadth (% Above 40-Day MA)", height=240), yaxis_range=[0, 100]
    )
    ui.plotly(fig).classes("w-full rounded-lg overflow-hidden")


def _create_ma_breadth_chart(df: pl.DataFrame) -> None:
    df_sorted = df.sort("trading_date").tail(90)
    fig = go.Figure()

    # Add middle zone
    fig.add_hrect(
        y0=40, y1=60, fillcolor=_rgba(COLORS["warning"], 0.08), layer="below", line_width=0
    )

    fig.add_trace(
        go.Scatter(
            x=df_sorted.get_column("trading_date").to_list(),
            y=df_sorted.get_column("pct_above_ma20").to_list(),
            mode="lines+markers",
            name="Above MA20",
            line=dict(color=COLORS["primary"], width=2.5, shape="spline"),
            marker=dict(
                size=5, color=COLORS["primary"], line=dict(width=1, color=THEME["surface"])
            ),
        )
    )
    fig.add_hline(y=50, line_dash="dot", line_color=THEME["text_muted"], opacity=0.6)
    fig.add_hline(
        y=65, line_dash="dash", line_color=COLORS["success"], opacity=0.5, annotation_text="Strong"
    )
    fig.add_hline(
        y=35, line_dash="dash", line_color=COLORS["error"], opacity=0.5, annotation_text="Weak"
    )
    fig.update_layout(**_create_chart_layout("MA20 Breadth", height=240), yaxis_range=[0, 100])
    ui.plotly(fig).classes("w-full rounded-lg overflow-hidden")
