"""Data Quality page - Validate data integrity."""

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

from nicegui import ui

from apps.nicegui.state import get_db_status
from apps.nicegui.components import (
    color_gray,
    color_info,
    color_success,
    color_warning,
    divider,
    kpi_grid,
    page_layout,
    SPACE_SM,
    SPACE_XL,
    SPACE_XS,
    theme_text_muted,
    theme_text_primary,
    theme_text_secondary,
)


def data_quality_page() -> None:
    """Render the data quality page."""
    with page_layout("Data Quality", "verified"):
        status = get_db_status()

        # Coverage summary
        ui.label("Coverage Summary").classes(f"text-xl font-semibold {SPACE_XL}").style(
            f"color: {theme_text_primary()};"
        )

        kpi_grid(
            [
                dict(
                    title="Symbols",
                    value=f"{int(status.get('symbols', 0)):,}",
                    icon="show_chart",
                    color=color_info(),
                ),
                dict(
                    title="Total Candles",
                    value=f"{int(status.get('total_candles', 0)):,}",
                    icon="candlestick_chart",
                    color=color_warning(),
                ),
                dict(
                    title="Date Range",
                    value=status.get("date_range", "-"),
                    icon="date_range",
                    color=color_gray(),
                ),
                dict(
                    title="Data Source",
                    value=status.get("data_source", "unknown").upper(),
                    icon="storage",
                    color=color_success(),
                ),
            ]
        )

        divider()

        # Validation info
        ui.label("Data Validation").classes(f"text-lg font-semibold {SPACE_SM}").style(
            f"color: {theme_text_primary()};"
        )

        with ui.column().classes("kpi-card"):
            ui.label("Dataset Information").classes(f"text-sm font-medium {SPACE_SM}").style(
                f"color: {theme_text_secondary()};"
            )
            if status.get("dataset_hash"):
                ui.label(f"Dataset Hash: {status.get('dataset_hash')}").classes(
                    f"text-xs font-mono {SPACE_XS}"
                ).style(f"color: {theme_text_muted()};")
            ui.label(
                "All data is validated before backtesting. Use this page to verify data integrity."
            ).classes("text-sm").style(f"color: {theme_text_muted()};")

        divider()

        with ui.expansion("Data Sources", icon="info").classes("w-full"):
            for text in [
                "Primary: Zerodha historical data (adjusted)",
                "Format: Parquet files with OHLCV data",
                "Frequency: Daily candles",
                "Adjustments: Corporate actions applied",
            ]:
                ui.label(text).classes(SPACE_SM).style(f"color: {theme_text_secondary()};")
