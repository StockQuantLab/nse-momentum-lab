"""Scans page - Momentum scan results (4% + 2LYNCH)."""

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

from apps.nicegui.components import page_layout, info_box, THEME


def scans_page() -> None:
    """Render the momentum scans page."""
    with page_layout("Scans", "radar"):
        info_box("Scans detect 4% breakouts with 2LYNCH filter confirmation.")

        with ui.column().classes("kpi-card p-8"):
            ui.label("Scan Results").classes("text-xl font-semibold mb-4").style(
                f"color: {THEME['text_primary']};"
            )
            ui.label("This page requires the API to be running for scan results.").classes(
                "mb-4"
            ).style(f"color: {THEME['text_secondary']};")

            ui.label("Start the API server: doppler run -- uv run nseml-api").classes(
                "font-mono text-sm px-3 py-2 rounded"
            ).style(
                f"background: {THEME['surface_hover']}; border: 1px solid {THEME['surface_border']}; color: {THEME['text_primary']}; border-radius: 6px;"
            )

            ui.label("Direct DuckDB scan results coming soon.").classes("text-sm mt-4").style(
                f"color: {THEME['text_muted']};"
            )
