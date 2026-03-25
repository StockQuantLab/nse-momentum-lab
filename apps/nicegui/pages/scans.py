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

from apps.nicegui.components import page_layout, info_box, empty_state, theme_text_muted


def scans_page() -> None:
    """Render the momentum scans page."""
    with page_layout("Scans", "radar"):
        info_box("Scans detect 4% breakouts with 2LYNCH filter confirmation.")

        empty_state(
            "API Required",
            "Scan results require the API server to be running. Start the API server to see live momentum scan results.",
            action_label="Copy API Command",
            action_callback=lambda: ui.run_javascript(
                "navigator.clipboard.writeText('doppler run -- uv run nseml-api');"
            ),
            icon="radar",
        )

        with ui.column().classes("items-center mt-8"):
            ui.label("Direct DuckDB scan results coming soon.").classes("text-sm").style(
                f"color: {theme_text_muted()};"
            )
