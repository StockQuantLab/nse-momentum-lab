"""Reusable UI components for NiceGUI dashboard.

Provides:
- THEME / COLORS dicts for consistent styling
- page_layout() context manager — sidebar nav + top bar + content area
- kpi_card / kpi_grid — aligned metric cards with Material icons
- nav_card — home-page navigation tiles
- apply_chart_theme — unified Plotly light/professional theme
- divider / info_box / export_button — utility widgets
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Path setup (must run before any project imports)
# ---------------------------------------------------------------------------
_apps_root = Path(__file__).resolve().parent.parent  # apps/nicegui/
_project_root = _apps_root.parent.parent  # project root
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))

from nicegui import ui

# ---------------------------------------------------------------------------
# Theme tokens
# ---------------------------------------------------------------------------
THEME = {
    "page_bg": "#f8fafc",  # slate-50   — page background
    "surface": "#ffffff",  # white      — card / drawer
    "surface_border": "#e2e8f0",  # slate-200  — card borders
    "surface_hover": "#f1f5f9",  # slate-100  — card hover
    "text_primary": "#0f172a",  # slate-900
    "text_secondary": "#475569",  # slate-600
    "text_muted": "#64748b",  # slate-500
    "primary": "#0f52ba",  # Sapphire Blue
    "primary_dark": "#1e40af",  # blue-800
    "divider": "#e2e8f0",  # slate-200
}

COLORS = {
    "success": "#22c55e",
    "error": "#ef4444",
    "warning": "#f59e0b",
    "info": "#3b82f6",
    "primary": "#3b82f6",
    "gray": "#64748b",
}

# ---------------------------------------------------------------------------
# Navigation definition (single source of truth)
# ---------------------------------------------------------------------------
NAV_ITEMS = [
    {"label": "Home", "icon": "home", "path": "/"},
    {"label": "Backtest Results", "icon": "bar_chart", "path": "/backtest"},
    {"label": "Trade Analytics", "icon": "analytics", "path": "/trade_analytics"},
    {"label": "Compare", "icon": "compare_arrows", "path": "/compare"},
    {"label": "Strategy", "icon": "tune", "path": "/strategy"},
    {"label": "Scans", "icon": "radar", "path": "/scans"},
    {"label": "Data Quality", "icon": "verified", "path": "/data_quality"},
    {"label": "Pipeline", "icon": "engineering", "path": "/pipeline"},
    {"label": "Paper Ledger", "icon": "receipt_long", "path": "/paper_ledger"},
    {"label": "Daily Summary", "icon": "today", "path": "/daily_summary"},
]

# ---------------------------------------------------------------------------
# CSS injected once per page
# ---------------------------------------------------------------------------
# Non-blocking font preload injected per-page via ui.add_head_html() in page_layout().
# We do NOT use @import here — CSS @import is render-blocking and causes slow first paint.
_FONT_HEAD_HTML = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" media="print" onload="this.media='all'">
"""

_PAGE_CSS = """
/* Typography — Inter with instant system-font fallback */
body, .q-app {
    font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont,
                 'Segoe UI', Roboto, Oxygen, sans-serif !important;
}

/* Subtle grid background for data pages */
.data-grid-bg {
    background-image:
        linear-gradient(%(surface_border)s22 1px, transparent 1px),
        linear-gradient(90deg, %(surface_border)s22 1px, transparent 1px);
    background-size: 40px 40px;
    background-position: -1px -1px;
}

/* KPI cards */
.kpi-card {
    background: %(surface)s;
    border: 1px solid %(surface_border)s;
    border-radius: 12px;
    padding: 20px;
    transition: border-color 0.2s, box-shadow 0.2s, transform 0.15s;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.kpi-card:hover {
    border-color: %(primary)s;
    box-shadow: 0 0 0 1px %(primary)s33, 0 4px 12px rgba(37,99,235,0.08);
    transform: translateY(-1px);
}

/* Staggered fade-in animation */
@keyframes fade-in-up {
    from { opacity: 0; transform: translateY(8px); }
    to { opacity: 1; transform: translateY(0); }
}
.kpi-card { animation: fade-in-up 0.3s ease-out backwards; }
.kpi-card:nth-child(1) { animation-delay: 0.05s; }
.kpi-card:nth-child(2) { animation-delay: 0.1s; }
.kpi-card:nth-child(3) { animation-delay: 0.15s; }
.kpi-card:nth-child(4) { animation-delay: 0.2s; }
.kpi-card:nth-child(5) { animation-delay: 0.25s; }
.kpi-card:nth-child(6) { animation-delay: 0.3s; }
.kpi-card:nth-child(7) { animation-delay: 0.35s; }
.kpi-card:nth-child(8) { animation-delay: 0.4s; }

/* Nav cards (home page) */
.nav-tile {
    background: %(surface)s;
    border: 1px solid %(surface_border)s;
    border-radius: 12px;
    padding: 24px;
    cursor: pointer;
    transition: border-color 0.2s, transform 0.15s, box-shadow 0.2s;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.nav-tile:hover {
    border-color: %(primary)s;
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.08); /* light theme lower opacity shadow */
}

/* Sidebar nav items */
.nav-item {
    border-radius: 8px;
    padding: 8px 12px;
    margin: 2px 8px;
    transition: background 0.15s;
    cursor: pointer;
    color: %(text_secondary)s;
}
.nav-item:hover {
    background: %(surface_hover)s;
    color: %(text_primary)s;
}
.nav-item-active {
    background: %(primary)s14;
    color: %(primary)s !important;
    font-weight: 600;
}

/* Quasar table overrides */
.q-table {
    background: %(surface)s !important;
    color: %(text_primary)s !important;
}
.q-table thead th {
    color: %(text_secondary)s !important;
    font-weight: 600;
    text-transform: uppercase;
    font-size: 0.75rem;
    letter-spacing: 0.05em;
    border-bottom: 1px solid %(surface_border)s !important;
}
.q-table tbody td {
    border-bottom: 1px solid %(divider)s !important;
    color: %(text_primary)s !important;
}
.q-table tbody tr:hover td {
    background: %(surface_hover)s !important;
}

/* Quasar tabs */
.q-tab {
    color: %(text_secondary)s !important;
}
.q-tab--active {
    color: %(primary)s !important;
    font-weight: 600;
}
.q-tabs__content { border-bottom: 2px solid %(surface_border)s; }

/* Quasar expansion */
.q-expansion-item {
    background: %(surface)s !important;
    border: 1px solid %(surface_border)s;
    border-radius: 10px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
.q-expansion-item__header { border-radius: 10px !important; }

/* Quasar select / inputs */
.q-field__native, .q-field__input {
    color: %(text_primary)s !important;
}
.q-field__label { color: %(text_secondary)s !important; }
.q-field--outlined .q-field__control:before {
    border-color: %(surface_border)s !important;
}
.q-field--outlined.q-field--focused .q-field__control:before {
    border-color: %(primary)s !important;
}

/* Info box */
.info-box {
    background: %(primary)s0d;
    border: 1px solid %(primary)s33;
    border-radius: 8px;
    padding: 14px 18px;
}

/* Code / terminal blocks */
.code-block {
    background: %(surface_hover)s;
    border: 1px solid %(surface_border)s;
    border-radius: 6px;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    font-size: 0.8rem;
}

/* ── Sidebar mini-mode (Quasar adds .q-drawer--mini when mini prop is set) ── */
/* Hide logo block + separator in mini mode */
.q-drawer--mini .sidebar-logo { display: none !important; }

/* Center icon and hide label in collapsed nav rows */
.q-drawer--mini .nav-row {
    justify-content: center !important;
    padding: 10px 0 !important;
    margin: 2px 4px !important;
    gap: 0 !important;
}
.q-drawer--mini .nav-label { display: none !important; }
.q-drawer--mini .nav-icon  { font-size: 1.3rem !important; }

/* Smooth width transition on the drawer itself */
.q-drawer { transition: width 0.2s ease !important; }
""" % {**THEME}


# ---------------------------------------------------------------------------
# page_layout — wraps every page
# ---------------------------------------------------------------------------
@contextmanager
def page_layout(title: str, icon: str = "bar_chart"):
    """Context manager that wraps every page with consistent chrome.

    Usage::

        def my_page():
            with page_layout("Backtest Results", "bar_chart"):
                ui.label("Hello")
    """
    # -- mode + palette -----------------------------------------------
    ui.dark_mode(False)
    ui.colors(primary=THEME["primary"])
    ui.query("body").style(f"background-color: {THEME['page_bg']}; color: {THEME['text_primary']};")
    # Inject font as non-blocking <link> tags (avoids render-blocking @import)
    ui.add_head_html(_FONT_HEAD_HTML)
    ui.add_css(_PAGE_CSS)
    ui.add_head_html(_KEYBINDINGS_HTML)

    # -- sidebar state: cycles expanded → mini → hidden → expanded
    _state = {"v": "expanded"}  # mutable cell for closure

    def _cycle_sidebar():
        if _state["v"] == "expanded":
            _state["v"] = "mini"
            drawer.props(add="mini")
        elif _state["v"] == "mini":
            _state["v"] = "hidden"
            drawer.hide()
        else:
            _state["v"] = "expanded"
            drawer.show()
            drawer.props(remove="mini")

    # -- header bar ---------------------------------------------------------
    with (
        ui.header()
        .classes("items-center px-4 py-0 shadow-sm")
        .style(
            f"background: {THEME['surface']}; "
            f"border-bottom: 1px solid {THEME['surface_border']}; "
            "height: 52px;"
        )
    ):
        # Hamburger — cycles sidebar through expanded / mini / hidden
        ui.button(icon="menu", on_click=_cycle_sidebar).props("flat round dense").classes(
            "text-slate-600"
        ).style(f"color: {THEME['text_secondary']};")

        ui.label("NSE Momentum Lab").classes("text-lg font-semibold ml-2").style(
            f"color: {THEME['text_primary']};"
        )

        # breadcrumb
        ui.label(f"  /  {title}").classes("text-sm ml-1").style(
            f"color: {THEME['text_secondary']};"
        )

        ui.space()

        # Dark mode toggle
        ui.button(icon="dark_mode", on_click=toggle_dark_mode).props("flat round dense").style(
            f"color: {THEME['text_secondary']};"
        )

        # Shortcuts help
        _shortcuts_dialog_instance = shortcuts_dialog()
        ui.button(icon="keyboard", on_click=_shortcuts_dialog_instance.open).props(
            "flat round dense"
        ).style(f"color: {THEME['text_secondary']};")

        # Right-side icon
        ui.icon(icon).classes("text-xl").style(f"color: {THEME['primary']};")

    # -- sidebar drawer -----------------------------------------------------
    with (
        ui.left_drawer(value=True, bordered=False)
        .props(
            "width=240 mini-width=56 breakpoint=0"  # breakpoint=0 → never auto-collapse
        )
        .classes("p-0")
        .style(
            f"background: {THEME['surface']}; "
            f"border-right: 1px solid {THEME['surface_border']}; "
            "transition: width 0.2s ease;"
        ) as drawer
    ):
        # Logo area — hidden in mini mode via q-drawer__mini CSS class
        with ui.column().classes("px-4 py-4 gap-0 sidebar-logo"):
            ui.label("Momentum Lab").classes("text-base font-bold").style(
                f"color: {THEME['text_primary']};"
            )
            ui.label("v0.1.0").classes("text-xs").style(f"color: {THEME['text_muted']};")

        ui.separator().classes("sidebar-logo").style(f"background: {THEME['surface_border']};")

        # Nav links
        with ui.column().classes("py-2 gap-0 w-full"):
            for item in NAV_ITEMS:
                is_active = item["label"] == title or (title == "Home" and item["path"] == "/")
                active_cls = " nav-item-active" if is_active else ""

                with (
                    ui.row()
                    .classes(f"nav-item{active_cls} items-center gap-3 w-full nav-row")
                    .on("click", lambda p=item["path"]: ui.navigate.to(p))
                ):
                    ui.icon(item["icon"]).classes("text-lg nav-icon")
                    ui.label(item["label"]).classes("text-sm nav-label")

    # -- main content area --------------------------------------------------
    with ui.column().classes("w-full max-w-7xl mx-auto px-8 py-6"):
        yield


# ---------------------------------------------------------------------------
# KPI card + grid
# ---------------------------------------------------------------------------
def kpi_card(
    title: str,
    value: str | float | int,
    subtitle: str | None = None,
    icon: str = "info",
    color: str = COLORS["primary"],
) -> None:
    """Render a single KPI card with a Material Design icon."""
    with ui.column().classes("kpi-card gap-1"):
        with ui.row().classes("items-center gap-3"):
            ui.icon(icon).classes("text-2xl").style(f"color: {color};")
            ui.label(title).classes("text-xs uppercase tracking-wide font-medium").style(
                f"color: {THEME['text_secondary']};"
            )
        ui.label(str(value)).classes("text-2xl font-bold mt-1").style(f"color: {color};")
        if subtitle:
            ui.label(subtitle).classes("text-xs").style(f"color: {THEME['text_muted']};")


def kpi_grid(
    cards: list[dict[str, Any]],
    columns: int = 4,
) -> None:
    """Render a row of KPI cards using ``ui.grid`` for equal-width alignment.

    Each dict in *cards* is passed as kwargs to :func:`kpi_card`.
    Keys: title, value, subtitle, icon, color.
    """
    with ui.grid(columns=columns).classes("w-full gap-4 mb-6"):
        for card in cards:
            kpi_card(**card)


# ---------------------------------------------------------------------------
# Navigation card (home page)
# ---------------------------------------------------------------------------
def nav_card(
    title: str,
    description: str,
    icon: str,
    target: str,
    color: str = COLORS["info"],
) -> None:
    """Render a navigation tile for the home page grid."""
    with ui.column().classes("nav-tile").on("click", lambda t=target: ui.navigate.to(t)):
        with ui.row().classes("items-center gap-3 mb-2"):
            ui.icon(icon).classes("text-2xl").style(f"color: {color};")
            ui.label(title).classes("text-base font-semibold").style(
                f"color: {THEME['text_primary']};"
            )
        ui.label(description).classes("text-sm leading-relaxed").style(
            f"color: {THEME['text_secondary']};"
        )


# ---------------------------------------------------------------------------
# Plotly chart theme
# ---------------------------------------------------------------------------
def apply_chart_theme(fig) -> None:
    """Apply consistent bright theme to a Plotly figure (mutates in place)."""
    fig.update_layout(
        paper_bgcolor=THEME["surface"],
        plot_bgcolor=THEME["page_bg"],
        font_color=THEME["text_primary"],
        font_family="Inter, sans-serif",
        title_font=dict(color=THEME["text_primary"], size=15, family="Inter, sans-serif"),
        margin=dict(l=40, r=20, t=48, b=40),
        xaxis=dict(
            gridcolor=THEME["surface_border"],
            zerolinecolor=THEME["surface_border"],
            linecolor=THEME["surface_border"],
            tickfont=dict(color=THEME["text_secondary"]),
        ),
        yaxis=dict(
            gridcolor=THEME["surface_border"],
            zerolinecolor=THEME["surface_border"],
            linecolor=THEME["surface_border"],
            tickfont=dict(color=THEME["text_secondary"]),
        ),
        legend=dict(
            bgcolor="rgba(255,255,255,0.8)",
            font_color=THEME["text_secondary"],
            bordercolor=THEME["surface_border"],
            borderwidth=1,
        ),
    )


# ---------------------------------------------------------------------------
# Utility widgets
# ---------------------------------------------------------------------------
def divider() -> None:
    """Render a styled divider."""
    ui.separator().classes("my-6").style(f"background: {THEME['surface_border']};")


def info_box(text: str, color: str = "blue") -> None:
    """Render an info callout with icon."""
    palette = {
        "blue": (COLORS["info"], "#dbeafe"),  # blue-100
        "green": (COLORS["success"], "#dcfce7"),  # green-100
        "yellow": (COLORS["warning"], "#fef9c3"),  # yellow-100
        "red": (COLORS["error"], "#fee2e2"),  # red-100
    }
    accent, bg = palette.get(color, palette["blue"])
    with (
        ui.row()
        .classes("info-box items-center gap-3 mb-4")
        .style(f"background: {bg}; border-color: {accent}66;")
    ):
        ui.icon("lightbulb").classes("text-lg").style(f"color: {accent};")
        ui.label(text).classes("text-sm").style(f"color: {accent};")


def export_button(
    data: Any,
    filename: str = "export.csv",
    label: str = "Download CSV",
) -> None:
    """Add a CSV export button with Material icon."""
    if data is None or (hasattr(data, "empty") and data.empty):
        return

    csv_content = data.to_csv(index=False)

    def do_download():
        ui.download(csv_content.encode("utf-8"), filename=filename)

    ui.button(label, icon="download", on_click=do_download).props("flat").classes("text-sm").style(
        f"color: {THEME['text_secondary']}; "
        f"border: 1px solid {THEME['surface_border']}; "
        "border-radius: 6px; padding: 6px 14px;"
    )


# ---------------------------------------------------------------------------
# Loading spinner
# ---------------------------------------------------------------------------
@contextmanager
def loading_spinner():
    """Show loading spinner during async operations."""
    spinner = ui.spinner("dots").classes("mt-8")
    try:
        yield
    finally:
        spinner.delete()


# ---------------------------------------------------------------------------
# Paginated table
# ---------------------------------------------------------------------------
def paginated_table(
    rows: list,
    columns: list,
    page_size: int = 50,
    row_key: Any = None,
) -> None:
    """Paginated table that only renders current page."""
    if not rows:
        ui.label("No data to display").style(f"color: {THEME['text_muted']};")
        return

    total_pages = (len(rows) + page_size - 1) // page_size
    state = {"page": 0}

    def show_page():
        start = state["page"] * page_size
        end = start + page_size

        with ui.column().classes("w-full"):
            ui.table(
                columns=columns,
                rows=rows[start:end],
                pagination=page_size,
                row_key="id" if rows and "id" in rows[0] else None,
            ).classes("w-full")

            with ui.row().classes("justify-between items-center mt-4 w-full"):
                ui.label(f"Showing {start + 1}-{min(end, len(rows))} of {len(rows)}").classes(
                    f"color: {THEME['text_muted']};"
                )
                with ui.row().classes("gap-2"):
                    ui.button(
                        "Previous",
                        on_click=lambda: (
                            state.update(page=max(0, state["page"] - 1)),
                            show_page(),
                        ),
                    ).props("flat dense").classes("" if state["page"] > 0 else "invisible")
                    ui.label(f"Page {state['page'] + 1} of {total_pages}").classes(
                        f"color: {THEME['text_secondary']};"
                    )
                    ui.button(
                        "Next",
                        on_click=lambda: (
                            state.update(page=min(total_pages - 1, state["page"] + 1)),
                            show_page(),
                        ),
                    ).props("flat dense").classes(
                        "" if state["page"] < total_pages - 1 else "invisible"
                    )

    show_page()


# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------
def empty_state(
    title: str,
    message: str,
    action_label: str | None = None,
    action_callback: Any = None,
    icon: str = "inbox",
) -> None:
    """Beautiful empty state component with optional action."""
    with ui.column().classes("items-center justify-center py-16 gap-4 w-full"):
        ui.icon(icon).classes("text-6xl opacity-50").style(f"color: {THEME['text_muted']};")
        ui.label(title).classes("text-xl font-semibold").style(f"color: {THEME['text_primary']};")
        ui.label(message).classes("text-center max-w-md").style(
            f"color: {THEME['text_secondary']};"
        )
        if action_label and action_callback:
            ui.button(action_label, on_click=action_callback).props("push color=primary").classes(
                "mt-4"
            )


# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
def page_header(
    title: str,
    subtitle: str | None = None,
    kpi_row: list[dict] | None = None,
) -> None:
    """Consistent page header with optional KPIs."""
    with ui.column().classes("mb-8 w-full"):
        with ui.column().classes("gap-1 mb-6"):
            ui.label(title).classes("text-2xl font-bold").style(f"color: {THEME['text_primary']};")
            if subtitle:
                ui.label(subtitle).classes("text-sm").style(f"color: {THEME['text_secondary']};")

        if kpi_row:
            kpi_grid(kpi_row, columns=len(kpi_row))


# ---------------------------------------------------------------------------
# Export menu
# ---------------------------------------------------------------------------
def export_menu(
    data: Any,
    filename_base: str,
    label: str = "Export",
) -> None:
    """Dropdown with multiple export formats."""
    if data is None or (hasattr(data, "empty") and data.empty):
        return

    with ui.button(label, icon="download").props("flat"):
        with ui.menu().props("anchor=top-end"):
            csv_content = data.to_csv(index=False)
            ui.menu_item(
                "Download as CSV",
                lambda: ui.download(csv_content.encode("utf-8"), filename=f"{filename_base}.csv"),
            )
            json_content = data.to_json(indent=2) if hasattr(data, "to_json") else str(data)
            ui.menu_item(
                "Download as JSON",
                lambda: ui.download(json_content.encode("utf-8"), filename=f"{filename_base}.json"),
            )


# ---------------------------------------------------------------------------
# Trade table with filters
# ---------------------------------------------------------------------------
def trade_table_with_filters(
    trades_df: pd.DataFrame,
    columns: list,
    rows: list,
    page_size: int = 50,
) -> None:
    """Trade table with symbol, exit reason, and P&L filters."""
    if not rows:
        empty_state(
            "No trades to display",
            "There are no trades matching the current filters.",
            icon="filter_list",
        )
        return

    filters = {"symbol": "", "exit_reason": "all", "min_pnl": None, "max_pnl": None}

    exit_reasons = ["all", *sorted(trades_df["exit_reason"].unique().tolist())]

    def update_filter(key: str, value: Any) -> None:
        filters[key] = value
        filtered_table.refresh()

    def clear_filters() -> None:
        filters["symbol"] = ""
        filters["exit_reason"] = "all"
        filters["min_pnl"] = None
        filters["max_pnl"] = None
        filtered_table.refresh()

    @ui.refreshable
    def filtered_table():
        filtered = rows
        if filters["symbol"]:
            filtered = [
                r for r in filtered if filters["symbol"].lower() in r.get("symbol", "").lower()
            ]
        if filters["exit_reason"] != "all":
            filtered = [r for r in filtered if r.get("exit_reason") == filters["exit_reason"]]
        if filters["min_pnl"] is not None:
            filtered = [
                r
                for r in filtered
                if float(r.get("pnl_pct", 0).replace("%", "")) >= filters["min_pnl"]
            ]
        if filters["max_pnl"] is not None:
            filtered = [
                r
                for r in filtered
                if float(r.get("pnl_pct", 0).replace("%", "")) <= filters["max_pnl"]
            ]

        paginated_table(filtered, columns, page_size=page_size)

    with ui.column().classes("w-full"):
        with ui.row().classes("gap-4 mb-4 items-end w-full flex-wrap"):
            ui.input(
                "Symbol",
                value=filters["symbol"],
                on_change=lambda e: update_filter("symbol", e.value),
            ).props("dense outlined clearable").classes("w-40")

            if len(exit_reasons) > 1:
                ui.select(
                    exit_reasons,
                    value=filters["exit_reason"],
                    label="Exit Reason",
                    on_change=lambda e: update_filter("exit_reason", e.value),
                ).props("dense outlined").classes("w-40")

            ui.input(
                "Min P&L %",
                on_change=lambda e: update_filter("min_pnl", float(e.value) if e.value else None),
            ).props("dense outlined type='number' clearable").classes("w-32")

            ui.input(
                "Max P&L %",
                on_change=lambda e: update_filter("max_pnl", float(e.value) if e.value else None),
            ).props("dense outlined type='number' clearable").classes("w-32")

            ui.button("Clear", on_click=clear_filters).props("flat").classes("mb-1")

        filtered_table()


# ---------------------------------------------------------------------------
# Dark mode theme
# ---------------------------------------------------------------------------
THEME_DARK = {
    "page_bg": "#0f172a",
    "surface": "#1e293b",
    "surface_border": "#334155",
    "surface_hover": "#334155",
    "text_primary": "#f1f5f9",
    "text_secondary": "#94a3b8",
    "text_muted": "#64748b",
    "primary": "#3b82f6",
    "primary_dark": "#2563eb",
    "divider": "#334155",
}

_dark_mode_state = {"enabled": False}


def toggle_dark_mode() -> None:
    """Toggle between light and dark theme."""
    _dark_mode_state["enabled"] = not _dark_mode_state["enabled"]
    current_theme = THEME_DARK if _dark_mode_state["enabled"] else THEME
    ui.dark_mode(_dark_mode_state["enabled"])
    ui.query("body").style(
        f"background-color: {current_theme['page_bg']}; color: {current_theme['text_primary']};"
    )


# ---------------------------------------------------------------------------
# Keyboard shortcuts
# ---------------------------------------------------------------------------
_KEYBINDINGS_HTML = """
<script>
document.addEventListener('keydown', (e) => {
    if (e.altKey && !e.ctrlKey && !e.shiftKey) {
        const shortcuts = {
            'g': () => window.location.href = '/',
            'b': () => window.location.href = '/backtest',
            't': () => window.location.href = '/trade_analytics',
            'c': () => window.location.href = '/compare',
            's': () => window.location.href = '/strategy',
            'r': () => window.location.href = '/scans',
            'd': () => window.location.href = '/data_quality',
            'p': () => window.location.href = '/pipeline',
            'l': () => window.location.href = '/paper_ledger',
            'y': () => window.location.href = '/daily_summary',
        };
        if (shortcuts[e.key]) {
            e.preventDefault();
            shortcuts[e.key]();
        }
    }
    if (e.key === '?' && !e.altKey && !e.ctrlKey) {
        e.preventDefault();
        if (window.showShortcutsDialog) {
            window.showShortcutsDialog();
        }
    }
});
</script>
"""


def shortcuts_dialog():
    """Show keyboard shortcuts dialog and return the dialog instance."""
    with ui.dialog() as dialog:
        with ui.card().classes("w-96"):
            ui.label("Keyboard Shortcuts").classes("text-xl font-bold mb-4").style(
                f"color: {THEME['text_primary']};"
            )
            shortcuts = [
                ("Alt+G", "Go to Home"),
                ("Alt+B", "Backtest Results"),
                ("Alt+T", "Trade Analytics"),
                ("Alt+C", "Compare"),
                ("Alt+S", "Strategy"),
                ("Alt+R", "Scans"),
                ("Alt+D", "Data Quality"),
                ("Alt+P", "Pipeline"),
                ("Alt+L", "Paper Ledger"),
                ("Alt+Y", "Daily Summary"),
                ("?", "Show shortcuts"),
            ]
            for key, action in shortcuts:
                with ui.row().classes("justify-between w-full py-1"):
                    ui.label(key).classes("font-mono text-sm px-2 py-1 rounded").style(
                        f"background: {THEME['surface_hover']}; color: {THEME['text_primary']};"
                    )
                    ui.label(action).classes("text-sm").style(f"color: {THEME['text_secondary']};")

            ui.button("Close", on_click=dialog.close).props("flat").classes("mt-4 w-full")

    return dialog


# ---------------------------------------------------------------------------
# Tour modal
# ---------------------------------------------------------------------------
_tour_completed = {"completed": False}


def show_tour() -> None:
    """Show first-time user tour."""
    if _tour_completed["completed"]:
        return

    steps = [
        {
            "title": "Welcome to NSE Momentum Lab",
            "content": "A local-first momentum research and backtest analysis platform. Let's take a quick tour.",
        },
        {
            "title": "Navigation",
            "content": "Use the sidebar to navigate between pages. Press Alt+? anytime to see keyboard shortcuts.",
        },
        {
            "title": "Backtest Results",
            "content": "View detailed analysis of your backtest experiments including equity curves, trade breakdown, and performance metrics.",
        },
        {
            "title": "Running a Backtest",
            "content": "Run a backtest from your terminal: 'doppler run -- uv run nseml-backtest --universe-size 2000'",
        },
        {
            "title": "All Set!",
            "content": "You're ready to start researching. Access this tour anytime from the home page.",
        },
    ]

    current_step = {"step": 0}

    @ui.refreshable
    def tour_dialog():
        step = steps[current_step["step"]]
        with ui.dialog() as dialog:
            with ui.card().classes("w-[500px]"):
                with ui.row().classes("justify-between items-center mb-4"):
                    ui.label(step["title"]).classes("text-lg font-semibold").style(
                        f"color: {THEME['text_primary']};"
                    )
                    ui.label(f"{current_step['step'] + 1} / {len(steps)}").classes(
                        f"color: {THEME['text_muted']};"
                    )

                ui.label(step["content"]).classes("mb-6").style(
                    f"color: {THEME['text_secondary']};"
                )

                with ui.row().classes("justify-end gap-2"):
                    if current_step["step"] > 0:
                        ui.button(
                            "Previous",
                            on_click=lambda: (
                                current_step.update(step=current_step["step"] - 1),
                                tour_dialog.refresh(),
                            ),
                        ).props("flat")

                    if current_step["step"] < len(steps) - 1:
                        ui.button(
                            "Next",
                            on_click=lambda: (
                                current_step.update(step=current_step["step"] + 1),
                                tour_dialog.refresh(),
                            ),
                        ).props("push color=primary")
                    else:
                        ui.button(
                            "Get Started",
                            on_click=lambda: (
                                _tour_completed.update(completed=True),
                                dialog.close(),
                            ),
                        ).props("push color=primary")

                    ui.button(
                        "Skip Tour",
                        on_click=lambda: (_tour_completed.update(completed=True), dialog.close()),
                    ).props("flat")

        dialog.open()

    tour_dialog()


# ---------------------------------------------------------------------------
# Experiment alerts
# ---------------------------------------------------------------------------
class ExperimentAlerts:
    """Manage alerts for new experiments."""

    def __init__(self):
        self._last_seen_count = 0
        self._alert_element = None

    def check_and_alert(self, experiments_df: pd.DataFrame) -> None:
        """Check for new experiments and show alert if found."""
        current_count = len(experiments_df)

        if current_count > self._last_seen_count and self._last_seen_count > 0:
            new_count = current_count - self._last_seen_count

            if self._alert_element:
                self._alert_element.delete()

            with ui.row().classes("fixed top-16 right-4 z-50") as alert_row:
                self._alert_element = alert_row
                with ui.column().classes("gap-2"):
                    with (
                        ui.card()
                        .classes("p-4 shadow-lg")
                        .style(
                            f"background: {THEME['surface']}; border: 1px solid {THEME['surface_border']};"
                        )
                    ):
                        with ui.row().classes("items-center gap-3"):
                            ui.icon("new_releases").classes("text-2xl").style(
                                f"color: {COLORS['success']};"
                            )
                            ui.label(
                                f"{new_count} new experiment{'s' if new_count > 1 else ''} available"
                            ).classes("font-medium").style(f"color: {THEME['text_primary']};")
                        with ui.row().classes("gap-2 mt-2"):
                            ui.button("View", on_click=lambda: ui.navigate.to("/backtest")).props(
                                "push color=primary flat"
                            )
                            ui.button("Dismiss", on_click=alert_row.delete).props("flat")

        self._last_seen_count = current_count


_experiment_alerts = ExperimentAlerts()
