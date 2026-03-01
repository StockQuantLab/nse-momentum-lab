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

/* KPI cards */
.kpi-card {
    background: %(surface)s;
    border: 1px solid %(surface_border)s;
    border-radius: 12px;
    padding: 20px;
    transition: border-color 0.2s, box-shadow 0.2s;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.kpi-card:hover {
    border-color: %(primary)s;
    box-shadow: 0 0 0 1px %(primary)s33, 0 4px 12px rgba(37,99,235,0.08);
}

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
