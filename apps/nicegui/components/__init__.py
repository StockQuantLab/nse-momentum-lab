"""Reusable UI components for NiceGUI dashboard.

Provides:
- THEME / COLORS dicts for consistent styling
- page_layout() context manager — sidebar nav + top bar + content area
- kpi_card / kpi_grid — aligned metric cards with Material icons
- nav_card — home-page navigation tiles
- apply_chart_theme — unified Plotly light/professional theme
- divider / info_box / export_button — utility widgets

Theme System:
- Uses CSS variables for dynamic theme switching
- Terminal mode: Dark, brutalist trading terminal aesthetic
- Clean mode: Light, modern SaaS dashboard aesthetic
"""

from __future__ import annotations

import inspect
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import polars as pl

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
# Theme definitions — Terminal (Dark) and Clean (Light)
# ---------------------------------------------------------------------------

# Terminal theme — Dark, brutalist trading terminal with neon green
THEME_TERMINAL = {
    "page_bg": "#0d1117",
    "surface": "#161b22",
    "surface_border": "#30363d",
    "surface_hover": "#21262d",
    "text_primary": "#f0f6fc",
    "text_secondary": "#8b949e",
    "text_muted": "#6e7681",
    "primary": "#00ff88",  # Classic terminal phosphor green
    "primary_dark": "#00cc6a",
    "divider": "#30363d",
}

COLORS_TERMINAL = {
    "success": "#00ff88",  # Neon green
    "error": "#ff6b6b",
    "warning": "#ffd93d",
    "info": "#6bcfff",
    "primary": "#00ff88",
    "gray": "#6e7681",
}

# Clean theme — Light, modern SaaS dashboard with indigo primary
THEME_CLEAN = {
    "page_bg": "#f8fafc",
    "surface": "#ffffff",
    "surface_border": "#e2e8f0",
    "surface_hover": "#f1f5f9",
    "text_primary": "#0f172a",
    "text_secondary": "#475569",
    "text_muted": "#64748b",
    "primary": "#6366f1",  # Indigo - more distinctive than standard blue
    "primary_dark": "#4f46e5",
    "divider": "#e2e8f0",
}

COLORS_CLEAN = {
    "success": "#22c55e",
    "error": "#ef4444",
    "warning": "#f59e0b",
    "info": "#6366f1",  # Match primary
    "primary": "#6366f1",
    "gray": "#64748b",
}

# Current active theme (starts with Terminal)
THEME = THEME_TERMINAL.copy()
COLORS = COLORS_TERMINAL.copy()

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
    {"label": "Market Monitor", "icon": "monitoring", "path": "/market_monitor"},
]

# ---------------------------------------------------------------------------
# CSS with theme variables — supports both Terminal and Clean modes
# ---------------------------------------------------------------------------

# Terminal fonts — IBM Plex Sans + Fira Code for that terminal aesthetic
_FONT_HEAD_TERMINAL = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Fira+Code:wght@400;500;600;700&display=swap" media="print" onload="this.media='all'">
"""

# Clean theme fonts — DM Sans (distinctive, not generic Inter) + JetBrains Mono
_FONT_HEAD_CLEAN = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" media="print" onload="this.media='all'">
"""


# Get current font based on theme mode
def _get_font_html() -> str:
    return _FONT_HEAD_TERMINAL if _theme_mode["terminal"] else _FONT_HEAD_CLEAN


# Base CSS using CSS variables — works for both themes
_PAGE_CSS_BASE = """
/* Typography — Terminal: Fira Code, Clean: DM Sans */
body, .q-app {
    font-family: var(--font-body, 'DM Sans', system-ui, -apple-system, sans-serif) !important;
}

/* Mono font for data and code — Fira Code for Terminal, JetBrains for Clean */
.mono-font, .kpi-card, .q-table, .q-input, .q-select, .q-btn {
    font-family: var(--font-mono, 'JetBrains Mono', 'Courier New', monospace) !important;
}
.q-table, .q-input, .q-select {
    letter-spacing: 0.02em;
}

/* Type scale — consistent heading sizes */
h1, .text-h1, .text-4xl { font-size: 2.25rem; font-weight: 700; letter-spacing: -0.02em; }
h2, .text-h2, .text-3xl { font-size: 1.75rem; font-weight: 600; letter-spacing: -0.01em; }
h3, .text-h3, .text-2xl { font-size: 1.5rem; font-weight: 600; letter-spacing: -0.01em; }
h4, .text-h4, .text-xl { font-size: 1.25rem; font-weight: 600; }
.text-lg { font-size: 1.1rem; font-weight: 500; }
.text-sm { font-size: 0.875rem; font-weight: 400; }
.text-xs { font-size: 0.75rem; font-weight: 400; }

/* Terminal scanline effect — only in terminal mode */
body.terminal-mode::after {
    content: "";
    position: fixed;
    top: 0;
    left: 0;
    width: 100vw;
    height: 100vh;
    background: repeating-linear-gradient(
        0deg,
        transparent,
        transparent 2px,
        rgba(255, 255, 255, 0.015) 2px,
        rgba(255, 255, 255, 0.015) 4px
    );
    pointer-events: none;
    z-index: 9999;
    opacity: 0.3;
}

/* KPI cards — uses theme variables, generous padding for breathing room */
.kpi-card {
    background: var(--theme-surface);
    border: 1px solid var(--theme-surface-border);
    border-radius: var(--card-radius, 4px);
    padding: 20px 24px;
    transition: all 0.15s ease;
    box-shadow: var(--card-shadow, 0 2px 8px rgba(0,0,0,0.4));
    position: relative;
}
.kpi-card::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    width: 2px;
    height: 100%;
    background: var(--theme-primary);
    opacity: 0;
    transition: opacity 0.15s;
}
.kpi-card:hover {
    border-color: var(--theme-primary);
    box-shadow: var(--card-hover-shadow, 0 0 20px rgba(74, 222, 128, 0.15));
    transform: translateX(2px);
}
.kpi-card:hover::before {
    opacity: 1;
}

/* Page content spacing */
.page-content {
    padding-top: 24px;
}
.page-header {
    margin-top: 24px;
    margin-bottom: 16px;
}

/* Terminal-style pulse animation */
@keyframes terminal-pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.7; }
}

/* Staggered fade-in animation */
@keyframes fade-in-terminal {
    from { opacity: 0; transform: translateY(4px); }
    to { opacity: 1; transform: translateY(0); }
}
.kpi-card { animation: fade-in-terminal 0.2s ease-out backwards; }
.kpi-card:nth-child(1) { animation-delay: 0.02s; }
.kpi-card:nth-child(2) { animation-delay: 0.04s; }
.kpi-card:nth-child(3) { animation-delay: 0.06s; }
.kpi-card:nth-child(4) { animation-delay: 0.08s; }
.kpi-card:nth-child(5) { animation-delay: 0.1s; }
.kpi-card:nth-child(6) { animation-delay: 0.12s; }
.kpi-card:nth-child(7) { animation-delay: 0.14s; }
.kpi-card:nth-child(8) { animation-delay: 0.16s; }

/* Nav tiles — uses theme variables */
.nav-tile {
    background: var(--theme-surface);
    border: 1px solid var(--theme-surface-border);
    border-radius: var(--tile-radius, 2px);
    padding: 20px;
    cursor: pointer;
    transition: all 0.1s;
    box-shadow: var(--tile-shadow, 0 2px 4px rgba(0,0,0,0.3));
    position: relative;
}
.nav-tile::after {
    content: ">>";
    position: absolute;
    right: 16px;
    top: 50%;
    transform: translateY(-50%);
    color: var(--theme-primary);
    opacity: 0;
    transition: opacity 0.15s;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
}
.nav-tile:hover {
    border-color: var(--theme-primary);
    transform: translateX(4px);
    box-shadow: 0 0 16px var(--theme-primary-alpha);
}
.nav-tile:hover::after {
    opacity: 1;
}

/* Sidebar nav items — uses theme variables */
.nav-item {
    border-radius: var(--nav-radius, 0);
    padding: 8px 16px;
    margin: 0;
    transition: all 0.1s;
    cursor: pointer;
    color: var(--theme-text-secondary);
    position: relative;
}
.nav-row {
    display: flex;
    align-items: center;
    gap: 12px;
}
.nav-item::before {
    content: ">";
    position: absolute;
    left: 8px;
    opacity: 0;
    color: var(--theme-primary);
    font-family: 'JetBrains Mono', monospace;
    transition: opacity 0.1s;
}
.nav-item:hover {
    background: var(--theme-surface-hover);
    color: var(--theme-text-primary);
    padding-left: 24px;
}
.nav-item:hover::before {
    opacity: 1;
}
.nav-item-active {
    background: var(--theme-primary-alpha);
    color: var(--theme-primary) !important;
    font-weight: 500;
}
.nav-item-active::before {
    content: ">";
    opacity: 1;
}
.nav-icon {
    width: 24px;
    flex: 0 0 24px;
    text-align: center;
    margin-left: 3px;
}
.nav-label {
    flex: 1;
    line-height: 1.2;
}

/* Quasar table overrides — uses theme variables, more vertical padding */
.q-table {
    background: var(--theme-surface) !important;
    color: var(--theme-text-primary) !important;
    border: 1px solid var(--theme-surface-border);
}
.q-table thead th {
    color: var(--theme-primary) !important;
    font-weight: 600;
    text-transform: uppercase;
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    border-bottom: 2px solid var(--theme-surface-border) !important;
    font-family: var(--font-mono);
    padding: 14px 16px !important;
}
.q-table tbody td {
    border-bottom: 1px solid var(--theme-divider) !important;
    color: var(--theme-text-primary) !important;
    font-family: var(--font-mono);
    padding: 12px 16px !important;
}
.q-table tbody tr:hover td {
    background: var(--theme-surface-hover) !important;
}

/* Quasar tabs — uses theme variables */
.q-tab {
    color: var(--theme-text-secondary) !important;
    font-family: 'JetBrains Mono', monospace;
    text-transform: uppercase;
    font-size: 0.75rem;
    letter-spacing: 0.05em;
}
.q-tab--active {
    color: var(--theme-primary) !important;
    font-weight: 600;
}
.q-tabs__content { border-bottom: 1px solid var(--theme-surface-border); }

/* Quasar expansion — uses theme variables */
.q-expansion-item {
    background: var(--theme-surface) !important;
    border: 1px solid var(--theme-surface-border);
    border-radius: 2px !important;
    box-shadow: 0 2px 4px rgba(0,0,0,0.2);
}
.q-expansion-item__header { border-radius: 2px !important; }

/* Quasar select / inputs — uses theme variables */
.q-field__native, .q-field__input {
    color: var(--theme-text-primary) !important;
    font-family: 'JetBrains Mono', monospace;
}
.q-field__label { color: var(--theme-text-secondary) !important; }
.q-field--outlined .q-field__control:before {
    border-color: var(--theme-surface-border) !important;
}
.q-field--outlined.q-field--focused .q-field__control:before {
    border-color: var(--theme-primary) !important;
    box-shadow: 0 0 8px var(--theme-primary-alpha);
}

/* Info box — uses theme variables */
.info-box {
    background: var(--info-box-bg);
    border: 1px solid var(--theme-primary);
    border-radius: 2px;
    padding: 12px 16px;
}
.info-box::before {
    content: "[INFO] ";
    color: var(--theme-primary);
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
}

/* Code / terminal blocks — uses theme variables */
.code-block {
    background: var(--code-bg, #000);
    border: 1px solid var(--theme-surface-border);
    border-radius: 2px;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    font-size: 0.8rem;
    color: var(--theme-primary);
}

/* Sidebar mini-mode */
.q-drawer--mini .sidebar-logo { display: none !important; }
.q-drawer--mini .nav-row {
    justify-content: center !important;
    padding: 10px 0 !important;
    margin: 2px 4px !important;
    gap: 0 !important;
}
.q-drawer--mini .nav-label { display: none !important; }
.q-drawer--mini .nav-icon  { font-size: 1.3rem !important; }
.q-drawer { transition: width 0.2s ease !important; }

/* Scrollbar — uses theme variables */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}
::-webkit-scrollbar-track {
    background: var(--theme-page-bg);
}
::-webkit-scrollbar-thumb {
    background: var(--theme-surface-border);
    border-radius: 0;
}
::-webkit-scrollbar-thumb:hover {
    background: var(--theme-text-muted);
}

/* Pagination controls — styled to match theme */
.q-table .q-pagination {
    color: var(--theme-text-secondary) !important;
}
.q-table .q-pagination .q-btn {
    color: var(--theme-text-secondary) !important;
    background: var(--theme-surface) !important;
    border: 1px solid var(--theme-surface-border) !important;
}
.q-table .q-pagination .q-btn:hover {
    background: var(--theme-surface-hover) !important;
    border-color: var(--theme-primary) !important;
    color: var(--theme-primary) !important;
}
.q-table .q-pagination .q-btn.q-btn--active {
    background: var(--theme-primary) !important;
    color: var(--theme-page-bg) !important;
    border-color: var(--theme-primary) !important;
}
.q-table .q-pagination__select {
    background: var(--theme-surface) !important;
    border: 1px solid var(--theme-surface-border) !important;
    color: var(--theme-text-primary) !important;
}

/* Negative value highlighting — auto-detect and color red */
.value-negative {
    color: var(--theme-color-error) !important;
    font-weight: 600;
}
.value-positive {
    color: var(--theme-color-success) !important;
}
.value-neutral {
    color: var(--theme-text-muted) !important;
}

/* Table row numbers column */
.row-number {
    color: var(--theme-text-muted) !important;
    font-size: 0.75rem !important;
    text-align: center !important;
    user-select: none;
}

/* Scrollable table container */
.scrollable-table {
    max-height: 450px;
    overflow-y: auto;
    overflow-x: auto;
    width: 100%;
    display: block;
}
.scrollable-table > div {
    overflow-x: auto !important;
}
.scrollable-table .q-table {
    width: max-content !important;
    min-width: 100%;
}
.scrollable-table .q-table__card {
    background: transparent !important;
    box-shadow: none !important;
    overflow-x: visible !important;
}

/* Force horizontal scroll on ALL tables */
.q-table__card {
    overflow-x: auto !important;
}
.q-table__container {
    overflow-x: visible !important;
}
.q-table {
    width: 100%;
}
"""


def _get_themed_css() -> str:
    """Generate complete CSS with variables for current theme."""
    theme = get_current_theme()
    colors = get_current_colors()

    # Build CSS variables
    css_vars = f"""
:root {{
    --theme-page-bg: {theme["page_bg"]};
    --theme-surface: {theme["surface"]};
    --theme-surface-border: {theme["surface_border"]};
    --theme-surface-hover: {theme["surface_hover"]};
    --theme-text-primary: {theme["text_primary"]};
    --theme-text-secondary: {theme["text_secondary"]};
    --theme-text-muted: {theme["text_muted"]};
    --theme-primary: {theme["primary"]};
    --theme-primary-dark: {theme["primary_dark"]};
    --theme-divider: {theme["divider"]};
    --theme-color-success: {colors["success"]};
    --theme-color-error: {colors["error"]};
    --theme-color-warning: {colors["warning"]};
    --theme-color-info: {colors["info"]};
    --theme-color-gray: {colors["gray"]};
"""

    if _theme_mode["terminal"]:
        # Terminal mode specific variables — Fira Code for that authentic terminal feel
        css_vars += """
    --font-body: 'IBM Plex Sans', system-ui, -apple-system, sans-serif;
    --font-mono: 'Fira Code', 'Courier New', monospace;
    --card-radius: 4px;
    --card-shadow: 0 2px 8px rgba(0,0,0,0.4);
    --card-hover-shadow: 0 0 20px rgba(0, 255, 136, 0.2), 0 2px 12px rgba(0,0,0,0.5);
    --tile-radius: 2px;
    --tile-shadow: 0 2px 4px rgba(0,0,0,0.3);
    --nav-radius: 0;
    --theme-primary-alpha: rgba(0, 255, 136, 0.15);
    --info-box-bg: rgba(0, 255, 136, 0.05);
    --code-bg: #000;
}}
/* Add terminal-mode class to body for terminal-specific effects */
body {{
    --font-body: 'IBM Plex Sans', system-ui, -apple-system, sans-serif !important;
    --font-mono: 'Fira Code', 'Courier New', monospace !important;
}}
"""
    else:
        # Clean mode specific variables — DM Sans for modern, professional look
        css_vars += """
    --font-body: 'DM Sans', system-ui, -apple-system, sans-serif;
    --font-mono: 'JetBrains Mono', 'SF Mono', 'Monaco', 'Courier New', monospace;
    --card-radius: 8px;
    --card-shadow: 0 1px 3px rgba(0,0,0,0.1);
    --card-hover-shadow: 0 4px 12px rgba(0,0,0,0.15);
    --tile-radius: 8px;
    --tile-shadow: 0 1px 3px rgba(0,0,0,0.1);
    --nav-radius: 6px;
    --theme-primary-alpha: rgba(99, 102, 241, 0.1);
    --info-box-bg: rgba(99, 102, 241, 0.05);
    --code-bg: #1e293b;
}}
/* Remove scanline effect in clean mode */
body::after {{
    display: none;
}}
"""

    return css_vars + _PAGE_CSS_BASE


# ---------------------------------------------------------------------------
# page_layout — wraps every page (theme-aware)
# ---------------------------------------------------------------------------
@contextmanager
def page_layout(title: str, icon: str = "bar_chart"):
    """Context manager that wraps every page with consistent chrome.

    Usage::

        def my_page():
            with page_layout("Backtest Results", "bar_chart"):
                ui.label("Hello")
    """
    # Get current theme settings
    is_terminal = _theme_mode["terminal"]

    # -- mode + palette -----------------------------------------------
    ui.dark_mode(is_terminal)
    ui.colors(primary=THEME["primary"])
    ui.query("body").style(f"background-color: {THEME['page_bg']}; color: {THEME['text_primary']};")

    # Inject font for current theme
    ui.add_head_html(_get_font_html())

    # Inject themed CSS with variables
    ui.add_css(_get_themed_css())

    # Inject keyboard shortcuts
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
        .classes("items-center px-4 py-0")
        .style(
            f"background: {THEME['surface']}; "
            f"border-bottom: 1px solid {THEME['surface_border']}; "
            "height: 48px;"
        )
    ):
        # Hamburger — cycles sidebar through expanded / mini / hidden
        ui.button(icon="menu", on_click=_cycle_sidebar).props("flat round dense").style(
            f"color: {THEME['text_secondary']};"
        )

        # Terminal-style title with status indicator
        with ui.row().classes("items-center gap-2 ml-2"):
            # Blinking green status dot
            ui.icon("circle").classes("text-xs").style(
                f"color: {THEME['primary']}; animation: terminal-pulse 2s infinite;"
            )
            ui.label("NSE_MOMENTUM_LAB").classes("text-sm font-semibold mono-font").style(
                f"color: {THEME['text_primary']}; letter-spacing: 0.1em;"
            )
            # Terminal-style version
            ui.label("v0.1.0").classes("text-xs").style(
                f"color: {THEME['text_muted']}; font-family: 'JetBrains Mono', monospace;"
            )

        # breadcrumb
        ui.label(f"// {title.upper()}").classes("text-sm ml-4 mono-font").style(
            f"color: {THEME['text_secondary']}; letter-spacing: 0.05em;"
        )

        ui.space()

        # Theme toggle (shows "TERMINAL" in Clean mode, "CLEAN" in Terminal mode)
        toggle_label = "TERMINAL" if not is_terminal else "CLEAN"
        ui.button(toggle_label, on_click=toggle_theme_mode).props("flat dense").classes(
            "text-xs mono-font px-3"
        ).style(
            f"color: {THEME['text_secondary']}; "
            f"border: 1px solid {THEME['surface_border']}; "
            "border-radius: 2px; padding: 4px 12px;"
        )

        # Shortcuts help
        _shortcuts_dialog_instance = shortcuts_dialog()
        ui.button("?", on_click=_shortcuts_dialog_instance.open).props("flat dense").classes(
            "mono-font text-xs px-2"
        ).style(
            f"color: {THEME['primary']}; "
            f"border: 1px solid {THEME['surface_border']}; "
            "border-radius: 2px;"
        )

        # Right-side icon
        ui.icon(icon).classes("text-lg").style(f"color: {THEME['primary']};")

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
        # Logo area — terminal style, hidden in mini mode
        with ui.column().classes("px-4 py-3 gap-1 sidebar-logo"):
            with ui.row().classes("items-center gap-2"):
                ui.icon("terminal").classes("text-sm").style(f"color: {THEME['primary']};")
                ui.label("NSEQ_LAB").classes("text-sm font-bold mono-font").style(
                    f"color: {THEME['text_primary']}; letter-spacing: 0.1em;"
                )
            with ui.row().classes("items-center gap-2"):
                ui.label("SYSTEM").classes("text-xs mono-font").style(
                    f"color: {THEME['text_muted']};"
                )
                ui.icon("circle").classes("text-xs").style(
                    f"color: {THEME['primary']}; animation: terminal-pulse 2s infinite;"
                )

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
    with ui.column().classes("w-full px-6 py-6"):
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
# Plotly chart theme (theme-aware)
# ---------------------------------------------------------------------------
def apply_chart_theme(fig) -> None:
    """Apply theme to a Plotly figure (mutates in place).

    Adds entrance animation and applies theme-specific styling.
    """
    is_terminal = _theme_mode["terminal"]
    theme = get_current_theme()

    # Use theme-specific fonts
    mono_font = "Fira Code, monospace" if is_terminal else "JetBrains Mono, monospace"
    body_font = "IBM Plex Sans, sans-serif" if is_terminal else "DM Sans, sans-serif"

    if is_terminal:
        # Terminal mode - dark theme with neon accents
        fig.update_layout(
            paper_bgcolor=theme["surface"],
            plot_bgcolor=theme["page_bg"],
            font_color=theme["text_primary"],
            font_family=mono_font,
            title_font=dict(color=theme["text_primary"], size=14, family=body_font),
            margin=dict(l=40, r=20, t=40, b=40),
            xaxis=dict(
                gridcolor=theme["surface_border"],
                zerolinecolor=theme["surface_border"],
                linecolor=theme["surface_border"],
                tickfont=dict(color=theme["text_secondary"], family=mono_font, size=10),
            ),
            yaxis=dict(
                gridcolor=theme["surface_border"],
                zerolinecolor=theme["surface_border"],
                linecolor=theme["surface_border"],
                tickfont=dict(color=theme["text_secondary"], family=mono_font, size=10),
            ),
            legend=dict(
                bgcolor="rgba(13, 17, 23, 0.9)",
                font_color=theme["text_secondary"],
                bordercolor=theme["surface_border"],
                borderwidth=1,
            ),
            hoverlabel=dict(
                bgcolor=theme["surface_border"],
                font_color=theme["text_primary"],
                font_size=11,
            ),
        )
    else:
        # Clean mode - light theme with indigo accents
        fig.update_layout(
            paper_bgcolor=theme["surface"],
            plot_bgcolor=theme["page_bg"],
            font_color=theme["text_primary"],
            font_family=body_font,
            title_font=dict(color=theme["text_primary"], size=14, family=body_font),
            margin=dict(l=40, r=20, t=40, b=40),
            xaxis=dict(
                gridcolor=theme["surface_border"],
                zerolinecolor=theme["surface_border"],
                linecolor=theme["surface_border"],
                tickfont=dict(color=theme["text_secondary"], family=mono_font, size=10),
            ),
            yaxis=dict(
                gridcolor=theme["surface_border"],
                zerolinecolor=theme["surface_border"],
                linecolor=theme["surface_border"],
                tickfont=dict(color=theme["text_secondary"], family=mono_font, size=10),
            ),
            legend=dict(
                bgcolor="rgba(255, 255, 255, 0.95)",
                font_color=theme["text_secondary"],
                bordercolor=theme["surface_border"],
                borderwidth=1,
            ),
            hoverlabel=dict(
                bgcolor=theme["surface_border"],
                font_color=theme["text_primary"],
                font_size=11,
            ),
        )

    # Add entrance animation for all traces
    for trace in fig.data:
        if hasattr(trace, "marker"):
            # Bar/scatter charts - fade in
            trace.opacity = 0.95
        elif hasattr(trace, "line"):
            # Line charts - animate drawing
            pass

    # Configure animation settings
    fig.update_layout(
        hovermode="x unified",
        transition_duration=400,
    )


# ---------------------------------------------------------------------------
# Utility widgets
# ---------------------------------------------------------------------------
def divider() -> None:
    """Render a styled divider."""
    ui.separator().classes("my-6").style(f"background: {THEME['surface_border']};")


def format_value(value: float, fmt: str = "{:.2f}") -> str:
    """Format a numeric value with color class for negative/positive.

    Returns a string with CSS class for coloring:
    - Negative values: 'value-negative' (red)
    - Positive values: 'value-positive' (green)
    - Zero: 'value-neutral' (muted)

    Usage in table cells:
        value_str, value_class = format_value(-5.2)
        ui.label(value_str).classes(value_class)
    """
    try:
        num_val = float(value)
    except ValueError, TypeError:
        return str(value), "value-neutral"

    formatted = fmt.format(num_val)

    if num_val < 0:
        return formatted, "value-negative"
    elif num_val > 0:
        return formatted, "value-positive"
    else:
        return formatted, "value-neutral"


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
    if data is None or (hasattr(data, "is_empty") and data.is_empty()):
        return

    csv_content = data.write_csv()

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
    page_size: int = 20,
    row_key: Any = None,
    on_row_click: Any = None,
) -> None:
    """Paginated table that only renders current page."""
    if not rows:
        ui.label("No data to display").style(f"color: {THEME['text_muted']};")
        return

    total_pages = (len(rows) + page_size - 1) // page_size
    state = {"page": 0}
    table_row_key = row_key or ("id" if rows and "id" in rows[0] else None)

    def _is_probable_row(payload: dict[str, Any]) -> bool:
        if not payload:
            return False
        row_markers = {
            "run_id",
            "strategy",
            "symbol",
            "date",
            "trade_date",
            "entry_time",
            "exit_time",
            "idx",
            "trade_row_id",
        }
        if row_markers.intersection(payload.keys()):
            return True
        pointer_event_keys = {
            "altKey",
            "ctrlKey",
            "metaKey",
            "shiftKey",
            "button",
            "buttons",
            "clientX",
            "clientY",
            "offsetX",
            "offsetY",
            "pageX",
            "pageY",
            "screenX",
            "screenY",
            "type",
            "isTrusted",
        }
        return not set(payload.keys()).issubset(pointer_event_keys)

    def _extract_row_payload(event: Any) -> dict[str, Any]:
        args = getattr(event, "args", None)

        if isinstance(args, dict):
            for key in ("row", "record", "item", "data"):
                val = args.get(key)
                if isinstance(val, dict):
                    return val
            for val in args.values():
                if isinstance(val, dict) and _is_probable_row(val):
                    return val
            return args if _is_probable_row(args) else {}

        if isinstance(args, list | tuple):
            if len(args) >= 2 and isinstance(args[1], dict) and _is_probable_row(args[1]):
                return args[1]
            for item in args:
                if isinstance(item, dict) and _is_probable_row(item):
                    return item
            for item in reversed(args):
                if isinstance(item, dict):
                    return item
            return {}

        return {}

    def show_page():
        start = state["page"] * page_size
        end = start + page_size

        with ui.column().classes("w-full"):
            with ui.element("div").style("width: 100%; overflow-x: auto;"):
                table = ui.table(
                    columns=columns,
                    rows=rows[start:end],
                    pagination={"rowsPerPage": page_size, "rowsPerPage_options": [10, 20, 50, 100]},
                    row_key=table_row_key,
                ).style("min-width: max-content;")

                if on_row_click:

                    async def _handle_row_click(e) -> None:
                        row_payload = _extract_row_payload(e)
                        if not row_payload:
                            return
                        try:
                            if not ui.context.client.has_socket_connection:
                                return
                        except AttributeError, RuntimeError:
                            return
                        result = on_row_click(row_payload)
                        if inspect.isawaitable(result):
                            await result

                    table.on("row-click", _handle_row_click)

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
    if data is None or (hasattr(data, "is_empty") and data.is_empty()):
        return

    with ui.button(label, icon="download").props("flat"):
        with ui.menu().props("anchor=top-end"):
            csv_content = data.write_csv()
            ui.menu_item(
                "Download as CSV",
                lambda: ui.download(csv_content.encode("utf-8"), filename=f"{filename_base}.csv"),
            )
            json_content = data.write_json() if hasattr(data, "write_json") else str(data)
            ui.menu_item(
                "Download as JSON",
                lambda: ui.download(json_content.encode("utf-8"), filename=f"{filename_base}.json"),
            )


# ---------------------------------------------------------------------------
# Trade table with filters
# ---------------------------------------------------------------------------
def trade_table_with_filters(
    trades_df: pl.DataFrame,
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

    exit_reasons = ["all", *sorted(trades_df["exit_reason"].unique().to_list())]

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
# Theme state management
# ---------------------------------------------------------------------------
_theme_mode = {"terminal": True}  # True = terminal, False = clean


def get_current_theme() -> dict:
    """Return the current active theme dictionary."""
    return THEME_TERMINAL if _theme_mode["terminal"] else THEME_CLEAN


def get_current_colors() -> dict:
    """Return the current active colors dictionary."""
    return COLORS_TERMINAL if _theme_mode["terminal"] else COLORS_CLEAN


def _get_css_variables() -> str:
    """Generate CSS variables for the current theme."""
    theme = get_current_theme()
    colors = get_current_colors()
    return f"""
:root {{
    --theme-page-bg: {theme["page_bg"]};
    --theme-surface: {theme["surface"]};
    --theme-surface-border: {theme["surface_border"]};
    --theme-surface-hover: {theme["surface_hover"]};
    --theme-text-primary: {theme["text_primary"]};
    --theme-text-secondary: {theme["text_secondary"]};
    --theme-text-muted: {theme["text_muted"]};
    --theme-primary: {theme["primary"]};
    --theme-primary-dark: {theme["primary_dark"]};
    --theme-divider: {theme["divider"]};
    --theme-color-success: {colors["success"]};
    --theme-color-error: {colors["error"]};
    --theme-color-warning: {colors["warning"]};
    --theme-color-info: {colors["info"]};
    --theme-color-gray: {colors["gray"]};
}}
"""


def toggle_theme_mode() -> None:
    """Toggle between Terminal (brutalist) and Clean (SaaS) themes."""
    global THEME, COLORS

    _theme_mode["terminal"] = not _theme_mode["terminal"]
    is_terminal = _theme_mode["terminal"]

    # Update global dictionaries
    THEME.clear()
    THEME.update(THEME_TERMINAL if is_terminal else THEME_CLEAN)
    COLORS.clear()
    COLORS.update(COLORS_TERMINAL if is_terminal else COLORS_CLEAN)

    # Update Quasar dark mode
    ui.dark_mode(is_terminal)
    ui.colors(primary=THEME["primary"])

    # Update CSS variables on the root element
    css_vars = _get_css_variables()
    ui.run_javascript(f"""
        const style = document.getElementById('theme-vars');
        if (style) {{
            style.remove();
        }}
        const newStyle = document.createElement('style');
        newStyle.id = 'theme-vars';
        newStyle.textContent = {css_vars!r};
        document.head.appendChild(newStyle);
    """)

    # Update body styles
    ui.query("body").style(f"background-color: {THEME['page_bg']}; color: {THEME['text_primary']};")

    # Update font
    if is_terminal:
        font_css = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=JetBrains+Mono:wght@400;500;600;700&display=swap" media="print" onload="this.media='all'">
"""
    else:
        font_css = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" media="print" onload="this.media='all'">
"""
    ui.add_head_html(font_css)

    # Force page reload to ensure all components re-render with new theme
    # Note: Experiment selection is preserved via sessionStorage (set on change in backtest_results.py)
    ui.run_javascript("window.location.reload();")


def toggle_dark_mode() -> None:
    """Toggle between light and dark theme (legacy, kept for compatibility)."""
    toggle_theme_mode()


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
        with (
            ui.card()
            .classes("w-[420px] mono-font")
            .style(f"background: {THEME['surface']}; border: 1px solid {THEME['surface_border']};")
        ):
            # Terminal-style header
            with (
                ui.row()
                .classes("justify-between items-center mb-4 pb-3")
                .style(f"border-bottom: 1px solid {THEME['surface_border']};")
            ):
                ui.label("[KEYBOARD_SHORTCUTS]").classes("text-sm font-semibold").style(
                    f"color: {THEME['primary']}; letter-spacing: 0.05em;"
                )
                ui.button("×", on_click=dialog.close).props("flat dense").style(
                    f"color: {THEME['text_secondary']}; font-size: 1.2rem;"
                )

            shortcuts = [
                ("Alt+G", "HOME"),
                ("Alt+B", "BACKTEST"),
                ("Alt+T", "TRADE ANALYTICS"),
                ("Alt+C", "COMPARE"),
                ("Alt+S", "STRATEGY"),
                ("Alt+R", "SCANS"),
                ("Alt+D", "DATA QUALITY"),
                ("Alt+P", "PIPELINE"),
                ("Alt+L", "PAPER LEDGER"),
                ("Alt+Y", "DAILY SUMMARY"),
                ("?", "HELP"),
            ]
            for key, action in shortcuts:
                with (
                    ui.row()
                    .classes("justify-between w-full py-1 items-center")
                    .style(
                        f"border-bottom: 1px dashed {THEME['surface_border']}; margin-bottom: 4px; padding-bottom: 4px;"
                    )
                ):
                    ui.label(key).classes("text-xs").style(
                        f"background: {THEME['surface_hover']}; color: {THEME['primary']}; "
                        "padding: 2px 8px; font-family: 'JetBrains Mono', monospace;"
                    )
                    ui.label(action).classes("text-xs").style(f"color: {THEME['text_secondary']};")

            ui.button("[CLOSE]", on_click=dialog.close).props("flat").classes(
                "mt-4 mono-font text-xs"
            ).style(
                f"color: {THEME['text_secondary']}; border: 1px solid {THEME['surface_border']};"
            )

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

    def check_and_alert(self, experiments_df: pl.DataFrame) -> None:
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
