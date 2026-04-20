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
from collections.abc import Callable, Iterator, Mapping
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
    "page_bg": "#0a0e14",  # Deep near-black for better contrast
    "surface": "#161b22",
    "surface_border": "#30363d",
    "surface_hover": "#21262d",
    "text_primary": "#f0f6fc",
    "text_secondary": "#8b949e",
    "text_muted": "#b4b9c1",  # WCAG AA compliant (7:1 on surface)
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
    "gray": "#9ca3af",  # Updated to match text_muted
}

# Clean theme — Light, modern SaaS dashboard with indigo primary
THEME_CLEAN = {
    "page_bg": "#f8fafc",
    "surface": "#ffffff",
    "surface_border": "#e2e8f0",
    "surface_hover": "#f1f5f9",
    "text_primary": "#0f172a",
    "text_secondary": "#475569",
    "text_muted": "#757985",  # WCAG AA compliant (5:1 on surface)
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

# ---------------------------------------------------------------------------
# Live theme proxy — always resolves to current theme without mutable dict state
# ---------------------------------------------------------------------------
_theme_mode = {"terminal": False}  # True = terminal, False = clean


class _LivePalette(Mapping):
    """Dynamic mapping proxy that always resolves against current theme mode."""

    def __init__(self, getter: Callable[[], dict[str, str]]):
        self._getter = getter

    def __getitem__(self, key: str) -> str:
        return self._getter()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._getter())

    def __len__(self) -> int:
        return len(self._getter())

    def as_dict(self) -> dict[str, str]:
        return dict(self._getter())


THEME = _LivePalette(lambda: THEME_TERMINAL if _theme_mode["terminal"] else THEME_CLEAN)
COLORS = _LivePalette(lambda: COLORS_TERMINAL if _theme_mode["terminal"] else COLORS_CLEAN)


# ---------------------------------------------------------------------------
# THEME COLOR CONSTANTS - convenience accessors for current theme
# ---------------------------------------------------------------------------
# These functions return the current theme value (changes with theme toggle)
def theme_text_primary() -> str:
    return THEME["text_primary"]


def theme_text_secondary() -> str:
    return THEME["text_secondary"]


def theme_text_muted() -> str:
    return THEME["text_muted"]


def theme_page_bg() -> str:
    return THEME["page_bg"]


def theme_surface() -> str:
    return THEME["surface"]


def theme_surface_border() -> str:
    return THEME["surface_border"]


def theme_surface_hover() -> str:
    return THEME["surface_hover"]


def theme_primary() -> str:
    return THEME["primary"]


def color_success() -> str:
    return COLORS["success"]


def color_error() -> str:
    return COLORS["error"]


def color_warning() -> str:
    return COLORS["warning"]


def color_info() -> str:
    return COLORS["info"]


def color_primary() -> str:
    return COLORS["primary"]


def color_gray() -> str:
    return COLORS["gray"]


def hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert a theme hex color to an rgba string (e.g. for Plotly).

    Handles 3-char and 6-char hex. Returns the original string unchanged
    if it doesn't look like a valid hex color.
    """
    color = hex_color.strip().lstrip("#")
    if len(color) == 3:
        color = "".join(ch * 2 for ch in color)
    if len(color) != 6:
        return hex_color
    r = int(color[0:2], 16)
    g = int(color[2:4], 16)
    b = int(color[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha})"


def safe_timer(delay: float, callback: Callable, once: bool = True) -> ui.timer:
    """Create a ui.timer whose callback is guarded against deleted-client errors.

    NiceGUI timers that fire after the browser tab closes or the user navigates
    away raise ``RuntimeError('The client this element belongs to has been deleted.')``.
    Wrapping the callback here prevents that error from propagating.
    """
    if inspect.iscoroutinefunction(callback):

        async def _guarded_async() -> None:
            try:
                await callback()
            except RuntimeError:
                pass

        return ui.timer(delay, _guarded_async, once=once)
    else:

        def _guarded() -> None:
            try:
                callback()
            except RuntimeError:
                pass

        return ui.timer(delay, _guarded, once=once)


# ---------------------------------------------------------------------------
# SPACING SYSTEM - 4px base scale for consistent rhythm
# ---------------------------------------------------------------------------
# Tailwind classes: spacing-1=4px, spacing-2=8px, spacing-3=12px, spacing-4=16px,
#                   spacing-6=24px, spacing-8=32px, spacing-12=48px, spacing-16=64px
SPACING_XS = "1"  # 4px  - tight grouping, related items
SPACING_SM = "2"  # 8px  - sibling spacing
SPACING_MD = "3"  # 12px - component internal spacing
SPACING_LG = "4"  # 16px - default gap between related elements
SPACING_XL = "6"  # 24px - section spacing
SPACING_2XL = "8"  # 32px - major section separation
SPACING_3XL = "12"  # 48px - page-level spacing
SPACING_4XL = "16"  # 64px - hero section spacing

# Backward compatibility dict (deprecated)
SPACING = {
    "xs": SPACING_XS,
    "sm": SPACING_SM,
    "md": SPACING_MD,
    "lg": SPACING_LG,
    "xl": SPACING_XL,
    "2xl": SPACING_2XL,
    "3xl": SPACING_3XL,
    "4xl": SPACING_4XL,
}

# Semantic spacing presets for common patterns
SPACE_CARD_INNER = "gap-3 p-4"  # Inside cards
SPACE_SECTION = "mb-8"  # Between sections
SPACE_SUBSECTION = "mb-6"  # Between subsections
SPACE_RELATED = "gap-2"  # Related items in a row
SPACE_GROUP_TIGHT = "gap-1"  # Tightly grouped items
SPACE_GRID_DEFAULT = "gap-4"  # Grid gaps
SPACE_FORM_ROW = "gap-3 mb-4"  # Form rows
SPACE_LG = "mb-4"  # Large spacing
SPACE_MD = "mb-3"  # Medium spacing
SPACE_SM = "gap-2"  # Small spacing
SPACE_XL = "mb-6"  # Extra large spacing
SPACE_XS = "mb-1"  # Extra small spacing

# Backward compatibility dict (deprecated)
SPACE = {
    "card_inner": SPACE_CARD_INNER,
    "section": SPACE_SECTION,
    "subsection": SPACE_SUBSECTION,
    "related": SPACE_RELATED,
    "group_tight": SPACE_GROUP_TIGHT,
    "grid_default": SPACE_GRID_DEFAULT,
    "form_row": SPACE_FORM_ROW,
    "xs": SPACE_XS,
    "sm": SPACE_SM,
    "md": SPACE_MD,
    "lg": SPACE_LG,
    "xl": SPACE_XL,
}

# ---------------------------------------------------------------------------
# TYPOGRAPHY SCALE - Modular type scale (1.25 ratio) with semantic tokens
# ---------------------------------------------------------------------------
# Based on 16px base: 12→14→16→20→24→32→40→48
# Each token includes: font-size, line-height, letter-spacing, font-weight

# Display - hero titles, page headers
TYPE_DISPLAY = "text-5xl font-bold leading-tight tracking-tight"  # 48px
TYPE_HERO = "text-4xl font-bold leading-tight tracking-tight"  # 36px

# Headings - section titles, card headers
TYPE_H1 = "text-3xl font-bold leading-tight"  # 30px
TYPE_H2 = "text-2xl font-semibold leading-snug"  # 24px
TYPE_H3 = "text-xl font-semibold leading-snug"  # 20px
TYPE_H4 = "text-lg font-medium leading-relaxed"  # 18px

# Body text
TYPE_BODY = "text-base leading-relaxed"  # 16px
TYPE_BODY_LG = "text-lg leading-relaxed"  # 18px (enhanced readability)

# UI elements - labels, captions, metadata
TYPE_LABEL = "text-sm font-medium leading-relaxed"  # 14px (form labels, KPI labels)
TYPE_CAPTION = "text-xs leading-relaxed"  # 12px (metadata, timestamps)
TYPE_MONO = "text-sm font-mono leading-relaxed"  # 14px monospace (code, IDs)

# Number display - tabular nums for data alignment
TYPE_NUMBER = "tabular-nums"  # Apply to any text class for aligned numbers
TYPE_NUMBER_LG = "text-2xl font-bold tabular-nums leading-tight"  # Large metrics
TYPE_NUMBER_MD = "text-xl font-semibold tabular-nums leading-tight"  # Medium metrics

# Combined presets for common patterns
TYPE_PRESET_PAGE_HEADER = "text-4xl font-bold leading-tight tracking-tight"
TYPE_PRESET_SECTION_HEADER = "text-xl font-semibold leading-snug"
TYPE_PRESET_CARD_TITLE = "text-lg font-medium leading-relaxed"
TYPE_PRESET_KPI_LABEL = "text-xs uppercase tracking-wide font-medium"
TYPE_PRESET_KPI_VALUE = "text-2xl font-bold tabular-nums leading-tight"
TYPE_PRESET_TABLE_HEADER = "text-xs uppercase tracking-wide font-semibold"
TYPE_PRESET_TABLE_CELL = "text-sm font-mono leading-relaxed tabular-nums"
TYPE_PRESET_NAV_LABEL = "text-sm font-medium leading-relaxed"
TYPE_PRESET_BUTTON = "text-sm font-medium leading-relaxed"

# Backward compatibility dicts (deprecated)
TYPE = {
    "display": TYPE_DISPLAY,
    "hero": TYPE_HERO,
    "h1": TYPE_H1,
    "h2": TYPE_H2,
    "h3": TYPE_H3,
    "h4": TYPE_H4,
    "body": TYPE_BODY,
    "body_lg": TYPE_BODY_LG,
    "label": TYPE_LABEL,
    "caption": TYPE_CAPTION,
    "mono": TYPE_MONO,
    "number": TYPE_NUMBER,
    "number_lg": TYPE_NUMBER_LG,
    "number_md": TYPE_NUMBER_MD,
}

TYPE_PRESET = {
    "page_header": TYPE_PRESET_PAGE_HEADER,
    "section_header": TYPE_PRESET_SECTION_HEADER,
    "card_title": TYPE_PRESET_CARD_TITLE,
    "kpi_label": TYPE_PRESET_KPI_LABEL,
    "kpi_value": TYPE_PRESET_KPI_VALUE,
    "table_header": TYPE_PRESET_TABLE_HEADER,
    "table_cell": TYPE_PRESET_TABLE_CELL,
    "nav_label": TYPE_PRESET_NAV_LABEL,
    "button": TYPE_PRESET_BUTTON,
}

# ---------------------------------------------------------------------------
# Financial glossary — plain-English definitions for KPI tooltips
# ---------------------------------------------------------------------------
METRIC_GLOSSARY: dict[str, str] = {
    "Calmar": "Annual return divided by worst drawdown. Above 2.0 is strong.",
    "Win Rate": "Percentage of trades that made money. Low win rate can still be profitable if winners are much larger than losers.",
    "Profit Factor": "Total money won divided by total money lost. Above 1.5 is good, above 2.0 is strong.",
    "Max Drawdown": "Largest peak-to-trough drop in portfolio value. Shows worst-case scenario.",
    "CAGR": "Compound Annual Growth Rate — smoothed yearly return as if growth were steady.",
    "Total P/L": "Net profit or loss in rupees across all trades.",
    "Total Return": "Percentage gain or loss on the starting capital.",
    "R-Multiple": "Trade result measured in risk units. 1R = you gained what you risked. -1R = you lost what you risked.",
    "Portfolio Base": "Starting capital the backtest assumes.",
    "Trades": "Total number of completed trades (entry + exit).",
    "Traded Symbols": "Number of different stocks that had at least one trade.",
    "Sharpe": "Risk-adjusted return metric. Above 1.0 is good, above 2.0 is strong.",
}

EXIT_GLOSSARY: dict[str, str] = {
    "TARGET": "Price reached the profit target.",
    "INITIAL_SL": "Price hit the initial stop loss.",
    "BREAKEVEN_SL": "Stop moved to entry — exited flat.",
    "TRAILING_SL": "Trailing stop locked in profit.",
    "TIME": "Position closed at market close.",
    "CANDLE_EXIT": "Exited after a fixed number of candles.",
    "DATA_INVALIDATION": "Price continuity guard triggered.",
}

# ---------------------------------------------------------------------------
# Plotly resize guard — prevents errors when charts are in hidden tabs
# ---------------------------------------------------------------------------
_PLOTLY_RESIZE_GUARD_HTML = """
<script>
(() => {
    if (window.__nsemlPlotlyResizeGuardInstalled) return;
    window.__nsemlPlotlyResizeGuardInstalled = true;

    const RESIZE_ERR = 'Resize must be passed a displayed plot div element';

    const isDisplayed = (el) => {
        if (!el || !el.isConnected) return false;
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden') return false;
        const rect = el.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
    };

    const wrapResize = (obj, key) => {
        if (!obj || typeof obj[key] !== 'function') return;
        const original = obj[key];
        if (original.__nsemlWrappedResize) return;

        const wrapped = function(gd, ...args) {
            if (!isDisplayed(gd)) {
                return Promise.resolve(gd);
            }
            try {
                const result = original.call(this, gd, ...args);
                if (result && typeof result.catch === 'function') {
                    return result.catch((err) => {
                        const msg = String(err && err.message ? err.message : err || '');
                        if (msg.includes(RESIZE_ERR)) return gd;
                        throw err;
                    });
                }
                return result;
            } catch (err) {
                const msg = String(err && err.message ? err.message : err || '');
                if (msg.includes(RESIZE_ERR)) return gd;
                throw err;
            }
        };
        wrapped.__nsemlWrappedResize = true;
        obj[key] = wrapped;
    };

    const patchPlotly = () => {
        if (window.Plotly) {
            wrapResize(window.Plotly, 'resize');
            wrapResize(window.Plotly, 'react');
            wrapResize(window.Plotly, 'newPlot');
            wrapResize(window.Plotly, 'restyle');
        }
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => setTimeout(patchPlotly, 100));
    } else {
        setTimeout(patchPlotly, 100);
    }
    setTimeout(patchPlotly, 2000);
})();
</script>
"""

# ---------------------------------------------------------------------------
# Navigation definition (single source of truth)
# ---------------------------------------------------------------------------
NAV_ITEMS: list[dict[str, str | None]] = [
    {"label": "Home", "icon": "home", "path": "/", "group": None},
    {"label": "Backtest Results", "icon": "bar_chart", "path": "/backtest", "group": "Analysis"},
    {
        "label": "Trade Analytics",
        "icon": "analytics",
        "path": "/trade_analytics",
        "group": "Analysis",
    },
    {"label": "Compare", "icon": "compare_arrows", "path": "/compare", "group": "Analysis"},
    {"label": "Strategy", "icon": "tune", "path": "/strategy", "group": "Research"},
    {"label": "Scans", "icon": "radar", "path": "/scans", "group": "Research"},
    {"label": "Data Quality", "icon": "verified", "path": "/data_quality", "group": "Research"},
    {"label": "Market Monitor", "icon": "monitor", "path": "/market_monitor", "group": "Research"},
    {"label": "Pipeline", "icon": "engineering", "path": "/pipeline", "group": "Operations"},
    {
        "label": "Paper Ledger",
        "icon": "receipt_long",
        "path": "/paper_ledger",
        "group": "Operations",
    },
    {"label": "Daily Summary", "icon": "today", "path": "/daily_summary", "group": "Operations"},
]

# ---------------------------------------------------------------------------
# CSS with theme variables — supports both Terminal and Clean modes
# ---------------------------------------------------------------------------

# Terminal fonts — IBM Plex Sans + Fira Code for that terminal aesthetic
_FONT_HEAD_TERMINAL = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="preload" as="style" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Fira+Code:wght@400;500;600;700&display=swap">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Fira+Code:wght@400;500;600;700&display=swap" media="print" onload="this.media='all'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Fira+Code:wght@400;500;600;700&display=swap"></noscript>
"""

# Clean theme fonts — DM Sans (distinctive, not generic Inter) + JetBrains Mono
_FONT_HEAD_CLEAN = """
<link rel="preconnect" href="https://fonts.googleapis.com" crossorigin>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="preload" as="style" href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" media="print" onload="this.media='all'">
<noscript><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap"></noscript>
"""


# Get current font based on theme mode
def _get_font_html() -> str:
    return _FONT_HEAD_TERMINAL if _theme_mode["terminal"] else _FONT_HEAD_CLEAN


# Base CSS using CSS variables — works for both themes
_PAGE_CSS_BASE = """
/* ============================================================================
   ACCESSIBILITY: Skip Navigation Link (A11Y-008)
   ============================================================================ */
.skip-link {
    position: absolute;
    top: -40px;
    left: 0;
    background: var(--theme-primary);
    color: var(--theme-page-bg);
    padding: 8px 16px;
    text-decoration: none;
    z-index: 10000;
    font-weight: 600;
    font-family: var(--font-mono);
    transition: top 0.2s;
}
.skip-link:focus {
    top: 0;
}

/* ============================================================================
   ACCESSIBILITY: Focus Indicators (A11Y-001)
   Visible focus state for keyboard navigation
   ============================================================================ */
/* Focus visible - only show for keyboard navigation, not mouse clicks */
*:focus-visible {
    outline: 2px solid var(--theme-primary) !important;
    outline-offset: 2px !important;
    border-radius: 2px;
}
/* Buttons need stronger focus */
.q-btn:focus-visible,
.q-item:focus-visible,
.nav-item:focus-visible {
    outline: 3px solid var(--theme-primary) !important;
    outline-offset: 2px !important;
    box-shadow: 0 0 0 4px var(--theme-primary-alpha) !important;
}
/* Table cells focus */
.q-table td:focus-visible,
.q-table th:focus-visible {
    outline: 2px solid var(--theme-primary) !important;
    background: var(--theme-surface-hover) !important;
}

/* ============================================================================
   RESPONSIVE: Touch Targets (RESP-002)
   Minimum 44x44px for all interactive elements (WCAG AAA)
   ============================================================================ */
.q-btn {
    min-height: 44px !important;
    min-width: 44px !important;
}
.q-btn--dense {
    min-height: 44px !important;
    min-width: 44px !important;
    padding: 0 12px !important;
}
.nav-item {
    min-height: 44px !important;
    display: flex !important;
    align-items: center !important;
}
.nav-tile {
    min-height: 44px !important;
    padding: 20px !important;
}
.q-item {
    min-height: 44px !important;
}
.q-pagination .q-btn {
    min-height: 44px !important;
    min-width: 44px !important;
}
/* Icon-only buttons need explicit touch targets */
.q-btn .q-icon {
    font-size: 20px;
}

/* ============================================================================
   ACCESSIBILITY: Color-Only Status Alternatives (A11Y-007)
   Add icon indicators for color-blind users
   ============================================================================ */
.value-positive::before {
    content: "↑ ";
    color: var(--theme-color-success);
    font-weight: 700;
}
.value-negative::before {
    content: "↓ ";
    color: var(--theme-color-error);
    font-weight: 700;
}
.value-neutral::before {
    content: "– ";
    color: var(--theme-color-gray);
    font-weight: 700;
}

/* ============================================================================
   TYPOGRAPHY: Type Scale & Readability (TYPE-001)
   Consistent type hierarchy, tabular numbers, improved readability
   ============================================================================ */
/* Base typography */
body {
    font-family: var(--font-body);
    font-size: 16px;
    line-height: 1.6;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

/* Tabular numbers for data alignment */
.tabular-nums {
    font-variant-numeric: tabular-nums;
    font-feature-settings: "tnum";
    letter-spacing: 0.02em; /* Slightly open for numbers */
}

/* Improved line heights for dark mode */
body[class*="terminal"] {
    line-height: 1.65;
}
body[class*="terminal"] .kpi-card {
    line-height: 1.5;
}

/* Uppercase styling with intentional letter-spacing */
.text-uppercase {
    letter-spacing: 0.08em; /* More open for small caps/uppercase */
}
.tracking-wide {
    letter-spacing: 0.05em;
}
.tracking-tight {
    letter-spacing: -0.02em; /* Tighter for large display text */
}

/* Readable text width for long content */
.readable-width {
    max-width: 65ch;
}

/* Typography scale - ensure minimum sizes */
.text-xs { font-size: 0.75rem; }    /* 12px */
.text-sm { font-size: 0.875rem; }   /* 14px */
.text-base { font-size: 1rem; }     /* 16px */
.text-lg { font-size: 1.125rem; }    /* 18px */
.text-xl { font-size: 1.25rem; }     /* 20px */
.text-2xl { font-size: 1.5rem; }     /* 24px */
.text-3xl { font-size: 1.875rem; }   /* 30px */
.text-4xl { font-size: 2.25rem; }    /* 36px */
.text-5xl { font-size: 3rem; }       /* 48px */

/* Leading utilities */
.leading-tight { line-height: 1.25; }
.leading-snug { line-height: 1.375; }
.leading-normal { line-height: 1.5; }
.leading-relaxed { line-height: 1.625; }
.leading-loose { line-height: 2; }

/* Kerning */
.font-kerning {
    font-kerning: normal;
    text-rendering: optimizeLegibility;
}

/* ============================================================================
   RESPONSIVE: Mobile Table Optimizations (RESP-003)
   Card-based view for small screens
   ============================================================================ */
@media (max-width: 768px) {
    .q-table {
        font-size: 0.8rem !important;
    }
    .q-table thead th {
        padding: 8px 12px !important;
        font-size: 0.65rem !important;
    }
    .q-table tbody td {
        padding: 10px 12px !important;
        font-size: 0.75rem !important;
    }
    /* Hide less important columns on mobile */
    .q-table .hide-mobile {
        display: none !important;
    }
    /* Stack rows on very small screens */
    @media (max-width: 480px) {
        .q-table tbody tr {
            display: block;
            margin-bottom: 12px;
            border: 1px solid var(--theme-surface-border);
        }
        .q-table tbody td {
            display: flex;
            justify-content: space-between;
            padding: 8px 12px !important;
            border-bottom: 1px solid var(--theme-surface-border) !important;
        }
        .q-table tbody td::before {
            content: attr(data-label);
            font-weight: 600;
            color: var(--theme-text-secondary);
            margin-right: 16px;
        }
        .q-table thead {
            display: none;
        }
    }
}

/* ============================================================================
   RESPONSIVE: Fluid Chart Heights (RESP-004)
   Charts adapt to viewport size
   ============================================================================ */
.plotly-graph-wrapper,
.plotly {
    min-height: 250px !important;
    max-height: 80vh !important;
    width: 100% !important;
}
@media (max-width: 768px) {
    .plotly-graph-wrapper,
    .plotly {
        min-height: 200px !important;
        max-height: 60vh !important;
    }
}

/* ============================================================================
   RESPONSIVE: Mobile Bottom Navigation (RESP-005)
   Bottom tab bar for mobile devices
   ============================================================================ */
.mobile-bottom-nav {
    display: none !important;
}
@media (max-width: 768px) {
    .mobile-bottom-nav {
        display: flex !important;
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        height: 56px;
        background: var(--theme-surface);
        border-top: 1px solid var(--theme-surface-border);
        justify-content: space-around;
        align-items: center;
        z-index: 1000;
        padding: 0 8px;
    }
    .mobile-bottom-nav-item {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: 8px 12px;
        color: var(--theme-text-secondary);
        text-decoration: none;
        font-size: 0.65rem;
        min-width: 48px;
    }
    .mobile-bottom-nav-item.active {
        color: var(--theme-primary);
    }
    .mobile-bottom-nav-item .q-icon {
        font-size: 1.4rem !important;
        margin-bottom: 2px;
    }
    /* Adjust main content for bottom nav */
    .q-page-container {
        padding-bottom: 60px !important;
    }
    /* Hide sidebar on mobile */
    .q-drawer {
        display: none !important;
    }
}

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

/* ============================================================================
   ACCESSIBILITY: Reduced Motion Support (THEME-003)
   Respect prefers-reduced-motion for users with vestibular disorders
   ============================================================================ */
@media (prefers-reduced-motion: reduce) {
    /* Disable scanline effect for motion-sensitive users */
    body.terminal-mode::after {
        display: none !important;
    }
    /* Reduce or disable animations */
    *,
    *::before,
    *::after {
        animation-duration: 0.01ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: 0.01ms !important;
    }
    .kpi-card {
        animation: none !important;
    }
    /* Keep essential transitions but make them instant */
    .nav-item:hover,
    .nav-tile:hover,
    .kpi-card:hover {
        transition: none !important;
    }
}

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
    transition: transform 0.15s ease, border-color 0.15s ease, box-shadow 0.15s ease;
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
    transition: transform 0.1s ease, border-color 0.1s ease, box-shadow 0.1s ease;
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
.nav-tile:focus-visible {
    outline: 3px solid var(--theme-primary) !important;
    outline-offset: 2px !important;
    box-shadow: 0 0 20px var(--theme-primary-alpha) !important;
}

/* Primary action card — larger, more prominent CTA */
.primary-action-card {
    background: var(--theme-surface);
    border: 2px solid var(--theme-primary);
    border-radius: var(--tile-radius, 4px);
    padding: 32px;
    transition: transform 0.15s ease, border-color 0.15s ease, box-shadow 0.15s ease;
    box-shadow: 0 0 20px var(--theme-primary-alpha), var(--tile-shadow, 0 4px 8px rgba(0,0,0,0.3));
    position: relative;
}
.primary-action-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 0 32px var(--theme-primary-alpha), 0 8px 16px rgba(0,0,0,0.4);
}
.primary-action-card:focus-visible {
    outline: 4px solid var(--theme-primary) !important;
    outline-offset: 3px !important;
    box-shadow: 0 0 40px var(--theme-primary-alpha), 0 8px 16px rgba(0,0,0,0.4) !important;
}

/* Sidebar nav items — uses theme variables */
.nav-item {
    border-radius: var(--nav-radius, 0);
    padding: 8px 16px;
    margin: 0;
    transition: transform 0.1s ease, background-color 0.1s ease;
    cursor: pointer;
    color: var(--theme-text-secondary);
    position: relative;
}
.nav-row {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-shrink: 0;
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
    min-width: 24px;
    max-width: 24px;
    flex: 0 0 24px;
    text-align: center;
    display: flex;
    align-items: center;
    justify-content: center;
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
    /* Tabular numbers for data alignment */
    font-variant-numeric: tabular-nums;
    font-feature-settings: "tnum";
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

    # Inject keyboard shortcuts and Plotly resize guard
    ui.add_head_html(_KEYBINDINGS_HTML)
    ui.add_head_html(_PLOTLY_RESIZE_GUARD_HTML)

    # Update page title for accessibility (A11Y-011)
    ui.run_javascript(f'document.title = "NSE Momentum Lab — {title}"')

    # Add live region for dynamic updates (A11Y-010)
    ui.element("div").props('aria-live="polite" aria-atomic="true"').style(
        "position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; "
        "overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0;"
    ).classes("live-region").style("display: none;")

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
        ui.button(icon="menu", on_click=_cycle_sidebar).props("flat round dense").props(
            'aria-label="Toggle sidebar navigation"'
        ).style(f"color: {THEME['text_secondary']};")

        # Terminal-style title with status indicator
        with ui.row().classes("items-center gap-2 ml-2"):
            # Blinking green status dot
            ui.icon("circle").classes("text-xs").props('aria-label="System status: ready"').style(
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
        ui.button(toggle_label, on_click=toggle_theme_mode).props("flat dense").props(
            f'aria-label="Switch to {toggle_label} theme"'
        ).classes("text-xs mono-font px-3").style(
            f"color: {THEME['text_secondary']}; "
            f"border: 1px solid {THEME['surface_border']}; "
            "border-radius: 2px; padding: 4px 12px;"
        )

        # Shortcuts help
        _shortcuts_dialog_instance = shortcuts_dialog()
        ui.button("?", on_click=_shortcuts_dialog_instance.open).props("flat dense").props(
            'aria-label="Show keyboard shortcuts"'
        ).classes("mono-font text-xs px-2").style(
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
            "width=240 mini-width=56 breakpoint=md"  # breakpoint=md → collapses on mobile (<1024px)
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
                ui.icon("circle").classes("text-xs").props(
                    'aria-label="System status: ready"'
                ).style(f"color: {THEME['primary']}; animation: terminal-pulse 2s infinite;")

        ui.separator().classes("sidebar-logo").style(f"background: {THEME['surface_border']};")

        # Nav links — grouped by section with visual separators
        with ui.column().classes("py-2 gap-0 w-full"):
            _prev_group: str | None = None
            for item in NAV_ITEMS:
                # Insert section divider between groups (skip Home)
                group = item.get("group")
                if group and group != _prev_group:
                    if _prev_group is not None:
                        ui.separator().style(
                            f"background: {THEME['surface_border']}; margin: 6px 16px; opacity: 0.5;"
                        )
                    _prev_group = group

                is_active = item["label"] == title or (title == "Home" and item["path"] == "/")
                active_cls = " nav-item-active" if is_active else ""

                with (
                    ui.row()
                    .classes(f"nav-item{active_cls} items-center gap-3 w-full nav-row")
                    .on("click", lambda p=item["path"]: ui.navigate.to(p))
                    .props(f'aria-label="Navigate to {item["label"]}" role="button" tabindex="0"')
                ):
                    ui.icon(item["icon"]).classes("text-lg nav-icon").props(
                        f'aria-label="{item["label"]}"'
                    )
                    ui.label(item["label"]).classes("text-sm nav-label").props(
                        "role='presentation'"
                    )

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
    color: str | None = None,
) -> None:
    """Render a single KPI card with a Material Design icon and optional glossary tooltip."""
    card_color = color or COLORS["primary"]
    tooltip_text = METRIC_GLOSSARY.get(title)
    with ui.column().classes("kpi-card gap-1"):
        with ui.row().classes("items-center gap-3"):
            ui.icon(icon).classes("text-2xl").style(f"color: {card_color};")
            title_label = (
                ui.label(title)
                .classes(TYPE_PRESET["kpi_label"])
                .style(f"color: {THEME['text_secondary']};")
            )
            if tooltip_text:
                with title_label:
                    ui.tooltip(tooltip_text).classes("text-sm").style(
                        f"background: {THEME['surface']}; color: {THEME['text_primary']}; "
                        f"border: 1px solid {THEME['surface_border']}; "
                        "max-width: 300px; padding: 8px 12px;"
                    )
        ui.label(str(value)).classes(f"{TYPE_PRESET['kpi_value']} mt-1").style(
            f"color: {card_color};"
        )
        if subtitle:
            ui.label(subtitle).classes("text-xs").style(f"color: {THEME['text_muted']};")


def kpi_grid(
    cards: list[dict[str, Any]],
    columns: int = 4,
    hero_index: int | None = None,
) -> None:
    """Render a row of KPI cards using ``ui.grid`` for equal-width alignment.

    Each dict in *cards* is passed as kwargs to :func:`kpi_card`.
    Keys: title, value, subtitle, icon, color, is_hero, trend, trend_label.

    Args:
        cards: List of card dictionaries
        columns: Number of columns in the grid
        hero_index: Index of the card to render as hero (larger, with trend)
    """
    with ui.grid(columns=columns).classes("w-full gap-4 mb-6"):
        for i, card in enumerate(cards):
            # Set is_hero based on hero_index if not already in card
            card_kwargs = {**card}
            if hero_index is not None and i == hero_index:
                card_kwargs["is_hero"] = True
            _render_kpi_card_with_features(**card_kwargs)


def _render_kpi_card_with_features(
    title: str,
    value: str | float | int,
    subtitle: str | None = None,
    icon: str = "info",
    color: str = COLORS["primary"],
    is_hero: bool = False,
    muted: bool = False,
    trend: float | None = None,
    trend_label: str | None = None,
) -> None:
    """Render a KPI card with optional hero, muted, and trend indicator.

    Tiers:
      - hero: large icon + value, colored, with trend
      - default: medium icon + value, colored
      - muted: small icon + value, gray — for metadata (data source, IDs)
    """
    if muted:
        # Metadata tier: visually quiet
        card_classes = "kpi-card gap-1 p-4"
        icon_size = "text-lg"
        value_cls = "text-lg font-semibold tabular-nums"
        label_cls = "text-xs uppercase tracking-wide font-medium"
        value_color = THEME["text_primary"]
        icon_color = THEME["text_muted"]
    elif is_hero:
        card_classes = "kpi-card gap-1 p-6"
        icon_size = "text-4xl"
        value_cls = "text-4xl font-bold tabular-nums"
        label_cls = "text-xs uppercase tracking-wide font-medium"
        value_color = color
        icon_color = color
    else:
        card_classes = "kpi-card gap-1"
        icon_size = "text-2xl"
        value_cls = "text-2xl font-bold tabular-nums"
        label_cls = "text-xs uppercase tracking-wide font-medium"
        value_color = color
        icon_color = color

    with ui.column().classes(card_classes):
        with ui.row().classes("items-center gap-3"):
            ui.icon(icon).classes(icon_size).style(f"color: {icon_color};")
            ui.label(title).classes(label_cls).style(f"color: {THEME['text_secondary']};")

        # Value and optional trend
        with ui.row().classes("items-baseline gap-2 mt-1"):
            ui.label(str(value)).classes(value_cls).style(f"color: {value_color};")
            if trend is not None:
                trend_icon = "↑" if trend > 0 else "↓" if trend < 0 else "→"
                trend_color = (
                    COLORS["success"]
                    if trend > 0
                    else COLORS["error"]
                    if trend < 0
                    else COLORS["gray"]
                )
                ui.label(f"{trend_icon} {abs(trend):.1f}%").classes(
                    f"{'text-lg' if is_hero else 'text-sm'} font-medium"
                ).style(f"color: {trend_color};")

        if trend_label:
            ui.label(trend_label).classes(f"{'text-sm' if is_hero else 'text-xs'}").style(
                f"color: {THEME['text_muted']};"
            )
        elif subtitle:
            ui.label(subtitle).classes("text-xs").style(f"color: {THEME['text_muted']};")


def kpi_section(title: str, cards: list[dict[str, Any]], columns: int = 4) -> None:
    """Render a titled section of KPI cards.

    Args:
        title: Section heading
        cards: List of card dictionaries for :func:`kpi_card`
        columns: Grid column count
    """
    ui.label(title).classes("text-xl font-semibold mb-4").style(f"color: {THEME['text_primary']};")
    kpi_grid(cards, columns=columns)


# ---------------------------------------------------------------------------
# Primary action card (prominent CTA for home page)
# ---------------------------------------------------------------------------
def primary_action_card(
    title: str,
    description: str,
    icon: str,
    target: str,
    subtitle: str | None = None,
) -> None:
    """Render a prominent call-to-action card for the home page.

    Larger and more visually prominent than nav_card, used for primary actions
    like "Run Your First Backtest" or "Analyze Your Results".
    """

    # Keyboard handler for Enter/Space keys (A11Y-013)
    def handle_key(e: dict) -> None:
        if e.get("key") in ("Enter", " "):
            ui.navigate.to(target)

    with (
        ui.column()
        .classes("primary-action-card cursor-pointer")
        .props('tabindex="0" role="button"')
        .on("click", lambda t=target: ui.navigate.to(t))
        .on("keydown", handle_key)
    ):
        with ui.row().classes("items-center gap-4 mb-3"):
            ui.icon(icon).classes("text-4xl").style(f"color: {THEME['primary']};")
            ui.label(title).classes("text-2xl font-bold").style(f"color: {THEME['text_primary']};")

        ui.label(description).classes("text-base leading-relaxed mb-3").style(
            f"color: {THEME['text_secondary']};"
        )

        if subtitle:
            ui.label(subtitle).classes(
                "text-xs uppercase tracking-wide font-semibold px-3 py-1 rounded"
            ).style(
                f"background: {THEME['primary']}; color: {THEME['page_bg']}; display: inline-block;"
            )


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

    # Keyboard handler for Enter/Space keys (A11Y-013)
    def handle_key(e: dict) -> None:
        if e.get("key") in ("Enter", " "):
            ui.navigate.to(target)

    with (
        ui.column()
        .classes("nav-tile")
        .props('tabindex="0" role="button"')
        .on("click", lambda t=target: ui.navigate.to(t))
        .on("keydown", handle_key)
    ):
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
                bgcolor=hex_to_rgba(theme["surface"], 0.9),
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
                bgcolor=hex_to_rgba(theme["surface"], 0.95),
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
    """Add a CSV export button with Material icon.

    Accepts a Polars DataFrame or a list of dicts.
    """
    if data is None or (hasattr(data, "is_empty") and data.is_empty()):
        return

    if isinstance(data, list):
        df = pl.DataFrame(data)
    else:
        df = data

    csv_content = df.write_csv()

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
    spinner = (
        ui.spinner("dots")
        .classes("mt-8")
        .props('role="status" aria-live="polite" aria-label="Loading..."')
    )
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
    aria_label: str = "Data table",
) -> None:
    """Sortable, paginated table using Quasar QTable native controls."""
    if not rows:
        ui.label("No data to display").style(f"color: {THEME['text_muted']};")
        return

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

    # Enable sorting and scope=col on all columns
    sortable_columns = [
        {**col, "sortable": True, "headerClasses": "scope-col"}
        if "headerClasses" not in col
        else {**col, "sortable": True, "headerClasses": col.get("headerClasses", "") + " scope-col"}
        for col in columns
    ]

    with ui.element("div").style("width: 100%; overflow-x: auto;"):
        table = (
            ui.table(
                columns=sortable_columns,
                rows=rows,
                pagination={"rowsPerPage": page_size, "rowsPerPage_options": [10, 20, 50, 100]},
                row_key=table_row_key,
            )
            .props(f'aria-label="{aria_label}"')
            .style("min-width: max-content;")
        )

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
        ui.icon(icon).classes("text-6xl opacity-50").props('aria-hidden="true"').style(
            f"color: {THEME['text_muted']};"
        )
        ui.label(title).classes("text-xl font-semibold").style(f"color: {THEME['text_primary']};")
        ui.label(message).classes("text-center max-w-md").style(
            f"color: {THEME['text_secondary']};"
        )
        if action_label and action_callback:
            ui.button(action_label, on_click=action_callback).props("push color=primary").classes(
                "mt-4"
            )


async def render_section_guarded(title: str, render_fn: Callable) -> None:
    """Render a section and keep the page alive if the section fails.

    Wraps section rendering in try/except, showing an empty_state instead
    of crashing the entire page. Essential for multi-section dashboards
    where each section runs independent queries.
    """
    try:
        await render_fn()
    except Exception as exc:
        import logging

        logging.getLogger(__name__).exception("Section %s failed: %s", title, exc)
        empty_state(
            f"{title} — Error",
            f"Could not load this section: {exc}",
            icon="error_outline",
        )


# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
def page_header(
    title: str,
    subtitle: str | None = None,
    kpi_row: list[dict] | None = None,
    level: int = 1,
) -> None:
    """Consistent page header with optional KPIs.

    Args:
        title: Page title
        subtitle: Optional subtitle
        kpi_row: Optional KPI cards
        level: Heading level (1 or 2)
    """
    with ui.column().classes("mb-8 w-full"):
        with ui.column().classes("gap-1 mb-6"):
            # Use semantic heading elements for accessibility (A11Y-012)
            heading_tag = f"h{level}"
            ui.html(
                f"<{heading_tag} class='text-2xl font-bold' style='color: {THEME['text_primary']}; margin: 0;'>{title}</{heading_tag}>"
            )
            if subtitle:
                ui.html(
                    f"<p class='text-sm' style='color: {THEME['text_secondary']}; margin: 4px 0 0;'>{subtitle}</p>"
                )

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
    row_key: str | None = None,
    on_row_click: callable | None = None,
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
                r for r in filtered if float(r.get("pnl_pct", 0) or 0) >= filters["min_pnl"]
            ]
        if filters["max_pnl"] is not None:
            filtered = [
                r for r in filtered if float(r.get("pnl_pct", 0) or 0) <= filters["max_pnl"]
            ]

        paginated_table(
            filtered,
            columns,
            page_size=page_size,
            row_key=row_key,
            on_row_click=on_row_click,
        )

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
# Strategy/exit badges and P&L formatting (ARIA-accessible)
# ---------------------------------------------------------------------------
_EXIT_COLORS: dict[str, str] = {
    "TARGET": "#10b981",
    "INITIAL_SL": "#ef4444",
    "BREAKEVEN_SL": "#f59e0b",
    "TRAILING_SL": "#f97316",
    "TIME": "#64748b",
    "DATA_INVALIDATION": "#64748b",
}

_EXIT_LABELS: dict[str, str] = {
    "TARGET": "Target",
    "INITIAL_SL": "Init SL",
    "BREAKEVEN_SL": "BE SL",
    "TRAILING_SL": "Trail SL",
    "TIME": "Time",
    "DATA_INVALIDATION": "Data Inv",
}

_STRAT_LABELS: dict[str, str] = {
    "thresholdbreakout": "Breakout",
    "2lynchbreakdown": "Breakdown",
    "epproxysameday": "EP SameDay",
    "2lynchbreakout": "Breakout",
}


def strat_badge(strategy: str) -> str:
    """Return an HTML badge for the strategy name with ARIA label."""
    label = _STRAT_LABELS.get(strategy, strategy)
    color = COLORS["primary"]
    return (
        f'<span aria-label="Strategy: {label}" style="background:{color};color:#fff;padding:2px 10px;'
        f"border-radius:3px;font-size:0.75rem;font-weight:600;"
        f'font-family:monospace;letter-spacing:0.05em">{label}</span>'
    )


def exit_badge(reason: str) -> str:
    """Return an HTML badge for the exit reason with ARIA label."""
    color = _EXIT_COLORS.get(reason, "#64748b")
    label = _EXIT_LABELS.get(reason, reason)
    tooltip = EXIT_GLOSSARY.get(reason, "")
    title_attr = f' title="{tooltip}"' if tooltip else ""
    aria_label = f' aria-label="{label}: {tooltip}"' if tooltip else f' aria-label="{label}"'
    return (
        f'<span{title_attr}{aria_label} style="background:{color};color:#fff;padding:2px 8px;'
        f"border-radius:3px;font-size:0.7rem;font-weight:600;cursor:help;"
        f'font-family:monospace">{label}</span>'
    )


def pnl_cell(value: float | int, prefix: str = "₹", suffix: str = "") -> str:
    """Format a P/L value as an HTML span with green/red coloring and ARIA label."""
    color = COLORS["success"] if value >= 0 else COLORS["error"]
    sign = "+" if value > 0 else ""
    formatted = (
        f"{prefix}{sign}{value:,.0f}{suffix}" if prefix == "₹" else f"{sign}{value:.4f}{suffix}"
    )
    aria_label = f"{'Gain' if value >= 0 else 'Loss'}: {formatted}"
    arrow = "↑" if value > 0 else ("↓" if value < 0 else "")
    return (
        f'<span aria-label="{aria_label}" '
        f'style="color:{color};font-weight:600;font-family:var(--font-mono)">'
        f"{arrow} {formatted}</span>"
    )


def value_label(value: str, is_positive: bool | None = None) -> None:
    """Display a value with visual indicators and ARIA labels for accessibility."""
    if is_positive is True:
        css_class = "value-positive"
        aria = f"Positive: {value}"
    elif is_positive is False:
        css_class = "value-negative"
        aria = f"Negative: {value}"
    else:
        css_class = ""
        aria = value
    ui.label(value).classes(css_class).props(f'aria-label="{aria}"')


# ---------------------------------------------------------------------------
# Theme state management
# ---------------------------------------------------------------------------
# NOTE: _theme_mode is defined earlier (near _LivePalette) to avoid a duplicate.


def get_current_theme() -> dict:
    """Return the current active theme dictionary."""
    return THEME.as_dict()


def get_current_colors() -> dict:
    """Return the current active colors dictionary."""
    return COLORS.as_dict()


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
    _theme_mode["terminal"] = not _theme_mode["terminal"]
    is_terminal = _theme_mode["terminal"]

    # _LivePalette proxies auto-resolve; just update CSS/Quasar/JS
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
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" media="print" onload="this.media='all'">
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
                ("Alt+W", "WALK FORWARD"),
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
