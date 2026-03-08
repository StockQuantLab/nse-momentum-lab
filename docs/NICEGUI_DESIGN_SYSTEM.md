# NiceGUI Dashboard - Design System & Component Library

This document describes the design system, component architecture, and styling patterns used in the NSE Momentum Lab dashboard. Use this as a reference for adding new features or adapting this to other projects.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Theme System](#theme-system)
3. [Component Library](#component-library)
4. [Styling Patterns](#styling-patterns)
5. [Page Layout](#page-layout)
6. [Performance Optimizations](#performance-optimizations)
7. [Common Patterns](#common-patterns)
8. [Adding New Pages](#adding-new-pages)

---

## Architecture Overview

### File Structure

```
apps/nicegui/
├── main.py                 # App entry point, route definitions
├── components/
│   └── __init__.py         # Theme system, components, CSS
├── pages/
│   ├── __init__.py
│   ├── home.py             # Dashboard home
│   ├── backtest_results.py # Experiment details
│   ├── compare_experiments.py
│   └── ...
├── state/
│   └── __init__.py         # Database connections, caching
└── paths.py                # Path helpers
```

### Key Principles

1. **Server-side state** - NiceGUI maintains state on the server, not in browser
2. **CSS variables for theming** - Dynamic theme switching without page rebuild
3. **Reusable components** - All UI elements are composable functions
4. **Async operations** - All DB calls use thread pool executor

---

## Theme System

### Dual Theme Architecture

The dashboard supports two themes with distinct aesthetics:

| Aspect | Terminal Mode | Clean Mode |
|--------|--------------|-------------|
| **Purpose** | Dark, brutalist trading terminal | Light, modern SaaS dashboard |
| **Background** | `#0d1117` (deep dark) | `#f8fafc` (off-white) |
| **Primary Color** | `#00ff88` (neon phosphor green) | `#6366f1` (indigo) |
| **Body Font** | IBM Plex Sans | DM Sans |
| **Mono Font** | Fira Code (for code aesthetic) | JetBrains Mono |
| **Border Radius** | 2-4px (sharp edges) | 6-8px (rounded) |
| **Shadows** | Heavy, colored glow | Subtle, gray |
| **Special Effects** | Scanline overlay | None |

### Theme Definition

```python
# In components/__init__.py

THEME_TERMINAL = {
    "page_bg": "#0d1117",
    "surface": "#161b22",
    "surface_border": "#30363d",
    "surface_hover": "#21262d",
    "text_primary": "#f0f6fc",
    "text_secondary": "#8b949e",
    "text_muted": "#6e7681",
    "primary": "#00ff88",
    "primary_dark": "#00cc6a",
    "divider": "#30363d",
}

THEME_CLEAN = {
    "page_bg": "#f8fafc",
    "surface": "#ffffff",
    "surface_border": "#e2e8f0",
    "surface_hover": "#f1f5f9",
    "text_primary": "#0f172a",
    "text_secondary": "#475569",
    "text_muted": "#64748b",
    "primary": "#6366f1",
    "primary_dark": "#4f46e5",
    "divider": "#e2e8f0",
}
```

### CSS Variables

All theme values are exposed as CSS variables for consistent styling:

```css
:root {
    --theme-page-bg: #0d1117;
    --theme-surface: #161b22;
    --theme-surface-border: #30363d;
    --theme-surface-hover: #21262d;
    --theme-text-primary: #f0f6fc;
    --theme-text-secondary: #8b949e;
    --theme-text-muted: #6e7681;
    --theme-primary: #00ff88;
    --theme-color-success: #00ff88;
    --theme-color-error: #ff6b6b;
    --theme-color-warning: #ffd93d;
    --theme-color-info: #6bcfff;
}
```

### Theme Toggle

```python
def toggle_theme_mode() -> None:
    """Toggle between Terminal and Clean themes."""
    global THEME, COLORS
    _theme_mode["terminal"] = not _theme_mode["terminal"]

    # Update globals
    THEME.update(THEME_TERMINAL if is_terminal else THEME_CLEAN)
    COLORS.update(COLORS_TERMINAL if is_terminal else COLORS_CLEAN)

    # Update Quasar mode
    ui.dark_mode(is_terminal)

    # Update CSS variables via JavaScript
    ui.run_javascript(f"""
        const cssVars = `{_get_css_variables()}`;
        // Update :root variables...
    """)
```

---

## Component Library

### Page Layout Wrapper

Every page uses `page_layout()` for consistent chrome:

```python
from apps.nicegui.components import page_layout

def my_page() -> None:
    with page_layout("Page Title", "icon"):
        # Page content here
        ui.label("Hello World")
```

**Features:**
- Collapsible sidebar (expanded → mini → hidden)
- Header with theme toggle, shortcuts help
- Responsive layout
- Full-width content area

### KPI Cards

```python
from apps.nicegui.components import kpi_grid, kpi_card

kpi_grid([
    dict(
        title="Total Return",
        value="193.9%",
        icon="attach_money",
        color=COLORS["success"],
    ),
    dict(
        title="Win Rate",
        value="51.3%",
        icon="target",
        color=COLORS["info"],
    ),
])
```

**Available icons:** Material Design Icons (use snake_case names)

### Navigation Cards (Home Page)

```python
from apps.nicegui.components import nav_card

nav_card(
    title="Backtest Results",
    description="Analyze stored 2LYNCH backtests",
    icon="bar_chart",
    target="/backtest",
    color=COLORS["success"],
)
```

### Tables

#### Basic Table with Horizontal Scroll

```python
with ui.element("div").style("width: 100%; overflow-x: auto;"):
    ui.table(
        columns=[
            {"name": "symbol", "label": "Symbol", "field": "symbol"},
            {"name": "pnl_pct", "label": "PnL %", "field": "pnl_pct"},
        ],
        rows=row_data,
        pagination={"rowsPerPage": 20, "rowsPerPage_options": [10, 20, 50, 100]},
    ).style("min-width: max-content;")
```

**Important:** Use `rowsPerPage` NOT `rows_per_page` (Quasar API)

#### Paginated Table (Custom Component)

```python
from apps.nicegui.components import paginated_table

paginated_table(
    columns=[
        {"name": "col", "label": "Column", "field": "col"},
    ],
    rows=row_data,
    page_size=20,
)
```

### Charts

```python
from apps.nicegui.components import apply_chart_theme
import plotly.graph_objects as go

fig = go.Figure()
fig.add_trace(go.Scatter(x=x_data, y=y_data))

apply_chart_theme(fig)  # Applies theme colors/fonts
ui.plotly(fig).classes("w-full h-80")
```

### Dividers and Spacing

```python
from apps.nicegui.components import divider

divider()  # Styled horizontal separator
```

### Empty State

```python
from apps.nicegui.components import empty_state

empty_state(
    title="No experiments found",
    message="Run a backtest first to see results.",
    icon="science",
)
```

---

## Styling Patterns

### Using Theme Colors

```python
from apps.nicegui.components import THEME, COLORS

# Background
ui.row().style(f"background: {THEME['surface']};")

# Text color
ui.label("Important").style(f"color: {COLORS['success']};")

# Border
ui.element().style(f"border: 1px solid {THEME['surface_border']};")
```

### Type Scale

```python
# Consistent heading sizes
ui.label("Title").classes("text-4xl font-bold")    # Page titles
ui.label("Heading").classes("text-xl font-semibold") # Section headers
ui.label("Subheading").classes("text-lg")            # Subsections
ui.label("Body").classes("text-base")                # Body text
ui.label("Small").classes("text-sm")                 # Secondary text
ui.label("Tiny").classes("text-xs")                  # Muted text
```

### Color Coding Values

```python
from apps.nicegui.components import format_value

# Returns (formatted_string, css_class)
value_str, value_class = format_value(-5.27)  # ("5.27%", "value-negative")
value_str, value_class = format_value(12.5)   # ("12.50%", "value-positive")

# Apply to label
ui.label(value_str).classes(value_class)
```

**Available classes:**
- `.value-negative` - Red for negative values
- `.value-positive` - Green for positive values
- `.value-neutral` - Gray for zero

---

## Page Layout

### Page Structure

```python
def my_page() -> None:
    with page_layout("Title", "icon"):
        # 1. Page header
        from apps.nicegui.components import page_header
        page_header(
            "Page Title",
            "Optional subtitle",
            kpi_row=[
                dict(title="Metric", value="123", icon="stats", color=COLORS["info"]),
            ]
        )

        # 2. Content sections
        from apps.nicegui.components import divider
        divider()

        ui.label("Section").classes("text-xl font-semibold mb-4")
        # Section content...

        # 3. Repeat sections as needed
```

### Grid Layouts

```python
# KPI Grid (auto-responsive)
with ui.grid(columns=4).classes("w-full gap-4"):
    # 4 columns on large screens, fewer on small
    for card in cards:
        kpi_card(**card)

# Two-column layout
with ui.row().classes("w-full gap-4"):
    with ui.column().classes("flex-1"):
        # Left content
    with ui.column().classes("flex-1"):
        # Right content
```

---

## Performance Optimizations

### 1. Disk Cache for Status

```python
# In state/__init__.py
_STATUS_CACHE_FILE = Path.home() / ".cache" / "nseml_dashboard_status.json"

def get_db_status() -> dict:
    # Check disk cache first (instant)
    cached = _load_status_from_disk()
    if cached:
        return cached
    # Fall back to DB query
    return _fetch_status_sync()
```

### 2. Lite Mode for Initial Load

```python
# Instant page load with cached/minimal data
status = await aget_db_status(lite=True)

# Background refresh after UI renders
ui.timer(0.5, lambda: aget_db_status(), once=True)
```

### 3. Thread Pool for Blocking Operations

```python
_executor = ThreadPoolExecutor(max_workers=1)

async def aget_experiments() -> pd.DataFrame:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _fetch_experiments_sync)
```

### 4. Paginated Tables

```python
# Only render current page, not all rows
def paginated_table(rows: list, columns: list, page_size: int = 20):
    total_pages = (len(rows) + page_size - 1) // page_size
    # Only show rows[start:end]
```

---

## Common Patterns

### Refreshable Content

```python
@ui.refreshable
def render_data():
    data = fetch_data()
    ui.label(data["value"])

# Call to refresh
render_data.refresh()
```

### Async Data Loading

```python
async def my_page():
    with page_layout("Title", "icon"):
        # Show loading state first
        with ui.column().classes("loading-state"):
            ui.spinner("dots")

        # Load data async
        data = await fetch_data_async()

        # Replace spinner with content
```

### Preserving State Across Theme Toggle

```python
# Before reload
ui.run_javascript("""
    sessionStorage.setItem('my_key', JSON.stringify(state));
    window.location.reload();
""")

# After load
ui.run_javascript("""
    const saved = sessionStorage.getItem('my_key');
    if (saved) {
        // Restore state
    }
    sessionStorage.removeItem('my_key');
""")
```

---

## Adding New Pages

### Step 1: Create Page File

```python
# apps/nicegui/pages/my_new_page.py

from __future__ import annotations
import sys
from pathlib import Path

_apps_root = Path(__file__).resolve().parent.parent
_project_root = _apps_root.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))

from nicegui import ui
from apps.nicegui.components import page_layout, divider, COLORS, THEME

def my_new_page() -> None:
    """Render my new page."""
    with page_layout("My Page", "icon"):
        page_header("My Page", "Description")

        divider()

        # Your content here
        ui.label("Hello World").style(f"color: {THEME['text_primary']};")
```

### Step 2: Register Route

```python
# In apps/nicegui/main.py

from apps.nicegui.pages.my_new_page import my_new_page

ui.page("/my_page")(my_new_page)
```

### Step 3: Add Navigation

```python
# In NAV_ITEMS list (components/__init__.py)
NAV_ITEMS.append({
    "label": "My Page",
    "icon": "star",
    "path": "/my_page",
})
```

---

## CSS Customization

### Adding Custom Classes

```python
# In components/__init__.py _PAGE_CSS_BASE

.my-custom-class {
    background: var(--theme-surface);
    border: 1px solid var(--theme-surface-border);
    padding: 16px;
}
```

### Responsive Design

```css
/* Mobile adjustments */
@media (max-width: 768px) {
    .kpi-grid {
        grid-template-columns: repeat(2, 1fr);
    }
}
```

---

## Fonts

### Terminal Mode
- **Body:** IBM Plex Sans (400, 500, 600)
- **Mono:** Fira Code (400, 500, 600, 700)

### Clean Mode
- **Body:** DM Sans (400, 500, 600, 700)
- **Mono:** JetBrains Mono (400, 500, 600)

### Import URL

```html
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Fira+Code:wght@400;500;600;700&display=swap">
```

---

## Quasar Components Reference

NiceGUI wraps Quasar (Vue 3 UI library). Key components:

| Component | Usage |
|-----------|-------|
| `ui.button()` | Buttons with props like `flat`, `dense`, `round` |
| `ui.select()` | Dropdowns, use `rowsPerPage` for pagination |
| `ui.table()` | Tables, use dict format for columns/rows |
| `ui.tabs()` + `ui.tab()` | Tabbed content |
| `ui.expansion()` | Collapsible sections |
| `ui.input()` | Text input fields |
| `ui.label()` | Text labels |

### Useful Props

```python
button.props("flat dense round")
select.props("outlined clearable")
table.props("flat bordered")
```

---

## Best Practices

### DO ✅

- Use `THEME` and `COLORS` dictionaries for colors
- Use semantic class names from Tailwind (`.text-sm`, `.font-semibold`)
- Use CSS variables for theme-specific values
- Cache expensive DB queries
- Use async for blocking operations
- Add `.style()` for dynamic theme values

### DON'T ❌

- Hardcode colors like `"#00ff88"` (use `THEME["primary"]`)
- Use generic fonts like `Arial` (use our defined fonts)
- Block the event loop with synchronous DB calls
- Use `rows_per_page` (use `rowsPerPage` for Quasar)
- Add `w-full` class to tables that need scroll (use `min-width: max-content`)

---

## Troubleshooting

### Theme not applying correctly

- Clear browser cache
- Check CSS variable names match exactly
- Verify `_theme_mode["terminal"]` is set correctly

### Tables not scrolling horizontally

- Remove `.classes("w-full")` from table
- Add wrapper: `with ui.element("div").style("overflow-x: auto")`
- Set table style: `.style("min-width: max-content")`

### Pagination showing wrong number of rows

- Use `rowsPerPage` not `rows_per_page`
- Check that `rowsPerPage_options` includes your desired page size

### Page loads slowly

- Check if status cache is being used (should have disk cache)
- Verify async operations are using thread pool
- Look for expensive queries on initial render

---

## Resources

- **NiceGUI Docs:** https://nicegui.io/
- **Quasar Docs:** https://quasar.dev/
- **Tailwind CSS:** https://tailwindcss.com/
- **Material Icons:** https://fonts.google.com/icons (use snake_case names)
- **Plotly:** https://plotly.com/python/

---

## Changelog

### 2026-03-07 - Major UI Refresh
- ✅ Typography refresh (DM Sans for Clean, Fira Code for Terminal)
- ✅ Color updates (Neon green for Terminal, Indigo for Clean)
- ✅ Spacing improvements (20px padding, 12px table cells)
- ✅ Pagination default increased to 20 rows
- ✅ Horizontal scrollbars on all tables
- ✅ Full-width layout (removed max-width constraint)
- ✅ State preservation across theme toggle
- ✅ Performance optimizations (disk cache, lite mode)

---

*Generated for NSE Momentum Lab v0.1.0*
