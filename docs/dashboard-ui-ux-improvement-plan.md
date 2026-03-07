# NSE Momentum Lab Dashboard — UI/UX Improvement Plan

**Date**: 2026-03-07
**Status**: Planning
**Priority**: Medium

---

## Executive Summary

The current NiceGUI dashboard has a solid foundation but needs improvements in three key areas:

1. **Performance** — Synchronous DB calls block UI, trade tables render all rows at once
2. **UX** — Missing empty states, no search/filter, poor experiment comparison flow
3. **Visual Polish** — Functional but forgettable aesthetic, inconsistent information density

This plan provides a structured roadmap for addressing these issues.

---

## Current Architecture Assessment

### What Works
- Clean, professional light theme with Inter typography
- Consistent component library (KPI cards, navigation, dividers)
- Well-structured 9-page navigation
- Plotly charts with consistent theming
- Persistent server-side state via NiceGUI (smart migration from Streamlit)
- Proper DuckDB thread safety with single-worker executor

### Critical Issues

| Issue | Impact | Priority |
|-------|--------|----------|
| Synchronous DB calls in pages | UI freezes on large queries | **HIGH** |
| No lazy loading for trade tables | 7000+ trades render at once | **HIGH** |
| No chart virtualization | Heavy Plotly figures slow page load | **MEDIUM** |
| Missing experiment cache invalidation | Stale data after new backtests | **MEDIUM** |
| Empty states are bare error messages | Poor user guidance | **MEDIUM** |
| No search/filter on trade tables | Hard to find specific data | **MEDIUM** |
| Compare page doesn't allow selection | Must compare all experiments | **LOW** |

---

## Phase 1: Critical Performance (1-2 days)

### 1.1 Make all DB calls async

**Files to modify**:
- `apps/nicegui/pages/backtest_results.py` (line 45)
- `apps/nicegui/pages/compare_experiments.py` (line 28)
- `apps/nicegui/pages/strategy_analysis.py` (line 46)
- `apps/nicegui/pages/trade_analytics.py` (line 39)

**Change pattern**:
```python
# Before:
experiments_df = get_experiments()

# After:
experiments_df = await aget_experiments()
```

Also make page functions async:
```python
# Before:
def backtest_page() -> None:

# After:
async def backtest_page() -> None:
```

### 1.2 Add loading states to all async operations

**Location**: `apps/nicegui/components/__init__.py`

Add loading component:
```python
@contextmanager
def loading_spinner():
    """Show loading spinner during async operations."""
    spinner = ui.spinner("dots").classes("mt-8")
    try:
        yield
    finally:
        spinner.delete()
```

**Usage in pages**:
```python
@ui.refreshable
async def render_experiment(exp_id: str) -> None:
    with loading_spinner():
        exp = await asyncio.get_event_loop().run_in_executor(
            None, lambda: get_experiment(exp_id)
        )
    # ... rest of render
```

### 1.3 Implement trade table pagination

**Location**: `apps/nicegui/components/__init__.py`

Add new component:
```python
def paginated_table(
    rows: list,
    columns: list,
    page_size: int = 50,
    row_key: Callable | None = None,
):
    """Paginated table that only renders current page."""
    state = ui.state({"page": 0, "total_pages": (len(rows) + page_size - 1) // page_size})

    def show_page():
        start = state.page * page_size
        end = start + page_size

        with ui.column().classes("w-full"):
            ui.table(
                columns=columns,
                rows=rows[start:end],
                pagination=page_size,
                row_key=row_key or (lambda r: r.get("id", hash(str(r)))),
            ).classes("w-full")

            with ui.row().classes("justify-between items-center mt-4"):
                ui.label(f"Showing {start + 1}-{min(end, len(rows))} of {len(rows)}").classes(
                    f"color: {THEME['text_muted']};"
                )
                with ui.row().classes("gap-2"):
                    ui.button(
                        "Previous",
                        on_click=lambda: setattr(state, "page", max(0, state.page - 1)) or show_page()
                    ).props("flat dense").classes(
                        lambda: "" if state.page > 0 else "invisible"
                    )
                    ui.label(f"Page {state.page + 1} of {state.total_pages}").classes(
                        f"color: {THEME['text_secondary']};"
                    )
                    ui.button(
                        "Next",
                        on_click=lambda: setattr(state, "page", min(state.total_pages - 1, state.page + 1)) or show_page()
                    ).props("flat dense").classes(
                        lambda: "" if state.page < state.total_pages - 1 else "invisible"
                    )

    show_page()
```

**Update backtest_results.py** to use `paginated_table` instead of `ui.table` for:
- Exit reasons table (line 292)
- R-multiple percentile table (line 358)
- Winners/Losers tables (lines 416, 427)
- Per-Stock table (line 451)

---

## Phase 2: Core UX Improvements (2-3 days)

### 2.1 Add empty state component

**Location**: `apps/nicegui/components/__init__.py`

```python
def empty_state(
    title: str,
    message: str,
    action_label: str | None = None,
    action_callback: Callable | None = None,
    icon: str = "inbox",
) -> None:
    """Beautiful empty state component with optional action."""
    with ui.column().classes("items-center justify-center py-16 gap-4"):
        ui.icon(icon).classes("text-6xl opacity-50").style(
            f"color: {THEME['text_muted']};"
        )
        ui.label(title).classes("text-xl font-semibold").style(
            f"color: {THEME['text_primary']};"
        )
        ui.label(message).classes("text-center max-w-md").style(
            f"color: {THEME['text_secondary']};"
        )
        if action_label and action_callback:
            ui.button(action_label, on_click=action_callback).props(
                "push color=primary"
            ).classes("mt-4")
```

**Apply to pages**:
- `backtest_results.py` — line 47 (no experiments)
- `backtest_results.py` — line 212 (no trades)
- `compare_experiments.py` — line 30 (no experiments)
- `trade_analytics.py` — line 42 (no experiments)
- `trade_analytics.py` — line 56 (no trades)

### 2.2 Add page header component

**Location**: `apps/nicegui/components/__init__.py`

```python
def page_header(
    title: str,
    subtitle: str | None = None,
    kpi_row: list[dict] | None = None,
) -> None:
    """Consistent page header with optional KPIs."""
    with ui.column().classes("mb-8"):
        with ui.column().classes("gap-1 mb-6"):
            ui.label(title).classes("text-2xl font-bold").style(
                f"color: {THEME['text_primary']};"
            )
            if subtitle:
                ui.label(subtitle).classes("text-sm").style(
                    f"color: {THEME['text_secondary']};"
                )

        if kpi_row:
            kpi_grid(kpi_row, columns=len(kpi_row))
```

**Apply to pages**:
- `backtest_results.py` — replace manual title + KPI grid
- `trade_analytics.py` — standardize header
- `compare_experiments.py` — add header
- `strategy_analysis.py` — add header

### 2.3 Add search/filter to trade tables

**Location**: `apps/nicegui/components/__init__.py`

```python
def trade_table_with_filters(
    trades_df: pd.DataFrame,
    columns: list,
    rows: list,
) -> None:
    """Trade table with symbol, exit reason, and P&L filters."""
    filters = ui.state({
        "symbol": "",
        "exit_reason": "all",
        "min_pnl": None,
        "max_pnl": None,
    })

    exit_reasons = ["all"] + sorted(trades_df["exit_reason"].unique().tolist())

    @ui.refreshable
    def filtered_table():
        filtered = rows
        if filters.symbol:
            filtered = [r for r in filtered if filters.symbol.lower() in r.get("symbol", "").lower()]
        if filters.exit_reason != "all":
            filtered = [r for r in filtered if r.get("exit_reason") == filters.exit_reason]
        if filters.min_pnl is not None:
            filtered = [r for r in filtered if float(r.get("pnl_pct", 0)) >= filters.min_pnl]
        if filters.max_pnl is not None:
            filtered = [r for r in filtered if float(r.get("pnl_pct", 0)) <= filters.max_pnl]

        paginated_table(filtered, columns, page_size=50)

    with ui.column().classes("w-full"):
        with ui.row().classes("gap-4 mb-4 items-end"):
            ui.input(
                "Symbol",
                value=filters.symbol,
                on_change=lambda e: update_filter("symbol", e.value)
            ).props("dense outlined clearable").classes("w-48")

            ui.select(
                exit_reasons,
                value="all",
                label="Exit Reason",
                on_change=lambda e: update_filter("exit_reason", e.value)
            ).props("dense outlined").classes("w-48")

            ui.input(
                "Min P&L %",
                on_change=lambda e: update_filter("min_pnl", float(e.value) if e.value else None)
            ).props("dense outlined type='number' clearable").classes("w-32")

            ui.input(
                "Max P&L %",
                on_change=lambda e: update_filter("max_pnl", float(e.value) if e.value else None)
            ).props("dense outlined type='number' clearable").classes("w-32")

            ui.button("Clear", on_click=clear_filters).props("flat").classes("mb-1")

        filtered_table()
```

### 2.4 Add experiment selector to Compare page

**Location**: `apps/nicegui/pages/compare_experiments.py`

```python
def compare_page() -> None:
    """Render the compare experiments page."""
    with page_layout("Compare", "compare_arrows"):
        experiments_df = get_experiments()

        if experiments_df.empty:
            empty_state(
                "No experiments to compare",
                "Run at least 2 backtests to compare experiments.",
                icon="compare_arrows",
            )
            return

        if len(experiments_df) < 2:
            empty_state(
                "Need more experiments",
                f"You have {len(experiments_df)} experiment(s). Run at least 2 backtests.",
                icon="compare_arrows",
            )
            return

        # Build experiment options
        exp_options = build_experiment_options(experiments_df)
        labels = list(exp_options.keys())

        # Experiment selector
        selected = ui.state({"exp1": labels[0], "exp2": labels[1] if len(labels) > 1 else labels[0]})

        with ui.row().classes("gap-4 mb-6 kpi-card p-4"):
            with ui.column().classes("flex-1"):
                ui.label("Experiment A").classes("text-sm font-medium mb-2").style(
                    f"color: {THEME['text_secondary']};"
                )
                ui.select(
                    labels,
                    value=selected.exp1,
                    on_change=lambda e: setattr(selected, "exp1", e.value) or render_comparison()
                ).classes("w-full")

            with ui.column().classes("flex-1"):
                ui.label("Experiment B").classes("text-sm font-medium mb-2").style(
                    f"color: {THEME['text_secondary']};"
                )
                ui.select(
                    labels,
                    value=selected.exp2,
                    on_change=lambda e: setattr(selected, "exp2", e.value) or render_comparison()
                ).classes("w-full")

        @ui.refreshable
        def render_comparison():
            exp1_id = exp_options.get(selected.exp1)
            exp2_id = exp_options.get(selected.exp2)

            if exp1_id == exp2_id:
                ui.label("Please select different experiments to compare.").classes(
                    "text-center py-8"
                ).style(f"color: {COLORS['warning']};")
                return

            exp1 = get_experiment(exp1_id)
            exp2 = get_experiment(exp2_id)

            # Side-by-side KPI comparison
            # ... render comparison
```

### 2.5 Add auto-refresh for new experiments

**Location**: `apps/nicegui/state/__init__.py`

Add event emission for new experiments:
```python
from nicegui import ui

_experiment_callbacks: list[Callable] = []

def on_new_experiments(callback: Callable) -> None:
    """Register callback for new experiments."""
    _experiment_callbacks.append(callback)

async def poll_new_experiments():
    """Check for new experiments and notify listeners."""
    global _experiments_cache, _experiments_cache_time

    old_count = len(_experiments_cache) if _experiments_cache is not None else 0
    await aget_experiments(force_refresh=True)
    new_count = len(_experiments_cache) if _experiments_cache is not None else 0

    if new_count > old_count:
        for cb in _experiment_callbacks:
            cb()
```

**Usage in backtest_results.py**:
```python
from apps.nicegui.state import on_new_experiments

def backtest_page() -> None:
    # ... existing code

    def on_new_exp():
        ui.notify("New experiment detected! Refresh the experiment list.", type="info")

    on_new_experiments(on_new_exp)

    # Poll every 30 seconds
    ui.timer(30, lambda: asyncio.create_task(poll_new_experiments()))
```

---

## Phase 3: Visual Polish (1-2 days)

### 3.1 Add subtle background grid

**Location**: `apps/nicegui/components/__init__.py` — modify `_PAGE_CSS`

```python
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

/* ... existing styles ... */
""" % {**THEME}
```

### 3.2 Improve chart visualizations

**A. Add benchmark to equity curve**

**Location**: `apps/nicegui/pages/backtest_results.py` — equity curve tab

```python
# In equity curve tab panel
with ui.tab_panel(tab_equity):
    if "pnl_pct" in trades_df.columns and "entry_date" in trades_df.columns:
        equity = trades_df.sort_values("entry_date").copy()
        equity["cumulative_return"] = equity["pnl_pct"].cumsum()

        # Get NIFTY benchmark data (if available)
        # This would require storing benchmark data in DB
        # For now, show drawdown fill instead

        # Calculate drawdown
        equity["cummax"] = equity["cumulative_return"].cummax()
        equity["drawdown"] = equity["cumulative_return"] - equity["cummax"]

        fig_eq = go.Figure()

        # Drawdown area
        fig_eq.add_trace(
            go.Scatter(
                x=equity["entry_date"],
                y=equity["drawdown"],
                fill="tozeroy",
                fillcolor=f"{COLORS['error']}22",
                line_color=COLORS["error"],
                name="Drawdown",
                hovertemplate="%{x}<br>Drawdown: %{y:.2f}%%<extra></extra>",
            )
        )

        # Equity line
        fig_eq.add_trace(
            go.Scatter(
                x=equity["entry_date"],
                y=equity["cumulative_return"],
                mode="lines",
                name="Cumulative Return %",
                line=dict(color=COLORS["primary"], width=2.5),
                hovertemplate="%{x}<br>Return: %{y:.2f}%%<extra></extra>",
            )
        )

        fig_eq.update_layout(
            title="Equity Curve with Drawdown",
            xaxis_title="Date",
            yaxis_title="Return %",
            hovermode="x unified",
        )
        apply_chart_theme(fig_eq)
        ui.plotly(fig_eq).classes("w-full h-80")
```

**B. Add monthly heatmap**

**Location**: Add new tab to backtest_results.py

```python
# Add to tabs:
tab_monthly = ui.tab("Monthly Heatmap")

# In tab_panels:
with ui.tab_panel(tab_monthly):
    if "entry_date" in trades_df.columns and "pnl_pct" in trades_df.columns:
        trades_df["year"] = pd.to_datetime(trades_df["entry_date"]).dt.year
        trades_df["month"] = pd.to_datetime(trades_df["entry_date"]).dt.month

        monthly_pivot = trades_df.pivot_table(
            index="year",
            columns="month",
            values="pnl_pct",
            aggfunc="sum",
            fill_value=0,
        )

        # Reorder columns Jan-Dec
        monthly_pivot = monthly_pivot.reindex(columns=range(1, 13))

        fig = px.imshow(
            monthly_pivot,
            labels=dict(x="Month", y="Year", color="Return %"),
            x=["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
            color_continuous_scale=[COLORS["error"], "#f1f5f9", COLORS["success"]],
            color_continuous_midpoint=0,
            title="Monthly Returns Heatmap",
        )
        fig.update_xaxes(side="top")
        apply_chart_theme(fig)
        ui.plotly(fig).classes("w-full h-80")
```

**C. Improve R-multiple histogram**

Add normal distribution overlay:
```python
# In R-Multiple tab panel
from scipy import stats

r_vals = trades_df["pnl_r"].dropna()
if len(r_vals) > 0:
    fig_r = go.Figure()

    # Histogram
    fig_r.add_trace(
        go.Histogram(
            x=r_vals,
            nbinsx=50,
            marker_color=COLORS["primary"],
            name="Distribution",
            opacity=0.7,
        )
    )

    # Normal distribution overlay
    mu, sigma = r_vals.mean(), r_vals.std()
    x_norm = np.linspace(r_vals.min(), r_vals.max(), 100)
    y_norm = stats.norm.pdf(x_norm, mu, sigma)
    # Scale to histogram height
    bin_width = (r_vals.max() - r_vals.min()) / 50
    y_norm_scaled = y_norm * len(r_vals) * bin_width

    fig_r.add_trace(
        go.Scatter(
            x=x_norm,
            y=y_norm_scaled,
            mode="lines",
            name="Normal Dist",
            line=dict(color=COLORS["error"], dash="dash"),
        )
    )

    fig_r.add_vline(x=0, line_dash="dash", line_color=COLORS["text_muted"])
    fig_r.add_vline(
        x=mu,
        line_dash="dot",
        line_color=COLORS["success"],
        annotation_text=f"Mean: {mu:.2f}R",
    )

    fig_r.update_layout(
        title="R-Multiple Distribution",
        xaxis_title="R-Multiple",
        yaxis_title="Count",
        barmode="overlay",
    )
    apply_chart_theme(fig_r)
    ui.plotly(fig_r).classes("w-full h-64")
```

### 3.3 Add export menu

**Location**: `apps/nicegui/components/__init__.py`

```python
def export_menu(
    data: pd.DataFrame,
    filename_base: str,
    label: str = "Export",
) -> None:
    """Dropdown with multiple export formats."""
    with ui.button(label, icon="download").props("flat") as btn:
        with ui.menu().props("anchor=top-end"):
            ui.menu_item(
                "Download as CSV",
                lambda: ui.download(data.to_csv(index=False).encode(), filename=f"{filename_base}.csv")
            )
            ui.menu_item(
                "Download as JSON",
                lambda: ui.download(data.to_json(indent=2).encode(), filename=f"{filename_base}.json")
            )
            ui.menu_item(
                "Copy to clipboard",
                lambda: ui.run_javascript(f"""
                    navigator.clipboard.writeText(`{data.to_csv(index=False)}`);
                """)
            )
```

### 3.4 Consider dark mode

**Location**: `apps/nicegui/components/__init__.py`

Add dark theme tokens:
```python
THEME_DARK = {
    "page_bg": "#0f172a",  # slate-900
    "surface": "#1e293b",  # slate-800
    "surface_border": "#334155",  # slate-700
    "surface_hover": "#334155",  # slate-700
    "text_primary": "#f1f5f9",  # slate-100
    "text_secondary": "#94a3b8",  # slate-400
    "text_muted": "#64748b",  # slate-500
    "primary": "#3b82f6",  # blue-500
    "primary_dark": "#2563eb",  # blue-600
    "divider": "#334155",  # slate-700
}

# Add theme toggle in page_layout header
def page_layout(title: str, icon: str = "bar_chart"):
    """Context manager that wraps every page with consistent chrome."""
    theme_mode = ui.state({"dark": False})

    def toggle_theme():
        theme_mode.dark = not theme_mode.dark
        current_theme = THEME_DARK if theme_mode.dark else THEME
        ui.query("body").style(f"background-color: {current_theme['page_bg']}; color: {current_theme['text_primary']};")

    # In header:
    ui.button(icon="dark_mode", on_click=toggle_theme).props("flat round dense")
```

---

## Phase 4: Advanced Features (3+ days)

### 4.1 Keyboard shortcuts

**Location**: Add to `page_layout` in `components/__init__.py`

```python
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
        };
        if (shortcuts[e.key]) {
            e.preventDefault();
            shortcuts[e.key]();
        }
    }
});
</script>
"""

def page_layout(title: str, icon: str = "bar_chart"):
    # ... existing code ...
    ui.add_head_html(_KEYBINDINGS_HTML)
```

**Add keyboard shortcuts help dialog**:
```python
def shortcuts_dialog():
    """Show keyboard shortcuts."""
    with ui.dialog() as dialog:
        with ui.card().classes("w-96"):
            ui.label("Keyboard Shortcuts").classes("text-xl font-bold mb-4")
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
                ("?", "Show this dialog"),
            ]
            for key, action in shortcuts:
                with ui.row().classes("justify-between w-full py-1"):
                    ui.label(key).classes("font-mono text-sm bg-gray-100 px-2 py-1 rounded")
                    ui.label(action).classes("text-sm")

            ui.button("Close", on_click=dialog.close).props("flat").classes("mt-4 w-full")

    dialog.open()
```

### 4.2 Quick tour modal

**Location**: `apps/nicegui/components/__init__.py`

```python
_tour_completed = ui.state({"completed": False})

def show_tour():
    """Show first-time user tour."""
    if _tour_completed.completed:
        return

    steps = [
        {
            "title": "Welcome to NSE Momentum Lab",
            "content": "A local-first momentum research and backtest analysis platform. Let's take a quick tour.",
            "target": None,
        },
        {
            "title": "Navigation",
            "content": "Use the sidebar to navigate between pages. Press Alt+? anytime to see keyboard shortcuts.",
            "target": ".q-drawer",
        },
        {
            "title": "Backtest Results",
            "content": "View detailed analysis of your backtest experiments including equity curves, trade breakdown, and performance metrics.",
            "target": None,
        },
        {
            "title": "Running a Backtest",
            "content": "Run a backtest from your terminal: 'doppler run -- uv run nseml-backtest --universe-size 2000'",
            "target": None,
        },
        {
            "title": "All Set!",
            "content": "You're ready to start researching. Access this tour anytime from the home page.",
            "target": None,
        },
    ]

    current_step = ui.state({"step": 0})

    @ui.refreshable
    def tour_dialog():
        step = steps[current_step.step]
        with ui.dialog() as dialog:
            with ui.card().classes("w-[500px]"):
                with ui.row().classes("justify-between items-center mb-4"):
                    ui.label(step["title"]).classes("text-lg font-semibold")
                    ui.label(f"{current_step.step + 1} / {len(steps)}").classes(
                        f"color: {THEME['text_muted']};"
                    )

                ui.label(step["content"]).classes("mb-6")

                with ui.row().classes("justify-end gap-2"):
                    if current_step.step > 0:
                        ui.button(
                            "Previous",
                            on_click=lambda: setattr(current_step, "step", current_step.step - 1) or tour_dialog.refresh()
                        ).props("flat")

                    if current_step.step < len(steps) - 1:
                        ui.button(
                            "Next",
                            on_click=lambda: setattr(current_step, "step", current_step.step + 1) or tour_dialog.refresh()
                        ).props("push color=primary")
                    else:
                        ui.button(
                            "Get Started",
                            on_click=lambda: setattr(_tour_completed, "completed", True) or dialog.close()
                        ).props("push color=primary")

                    ui.button(
                        "Skip Tour",
                        on_click=lambda: setattr(_tour_completed, "completed", True) or dialog.close()
                    ).props("flat")

    tour_dialog()
```

### 4.3 Alert system for new experiments

**Location**: `apps/nicegui/components/__init__.py`

```python
class ExperimentAlerts:
    """Manage alerts for new experiments."""

    def __init__(self):
        self._last_seen_count = 0
        self._alert_element = None

    def check_and_alert(self, experiments_df: pd.DataFrame):
        """Check for new experiments and show alert if found."""
        current_count = len(experiments_df)

        if current_count > self._last_seen_count and self._last_seen_count > 0:
            new_count = current_count - self._last_seen_count

            # Remove existing alert
            if self._alert_element:
                self._alert_element.delete()

            # Show new alert
            with ui.row().classes("fixed top-16 right-4 z-50") as alert_row:
                self._alert_element = alert_row
                with ui.column().classes("gap-2"):
                    ui.notify(
                        f"{new_count} new experiment{'s' if new_count > 1 else ''} detected!",
                        type="positive",
                        position="top-right",
                        close_btn=True,
                    )
                    with ui.card().classes("p-4 shadow-lg"):
                        with ui.row().classes("items-center gap-3"):
                            ui.icon("new_releases").classes("text-2xl").style(
                                f"color: {COLORS['success']};"
                            )
                            ui.label(f"{new_count} new experiment{'s' if new_count > 1 else ''} available").classes(
                                "font-medium"
                            )
                        with ui.row().classes("gap-2 mt-2"):
                            ui.button(
                                "View",
                                on_click=lambda: ui.navigate.to("/backtest")
                            ).props("push color=primary flat")
                            ui.button(
                                "Dismiss",
                                on_click=alert_row.delete
                            ).props("flat")

        self._last_seen_count = current_count


# Singleton instance
_experiment_alerts = ExperimentAlerts()
```

---

## Implementation Checklist

### Phase 1: Performance
- [ ] Convert `get_experiments()` to `aget_experiments()` in all pages
- [ ] Make page functions async
- [ ] Add loading state component
- [ ] Implement `paginated_table` component
- [ ] Replace all `ui.table` calls with `paginated_table` for large datasets

### Phase 2: UX
- [ ] Create `empty_state` component
- [ ] Apply empty states to all pages
- [ ] Create `page_header` component
- [ ] Apply page headers to all pages
- [ ] Create `trade_table_with_filters` component
- [ ] Add filters to backtest_results trade tables
- [ ] Add experiment selector to Compare page
- [ ] Implement `poll_new_experiments`
- [ ] Add auto-refresh timer to backtest page

### Phase 3: Visual Polish
- [ ] Add grid background CSS
- [ ] Add fade-in animations to KPI cards
- [ ] Add drawdown to equity curve
- [ ] Add monthly heatmap tab
- [ ] Add normal distribution overlay to R-multiple chart
- [ ] Create `export_menu` component
- [ ] Add dark mode toggle
- [ ] Define dark theme tokens

### Phase 4: Advanced
- [ ] Add keyboard shortcuts JavaScript
- [ ] Create shortcuts dialog
- [ ] Implement show_tour modal
- [ ] Create ExperimentAlerts class
- [ ] Wire up new experiment alerts

---

## Design Decisions Record

### NiceGUI vs Streamlit (Already Decided)
- **Decision**: Use NiceGUI for persistent server-side state
- **Rationale**: Streamlit's re-run on every interaction caused threading issues with DuckDB
- **Date**: 2026-03-01

### Single-worker Executor (Already Implemented)
- **Decision**: Use `ThreadPoolExecutor(max_workers=1)` for DB calls
- **Rationale**: DuckDB connections are not thread-safe for concurrent access
- **Status**: Implemented in `state/__init__.py`

### Light Theme Default
- **Decision**: Default to light theme
- **Rationale**: Financial applications traditionally use light themes for better readability
- **Future**: Dark mode as optional toggle

### Pagination vs Virtual Scroll
- **Decision**: Use pagination for large tables
- **Rationale**: Simpler implementation, better performance with NiceGUI's table component
- **Alternative Considered**: Virtual scrolling (more complex)

---

## Open Questions

1. **Benchmark Data**: Should we store NIFTY/SENSEX benchmark data for comparison?
   - Requires additional data pipeline
   - Significant value for users

2. **User Preferences**: Should we persist user preferences (theme, sidebar state)?
   - Requires browser storage or server-side user model
   - Nice improvement for repeat users

3. **Real-time Updates**: Should we use WebSocket for real-time experiment updates?
   - Currently using polling every 30 seconds
   - WebSocket would be instant but adds complexity

4. **Mobile Responsiveness**: Should we optimize for mobile/tablet?
   - Current design is desktop-first
   - Financial tools rarely used on mobile

---

## References

- NiceGUI Documentation: https://nicegui.io/
- Plotly Theme Guide: https://plotly.com/python/templates/
- DuckDB Python Docs: https://duckdb.org/docs/api/python/
- Material Design Icons: https://fonts.google.com/icons (used via icon names)
