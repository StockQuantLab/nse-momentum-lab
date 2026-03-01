# NiceGUI Dashboard Migration Plan

**Decision Date**: 2026-03-01
**Status**: ✅ Migration Complete (2026-03-01). Streamlit code removed. NiceGUI is the only dashboard.
**Replaced**: `apps/dashboard/` (Streamlit, removed) with `apps/nicegui/` (10 pages)
**Why Not Taipy**: Taipy 4.1.1 requires pyarrow 18.1.0 which has no pre-built wheels for Python 3.14 on Windows.

---

## Why NiceGUI

Streamlit was abandoned due to three compounding problems:
1. **Stateless re-run model** — every slider move re-runs the entire script
2. **Threading bug** — `@st.cache_data` callbacks run concurrently → DuckDB NULL dereference
3. **`ModuleNotFoundError: No module named 'apps'`** — import path issues

NiceGUI solves all three:
- **Persistent state** — variables persist across interactions; no full re-runs
- **Explicit state management** — `ui.state()` makes reactivity clear
- **Normal Python execution** — runs as a FastAPI app; standard import paths work
- **DuckDB friendly** — single connection works perfectly; no threading issues
- **Python 3.14 compatible** — no compilation required
- **Fast & modern** — Vue.js frontend with WebSocket reactivity

---

## Current Streamlit Page Inventory

| Page | File | Function |
|---|---|---|
| Home | `Home.py` | Status overview, recent experiments |
| Chat | `01_Chat.py` | LLM chat with pipeline context |
| Pipeline Status | `02_Pipeline_Status.py` | Job run history |
| Scans | `03_Scans.py` | Daily signal scan results |
| Experiments | `04_Experiments.py` | Experiment registry |
| Paper Ledger | `05_Paper_Ledger.py` | Paper trading P&L |
| Daily Summary | `06_Daily_Summary.py` | Daily metrics |
| Data Quality | `07_Data_Quality.py` | Parquet data QA |
| Run Pipeline | `08_Run_Pipeline.py` | Trigger ingestion/backtest |
| Compare Experiments | `12_Compare_Experiments.py` | Side-by-side exp comparison |
| Strategy Analysis | `13_Strategy_Analysis.py` | Filter pass rates, signal quality |
| Trade Analytics | `14_Trade_Analytics.py` | Per-trade deep-dive |
| Backtest Results | `15_Backtest_Results.py` | Equity curve, exit reasons, P&L |

---

## Target Architecture

```
apps/
  nicegui/
    __init__.py
    main.py                  # Entry point: NiceGUI app setup
    app.py                   # FastAPI app wrapper (optional)
    pages/
      __init__.py
      home.py                # Dashboard home
      backtest_results.py    # Experiment details
      trade_analytics.py     # Per-trade analysis
      compare_experiments.py # Side-by-side comparison
      strategy_analysis.py   # Filter sensitivity
      scans.py               # Daily scan results
      data_quality.py        # Data QA
      pipeline.py            # Job monitoring
      paper_ledger.py        # Paper trading
      daily_summary.py       # Daily metrics
      chat.py                # LLM assistant
    components/
      __init__.py
      kpi_card.py            # Reusable KPI cards
      trade_table.py         # Trade table with filters
      equity_chart.py        # Plotly equity curve
      nav_bar.py             # Navigation bar
    state/
      __init__.py
      shared_state.py        # Global state (DB connection, etc.)
    services/
      __init__.py
      backtest_service.py    # Run backtests via CLI
      scan_service.py        # Run scans
```

---

## NiceGUI Core Concepts Mapped to This Project

### State Management

NiceGUI uses reactive state with `ui.state()` and `ui.run_method()`:

```python
# state/shared_state.py
from nicegui import ui
from nse_momentum_lab.db.market_db import get_market_db

class GlobalState:
    db = get_market_db()  # Single connection, never re-created
    experiments = ui.state([])  # Reactive list
    selected_exp_id = ui.state(None)  # Reactive variable

# Pages access state
state = GlobalState()
```

### Page Structure

```python
# pages/backtest_results.py
from nicegui import ui

class BacktestResultsPage:
    def __init__(self):
        self.state = GlobalState()

    def render(self):
        ui.label("Backtest Results").classes("text-2xl")

        # Selector
        exp_select = ui.select(
            options=self.state.experiments,
            value=self.state.selected_exp_id,
            on_change=self.on_experiment_change
        )

        # KPIs (reactive)
        with ui.grid(columns=5):
            self.total_return = ui.label("-")
            self.ann_return = ui.label("-")
            self.win_rate = ui.label("-")
            self.max_dd = ui.label("-")
            self.calmar = ui.label("-")

        # Trade table
        self.trade_table = ui.table(
            columns=[...],
            rows=[],
            pagination=True,
            rows_per_page=50
        )

    def on_experiment_change(self, e):
        exp_id = e.value
        exp_data = self.state.db.get_experiment(exp_id)
        self.update_kpis(exp_data)
        self.update_trades(exp_id)
```

### DuckDB Connection Pattern

```python
# No changes needed to market_db.py
# get_market_db() singleton works perfectly with NiceGUI
from nse_momentum_lab.db.market_db import get_market_db

def load_trades(exp_id: str) -> pd.DataFrame:
    db = get_market_db()  # Returns existing connection
    df = db.get_experiment_trades(exp_id)
    return df.to_pandas()
```

### Arrow / Large Result Sets

NiceGUI's `ui.table` accepts pandas DataFrames. For Arrow tables, convert once:

```python
import pyarrow as pa

def load_feat_daily_arrow(symbols: list[str]) -> pa.Table:
    db = get_market_db()
    return db.con.execute(
        "SELECT * FROM feat_daily WHERE symbol = ANY(?)", [symbols]
    ).arrow()

# In page:
arrow_table = load_feat_daily_arrow(symbols)
df = arrow_table.to_pandas()
ui.table(df)
```

---

## Migration Phases

### Phase 1 — Foundation (Day 1)
**Goal**: NiceGUI running with Home + Backtest Results pages.

1. ✅ `uv add nicegui` — add to `pyproject.toml`
2. Create `apps/nicegui/main.py` entry point
3. Create `apps/nicegui/state/shared_state.py`:
   - Global DB connection
   - Experiment list state
4. Create `apps/nicegui/pages/home.py`:
   - DB status cards
   - Recent experiments table
   - Navigation links
5. Create `apps/nicegui/pages/backtest_results.py`:
   - Experiment selector dropdown
   - KPI cards (total return, win rate, etc.)
   - Trade table with pagination
6. Create `apps/nicegui/components/kpi_card.py`:
   - Reusable card component
7. Test: connection persists; sliders don't cause full re-runs

### Phase 2 — Analytics Pages (Day 2)
**Goal**: All read-only analytics pages live.

8. `apps/nicegui/pages/trade_analytics.py`:
   - Per-trade filters by exit reason
   - Distribution charts
9. `apps/nicegui/pages/compare_experiments.py`:
   - Multi-selector for experiments
   - Side-by-side metrics table
   - Overlaid equity curves
10. `apps/nicegui/pages/strategy_analysis.py`:
    - Filter pass rates
    - Parameter sensitivity heatmaps
11. `apps/nicegui/pages/scans.py`:
    - Daily signal results
    - Date range picker
12. `apps/nicegui/pages/data_quality.py`:
    - Parquet row counts
    - Date range validation

### Phase 3 — Interactive Features (Day 3)
**Goal**: Run backtests and scans from UI.

13. Create `apps/nicegui/services/backtest_service.py`:
    - Wrapper around `DuckDBBacktestRunner`
    - Runs in subprocess (async)
    - Progress updates via WebSocket
14. Create `apps/nicegui/components/run_backtest.py`:
    - Parameter form (sliders, inputs)
    - "Run" button with progress bar
    - Auto-redirect to results on completion
15. Create `apps/nicegui/pages/paper_ledger.py`:
    - Paper trade tracking
    - P&L calculations

### Phase 4 — Remaining Pages (Day 4)
**Goal**: Full feature parity.

16. `apps/nicegui/pages/daily_summary.py`:
    - Daily metrics
    - Date picker
17. `apps/nicegui/pages/pipeline.py`:
    - Job run history
    - Status indicators
18. `apps/nicegui/pages/chat.py`:
    - LLM chat interface
    - Pipeline context awareness
19. Add CLI entrypoint: `nseml-nicegui` or update `nseml-dashboard`
20. Delete `apps/dashboard/` entirely

---

## Key Implementation Notes

### Navigation Pattern

NiceGUI has a built-in navigation component or use custom links:

```python
# components/nav_bar.py
from nicegui import ui

def nav_bar():
    with ui.row().classes("gap-2 p-4 bg-gray-800"):
        ui.button("Home", on_click=lambda: ui.navigate_to("/"))
        ui.button("Backtest", on_click=lambda: ui.navigate_to("/backtest"))
        ui.button("Trades", on_click=lambda: ui.navigate_to("/trade_analytics"))
        # ... etc
```

### Charts with Plotly

NiceGUI has native Plotly support via `ui.plotly`:

```python
import plotly.graph_objects as go

def equity_curve(trades_df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trades_df["entry_date"],
        y=trades_df["cumulative_return"],
        mode="lines"
    ))
    ui.plotly(fig).classes("w-full h-96")
```

### Tables with Filters

```python
# NiceGUI table supports:
# - Pagination
# - Sorting
# - Row selection
# - Custom cell rendering

self.trade_table = ui.table(
    columns=[
        {"name": "symbol", "label": "Symbol", "field": "symbol"},
        {"name": "entry_date", "label": "Entry", "field": "entry_date"},
        {"name": "pnl_pct", "label": "P&L %", "field": "pnl_pct"},
        # ...
    ],
    rows=[],
    pagination=True,
    rows_per_page=50,
    sort=True,
    on_select=self.on_trade_select
)
```

### TIME Column Handling

Same as Taipy plan - DuckDB `TIME` → `datetime.time`:

```python
trades_df["entry_time"] = trades_df["entry_time"].apply(
    lambda t: str(t)[:5] if t else ""
)
```

### Running Backtests from UI

Use subprocess to avoid blocking the UI:

```python
import asyncio
from nicegui import ui

async def run_backtest(params: BacktestParams):
    """Run backtest in subprocess, update progress."""
    proc = await asyncio.create_subprocess_exec(
        "uv", "run", "nseml-backtest",
        "--universe-size", str(params.universe_size),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    # Update progress as output arrives
    async for line in proc.stdout:
        ui.update(progress_text=line.decode())

    await proc.wait()
    ui.navigate_to(f"/backtest?exp_id={result_exp_id}")
```

---

## Package Installation

```toml
# pyproject.toml additions
[project.dependencies]
nicegui = ">=3.8"  # Supports Python 3.14
```

```bash
uv add nicegui
```

---

## CLI Entry Point

```toml
[project.scripts]
nseml-dashboard = "apps.nicegui.main:main"  # Point to NiceGUI
```

Run:
```bash
doppler run -- uv run nseml-dashboard
# Or
doppler run -- uv run python -m apps.nicegui.main
```

---

## Files to Delete After Migration

```
apps/dashboard/          # entire Streamlit dashboard
apps/dashboard/Home.py
apps/dashboard/utils.py
apps/dashboard/.streamlit/
apps/dashboard/pages/
```

---

## Reference Links

- NiceGUI docs: https://nicegui.io/documentation
- NiceGUI state management: https://nicegui.io/documentation/state
- NiceGUI tables: https://nicegui.io/documentation/table
- NiceGUI charts: https://nicegui.io/documentation/chart
- DuckDB Arrow integration: https://duckdb.org/docs/guides/python/arrow.html

---

## Comparison: NiceGUI vs Taipy vs Streamlit

| Feature | Streamlit | Taipy | NiceGUI |
|---|---|---|---|
| Python 3.14 support | ✅ | ❌ (needs pyarrow build) | ✅ |
| Persistent state | ❌ (re-runs everything) | ✅ (Data Nodes) | ✅ (ui.state) |
| DuckDB friendly | ❌ (threading issues) | ✅ | ✅ |
| Scenario manager | ❌ | ✅ (built-in) | ⚠️ (custom) |
| Learning curve | Low | Medium | Low |
| Install complexity | Simple | Complex (many deps) | Simple |
| Startup time | Slow (5-10s) | Medium (2-5s) | Fast (<1s) |
| WebSocket support | ⚠️ | ✅ | ✅ (native) |
| Custom components | Hard | Medium | Easy (Vue.js) |

---

## Session Context

This plan was created after the Taipy migration plan failed due to Python 3.14 compatibility issues. NiceGUI provides the same key benefits (persistent state, DuckDB friendly) with better Python 3.14 support and a simpler API.

**Production config remains:**
| Config | Value |
|---|---|
| `entry_cutoff_minutes` | 60 |
| `universe_size` | 2000 |
| `bad_5min_guard` | 1.5x threshold |
| `filter_l` | `close > MA20` |
| **Calmar** | **43.67** |
| **Ann Ret** | **193.9%** |
| **Max DD** | **4.4%** |
| Best exp_id | `429c79ac45b65086` |
