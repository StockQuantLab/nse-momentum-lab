"""Home page - Dashboard overview with navigation and status."""

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

from apps.nicegui.state import aget_db_status, aget_experiments
from apps.nicegui.components import (
    page_layout,
    kpi_grid,
    nav_card,
    divider,
    primary_action_card,
    safe_timer,
    paginated_table,
    SPACE_SECTION,
    SPACE_GRID_DEFAULT,
    SPACE_MD,
    SPACE_GROUP_TIGHT,
    TYPE_BODY_LG,
    theme_text_primary,
    theme_text_secondary,
    theme_text_muted,
    theme_surface_hover,
    theme_surface_border,
    color_success,
    color_error,
    color_warning,
    color_info,
    color_primary,
    color_gray,
)


async def home_page() -> None:
    """Render the home page with instant loading and background refresh."""

    # Import get_db_status for sync instant load (uses cache)
    from apps.nicegui.state import get_db_status

    # Get status instantly (from cache or minimal defaults)
    status = get_db_status()
    if not status or status.get("symbols", 0) == 0:
        # No cache - use minimal defaults for instant render
        status = {
            "data_source": "parquet",
            "symbols": 0,
            "total_candles": 0,
            "date_range": "Loading...",
        }

    # Get experiments (usually fast with cache)
    experiments_df = await aget_experiments()

    with page_layout("Home", "home"):
        ui.html(
            f'<h1 class="text-4xl font-bold" style="color: {theme_text_primary()};">NSE Momentum Lab</h1>'
        )
        ui.label("Local-first momentum research and backtest analysis").classes(
            f"{TYPE_BODY_LG} {SPACE_SECTION}"
        ).style(f"color: {theme_text_secondary()};")

        # Primary CTA — the ONE thing a user should do next
        has_experiments = not experiments_df.is_empty()
        if not has_experiments:
            primary_action_card(
                "Run Your First Backtest",
                "Start exploring momentum strategies with a quick backtest on NSE data. "
                "Results will appear here for detailed analysis.",
                "play_arrow",
                "/",
                subtitle="GET STARTED",
            )
        else:
            primary_action_card(
                "Analyze Your Results",
                f"You have {len(experiments_df)} backtest experiment(s) ready for analysis. "
                "Dive into equity curves, trade breakdowns, and performance metrics.",
                "bar_chart",
                "/backtest",
                subtitle="VIEW RESULTS",
            )

        divider()

        # Navigation Cards — grouped into collapsible sections
        with (
            ui.expansion("Core Analysis", icon="bar_chart")
            .classes("w-full")
            .props("default-opened")
        ):
            # Asymmetric grid: primary card gets more visual weight
            with ui.row().classes("w-full gap-4"):
                with ui.column().classes("flex-2"):
                    nav_card(
                        "Backtest Results",
                        "Analyze stored 2LYNCH backtests from DuckDB",
                        "bar_chart",
                        "/backtest",
                        color_success(),
                    )
                with ui.column().classes(f"flex-1 {SPACE_GRID_DEFAULT}"):
                    nav_card(
                        "Trade Analytics",
                        "Inspect trade distributions and exit behavior",
                        "analytics",
                        "/trade_analytics",
                        color_info(),
                    )
                    nav_card(
                        "Compare Experiments",
                        "Compare multiple experiment runs side-by-side",
                        "compare_arrows",
                        "/compare",
                        color_primary(),
                    )
                    nav_card(
                        "Symbol Performance",
                        "Per-symbol P/L breakdown and ranking",
                        "leaderboard",
                        "/symbols",
                        color_warning(),
                    )

        with ui.expansion("Research Tools", icon="science").classes("w-full"):
            with ui.grid(columns=2).classes(f"w-full {SPACE_GRID_DEFAULT}"):
                nav_card(
                    "Strategy Analysis",
                    "Analyze parameter sensitivity across runs",
                    "tune",
                    "/strategy",
                    color_info(),
                )
                nav_card(
                    "Momentum Scans",
                    "View 4% + 2LYNCH scan results and pass rates",
                    "radar",
                    "/scans",
                    color_warning(),
                )
                nav_card(
                    "Data Quality",
                    "Validate data integrity and completeness",
                    "verified",
                    "/data_quality",
                    color_success(),
                )
                nav_card(
                    "Market Monitor",
                    "Track Stockbee-style market regime and breadth",
                    "monitor",
                    "/market_monitor",
                    color_info(),
                )

        with ui.expansion("Operations", icon="engineering").classes("w-full"):
            with ui.grid(columns=2).classes(f"w-full {SPACE_GRID_DEFAULT}"):
                nav_card(
                    "Paper Ledger",
                    "Track paper trading sessions and execution state",
                    "receipt_long",
                    "/paper_ledger",
                    color_warning(),
                )
                nav_card(
                    "Pipeline Status",
                    "Monitor job execution and status",
                    "engineering",
                    "/pipeline",
                    color_gray(),
                )
                nav_card(
                    "Daily Summary",
                    "Daily market overview and statistics",
                    "today",
                    "/daily_summary",
                    color_info(),
                )

        # System status — collapsed by default, available when needed
        with ui.expansion("System Status", icon="storage").classes("w-full"):
            ui.label("Data Overview").classes("text-xl font-semibold mb-4").style(
                f"color: {theme_text_primary()};"
            )
            kpi_grid(
                [
                    dict(
                        title="Data Source",
                        value=status.get("data_source", "unknown").upper(),
                        icon="storage",
                        color=color_info(),
                        muted=True,
                    ),
                    dict(
                        title="Symbols",
                        value=f"{int(status.get('symbols', 0)):,}",
                        icon="show_chart",
                        color=color_success(),
                    ),
                    dict(
                        title="Daily Candles",
                        value=f"{int(status.get('total_candles', 0)):,}",
                        icon="candlestick_chart",
                        color=color_warning(),
                    ),
                    dict(
                        title="Experiments",
                        value=f"{len(experiments_df):,}",
                        icon="science",
                        color=color_primary(),
                    ),
                ],
                columns=4,
            )

            # Dataset info
            with ui.column().classes(f"{SPACE_SECTION} gap-2"):
                if status.get("dataset_hash"):
                    ui.label(f"Dataset hash: {status.get('dataset_hash')}").classes(
                        "text-sm font-mono"
                    ).style(f"color: {theme_text_muted()};")
                if status.get("date_range"):
                    ui.label(f"Date range: {status.get('date_range')}").classes("text-sm").style(
                        f"color: {theme_text_muted()};"
                    )

            # Latest Experiment — shown inline under status
            if not experiments_df.is_empty():
                ui.separator().classes("my-4")
                ui.label("Latest Experiment").classes("text-xl font-semibold mb-4").style(
                    f"color: {theme_text_primary()};"
                )
                latest = experiments_df.row(0, named=True)
                ret_val = float(latest.get("total_return_pct", 0))
                kpi_grid(
                    [
                        dict(
                            title="Total Return",
                            value=f"{ret_val:.2f}%",
                            icon="attach_money",
                            color=color_success() if ret_val > 0 else color_error(),
                            is_hero=True,
                            trend=ret_val,
                            trend_label="All-time performance",
                        ),
                        dict(
                            title="Win Rate",
                            value=f"{float(latest.get('win_rate_pct', 0)):.1f}%",
                            icon="target",
                            color=color_info(),
                        ),
                        dict(
                            title="Trades",
                            value=f"{int(latest.get('total_trades', 0)):,}",
                            icon="bar_chart",
                            color=color_warning(),
                        ),
                        dict(
                            title="Exp ID",
                            value=str(latest.get("exp_id", "-")),
                            icon="tag",
                            color=color_gray(),
                            muted=True,
                        ),
                    ],
                    columns=4,
                    hero_index=0,
                )

        # Recent Experiments — collapsed by default
        if not experiments_df.is_empty():
            with ui.expansion("Recent Experiments", icon="science").classes("w-full"):
                recent = experiments_df.head(20)
                rows = []
                for r in recent.iter_rows(named=True):
                    ret = float(r.get("total_return_pct", 0) or 0)
                    rows.append(
                        {
                            "exp_id": str(r.get("exp_id", "-")),
                            "strategy": str(r.get("strategy_name", "-")),
                            "period": f"{r.get('start_year', '?')}-{r.get('end_year', '?')}",
                            "trades": int(r.get("total_trades", 0) or 0),
                            "win_rate": f"{float(r.get('win_rate_pct', 0) or 0):.1f}%",
                            "return": f"{ret:.1f}%",
                        }
                    )
                cols = [
                    {"name": "exp_id", "label": "Exp ID", "field": "exp_id", "sortable": True},
                    {
                        "name": "strategy",
                        "label": "Strategy",
                        "field": "strategy",
                        "sortable": True,
                    },
                    {"name": "period", "label": "Period", "field": "period"},
                    {"name": "trades", "label": "Trades", "field": "trades", "sortable": True},
                    {
                        "name": "win_rate",
                        "label": "Win Rate",
                        "field": "win_rate",
                        "sortable": True,
                    },
                    {"name": "return", "label": "Return %", "field": "return", "sortable": True},
                ]
                paginated_table(rows, cols, page_size=10)

        # Quick Start Commands — collapsed by default, developer-only reference
        with ui.expansion("Quick Start Commands", icon="terminal").classes("w-full"):
            commands = [
                ("Sync dependencies", "uv sync"),
                ("Start database", "doppler run -- docker compose up -d"),
                (
                    "Run backtest",
                    "doppler run -- uv run nseml-backtest --universe-size 500 --start-year 2015 --end-year 2025",
                ),
                ("Launch dashboard", "doppler run -- uv run nseml-dashboard"),
            ]
            for desc, cmd in commands:
                with ui.column().classes(f"w-full {SPACE_MD} {SPACE_GROUP_TIGHT}"):
                    ui.label(desc).classes("text-xs uppercase tracking-wide").style(
                        f"color: {theme_text_muted()};"
                    )
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.label("$").classes("font-mono").style(f"color: {color_success()};")
                        ui.label(cmd).classes(
                            "flex-grow font-mono text-sm px-3 py-2 rounded"
                        ).style(
                            f"background: {theme_surface_hover()}; "
                            f"border: 1px solid {theme_surface_border()}; "
                            f"color: {theme_text_primary()}; "
                            f"border-radius: 6px;"
                        )

        # Footer
        divider()
        with (
            ui.row()
            .classes("w-full justify-between items-center text-sm")
            .style(f"color: {theme_text_muted()};")
        ):
            ui.label("Python 3.14 | NiceGUI | DuckDB")
            ui.label("NSE Momentum Lab v0.1.0")

        # Background refresh: fetch full status after page renders
        async def refresh_status():
            """Refresh status in background after initial render."""
            _ = await aget_db_status()  # Populates cache for next interaction

        safe_timer(0.5, refresh_status, once=True)
