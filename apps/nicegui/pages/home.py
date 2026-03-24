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
    COLORS,
    THEME,
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
        ui.label("NSE Momentum Lab").classes("text-4xl font-bold mb-1").style(
            f"color: {THEME['text_primary']};"
        )
        ui.label("Local-first momentum research and backtest analysis").classes(
            "text-lg mb-6"
        ).style(f"color: {THEME['text_secondary']};")

        divider()

        # Database Status KPIs
        ui.label("Database Status").classes("text-xl font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        kpi_grid(
            [
                dict(
                    title="Data Source",
                    value=status.get("data_source", "unknown").upper(),
                    icon="storage",
                    color=COLORS["info"],
                ),
                dict(
                    title="Symbols",
                    value=f"{int(status.get('symbols', 0)):,}",
                    icon="show_chart",
                    color=COLORS["success"],
                ),
                dict(
                    title="Daily Candles",
                    value=f"{int(status.get('total_candles', 0)):,}",
                    icon="candlestick_chart",
                    color=COLORS["warning"],
                ),
                dict(
                    title="Experiments",
                    value=f"{len(experiments_df):,}",
                    icon="science",
                    color=COLORS["primary"],
                ),
            ]
        )

        # Dataset info
        with ui.column().classes("mb-6 gap-1"):
            if status.get("dataset_hash"):
                ui.label(f"Dataset hash: {status.get('dataset_hash')}").classes(
                    "text-sm font-mono"
                ).style(f"color: {THEME['text_muted']};")
            if status.get("date_range"):
                ui.label(f"Date range: {status.get('date_range')}").classes("text-sm").style(
                    f"color: {THEME['text_muted']};"
                )

        divider()

        # Latest Experiment
        ui.label("Latest Experiment").classes("text-xl font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        if experiments_df.is_empty():
            with ui.column().classes("kpi-card p-6"):
                ui.label("No experiments found").style(f"color: {THEME['text_secondary']};")
                ui.label("Run: doppler run -- uv run nseml-backtest").classes(
                    "text-sm font-mono mt-2"
                ).style(f"color: {THEME['text_muted']};")
        else:
            latest = experiments_df.row(0, named=True)
            ret_val = float(latest.get("total_return_pct", 0))
            kpi_grid(
                [
                    dict(
                        title="Exp ID",
                        value=str(latest.get("exp_id", "-"))[:12],
                        icon="tag",
                        color=COLORS["gray"],
                    ),
                    dict(
                        title="Return",
                        value=f"{ret_val:.2f}%",
                        icon="attach_money",
                        color=COLORS["success"] if ret_val > 0 else COLORS["error"],
                    ),
                    dict(
                        title="Win Rate",
                        value=f"{float(latest.get('win_rate_pct', 0)):.1f}%",
                        icon="target",
                        color=COLORS["info"],
                    ),
                    dict(
                        title="Trades",
                        value=f"{int(latest.get('total_trades', 0)):,}",
                        icon="bar_chart",
                        color=COLORS["warning"],
                    ),
                ]
            )

        divider()

        # Navigation Cards — grouped by category for better visual hierarchy
        ui.label("Core Analysis").classes("text-xl font-semibold mb-4").style(
            f"color: {THEME['text_primary']};"
        )

        with ui.grid(columns=3).classes("w-full gap-4 mb-6"):
            nav_card(
                "Backtest Results",
                "Analyze stored 2LYNCH backtests from DuckDB",
                "bar_chart",
                "/backtest",
                COLORS["success"],
            )
            nav_card(
                "Trade Analytics",
                "Inspect trade distributions and exit behavior",
                "analytics",
                "/trade_analytics",
                COLORS["info"],
            )
            nav_card(
                "Compare Experiments",
                "Compare multiple experiment runs side-by-side",
                "compare_arrows",
                "/compare",
                COLORS["primary"],
            )

        ui.label("Research Tools").classes("text-lg font-semibold mb-3 mt-2").style(
            f"color: {THEME['text_secondary']};"
        )

        with ui.grid(columns=3).classes("w-full gap-4 mb-6"):
            nav_card(
                "Strategy Analysis",
                "Analyze parameter sensitivity across runs",
                "tune",
                "/strategy",
                COLORS["info"],
            )
            nav_card(
                "Momentum Scans",
                "View 4% + 2LYNCH scan results and pass rates",
                "radar",
                "/scans",
                COLORS["warning"],
            )
            nav_card(
                "Data Quality",
                "Validate data integrity and completeness",
                "verified",
                "/data_quality",
                COLORS["success"],
            )
            nav_card(
                "Market Monitor",
                "Track Stockbee-style market regime and breadth",
                "monitoring",
                "/market_monitor",
                COLORS["info"],
            )

        ui.label("Operations").classes("text-lg font-semibold mb-3 mt-2").style(
            f"color: {THEME['text_secondary']};"
        )

        with ui.grid(columns=4).classes("w-full gap-4 mb-6"):
            nav_card(
                "Walk Forward",
                "Review validation folds and rerun promotion-gate checks",
                "view_week",
                "/walk_forward",
                COLORS["primary"],
            )
            nav_card(
                "Paper Ledger",
                "Track paper trading sessions and execution state",
                "receipt_long",
                "/paper_ledger",
                COLORS["warning"],
            )
            nav_card(
                "Pipeline Status",
                "Monitor job execution and status",
                "engineering",
                "/pipeline",
                COLORS["gray"],
            )
            nav_card(
                "Daily Summary",
                "Daily market overview and statistics",
                "today",
                "/daily_summary",
                COLORS["info"],
            )

        divider()

        # Quick Start Commands
        with ui.expansion("Quick Start Commands", icon="terminal").classes("w-full"):
            ui.label("Run these commands in your terminal:").classes("mb-3").style(
                f"color: {THEME['text_secondary']};"
            )
            commands = [
                "uv sync",
                "doppler run -- docker compose up -d",
                "doppler run -- uv run nseml-backtest --universe-size 500 --start-year 2015 --end-year 2025",
                "doppler run -- uv run nseml-dashboard",
            ]
            for cmd in commands:
                with ui.row().classes("w-full items-center gap-2 mb-2"):
                    ui.label("$").classes("font-mono").style(f"color: {COLORS['success']};")
                    ui.label(cmd).classes("flex-grow font-mono text-sm px-3 py-2 rounded").style(
                        f"background: {THEME['surface_hover']}; border: 1px solid {THEME['surface_border']}; color: {THEME['text_primary']}; border-radius: 6px;"
                    )

        # Footer
        divider()
        with (
            ui.row()
            .classes("w-full justify-between items-center text-sm")
            .style(f"color: {THEME['text_muted']};")
        ):
            ui.label("Python 3.14 | NiceGUI | DuckDB")
            ui.label("NSE Momentum Lab v0.1.0")

        # Background refresh: fetch full status after page renders
        async def refresh_status():
            """Refresh status in background after initial render."""
            _ = await aget_db_status()  # Populates cache for next interaction
            # Update happens via cache, user sees fresh data on next interaction
            # or we could force a refresh of specific elements

        ui.timer(0.5, refresh_status, once=True)
