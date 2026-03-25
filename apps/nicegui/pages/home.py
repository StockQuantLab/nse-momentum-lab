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
    kpi_section,
    SPACE_SECTION,
    SPACE_GRID_DEFAULT,
    SPACE_MD,
    SPACE_GROUP_TIGHT,
    TYPE_BODY_LG,
    TYPE_PRESET_PAGE_HEADER,
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
        ui.label("NSE Momentum Lab").classes(TYPE_PRESET_PAGE_HEADER).style(
            f"color: {theme_text_primary()};"
        )
        ui.label("Local-first momentum research and backtest analysis").classes(
            f"{TYPE_BODY_LG} {SPACE_SECTION}"
        ).style(f"color: {theme_text_secondary()};")

        divider()

        # Primary CTA - most important action
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

        # Database Status KPIs - grouped for clarity with semantic spacing
        ui.label("Database Status").classes("text-xl font-semibold").style(
            f"color: {theme_text_primary()};"
        )

        kpi_section(
            "Data Overview",
            [
                dict(
                    title="Data Source",
                    value=status.get("data_source", "unknown").upper(),
                    icon="storage",
                    color=color_info(),
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

        # Dataset info - using semantic spacing constant
        with ui.column().classes(f"{SPACE_SECTION} gap-2"):
            if status.get("dataset_hash"):
                ui.label(f"Dataset hash: {status.get('dataset_hash')}").classes(
                    "text-sm font-mono"
                ).style(f"color: {theme_text_muted()};")
            if status.get("date_range"):
                ui.label(f"Date range: {status.get('date_range')}").classes("text-sm").style(
                    f"color: {theme_text_muted()};"
                )

        divider()

        # Latest Experiment - show as hero if available
        ui.label("Latest Experiment").classes("text-xl font-semibold mb-6").style(
            f"color: {theme_text_primary()};"
        )

        if experiments_df.is_empty():
            with ui.column().classes("kpi-card p-6"):
                ui.label("No experiments found").style(f"color: {theme_text_secondary()};")
                ui.label("Run: doppler run -- uv run nseml-backtest").classes(
                    "text-sm font-mono mt-2"
                ).style(f"color: {theme_text_muted()};")
        else:
            latest = experiments_df.row(0, named=True)
            ret_val = float(latest.get("total_return_pct", 0))
            # Use kpi_grid with hero_index=0 for first card as hero
            kpi_grid(
                [
                    dict(
                        title="Total Return",
                        value=f"{ret_val:.2f}%",
                        icon="attach_money",
                        color=color_success() if ret_val > 0 else color_error(),
                        is_hero=True,  # First card is hero
                        trend=ret_val,  # Shows trend indicator
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
                        value=str(latest.get("exp_id", "-"))[:12],
                        icon="tag",
                        color=color_gray(),
                    ),
                ],
                columns=4,
                hero_index=0,  # First card is hero
            )

        divider()

        # Navigation Cards — grouped by category with visual hierarchy
        # Use varied spacing for rhythm
        ui.label("Core Analysis").classes("text-xl font-semibold mb-4").style(
            f"color: {theme_text_primary()};"
        )

        # Asymmetric grid: 2 primary cards + 1 secondary creates visual interest
        with ui.row().classes(f"w-full gap-4 {SPACE_SECTION}"):
            # Primary backtest card - gets more visual weight
            with ui.column().classes("flex-2"):
                nav_card(
                    "Backtest Results",
                    "Analyze stored 2LYNCH backtests from DuckDB",
                    "bar_chart",
                    "/backtest",
                    color_success(),
                )
            # Secondary cards in a narrower column
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

        ui.label("Research Tools").classes("text-lg font-semibold mb-4").style(
            f"color: {theme_text_secondary()};"
        )

        # 2x2 grid instead of 3-column - breaks monotony, feels more grounded
        with ui.grid(columns=2).classes(f"w-full {SPACE_GRID_DEFAULT} {SPACE_SECTION}"):
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

        ui.label("Operations").classes("text-lg font-semibold mb-4").style(
            f"color: {theme_text_secondary()};"
        )

        # 4-column grid works here for compact utility items
        with ui.grid(columns=4).classes(f"w-full {SPACE_GRID_DEFAULT} {SPACE_SECTION}"):
            nav_card(
                "Walk Forward",
                "Review validation folds and rerun promotion-gate checks",
                "view_week",
                "/walk_forward",
                color_primary(),
            )
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

        divider()

        # Quick Start Commands - enhanced with semantic spacing
        with ui.expansion("Quick Start Commands", icon="terminal").classes("w-full"):
            ui.label("Run these commands in your terminal:").classes("mb-3").style(
                f"color: {theme_text_secondary()};"
            )
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

        ui.timer(0.5, refresh_status, once=True)
