"""Pipeline Status page - DuckDB operational monitoring and CLI commands."""

from __future__ import annotations

import sys
from pathlib import Path

_apps_root = Path(__file__).resolve().parent.parent
_project_root = _apps_root.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))
if str(_apps_root) not in sys.path:
    sys.path.insert(0, str(_apps_root))

from nicegui import ui

from apps.nicegui.components import (
    divider,
    kpi_grid,
    page_layout,
    paginated_table,
    SPACE_LG,
    theme_text_primary,
    theme_text_secondary,
    color_success,
    color_info,
    color_warning,
    color_gray,
)
from apps.nicegui.state import aget_db_status, get_backtest_db_ro, get_db


def _fmt_count(n: int | None) -> str:
    if n is None:
        return "0"
    return f"{int(n):,}"


async def pipeline_page() -> None:
    """Render the pipeline status page with DuckDB operational data."""
    with page_layout("Pipeline", "engineering"):
        status = await aget_db_status()

        # KPI cards
        experiments_count = int(status.get("experiments", 0) or 0)
        symbols_count = int(status.get("symbols", 0) or 0)
        date_range = status.get("date_range", "-")

        kpi_grid(
            [
                dict(
                    title="Market Tables",
                    value=status.get("market_tables", "-"),
                    subtitle="Tables in market.duckdb",
                    icon="table_chart",
                    color=color_info(),
                ),
                dict(
                    title="Backtest Experiments",
                    value=str(experiments_count),
                    subtitle="Completed experiments",
                    icon="science",
                    color=color_success(),
                ),
                dict(
                    title="Symbols Covered",
                    value=_fmt_count(symbols_count),
                    subtitle="Distinct symbols",
                    icon="bar_chart",
                    color=color_warning(),
                ),
                dict(
                    title="Date Range",
                    value=date_range,
                    subtitle="Data coverage",
                    icon="date_range",
                    color=color_gray(),
                ),
            ],
            columns=4,
        )

        divider()

        # Runtime tables status
        ui.label("Runtime Table Status").classes(f"text-lg font-semibold {SPACE_LG}").style(
            f"color: {theme_text_primary()};"
        )

        table_rows = []

        # Market DB tables
        db = get_db()
        market_tables = [
            ("feat_daily_core", "Daily features"),
            ("feat_daily", "Daily features view"),
            ("market_monitor_daily", "Market breadth"),
        ]
        for table_name, desc in market_tables:
            rows = db.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
            count = rows[0]["cnt"] if rows else 0
            sym_rows = db.execute(f"SELECT COUNT(DISTINCT symbol) as cnt FROM {table_name}")
            sym_count = sym_rows[0]["cnt"] if sym_rows else 0
            date_rows = db.execute(
                f"SELECT MIN(trading_date) as mn, MAX(trading_date) as mx FROM {table_name}"
            )
            dr = date_rows[0] if date_rows else {}
            table_rows.append(
                {
                    "table": table_name,
                    "description": desc,
                    "rows": _fmt_count(count),
                    "symbols": _fmt_count(sym_count),
                    "date_range": f"{dr.get('mn', '-')} → {dr.get('mx', '-')}",
                    "status": "READY" if count > 0 else "EMPTY",
                }
            )

        # Backtest DB tables
        bt_db = get_backtest_db_ro()
        bt_tables = [
            ("bt_experiment", "Backtest experiments"),
            ("bt_trade", "Backtest trades"),
            ("bt_yearly_metric", "Yearly metrics"),
            ("bt_execution_diagnostic", "Execution diagnostics"),
        ]
        for table_name, desc in bt_tables:
            rows = bt_db.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
            count = rows[0]["cnt"] if rows else 0
            table_rows.append(
                {
                    "table": table_name,
                    "description": desc,
                    "rows": _fmt_count(count),
                    "symbols": "-",
                    "date_range": "-",
                    "status": "READY" if count > 0 else "EMPTY",
                }
            )

        cols = [
            {"name": "table", "label": "Table", "field": "table", "sortable": True},
            {
                "name": "description",
                "label": "Description",
                "field": "description",
                "sortable": True,
            },
            {"name": "rows", "label": "Rows", "field": "rows", "sortable": True},
            {
                "name": "symbols",
                "label": "Symbols",
                "field": "symbols",
                "sortable": True,
            },
            {
                "name": "date_range",
                "label": "Date Range",
                "field": "date_range",
                "sortable": True,
            },
            {"name": "status", "label": "Status", "field": "status", "sortable": True},
        ]
        paginated_table(table_rows, cols)

        divider()

        # CLI Commands
        with ui.expansion("Operator Commands", value=False).classes("w-full"):
            commands = [
                ("Ingest today's candles", "doppler run -- uv run nseml-kite-ingest --today"),
                (
                    "Build missing features",
                    "doppler run -- uv run nseml-build-features --missing",
                ),
                (
                    "Update market monitor",
                    "doppler run -- uv run nseml-market-monitor --incremental --since LAST_DATE",
                ),
                (
                    "Run DQ scan",
                    "doppler run -- uv run nseml-hygiene --refresh --full",
                ),
                ("Verify DB health", "doppler run -- uv run nseml-db-verify"),
                (
                    "Run backtest",
                    "doppler run -- uv run nseml-backtest --preset BREAKOUT_4PCT",
                ),
                (
                    "EOD pipeline",
                    "doppler run -- uv run nseml-eod",
                ),
            ]
            for label, cmd in commands:
                with ui.row().classes("items-center gap-2 w-full mb-2"):
                    ui.label(label).classes("text-sm w-48").style(
                        f"color: {theme_text_secondary()};"
                    )
                    ui.code(cmd, language="bash").classes("flex-1 text-xs")
                    ui.button(
                        icon="content_copy",
                        on_click=lambda c=cmd: ui.run_javascript(
                            f"navigator.clipboard.writeText('{c}');"
                        ),
                    ).props("flat dense size=sm")
