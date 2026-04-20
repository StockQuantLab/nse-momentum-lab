"""Data Quality page - Symbol health, coverage analytics, and storage stats."""

from __future__ import annotations

import sys
from datetime import date as _date, timedelta
from pathlib import Path
from typing import Any

_apps_root = Path(__file__).resolve().parent.parent
_project_root = _apps_root.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))
if str(_apps_root) not in sys.path:
    sys.path.insert(0, str(_apps_root))

import plotly.graph_objects as go
from nicegui import ui

from apps.nicegui.state import (
    aget_data_quality_metrics,
    adelete_experiments_write,
    aget_available_symbols,
    aget_date_coverage,
    aget_freshness_buckets,
    aget_price_anomalies,
    aget_symbol_coverage,
    aget_symbol_gaps,
    aget_symbol_profile,
    aget_top_gaps,
    aget_universe_timeline,
    build_experiment_options,
    get_experiments,
    get_ingestion_log,
    get_ingestion_status,
    trigger_missing_ingestion,
)
from apps.nicegui.components import (
    apply_chart_theme,
    color_error,
    color_gray,
    color_info,
    color_primary,
    color_success,
    color_warning,
    divider,
    export_button,
    kpi_grid,
    page_layout,
    paginated_table,
    safe_timer,
    SPACE_LG,
    SPACE_SM,
    SPACE_XS,
    theme_surface_border,
    theme_text_muted,
    theme_text_primary,
    theme_text_secondary,
)


# ---------------------------------------------------------------------------
# Debounce helper
# ---------------------------------------------------------------------------

_debounce_timers: dict[str, Any] = {}


def _debounce(key: str, fn, delay: float = 0.3) -> None:
    """Cancel any pending timer for *key* and schedule *fn* after *delay*."""
    if key in _debounce_timers:
        try:
            _debounce_timers[key].deactivate()
        except Exception:
            pass
    _debounce_timers[key] = safe_timer(delay, fn, once=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fmt_bytes(n: int) -> str:
    if n >= 1 << 30:
        return f"{n / (1 << 30):.1f} GB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.0f} MB"
    return f"{n / 1e3:.0f} KB"


def _copy_to_clipboard(text: str) -> None:
    """Copy *text* to the system clipboard via JavaScript."""
    escaped = text.replace("\\", "\\\\").replace("'", "\\'")
    ui.run_javascript(f"navigator.clipboard.writeText('{escaped}')")
    ui.notify("Copied to clipboard", type="positive")


# ---------------------------------------------------------------------------
# Overview tab (existing content)
# ---------------------------------------------------------------------------


def _render_overview_kpis(m: dict) -> None:
    covered = m.get("covered_tradeable_count", m.get("active_count", 0))
    missing = m.get("missing_tradeable_count", 0)
    dead = m.get("dead_count", 0)
    total = m.get("total_parquet_symbols", 0)
    coverage = m.get("coverage_pct", 0)
    local_coverage = m.get("local_coverage_pct", 0)

    kpi_grid(
        [
            dict(
                title="Tradeable (NSE)",
                value=f"{m.get('tradeable_count', 0):,}",
                subtitle="Symbols in Kite instrument master (EQ segment)",
                icon="verified",
                color=color_info(),
            ),
            dict(
                title="Covered Tradeable",
                value=f"{covered:,}",
                subtitle=f"{coverage}% of tradeable universe",
                icon="check_circle",
                color=color_success(),
            ),
            dict(
                title="Missing Tradeable",
                value=f"{missing:,}",
                subtitle=f"{100 - coverage:.1f}% of tradeable universe" if coverage else "",
                icon="trending_down",
                color=color_warning() if missing > 0 else color_gray(),
            ),
            dict(
                title="Dead Symbols",
                value=f"{dead:,}",
                subtitle=f"Not in current master — {local_coverage:.1f}% of local valid"
                if total
                else "",
                icon="dangerous",
                color=color_error() if dead > 0 else color_gray(),
            ),
        ]
    )


def _render_parquet_kpis(m: dict) -> None:
    kpi_grid(
        [
            dict(
                title="Daily Symbols",
                value=f"{m.get('daily_symbols', 0):,}",
                subtitle=f"{_fmt_bytes(m.get('daily_size_bytes', 0))} on disk",
                icon="today",
                color=color_info(),
            ),
            dict(
                title="5-Min Symbols",
                value=f"{m.get('five_min_symbols', 0):,}",
                subtitle=f"{_fmt_bytes(m.get('five_min_size_bytes', 0))} on disk",
                icon="access_time",
                color=color_warning(),
            ),
            dict(
                title="Parquet Total",
                value=_fmt_bytes(m.get("total_parquet_bytes", 0)),
                icon="folder",
                color=color_gray(),
            ),
            dict(
                title="DuckDB Size",
                value=_fmt_bytes(m.get("duckdb_size_bytes", 0)),
                icon="database",
                color=color_gray(),
            ),
        ]
    )
    date_range = m.get("date_range", "-")
    if date_range and date_range != "-":
        ui.label(f"Date Range: {date_range}").classes("text-sm").style(
            f"color: {theme_text_muted()};"
        )


def _render_timestamp_alignment_section(m: dict) -> None:
    issue_count = int(m.get("five_min_timestamp_issue_count", 0))
    sample: list[dict] = m.get("five_min_timestamp_issue_sample", [])
    ui.label("5-Min Timestamp Alignment").classes(f"text-lg font-semibold {SPACE_SM}").style(
        f"color: {theme_text_primary()};"
    )
    if issue_count == 0:
        ui.label("All 5-minute parquet files start at 09:15 IST.").classes("text-sm").style(
            f"color: {theme_text_muted()};"
        )
        return

    ui.label(
        f"{issue_count:,} 5-minute parquet files do not start at 09:15 IST. "
        "Legacy UTC-naive files start at 03:45 and need a timestamp repair."
    ).classes("text-sm").style(f"color: {theme_text_muted()};")
    rows = sample or [{"symbol": "-", "year": "-", "first_candle_time": "-", "status": "-"}]
    columns = [
        {
            "name": "symbol",
            "label": "Symbol",
            "field": "symbol",
            "align": "left",
            "classes": "font-mono text-sm",
        },
        {
            "name": "year",
            "label": "Year",
            "field": "year",
            "align": "left",
            "classes": "font-mono text-sm",
        },
        {
            "name": "first_candle_time",
            "label": "First Candle",
            "field": "first_candle_time",
            "align": "left",
            "classes": "font-mono text-sm",
        },
        {
            "name": "status",
            "label": "Status",
            "field": "status",
            "align": "left",
            "classes": "text-sm",
        },
    ]
    paginated_table(
        rows=rows,
        columns=columns,
        row_key="symbol",
        page_size=10,
        aria_label="Timestamp alignment issues",
    )


def _render_coverage_gap_table(m: dict) -> None:
    missing: list[str] = m.get("missing_tradeable_sample", [])
    missing_count = int(m.get("missing_tradeable_count", 0))
    ui.label(
        f"These symbols are in the current NSE master but are not present in local parquet. "
        f"Showing {len(missing):,} of {missing_count:,} missing tradeable symbols."
    ).classes("text-sm").style(f"color: {theme_text_muted()};")
    if not missing:
        ui.label("No missing tradeable symbols detected.").classes("text-sm").style(
            f"color: {theme_text_muted()};"
        )
        return
    rows = [{"symbol": symbol, "status": "Missing from local parquet"} for symbol in missing]
    columns = [
        {
            "name": "symbol",
            "label": "Symbol",
            "field": "symbol",
            "align": "left",
            "classes": "font-mono text-sm",
        },
        {
            "name": "status",
            "label": "Status",
            "field": "status",
            "align": "left",
            "classes": "text-sm",
        },
    ]
    paginated_table(rows=rows, columns=columns, row_key="symbol", page_size=25)


def _render_tables_section(m: dict) -> None:
    tables: dict[str, int] = m.get("tables", {})
    if not tables:
        return
    ui.label("DuckDB Tables").classes(f"text-lg font-semibold {SPACE_SM}").style(
        f"color: {theme_text_primary()};"
    )
    rows = [
        {"table": name, "rows": count}
        for name, count in sorted(tables.items(), key=lambda x: -x[1])
    ]
    columns = [
        {
            "name": "table",
            "label": "Table",
            "field": "table",
            "align": "left",
            "classes": "font-mono text-sm",
        },
        {
            "name": "rows",
            "label": "Row Count",
            "field": "rows",
            "align": "right",
            "classes": "font-mono text-sm tabular-nums",
            "format": "val => val == null ? '-' : val.toLocaleString()",
        },
    ]
    paginated_table(
        rows=rows, columns=columns, row_key="table", page_size=20, aria_label="DuckDB tables"
    )


def _render_actions_section(m: dict) -> None:
    with ui.column().classes(f"kpi-card {SPACE_LG}"):
        ui.label("Data Hygiene CLI").classes("text-sm font-medium").style(
            f"color: {theme_text_secondary()};"
        )
        for cmd, desc in [
            ("doppler run -- uv run nseml-hygiene --dry-run", "Preview dead symbols"),
            ("doppler run -- uv run nseml-hygiene --list-dead", "List dead symbol names"),
            ("doppler run -- uv run nseml-hygiene --purge --confirm", "Execute purge"),
            ("doppler run -- uv run nseml-build-features", "Rebuild feat_daily after purge"),
        ]:
            with ui.row().classes("items-center gap-3"):
                ui.label("$").classes("text-xs font-mono").style(f"color: {color_success()};")
                ui.label(cmd).classes("text-sm font-mono flex-1").style(
                    f"color: {theme_text_primary()};"
                )
                ui.label(f"# {desc}").classes("text-xs").style(f"color: {theme_text_muted()};")
                ui.button(icon="content_copy").props(
                    'flat round dense size=sm aria-label="Copy command to clipboard"'
                ).tooltip("Copy command").on_click(lambda c=cmd: _copy_to_clipboard(c))
        report_path = m.get("latest_hygiene_report")
        if report_path:
            divider()
            ui.label(f"Latest report: {Path(report_path).name}").classes(
                f"text-xs font-mono {SPACE_XS}"
            ).style(f"color: {theme_text_muted()};")


def _render_data_sources() -> None:
    with ui.expansion("Data Sources", icon="info").classes("w-full"):
        for text in [
            "Primary: Zerodha Kite historical data (adjusted)",
            "Format: Parquet files with OHLCV data",
            "Frequencies: Daily and 5-minute candles",
            "Source of truth: data/raw/kite/instruments/NSE.csv (segment=NSE, type=EQ)",
            "Dead symbols: parquet dirs not in current instrument master",
        ]:
            ui.label(text).classes(SPACE_SM).style(f"color: {theme_text_secondary()};")


def _render_ingestion_section(metrics: dict) -> None:
    missing_count = int(metrics.get("missing_tradeable_count", 0))
    if missing_count == 0:
        ui.label("Coverage complete — no missing tradeable symbols.").classes("text-sm").style(
            f"color: {theme_text_muted()};"
        )
        return

    ui.label(
        f"{missing_count:,} symbols in the Kite master have no local parquet. "
        "Ingestion writes parquet only — no DuckDB changes during the run."
    ).classes("text-sm").style(f"color: {theme_text_muted()};")

    with ui.column().classes(f"kpi-card w-full {SPACE_LG}"):
        with ui.row().classes(f"gap-4 items-end flex-wrap {SPACE_SM}"):
            _default_end = _date.today()
            _default_start = _default_end - timedelta(days=90)
            start_inp = ui.input("From date", value=str(_default_start)).props(
                "dense outlined clearable"
            )
            end_inp = ui.input("To date", value=str(_default_end)).props("dense outlined clearable")
            daily_cb = ui.checkbox("Daily", value=True)
            fivemin_cb = ui.checkbox("5-min", value=True)

        with ui.row().classes(f"gap-3 items-center {SPACE_SM}"):
            run_btn = ui.button("Run Ingestion", icon="cloud_download").props(
                "outlined color=primary"
            )
            status_lbl = (
                ui.label(f"Status: {get_ingestion_status()}")
                .classes("text-sm font-mono")
                .style(f"color: {theme_text_secondary()};")
            )

        ui.label(
            "After ingestion completes, rebuild features: "
            "doppler run -- uv run nseml-build-features"
        ).classes("text-xs font-mono").style(f"color: {theme_text_muted()};")

        log_view = ui.log(max_lines=300).classes("w-full").style("height: 280px;")
        seen = [0]
        _poll_timers: dict[str, Any] = {}

        def _poll_log() -> None:
            current_status = get_ingestion_status()
            status_lbl.set_text(f"Status: {current_status}")
            run_btn.set_enabled(current_status != "running")

            if current_status == "running":
                lines = get_ingestion_log()
                if len(lines) < seen[0]:
                    seen[0] = 0
                new_lines = lines[seen[0] :]
                for ln in new_lines:
                    log_view.push(ln)
                seen[0] = len(lines)
                # Fast polling during active ingestion
                if "active" not in _poll_timers:
                    if "idle" in _poll_timers:
                        try:
                            _poll_timers["idle"].deactivate()
                        except Exception:
                            pass
                        del _poll_timers["idle"]
                    _poll_timers["active"] = safe_timer(1.0, _poll_log, once=False)
            else:
                # Slow polling when idle
                if "idle" not in _poll_timers:
                    if "active" in _poll_timers:
                        try:
                            _poll_timers["active"].deactivate()
                        except Exception:
                            pass
                        del _poll_timers["active"]
                    _poll_timers["idle"] = safe_timer(3.0, _poll_log, once=False)

        _poll_timers["idle"] = safe_timer(3.0, _poll_log, once=False)

        def on_run_ingestion() -> None:
            try:
                sd = _date.fromisoformat(start_inp.value.strip())
                ed = _date.fromisoformat(end_inp.value.strip())
            except ValueError as ve:
                ui.notify(f"Invalid date: {ve}", type="negative")
                return
            if sd > ed:
                ui.notify("From date must be before To date", type="negative")
                return
            if not daily_cb.value and not fivemin_cb.value:
                ui.notify("Select at least one dataset (Daily or 5-min)", type="warning")
                return
            try:
                log_view.clear()
            except Exception:
                pass
            seen[0] = 0
            err = trigger_missing_ingestion(
                start_date=sd,
                end_date=ed,
                run_daily=daily_cb.value,
                run_5min=fivemin_cb.value,
            )
            if err:
                ui.notify(err, type="negative")
            else:
                run_btn.set_enabled(False)

        run_btn.on_click(on_run_ingestion)
        start_inp.on("keydown.enter", on_run_ingestion)
        end_inp.on("keydown.enter", on_run_ingestion)


def _render_backtest_management() -> None:
    ui.label(
        "Delete stale or incorrect experiments from backtest.duckdb. "
        "Both the write DB and the dashboard copy are updated immediately."
    ).classes("text-sm").style(f"color: {theme_text_muted()};")

    @ui.refreshable
    def _experiment_list() -> None:
        exps = get_experiments(force_refresh=True)
        if exps.is_empty():
            ui.label("No experiments found.").classes("text-sm mt-2").style(
                f"color: {theme_text_muted()};"
            )
            return
        options = build_experiment_options(exps)
        exp_ids_all = exps["exp_id"].to_list()

        with ui.column().classes(f"w-full {SPACE_SM}"):
            for label, exp_id in options.items():
                with (
                    ui.row()
                    .classes("items-center w-full justify-between")
                    .style(
                        f"border: 1px solid {theme_surface_border()}; border-radius:6px; padding:6px 10px;"
                    )
                ):
                    ui.label(label).classes("text-xs font-mono flex-1").style(
                        f"color: {theme_text_secondary()};"
                    )

                    with ui.dialog() as _single_dlg, ui.card():
                        ui.label("Delete experiment?").classes("text-base font-semibold")
                        ui.label(label).classes("text-sm font-mono").style(
                            f"color: {theme_text_secondary()};"
                        )
                        ui.label("This cannot be undone.").classes("text-sm").style(
                            f"color: {theme_text_muted()};"
                        )
                        with ui.row().classes("gap-3 mt-4 justify-end"):
                            ui.button("Cancel", on_click=_single_dlg.close).props("flat")

                            async def _confirm_delete(
                                eid: str = exp_id,
                                lb: str = label,
                            ) -> None:
                                _single_dlg.close()
                                _, err = await adelete_experiments_write([eid])
                                if err:
                                    ui.notify(f"Error: {err}", type="negative")
                                else:
                                    ui.notify(f"Deleted {lb[:40]}", type="positive")
                                    _experiment_list.refresh()

                            ui.button("Delete", color="negative", on_click=_confirm_delete)

                    ui.button(icon="delete", on_click=_single_dlg.open).props(
                        'flat round dense size=sm color=negative aria-label="Delete this experiment"'
                    ).tooltip("Delete this experiment")

            if len(exp_ids_all) > 1:
                with ui.dialog() as _confirm_dlg, ui.card():
                    ui.label(f"Delete all {len(exp_ids_all)} experiments?").classes(
                        "text-base font-semibold"
                    )
                    ui.label("This cannot be undone.").classes("text-sm").style(
                        f"color: {theme_text_muted()};"
                    )
                    with ui.row().classes("gap-3 mt-4 justify-end"):
                        ui.button("Cancel", on_click=_confirm_dlg.close).props("flat")

                        async def _delete_all() -> None:
                            _confirm_dlg.close()
                            count, err = await adelete_experiments_write(exp_ids_all)
                            if err:
                                ui.notify(f"Error: {err}", type="negative")
                            else:
                                ui.notify(f"Deleted {count} experiments", type="positive")
                                _experiment_list.refresh()

                        ui.button(
                            f"Delete All {len(exp_ids_all)}", color="negative", on_click=_delete_all
                        )

                ui.button(
                    f"Delete All {len(exp_ids_all)} Experiments",
                    icon="delete_sweep",
                    on_click=_confirm_dlg.open,
                ).props("outline color=negative").classes(f"{SPACE_SM}")

    _experiment_list()


async def _render_overview_tab(metrics: dict) -> None:
    """Render the Overview tab — grouped into collapsible sections."""
    with ui.expansion("Symbol Coverage", icon="verified").classes("w-full").props("default-opened"):
        ui.label("Tradeable symbol coverage vs NSE instrument master.").classes(
            "text-xs mb-2"
        ).style(f"color: {theme_text_muted()};")
        _render_overview_kpis(metrics)
        _render_coverage_gap_table(metrics)

    with ui.expansion("Storage & Schema", icon="storage").classes("w-full"):
        ui.label("Parquet file storage and DuckDB table statistics.").classes("text-xs mb-2").style(
            f"color: {theme_text_muted()};"
        )
        _render_parquet_kpis(metrics)
        _render_timestamp_alignment_section(metrics)
        _render_tables_section(metrics)

    with ui.expansion("Operations", icon="engineering").classes("w-full"):
        ui.label("Data hygiene, ingestion, and backtest experiment management.").classes(
            "text-xs mb-2"
        ).style(f"color: {theme_text_muted()};")
        _render_actions_section(metrics)
        divider()
        ui.label("Missing Symbol Ingestion").classes(f"text-lg font-semibold {SPACE_SM}").style(
            f"color: {theme_text_primary()};"
        )
        _render_ingestion_section(metrics)
        divider()
        ui.label("Backtest Management").classes(f"text-lg font-semibold {SPACE_SM}").style(
            f"color: {theme_text_primary()};"
        )
        _render_backtest_management()

    _render_data_sources()


# ---------------------------------------------------------------------------
# Universe Timeline tab
# ---------------------------------------------------------------------------


async def _render_universe_tab() -> None:
    timeline = await aget_universe_timeline()
    if not timeline:
        ui.label("No data available in v_daily.").style(f"color: {theme_text_muted()};")
        return

    earliest = timeline[0]
    latest = timeline[-1]
    peak = max(timeline, key=lambda r: r["symbol_count"])

    kpi_grid(
        [
            dict(
                title=f"Earliest ({earliest['year']})",
                value=f"{earliest['symbol_count']:,}",
                icon="history",
                color=color_gray(),
            ),
            dict(
                title=f"Latest ({latest['year']})",
                value=f"{latest['symbol_count']:,}",
                icon="today",
                color=color_info(),
            ),
            dict(
                title=f"Peak ({peak['year']})",
                value=f"{peak['symbol_count']:,}",
                icon="trending_up",
                color=color_success(),
            ),
            dict(
                title="Total Rows",
                value=f"{sum(r['total_rows'] for r in timeline):,}",
                icon="storage",
                color=color_gray(),
            ),
        ]
    )

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=[r["year"] for r in timeline],
            y=[r["symbol_count"] for r in timeline],
            text=[f"{r['symbol_count']:,}" for r in timeline],
            textposition="outside",
            marker_color=color_primary(),
        )
    )
    fig.update_layout(
        title="Distinct Symbols Per Year",
        xaxis_title="Year",
        yaxis_title="Symbol Count",
        xaxis=dict(dtick=1),
    )
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full h-80")


# ---------------------------------------------------------------------------
# Coverage tab
# ---------------------------------------------------------------------------


async def _render_coverage_tab() -> None:
    coverage_data = await aget_symbol_coverage()
    date_cov = await aget_date_coverage()

    if not coverage_data:
        ui.label("No data available.").style(f"color: {theme_text_muted()};")
        return

    avg_cov = round(sum(r["coverage_pct"] for r in coverage_data) / len(coverage_data), 1)
    below_90 = sum(1 for r in coverage_data if r["coverage_pct"] < 90)
    below_50 = sum(1 for r in coverage_data if r["coverage_pct"] < 50)

    kpi_grid(
        [
            dict(
                title="Total Symbols",
                value=f"{len(coverage_data):,}",
                icon="analytics",
                color=color_info(),
            ),
            dict(
                title="Avg Coverage",
                value=f"{avg_cov}%",
                icon="pie_chart",
                color=color_success() if avg_cov >= 95 else color_warning(),
            ),
            dict(
                title="Below 90%",
                value=f"{below_90:,}",
                icon="warning",
                color=color_warning() if below_90 > 0 else color_gray(),
            ),
            dict(
                title="Below 50%",
                value=f"{below_50:,}",
                icon="error",
                color=color_error() if below_50 > 0 else color_gray(),
            ),
        ]
    )

    # -- Per-symbol table with search and filter --
    ui.label("Per-Symbol Coverage").classes(f"text-lg font-semibold {SPACE_SM}").style(
        f"color: {theme_text_primary()};"
    )

    search_input = (
        ui.input("Search symbol", placeholder="e.g. RELIANCE")
        .props("dense outlined clearable")
        .classes("w-48")
    )
    bucket_select = (
        ui.select(
            {"all": "All", "gt95": "> 95%", "90_95": "90-95%", "lt90": "< 90%", "lt50": "< 50%"},
            value="all",
            label="Coverage Filter",
        )
        .props("dense outlined")
        .classes("w-40")
    )

    @ui.refreshable
    def _coverage_table() -> None:
        filtered = coverage_data
        search = (search_input.value or "").strip().upper()
        if search:
            filtered = [r for r in filtered if search in r["symbol"]]
        bkt = bucket_select.value
        if bkt == "gt95":
            filtered = [r for r in filtered if r["coverage_pct"] > 95]
        elif bkt == "90_95":
            filtered = [r for r in filtered if 90 <= r["coverage_pct"] <= 95]
        elif bkt == "lt90":
            filtered = [r for r in filtered if r["coverage_pct"] < 90]
        elif bkt == "lt50":
            filtered = [r for r in filtered if r["coverage_pct"] < 50]
        # Sort worst first
        filtered = sorted(filtered, key=lambda r: r["coverage_pct"])

        columns = [
            {
                "name": "symbol",
                "label": "Symbol",
                "field": "symbol",
                "align": "left",
                "classes": "font-mono text-sm",
            },
            {"name": "first_date", "label": "First Date", "field": "first_date", "align": "left"},
            {"name": "last_date", "label": "Last Date", "field": "last_date", "align": "left"},
            {
                "name": "distinct_days",
                "label": "Days",
                "field": "distinct_days",
                "align": "right",
                "classes": "tabular-nums",
            },
            {
                "name": "coverage_pct",
                "label": "Coverage %",
                "field": "coverage_pct",
                "align": "right",
                "classes": "tabular-nums",
                "format": "val => val == null ? '-' : val.toFixed(1)",
            },
            {
                "name": "gap_estimate",
                "label": "Est. Gaps",
                "field": "gap_estimate",
                "align": "right",
                "classes": "tabular-nums",
            },
        ]
        rows = filtered
        paginated_table(
            rows=rows,
            columns=columns,
            row_key="symbol",
            page_size=25,
            aria_label="Per-symbol coverage data",
        )
        export_button(rows, filename="symbol_coverage.csv", label="Export CSV")

    _coverage_table()
    search_input.on("update:model-value", lambda: _debounce("coverage", _coverage_table.refresh))
    bucket_select.on("update:model-value", lambda: _debounce("coverage", _coverage_table.refresh))

    divider()

    # -- Date-level coverage line chart --
    if date_cov:
        ui.label("Symbols Reporting Per Date").classes(f"text-lg font-semibold {SPACE_SM}").style(
            f"color: {theme_text_primary()};"
        )
        dates = [r["trading_date"] for r in date_cov]
        counts = [r["symbol_count"] for r in date_cov]
        median_count = sorted(counts)[len(counts) // 2] if counts else 0

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=counts,
                mode="lines",
                name="Symbols",
                line=dict(color=color_primary(), width=1),
            )
        )
        fig.add_hline(
            y=median_count,
            line_dash="dash",
            annotation_text=f"Median: {median_count:,}",
            line_color=color_gray(),
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Symbol Count",
            title="Daily Symbol Coverage Over Time",
        )
        apply_chart_theme(fig)
        ui.plotly(fig).classes("w-full h-80")


# ---------------------------------------------------------------------------
# Gaps tab
# ---------------------------------------------------------------------------


async def _render_gaps_tab() -> None:
    gaps = await aget_top_gaps()
    if not gaps:
        ui.label("No gaps > 5 calendar days detected.").style(f"color: {theme_text_muted()};")
        return

    unique_syms = len({g["symbol"] for g in gaps})
    largest = gaps[0]["gap_days"] if gaps else 0

    kpi_grid(
        [
            dict(
                title="Total Gaps",
                value=f"{len(gaps):,}",
                icon="broken_image",
                color=color_warning(),
            ),
            dict(
                title="Largest Gap", value=f"{largest} days", icon="event_busy", color=color_error()
            ),
            dict(
                title="Affected Symbols",
                value=f"{unique_syms:,}",
                icon="people",
                color=color_info(),
            ),
        ]
    )

    ui.label("Top Gaps (> 5 calendar days)").classes(f"text-lg font-semibold {SPACE_SM}").style(
        f"color: {theme_text_primary()};"
    )

    columns = [
        {
            "name": "symbol",
            "label": "Symbol",
            "field": "symbol",
            "align": "left",
            "classes": "font-mono text-sm",
        },
        {"name": "gap_start", "label": "Gap Start", "field": "gap_start", "align": "left"},
        {"name": "gap_end", "label": "Gap End", "field": "gap_end", "align": "left"},
        {
            "name": "gap_days",
            "label": "Calendar Days",
            "field": "gap_days",
            "align": "right",
            "classes": "tabular-nums font-semibold",
        },
    ]
    paginated_table(
        rows=gaps,
        columns=columns,
        row_key=None,
        page_size=25,
        aria_label="Top gaps by calendar days",
    )

    divider()

    # -- Symbol drill-down --
    ui.label("Symbol Gap Drill-Down").classes(f"text-lg font-semibold {SPACE_SM}").style(
        f"color: {theme_text_primary()};"
    )
    sym_input = (
        ui.input("Enter symbol", placeholder="e.g. RELIANCE")
        .props("dense outlined clearable")
        .classes("w-48")
    )

    @ui.refreshable
    async def _symbol_gap_detail() -> None:
        sym = (sym_input.value or "").strip().upper()
        if not sym:
            ui.label("Enter a symbol above to see its gaps.").classes("text-sm").style(
                f"color: {theme_text_muted()};"
            )
            return
        sym_gaps = await aget_symbol_gaps(sym)
        if not sym_gaps:
            ui.label(f"No gaps > 3 days found for {sym}.").classes("text-sm").style(
                f"color: {theme_text_muted()};"
            )
            return
        ui.label(f"{len(sym_gaps)} gap(s) for {sym}:").classes("text-sm font-semibold")
        cols = [
            {"name": "gap_start", "label": "Gap Start", "field": "gap_start", "align": "left"},
            {"name": "gap_end", "label": "Gap End", "field": "gap_end", "align": "left"},
            {
                "name": "gap_days",
                "label": "Calendar Days",
                "field": "gap_days",
                "align": "right",
                "classes": "tabular-nums",
            },
        ]
        paginated_table(
            rows=sym_gaps,
            columns=cols,
            row_key="gap_start",
            page_size=25,
            aria_label="Gap drill-down for selected symbol",
        )

    await _symbol_gap_detail()
    sym_input.on("update:model-value", lambda: _debounce("gap_drill", _symbol_gap_detail.refresh))


# ---------------------------------------------------------------------------
# Freshness tab
# ---------------------------------------------------------------------------


async def _render_freshness_tab() -> None:
    buckets = await aget_freshness_buckets()
    if not buckets:
        ui.label("No data available.").style(f"color: {theme_text_muted()};")
        return

    total = sum(b["count"] for b in buckets)
    colors_map = {
        "Fresh (<7d)": color_success(),
        "Recent (7-30d)": color_info(),
        "Stale (30-90d)": color_warning(),
        "Very Stale (>90d)": color_error(),
    }

    kpi_grid(
        [
            dict(
                title=b["bucket"],
                value=f"{b['count']:,}",
                subtitle=f"{round(b['count'] / max(total, 1) * 100, 1)}%",
                icon="schedule",
                color=colors_map.get(b["bucket"], color_gray()),
            )
            for b in buckets
        ]
    )

    fig = go.Figure(
        data=[
            go.Pie(
                labels=[b["bucket"] for b in buckets],
                values=[b["count"] for b in buckets],
                marker=dict(colors=[colors_map.get(b["bucket"], color_gray()) for b in buckets]),
                textinfo="label+value+percent",
                hole=0.4,
            )
        ]
    )
    fig.update_layout(title="Data Freshness Distribution")
    apply_chart_theme(fig)
    ui.plotly(fig).classes("w-full h-80")

    for b in buckets:
        symbols = b.get("symbols", [])
        with ui.expansion(f"{b['bucket']} — {b['count']:,} symbols", icon="list").classes("w-full"):
            if symbols:
                ui.label(", ".join(symbols[:50])).classes("text-xs font-mono").style(
                    f"color: {theme_text_secondary()};"
                )
                if b["count"] > 50:
                    ui.label(f"... and {b['count'] - 50} more").classes("text-xs").style(
                        f"color: {theme_text_muted()};"
                    )
            else:
                ui.label("No symbols in this bucket.").style(f"color: {theme_text_muted()};")


# ---------------------------------------------------------------------------
# Anomalies tab
# ---------------------------------------------------------------------------


async def _render_anomalies_tab() -> None:
    anomalies = await aget_price_anomalies()
    if not anomalies:
        ui.label("No price anomalies detected.").style(f"color: {theme_text_muted()};")
        return

    # Count by type
    by_type: dict[str, int] = {}
    for a in anomalies:
        by_type[a["issue"]] = by_type.get(a["issue"], 0) + 1

    type_colors = {
        "OHLC Invalid": color_error(),
        "Zero Volume": color_warning(),
        "Extreme Move": color_info(),
        "Zero/Negative Price": color_error(),
    }
    kpi_grid(
        [
            dict(
                title=issue,
                value=f"{count:,}",
                icon="report_problem",
                color=type_colors.get(issue, color_gray()),
            )
            for issue, count in sorted(by_type.items(), key=lambda x: -x[1])
        ]
    )

    # Filterable table
    issue_options = {"all": "All"} | {k: k for k in sorted(by_type.keys())}
    issue_filter = (
        ui.select(issue_options, value="all", label="Issue Type")
        .props("dense outlined")
        .classes("w-48")
    )
    sym_filter = (
        ui.input("Filter symbol", placeholder="e.g. TCS")
        .props("dense outlined clearable")
        .classes("w-48")
    )

    @ui.refreshable
    def _anomaly_table() -> None:
        filtered = anomalies
        if issue_filter.value and issue_filter.value != "all":
            filtered = [a for a in filtered if a["issue"] == issue_filter.value]
        search = (sym_filter.value or "").strip().upper()
        if search:
            filtered = [a for a in filtered if search in a["symbol"]]

        columns = [
            {
                "name": "symbol",
                "label": "Symbol",
                "field": "symbol",
                "align": "left",
                "classes": "font-mono text-sm",
            },
            {"name": "trading_date", "label": "Date", "field": "trading_date", "align": "left"},
            {"name": "issue", "label": "Issue", "field": "issue", "align": "left"},
            {
                "name": "detail",
                "label": "Detail",
                "field": "detail",
                "align": "left",
                "classes": "font-mono text-sm",
            },
        ]
        paginated_table(
            rows=filtered, columns=columns, row_key=None, page_size=25, aria_label="Price anomalies"
        )
        export_button(filtered, filename="anomalies.csv", label="Export CSV")

    _anomaly_table()
    issue_filter.on("update:model-value", lambda: _debounce("anomaly", _anomaly_table.refresh))
    sym_filter.on("update:model-value", lambda: _debounce("anomaly", _anomaly_table.refresh))


# ---------------------------------------------------------------------------
# Symbol Lookup tab
# ---------------------------------------------------------------------------


async def _render_symbol_lookup_tab() -> None:
    symbols = await aget_available_symbols()

    sym_select = (
        ui.select(
            options=symbols,
            with_input=True,
            label="Search Symbol",
            value=None,
        )
        .props("dense outlined clearable")
        .classes("w-64")
    )

    @ui.refreshable
    async def _profile_card() -> None:
        sym = sym_select.value
        if not sym:
            ui.label("Select a symbol to view its data profile.").classes("text-sm").style(
                f"color: {theme_text_muted()};"
            )
            return

        profile = await aget_symbol_profile(sym)
        if profile is None:
            ui.label(f"No data found for {sym}.").style(f"color: {theme_text_muted()};")
            return

        kpi_grid(
            [
                dict(
                    title="Daily Range",
                    value=f"{profile['daily_first']} to {profile['daily_last']}",
                    icon="date_range",
                    color=color_info(),
                ),
                dict(
                    title="Daily Rows",
                    value=f"{profile['daily_rows']:,}",
                    icon="storage",
                    color=color_info(),
                ),
                dict(
                    title="Coverage",
                    value=f"{profile['daily_coverage_pct']}%",
                    icon="pie_chart",
                    color=color_success()
                    if profile["daily_coverage_pct"] >= 95
                    else color_warning(),
                ),
                dict(
                    title="Gaps (>3d)",
                    value=f"{len(profile['gaps'])}",
                    icon="broken_image",
                    color=color_warning() if profile["gaps"] else color_success(),
                ),
            ]
        )

        if profile.get("fivemin_rows", 0) > 0:
            kpi_grid(
                [
                    dict(
                        title="5-Min Range",
                        value=f"{profile['fivemin_first']} to {profile['fivemin_last']}",
                        icon="access_time",
                        color=color_warning(),
                    ),
                    dict(
                        title="5-Min Rows",
                        value=f"{profile['fivemin_rows']:,}",
                        icon="storage",
                        color=color_warning(),
                    ),
                    dict(
                        title="5-Min Days",
                        value=f"{profile['fivemin_days']:,}",
                        icon="calendar_month",
                        color=color_warning(),
                    ),
                ]
            )

        gaps = profile.get("gaps", [])
        if gaps:
            ui.label("Gaps (> 3 calendar days)").classes(f"text-lg font-semibold {SPACE_SM}").style(
                f"color: {theme_text_primary()};"
            )
            cols = [
                {"name": "gap_start", "label": "Start", "field": "gap_start", "align": "left"},
                {"name": "gap_end", "label": "End", "field": "gap_end", "align": "left"},
                {
                    "name": "gap_days",
                    "label": "Days",
                    "field": "gap_days",
                    "align": "right",
                    "classes": "tabular-nums",
                },
            ]
            paginated_table(
                rows=gaps,
                columns=cols,
                row_key="gap_start",
                page_size=25,
                aria_label="Symbol profile gaps",
            )
        else:
            ui.label("No significant gaps detected.").classes("text-sm").style(
                f"color: {color_success()};"
            )

    await _profile_card()
    sym_select.on("update:model-value", lambda: _debounce("profile", _profile_card.refresh))


# ---------------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------------


async def data_quality_page() -> None:
    """Render the data quality page with tabbed analytics."""
    with page_layout("Data Quality", "verified"):
        metrics = await aget_data_quality_metrics()

        tabs = ui.tabs().classes("w-full")
        with tabs:
            tab_overview = ui.tab("Overview", icon="dashboard")
            tab_universe = ui.tab("Universe", icon="timeline")
            tab_coverage = ui.tab("Coverage", icon="analytics")
            tab_gaps = ui.tab("Gaps", icon="broken_image")
            tab_freshness = ui.tab("Freshness", icon="schedule")
            tab_anomalies = ui.tab("Anomalies", icon="report_problem")
            tab_lookup = ui.tab("Symbol Lookup", icon="search")

        with ui.tab_panels(tabs, value=tab_overview).classes("w-full"):
            with ui.tab_panel(tab_overview):
                await _render_overview_tab(metrics)

            with ui.tab_panel(tab_universe):
                await _render_universe_tab()

            with ui.tab_panel(tab_coverage):
                await _render_coverage_tab()

            with ui.tab_panel(tab_gaps):
                await _render_gaps_tab()

            with ui.tab_panel(tab_freshness):
                await _render_freshness_tab()

            with ui.tab_panel(tab_anomalies):
                await _render_anomalies_tab()

            with ui.tab_panel(tab_lookup):
                await _render_symbol_lookup_tab()
