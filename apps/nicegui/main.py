"""NSE Momentum Lab - NiceGUI Dashboard Entry Point

Persistent-state dashboard for momentum strategy backtesting and analysis.
Uses NiceGUI for reactive server-side state (no page re-runs).

Run: doppler run -- uv run nseml-dashboard
"""

from __future__ import annotations

import asyncio
import sys
import warnings
import logging
from pathlib import Path

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
if str(_root / "src") not in sys.path:
    sys.path.insert(0, str(_root / "src"))

from nicegui import app, ui

# Import pages with full paths
from apps.nicegui.pages.home import home_page
from apps.nicegui.pages.backtest_results import backtest_page
from apps.nicegui.pages.trade_analytics import trade_analytics_page
from apps.nicegui.pages.compare_experiments import compare_page
from apps.nicegui.pages.strategy_analysis import strategy_page
from apps.nicegui.pages.scans import scans_page
from apps.nicegui.pages.data_quality import data_quality_page
from apps.nicegui.pages.pipeline import pipeline_page
from apps.nicegui.pages.paper_ledger_v2 import paper_ledger_v2_page
from apps.nicegui.pages.daily_summary import daily_summary_page
from apps.nicegui.pages.market_monitor import market_monitor_page
from apps.nicegui.state import shutdown_dashboard_resources


# Each page function calls page_layout() itself — no wrapper needed.
ui.page("/")(home_page)
ui.page("/backtest")(backtest_page)
ui.page("/trade_analytics")(trade_analytics_page)
ui.page("/compare")(compare_page)
ui.page("/strategy")(strategy_page)
ui.page("/scans")(scans_page)
ui.page("/data_quality")(data_quality_page)
ui.page("/pipeline")(pipeline_page)
ui.page("/paper_ledger")(paper_ledger_v2_page)
ui.page("/daily_summary")(daily_summary_page)
ui.page("/market_monitor")(market_monitor_page)


@app.on_shutdown
def _cleanup_dashboard_resources() -> None:
    shutdown_dashboard_resources()


def main() -> None:
    """Entry point for running the dashboard."""
    # Suppress cosmetic urllib3/chardet version mismatch from `requests`.
    # Must be called here (after all imports) so the warning registry is already set.
    warnings.filterwarnings("ignore", message=".*urllib3.*")
    warnings.filterwarnings("ignore", message=".*chardet.*")
    warnings.filterwarnings("ignore", message=".*charset_normalizer.*")
    if sys.platform == "win32":
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="'asyncio.WindowsSelectorEventLoopPolicy' is deprecated and slated for removal in Python 3.16",
                category=DeprecationWarning,
            )
            warnings.filterwarnings(
                "ignore",
                message="'asyncio.set_event_loop_policy' is deprecated and slated for removal in Python 3.16",
                category=DeprecationWarning,
            )
            policy_cls = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
            if policy_cls is not None:
                asyncio.set_event_loop_policy(policy_cls())

    # Suppress Windows asyncio ConnectionResetError noise during cleanup.
    # Setting level above CRITICAL silences all asyncio log messages.
    logging.getLogger("asyncio").setLevel(logging.CRITICAL + 1)

    try:
        ui.run(
            title="NSE Momentum Lab",
            port=8501,
            reload=False,
            show=False,
            dark=False,
        )
    except KeyboardInterrupt:
        print("Dashboard stopped.")
    finally:
        shutdown_dashboard_resources()


if __name__ in {"__main__", "__mp_main__"}:
    main()
