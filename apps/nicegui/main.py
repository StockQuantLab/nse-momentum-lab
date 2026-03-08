"""NSE Momentum Lab - NiceGUI Dashboard Entry Point

Persistent-state dashboard for momentum strategy backtesting and analysis.
Uses NiceGUI for reactive server-side state (no page re-runs).

Run: doppler run -- uv run nseml-dashboard
"""

from __future__ import annotations

import sys
import warnings
import logging
from pathlib import Path

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
if str(_root / "src") not in sys.path:
    sys.path.insert(0, str(_root / "src"))

from nicegui import ui

# Import pages with full paths
from apps.nicegui.pages.home import home_page
from apps.nicegui.pages.backtest_results import backtest_page
from apps.nicegui.pages.trade_analytics import trade_analytics_page
from apps.nicegui.pages.compare_experiments import compare_page
from apps.nicegui.pages.strategy_analysis import strategy_page
from apps.nicegui.pages.scans import scans_page
from apps.nicegui.pages.data_quality import data_quality_page
from apps.nicegui.pages.pipeline import pipeline_page
from apps.nicegui.pages.paper_ledger import paper_ledger_page
from apps.nicegui.pages.daily_summary import daily_summary_page


# Each page function calls page_layout() itself — no wrapper needed.
ui.page("/")(home_page)
ui.page("/backtest")(backtest_page)
ui.page("/trade_analytics")(trade_analytics_page)
ui.page("/compare")(compare_page)
ui.page("/strategy")(strategy_page)
ui.page("/scans")(scans_page)
ui.page("/data_quality")(data_quality_page)
ui.page("/pipeline")(pipeline_page)
ui.page("/paper_ledger")(paper_ledger_page)
ui.page("/daily_summary")(daily_summary_page)


def main() -> None:
    """Entry point for running the dashboard."""
    # Suppress cosmetic urllib3/chardet version mismatch from `requests`.
    # Must be called here (after all imports) so the warning registry is already set.
    warnings.filterwarnings("ignore", message=".*urllib3.*")
    warnings.filterwarnings("ignore", message=".*chardet.*")
    warnings.filterwarnings("ignore", message=".*charset_normalizer.*")

    # Suppress Windows asyncio ConnectionResetError during cleanup
    # This is a cosmetic issue on Windows when connections close abruptly
    logging.getLogger("asyncio").setLevel(logging.CRITICAL + 1)

    # Suppress the traceback for ConnectionResetError exceptions
    class ConnectionResetFilter(logging.Filter):
        def filter(self, record):
            return "ConnectionResetError" not in record.getMessage()

    asyncio_logger = logging.getLogger("asyncio")
    asyncio_logger.addFilter(ConnectionResetFilter())

    ui.run(
        title="NSE Momentum Lab",
        port=8501,
        reload=False,
        show=False,
        dark=False,
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
