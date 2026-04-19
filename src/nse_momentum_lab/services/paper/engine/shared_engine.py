"""Public API for the paper trading shared engine.

Re-exports the core evaluation, execution, and orchestration functions
so that consumers (scripts, CLI, tests) import from a single location.
The actual implementations live in paper_runtime.py, bar_orchestrator.py,
and paper_session_driver.py.
"""

from nse_momentum_lab.services.paper.engine.bar_orchestrator import (
    SessionPositionTracker,
    TrackedPosition,
    minimum_trade_notional_for,
    select_entries_for_bar,
    should_process_symbol,
    slot_capital_for,
)
from nse_momentum_lab.services.paper.engine.paper_runtime import (
    ACTIVE_SESSION_STATUSES,
    OPEN_SIGNAL_STATES,
    PaperRuntimeState,
    SymbolRuntimeState,
    build_summary_feed_state,
    enforce_session_risk_controls,
    evaluate_candle,
    execute_entry,
    seed_candidates_from_market_db,
)
from nse_momentum_lab.services.paper.engine.paper_session_driver import (
    complete_session,
    process_closed_bar_group,
)
from nse_momentum_lab.services.paper.engine.strategy_presets import (
    PaperStrategyConfig,
    get_paper_strategy_config,
    list_all_accepted_names,
    list_available_strategies,
    resolve_strategy_key,
)

__all__ = [
    "ACTIVE_SESSION_STATUSES",
    "OPEN_SIGNAL_STATES",
    "PaperRuntimeState",
    "PaperStrategyConfig",
    "SessionPositionTracker",
    "SymbolRuntimeState",
    "TrackedPosition",
    "build_summary_feed_state",
    "complete_session",
    "enforce_session_risk_controls",
    "evaluate_candle",
    "execute_entry",
    "get_paper_strategy_config",
    "list_all_accepted_names",
    "list_available_strategies",
    "minimum_trade_notional_for",
    "process_closed_bar_group",
    "resolve_strategy_key",
    "seed_candidates_from_market_db",
    "select_entries_for_bar",
    "should_process_symbol",
    "slot_capital_for",
]
