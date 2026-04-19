# Paper Trading Parity Rework Design

**Date**: 2026-04-18
**Status**: Implementation complete — Phase 6 (tests) pending
**Author**: Claude + Kannan
**Approach**: CPR Pattern Clone (Approach 1)

## Implementation Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Foundation: paper_db.py, replica.py, replica_consumer.py, strategy_presets.py | ✅ Complete |
| Phase 2 | Shared Engine: shared_engine.py, paper_runtime.py, paper_session_driver.py, bar_orchestrator.py | ✅ Complete |
| Phase 3 | Feeds: candle_builder.py, local_ticker.py, kite_ticker.py | ✅ Complete |
| Phase 4 | Notifications: telegram.py (HTML-escaped, token-redacted), alert_dispatcher.py (retry+audit) | ✅ Complete |
| Phase 5 | Scripts + CLI: paper_replay.py, paper_live.py, multi_variant.py, cli/paper_v2.py (14 subcommands) | ✅ Complete |
| Phase 6 | Testing: unit tests, parity tests, feed/recovery/replica/notification tests | 🔲 Pending |
| Phase 7 | Dashboard: paper_ledger_v2.py at /paper_ledger using ReplicaConsumer | ✅ Complete |
| Phase 8 | Switch + Delete: old files removed, API/agents migrated, docs updated, pyproject.toml repointed | ✅ Complete |

### Bug Fixes Applied (5 review rounds)

**P0 Crashes fixed:**
- `replica.py` singular→plural table names; `_dirty` cleared after sync
- `alert_dispatcher.py` correct method (`insert_alert_log`), `TRADE_CLOSED` toggle, retry dedup
- `paper_live.py` wrong module import for `TelegramNotifier`
- `paper_live.py` + `paper_replay.py` `"strategy"` → `"strategy_name"` key
- `multi_variant.py` + `cli/paper_v2.py` `insert_session` → `create_session` with correct kwargs
- `paper_session_driver.py` exit position captured BEFORE `tracker.record_close()` pops it
- `paper_runtime.py` `_minutes_from_open` handles float epoch seconds from `ClosedCandle`

**P1 Silent wrong behavior fixed:**
- `PaperDB.__init__` calls `self.connect()` — DB opens on construction
- `ReplicaConsumer.execute()` returns `list[dict]`, not raw DuckDB connection
- `seed_candidates_from_market_db()` inserts signal rows and stores `signal_id` in `setup_row`
- `select_entries_for_bar()` ranks by `selection_score` desc (backtest parity)
- `enforce_session_risk_controls()` sums open unrealized P&L per-symbol
- Flatten called before `complete_session()` for all controlled-exit paths (STOPPING/CANCELLED/RISK_BREACH)
- `_record_close_in_db()` writes `closed_at` so positions leave `list_open_positions()`
- `execute_entry()` persists `signal_id` in position metadata; close path preserves it
- Strategy overrides loaded from session and passed to `get_paper_strategy_config(overrides=...)`
- `entry_cutoff_minutes` reads from session config, not a hardcoded default
- `patch_position_metadata()` writes `last_mark_price` each HOLD bar for crash-recovery flatten
- `flatten_open_positions()` accepts `mark_prices` dict; uses correct SHORT close side and P&L formula
- Bar-group processing wrapped in `PaperDB.transaction()` — atomic commit per bar group
- `threading.RLock` used so `transaction()` can re-acquire inside `_execute()`
- `/api/paper/positions` uses serializer methods (not raw `db.execute()`)
- `/api/dashboard/summary` accesses row dicts, not tuples
- `paper_ledger_v2.py` uses `fill_ts` and `created_at` matching actual DuckDB schema
- Telegram `html.escape()` applied to both subject AND body
- `alert_dispatcher.py` redacts bot-token URLs before storing in `alert_log`

**P2 Spec deviations fixed:**
- Risk controls run before Step 5 prune (per spec ordering)
- `complete_session()` triggers replica snapshot sync
- `_evaluate_entry` uses `session_low`/`session_high` from candle history
- `math.isfinite` guards on min/max generators; `> 0` guard on short ATR cap

### Known Deferred Gap
- **Backtest max-stop-distance filter**: backtest filters entries where initial stop would be too wide; paper engine admits those same entries. Intentionally deferred — minor parity gap, low impact in practice.

## Problem Statement

The current paper trading system lacks parity with the backtest engine, uses PostgreSQL for paper state (unnecessary overhead), has no dashboard replica pattern (Windows DuckDB locking issues during live trading), and has no Telegram notifications. Walk-forward as a paper-trading gate is unused; walk-forward as a research protocol remains in backtest/optimizer code.

## Goals

1. **Backtest/Replay/Live parity** — shared evaluation path guarantees identical entry/exit decisions across all three modes
2. **DuckDB-only paper state** — single database, no PostgreSQL dependency for paper trading
3. **Replica pattern for dashboard** — dashboard reads snapshots, never blocks live writer
4. **Telegram alerts** — trade open/close, risk limits, session lifecycle, daily summaries
5. **Aggressive cleanup** — remove walk-forward paper-gate, old paper trading engine, dead code; keep `daily-sim` as fast parity probe and walk-forward research protocol in backtest
6. **Lean codebase** — multi-variant planner survives, everything else stripped

## Supported Strategies

| Registry Canonical Key | Aliases (backward-compat) | Direction | Ranking Weights |
|------------------------|--------------------------|-----------|-----------------|
| `2lynchbreakout` | `thresholdbreakout`, `indian2lynch` | LONG | c_strength, y_score, n_score, r2_quality |
| `2lynchbreakdown` | `thresholdbreakdown` | SHORT | h_quality, r2_quality, c_strength |
| `episodicpivot` | `epproxysameday` | LONG | EP gap-based setup logic |

The canonical names match the registry in `strategy_registry.py`. Aliases resolve to canonical keys at the paper trading layer.

`strategy_presets.py` alias table:
```python
STRATEGY_ALIASES = {
    "thresholdbreakout": "2lynchbreakout",
    "indian2lynch": "2lynchbreakout",
    "thresholdbreakdown": "2lynchbreakdown",
    "epproxysameday": "episodicpivot",
}
```

Existing session IDs and backtest experiment records that use alias names will continue to resolve correctly through the alias table. No migration needed.

## Execution Modes

| Mode | Feed Source | Data Path | Use Case |
|------|-------------|-----------|----------|
| **Backtest** | DuckDB views (v_daily, v_5min) | Multi-year batch | Strategy research |
| **Paper Replay** | LocalTickerAdapter (DuckDB → ClosedCandle) | Single/multi-day via live pipeline | Parity verification, pre-live validation |
| **Paper Live** | KiteTickerAdapter (WebSocket → ClosedCandle) | Real-time 5-min bars | Production paper trading |

---

## Architecture

### Module Structure (New)

```
src/nse_momentum_lab/services/paper/     # Complete rewrite
├── __init__.py
├── engine/
│   ├── __init__.py
│   ├── shared_engine.py        # Shared entry/exit logic for all 3 modes
│   ├── paper_runtime.py        # PaperRuntimeState, SymbolRuntimeState, risk controls
│   ├── paper_session_driver.py # Canonical 5-step bar processing loop
│   ├── bar_orchestrator.py     # SessionPositionTracker, capital management
│   └── strategy_presets.py     # Strategy config resolution + alias table
├── feeds/
│   ├── __init__.py
│   ├── local_ticker.py         # LocalTickerAdapter (DuckDB → ClosedCandle)
│   ├── kite_ticker.py          # KiteTickerAdapter (WebSocket → ClosedCandle)
│   └── candle_builder.py       # FiveMinuteCandleBuilder
├── notifiers/
│   ├── __init__.py
│   ├── telegram.py             # TelegramNotifier (httpx, HTML formatting)
│   └── alert_dispatcher.py     # Async dispatch queue with retry
├── db/
│   ├── __init__.py
│   ├── paper_db.py             # DuckDB paper state tables
│   ├── replica.py              # Engine-side snapshot writer
│   └── replica_consumer.py     # Dashboard-side read-only reader
├── scripts/
│   ├── __init__.py
│   ├── paper_replay.py         # Replay: live pipeline + LocalTickerAdapter
│   └── paper_live.py           # Live: live pipeline + KiteTickerAdapter
└── multi_variant.py            # Multi-variant session planner (kept from original)
```

### Shared Engine Pattern

The core principle from CPR: **one evaluation path, three data sources**. The shared engine wraps the repo's existing backtest primitives — strategy-specific candidate SQL (from `strategy_registry.py`), entry-vs-hold filter separation, bulk intraday resolution (from `intraday_execution.py`), R-ladder ratchets, gap-through-stop handling, and weak-close carry behavior — rather than re-encoding them from CPR.

```python
# shared_engine.py — routes through existing backtest primitives
async def evaluate_candle(
    symbol: str,
    candle: ClosedCandle,
    runtime_state: PaperRuntimeState,
    tracker: SessionPositionTracker,
    params: BacktestParams,
    strategy: str,
) -> EvaluateResult:
    """
    Delegates to the same strategy_registry + intraday_execution logic
    used by the backtest runner. No re-encoding of rules.
    """
    ...

# paper_session_driver.py — canonical 5-step bar processing loop
async def process_closed_bar_group(
    session_id: str,
    bar_candles: list[ClosedCandle],
    runtime_state: PaperRuntimeState,
    tracker: SessionPositionTracker,
    params: BacktestParams,
    strategy: str,
    feed_source: str,  # "backtest" | "replay" | "live"
) -> BarGroupResult:
    """
    5-step process (identical to CPR):
    1. Check exits / advance trailing stops
    2. Evaluate entry candidates (via existing strategy_registry)
    3. Select + execute entries (via existing intraday_execution)
    4. Apply risk controls (daily loss, drawdown, flatten time)
    5. Prune symbol universe
    """
    ...
```

### Data Flow

```
                        ┌─────────────────────┐
                        │   DuckDB Market DB   │
                        │ (v_daily, v_5min,    │
                        │  feat_daily)         │
                        └──────┬──────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
        ┌──────────┐   ┌──────────────┐  ┌──────────────┐
        │ Backtest  │   │ Paper Replay │  │ Paper Live   │
        │ Runner    │   │              │  │              │
        │           │   │ LocalTicker  │  │ KiteTicker   │
        │ (batch)   │   │ Adapter      │  │ Adapter      │
        └─────┬─────┘   └──────┬───────┘  └──────┬───────┘
              │                │                  │
              │         ┌──────┴──────────────────┘
              │         │
              │         ▼
              │   ┌──────────────────┐
              │   │  Shared Engine   │
              │   │  (delegates to   │
              │   │   strategy_      │
              │   │   registry +     │
              │   │   intraday_exec) │
              │   └──────┬───────────┘
              │          │
              ▼          ▼
        ┌──────────────────────────┐
        │  DuckDB Paper DB         │
        │  (paper.duckdb)          │
        │  - paper_sessions        │
        │  - paper_signals         │
        │  - paper_session_signals │
        │  - paper_positions       │
        │  - paper_orders          │
        │  - paper_fills           │
        │  - paper_order_events    │
        │  - paper_bar_checkpoints │
        │  - paper_feed_state      │
        │  - alert_log             │
        └───────────┬──────────────┘
                    │
            ┌───────┴───────┐
            │  Snapshot Sync │ (5s debounce)
            │  ATTACH →      │
            │  COPY to       │
            │  paper_dash    │
            └───────┬───────┘
                    │
                    ▼
        ┌──────────────────────────┐
        │  Replica Consumer        │
        │  (Dashboard reads only)  │
        └──────────────────────────┘
```

---

## Component Designs

### 1. Shared Engine (`engine/shared_engine.py`)

**Design principle: route through existing backtest primitives, not re-encode from CPR.**

The shared engine imports and delegates to the existing NSEML backtest modules:
- `strategy_registry.py` → strategy-specific candidate SQL, filter separation
- `intraday_execution.py` → bulk intraday resolution, entry price calculation
- `candidate_builder.py` → ranking functions (breakout/breakdown)

New code in shared_engine.py is limited to:
- `evaluate_candle()` — orchestrates the delegation to existing primitives
- `execute_entry()` — position opening with slippage model (from existing engine.py)
- `compute_stop_levels()` — ATR-based initial stop, breakeven, trailing stop (from existing engine.py ExitPolicyConfig)
- R-ladder ratchet logic (from existing intraday_execution.py)
- Gap-through-stop handling (from existing engine.py)

Strategy routing via `strategy_presets.py` with explicit alias table (see Supported Strategies section above):
- `2lynchbreakout` → breakout ranking (c_strength, y_score, n_score, r2_quality)
- `2lynchbreakdown` → breakdown ranking (h_quality, r2_quality, c_strength)
- `episodicpivot` → EP proxy same-day logic

### 2. Paper Runtime (`engine/paper_runtime.py`)

Adapted from CPR's `paper_runtime.py`:
- `PaperRuntimeState` — session-level state (symbol states, risk metrics, alert dedup)
- `SymbolRuntimeState` — per-symbol state (candles, setup data, position flags)
- `enforce_session_risk_controls()` — daily loss limit (5%), drawdown (15%), flatten time (15:15)
- `flatten_session_positions()` — close all positions, send alerts
- `build_summary_feed_state()` — feed telemetry for dashboard

### 3. Paper Session Driver (`engine/paper_session_driver.py`)

Adapted from CPR's `paper_session_driver.py`:
- `process_closed_bar_group()` — canonical 5-step bar processing loop
- `complete_session()` — finalize session status, trigger snapshot sync

### 4. Bar Orchestrator (`engine/bar_orchestrator.py`)

Adapted from CPR's `bar_orchestrator.py`:
- `SessionPositionTracker` — in-memory position book with capital management
- `compute_position_qty()` — all-or-nothing sizing with minimum notional
- `select_entries_for_bar()` — alphabetical ordering, slot-limited

### 5. LocalTickerAdapter (`feeds/local_ticker.py`)

Adapted from CPR's `LocalTickerAdapter`:
- Reads from `v_5min` DuckDB view (NSEML's equivalent of `intraday_day_pack`)
- Emits `ClosedCandle` objects on demand via `drain_closed()`
- Global bar cursor broadcasts to all registered sessions
- Thread-safe with RLock
- Marker attribute `_local_feed = True` for detection

### 6. KiteTickerAdapter (`feeds/kite_ticker.py`)

Adapted from CPR's `KiteTickerAdapter`:
- Wraps existing `kiteconnect.KiteTicker` (already in `services/kite/ticker.py`)
- Thread-safe per-session `FiveMinuteCandleBuilder` instances
- `_on_ticks()` — batch processing, timestamp coercion (exchange_timestamp preferred)
- `synthesize_quiet_symbols()` — inject last-known-price for symbols not receiving ticks
- `recover_connection()` — watchdog recreates KiteTicker if socket stays down > 20s
- `health_stats()` / `symbol_coverage()` — telemetry

### 7. FiveMinuteCandleBuilder (`feeds/candle_builder.py`)

Adapted from CPR's `FiveMinuteCandleBuilder`:
- Thread-safe (RLock) aggregation of snapshots into deterministic 5-min OHLCV candles
- `ClosedCandle` and `MarketSnapshot` frozen dataclasses

### 8. DuckDB Paper DB (`db/paper_db.py`)

**Full DuckDB schema covering all current PostgreSQL paper tables:**

| DuckDB Table | Replaces PostgreSQL Model | Purpose |
|-------------|--------------------------|---------|
| `paper_sessions` | `PaperSession` | Session lifecycle, params, risk config |
| `paper_signals` | `Signal` | Signal state machine (NEW→QUALIFIED→ALERTED→ENTERED→MANAGED→EXITED→ARCHIVED) |
| `paper_session_signals` | `PaperSessionSignal` | Signal-to-session link with rank, score, decision |
| `paper_positions` | `PaperPosition` | Open/closed positions with P&L |
| `paper_orders` | `PaperOrder` | Order records with broker status |
| `paper_fills` | `PaperFill` | Execution fills with slippage |
| `paper_order_events` | `PaperOrderEvent` | Order state transitions (audit trail) |
| `paper_bar_checkpoints` | `PaperBarCheckpoint` | Bar-group watermark for replay resume (session_id, bar_end_ts) |
| `paper_feed_state` | `PaperFeedState` | Feed health telemetry |
| `alert_log` | (new) | Alert dispatch audit (channel, status, timestamp) |

Session lifecycle: PLANNING → ACTIVE → RUNNING → COMPLETED/FAILED/CANCELLED
Kill switch via session status (PAUSED → STOPPING causes graceful shutdown).

### 9. Replica System

**Starts simple, escalates only if needed.**

Phase 1: Use the existing `market_db.py` attach/copy-table pattern to create `paper_dashboard.duckdb`:
```python
# Leverages existing Windows-tolerant snapshot mechanism from market_db.py:588
def refresh_paper_dashboard(source_path: Path, dashboard_path: Path) -> None:
    """ATTACH source, COPY tables to dashboard file, DETACH."""
    ...
```

Phase 2 (only if copy-table snapshots fail under load): Add CPR-style pointer-file versioning:
- `ReplicaSync` class with `mark_dirty()` / `maybe_sync()` / `force_sync()`
- 5-second debounce, atomic rename, keeps latest 2 versions
- Windows-critical: passes source_conn to avoid DuckDB exclusive locking

**Dashboard-side reader (`db/replica_consumer.py`):**
- `ReplicaConsumer` with `get_connection()` — reads from `paper_dashboard.duckdb`
- Refresh check on TTL expiry (configurable, default 30s)
- Crash recovery: re-copy from source if dashboard file is corrupted

### 10. Telegram Notifications (`notifiers/telegram.py`)

Port of CPR's Telegram integration:
- Bot token + chat IDs from Doppler env vars: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_IDS`
- `TelegramNotifier` using `httpx.AsyncClient`, 10s timeout
- HTML parse_mode with `html.escape()` for all dynamic content, link previews disabled
- **Security rule**: alerts never include raw connection URLs, API tokens, or unescaped HTML. All dynamic values wrapped in `html.escape()`. On retry failure, `httpx` exception strings and request objects are redacted before logging to `alert_log` — strip URL paths containing bot tokens, replace with `[REDACTED_URL]`.
- Message types: TRADE_OPENED, TRADE_CLOSED, SL_HIT, TARGET_HIT, TRAIL_STOP, SESSION_STARTED, SESSION_COMPLETED, SESSION_ERROR, FEED_STALE, FEED_RECOVERED, DAILY_LOSS_LIMIT, DRAWDOWN_LIMIT, FLATTEN_EOD, DAILY_PNL_SUMMARY
- Toggle controls: individual booleans per alert type (from Doppler/settings)

### 11. Alert Dispatcher (`notifiers/alert_dispatcher.py`)

Port of CPR's `AlertDispatcher`:
- `asyncio.Queue(maxsize=100)` for non-blocking enqueue
- `_send_with_retry()` — up to 3 retries with exponential backoff (1s, 2s, 4s)
- Logs to `alert_log` DuckDB table for audit trail
- `shutdown()` — graceful drain with 120s timeout

### 12. Paper Replay (`scripts/paper_replay.py`)

- Creates session in DuckDB paper DB
- Loads historical 5-min candles from DuckDB `v_5min`
- Uses `LocalTickerAdapter` to feed closed candles to the live pipeline
- Calls `paper_session_driver.process_closed_bar_group()` for each bar
- **Multi-day replay with resume**:
  - Tracks progress via bar-group watermarks in `paper_bar_checkpoints` (session_id, bar_end_ts)
  - On resume, queries the latest committed bar_end_ts and skips already-processed bar groups
  - One checkpoint row per committed bar group, written in same transaction as fills/state
  - Trade-date watermark: `last_processed_trade_date` stored in session metadata
- Auto-archives completed sessions

### 13. Paper Live (`scripts/paper_live.py`)

- Creates session in DuckDB paper DB
- Connects to Kite WebSocket via `KiteTickerAdapter`
- Adaptive poll interval: speeds up near candle close (0.5s within 5s)
- Session supervision loop checks DB status for PAUSED/STOPPING
- Stale detection: streak counter, alerts at streak=3, exits after 600s
- Auto-flattens positions on STALE/FAILED exit
- Uses `synthesize_quiet_symbols()` for symbols not receiving ticks
- **Pause/resume recovery**: on resume after pause, replays missed bars from DuckDB `v_5min` to catch up

### 14. Multi-Variant Planner (`multi_variant.py`)

Kept from original design, adapted for new DuckDB paper DB:
- `PAPER_STANDARD_MATRIX` — canonical strategy variants per strategy type
- Runs multiple variants concurrently
- Single-process multi-variant sharing one adapter

### 15. Dashboard Integration

**Paper Ledger page rewrite:**
- Uses `ReplicaConsumer` reading from `paper_dashboard.duckdb` instead of PostgreSQL
- Active sessions with auto-refresh (30s TTL)
- KPI grid: Session Status, Open Positions, P&L, Feed State
- Tables: Positions, Orders, Fills, Activity
- Session lifecycle commands
- **Stale replica alarm**: dashboard shows warning when replica age exceeds configurable threshold (default 60s)

**Dashboard DB Proxy:**
- `_DashboardDBProxy` resolves to current dashboard DuckDB file
- All reads via `ThreadPoolExecutor(max_workers=3)`
- TTL caching: sessions (30s), positions (60s)

---

## CLI Commands (New)

Replace all existing paper CLI commands. New entry point: `nseml-paper`

```bash
# Session management
doppler run -- uv run nseml-paper prepare --strategy thresholdbreakout --trade-date TODAY
doppler run -- uv run nseml-paper status [--session-id ID]
doppler run -- uv run nseml-paper stop --session-id ID
doppler run -- uv run nseml-paper pause --session-id ID
doppler run -- uv run nseml-paper resume --session-id ID
doppler run -- uv run nseml-paper flatten --session-id ID
doppler run -- uv run nseml-paper archive --session-id ID

# Readiness check (kept from original)
doppler run -- uv run nseml-paper daily-prepare --trade-date DATE --strategy STRATEGY

# Fast parity probe (kept from original - cheapest pre-live validation)
doppler run -- uv run nseml-paper daily-sim --trade-date DATE --strategy STRATEGY

# Replay (local feed, uses live pipeline)
doppler run -- uv run nseml-paper replay --session-id ID [--from DATE] [--to DATE]
doppler run -- uv run nseml-paper daily-replay --trade-date DATE

# Live (Kite WebSocket)
doppler run -- uv run nseml-paper live --session-id ID
doppler run -- uv run nseml-paper daily-live --trade-date DATE

# Multi-variant (daily workflow)
doppler run -- uv run nseml-paper daily-start --trade-date DATE

# Feed streaming (kept from original - useful for debugging)
doppler run -- uv run nseml-paper stream --session-id ID
```

---

## Deletion Manifest

### Scope clarification: walk-forward paper-gate vs research protocol

**What gets deleted**: Walk-forward as a paper-trading gate (CLI commands, PostgreSQL tables, session validation, daily startup checks). The `walk-forward` and `walk-forward-cleanup` subcommands in paper.py are already orphaned (not registered in `build_parser()`).

**What stays**: Walk-forward as a research protocol in `services/backtest/walkforward.py` (window generation), `protocols.py` (walk-forward protocol types), `optimizer.py` (walk-forward methods). These support research workflows independent of paper trading. The `DuckDBBacktestRunner` already uses `WalkForwardFramework` for fold generation. Removing this would break existing backtest capabilities.

### Files to DELETE entirely:

| File | Reason |
|------|--------|
| `src/nse_momentum_lab/services/paper/engine.py` | Full paper rewrite |
| `src/nse_momentum_lab/services/paper/runtime.py` | Full paper rewrite |
| `src/nse_momentum_lab/services/paper/session_planner.py` | Replaced by multi_variant.py |
| `src/nse_momentum_lab/services/paper/live_watchlist.py` | Logic moves to shared_engine.py |

**Note**: `candidate_builder.py` is NOT deleted — see cutover plan below.
| `src/nse_momentum_lab/cli/paper.py` | Full rewrite |
| `src/nse_momentum_lab/cli/paper_plan.py` | Replaced by multi_variant.py |
| `src/nse_momentum_lab/db/paper.py` | Replaced by DuckDB paper_db.py |
| `db/init/006_walk_forward_fold.sql` | Walk-forward paper-gate removal (PostgreSQL table) |
| `tests/unit/cli/test_paper.py` | Full paper rewrite |
| `tests/unit/db/test_paper.py` | Full paper rewrite |
| `tests/unit/services/paper/test_engine.py` | Full paper rewrite |
| `tests/unit/services/paper/test_runtime.py` | Full paper rewrite |
| `tests/unit/services/paper/test_session_planner.py` | Full paper rewrite |

### Code to REMOVE from existing files:

| File | What to Remove |
|------|----------------|
| `db/models.py` | WalkForwardFold class, 9 PostgreSQL paper models (PaperSession, Signal, PaperOrder, PaperFill, PaperPosition, PaperSessionSignal, PaperOrderEvent, PaperFeedState, PaperBarCheckpoint) |
| `apps/nicegui/state/__init__.py` | PostgreSQL paper imports |
| `pyproject.toml` | Keep `nseml-paper` (repoint to new CLI). Remove only `nseml-kite-paper` and `nseml-paper-plan` aliases. |

### Code to KEEP (walk-forward research protocol):

| File | What Stays |
|------|------------|
| `services/backtest/walkforward.py` | WalkForwardFramework — window generation for research |
| `services/backtest/protocols.py` | WalkForwardProtocol types — research protocol support |
| `services/backtest/optimizer.py` | walk-forward methods — backtest optimization |
| `services/backtest/__init__.py` | walk-forward re-exports — backtest API |
| `services/research/validation.py` | walk-forward protocol validation |
| `db/market_db.py` | `list_experiments_for_wf_run_id()` — backtest queries |

### Files to REWRITE:

| File | Reason |
|------|--------|
| `apps/nicegui/pages/paper_ledger.py` | Use ReplicaConsumer instead of PostgreSQL |
| `apps/nicegui/state/__init__.py` | Remove PostgreSQL paper imports, use ReplicaConsumer |
| `src/nse_momentum_lab/api/app.py` | Migrate `/api/paper/*` endpoints from PostgreSQL to DuckDB paper_db |
| `src/nse_momentum_lab/agents/agent.py` | Migrate PaperPosition/Signal imports from PostgreSQL ORM to DuckDB queries |
| `docs/reference/COMMANDS.md` | Update paper commands |
| `docs/dev/AGENTS.md` | Remove walk-forward paper-gate references |
| `docs/architecture/PAPER_TRADING_WALK_FORWARD_PLAN.md` | Update to remove paper-gate sections, keep research protocol docs |

### candidate_builder.py Cutover Plan

`candidate_builder.py` is imported 6 times by `DuckDBBacktestRunner` and is part of the backtest critical path. It cannot be deleted during the paper trading rework.

1. **Phase 2**: The shared engine delegates to candidate_builder.py's ranking functions (no changes to the file)
2. **Phase 8**: Instead of deleting candidate_builder.py, it stays in `services/paper/candidate_builder.py` as a neutral shared module
3. **Future cleanup** (optional, separate PR): Move candidate_builder.py to `services/backtest/` or a shared location, repoint DuckDBBacktestRunner imports

### New files to CREATE:

| File | Purpose |
|------|---------|
| `src/nse_momentum_lab/services/paper/engine/__init__.py` | Engine package |
| `src/nse_momentum_lab/services/paper/engine/shared_engine.py` | Shared entry/exit logic (delegates to existing backtest primitives) |
| `src/nse_momentum_lab/services/paper/engine/paper_runtime.py` | Session/symbol state, risk controls |
| `src/nse_momentum_lab/services/paper/engine/paper_session_driver.py` | Canonical 5-step bar loop |
| `src/nse_momentum_lab/services/paper/engine/bar_orchestrator.py` | Position tracker |
| `src/nse_momentum_lab/services/paper/engine/strategy_presets.py` | Strategy config resolution + alias table |
| `src/nse_momentum_lab/services/paper/feeds/__init__.py` | Feeds package |
| `src/nse_momentum_lab/services/paper/feeds/local_ticker.py` | LocalTickerAdapter |
| `src/nse_momentum_lab/services/paper/feeds/kite_ticker.py` | KiteTickerAdapter |
| `src/nse_momentum_lab/services/paper/feeds/candle_builder.py` | FiveMinuteCandleBuilder |
| `src/nse_momentum_lab/services/paper/notifiers/__init__.py` | Notifiers package |
| `src/nse_momentum_lab/services/paper/notifiers/telegram.py` | TelegramNotifier |
| `src/nse_momentum_lab/services/paper/notifiers/alert_dispatcher.py` | Async dispatch queue |
| `src/nse_momentum_lab/services/paper/db/__init__.py` | DB package |
| `src/nse_momentum_lab/services/paper/db/paper_db.py` | DuckDB paper state (full schema) |
| `src/nse_momentum_lab/services/paper/db/replica.py` | Snapshot writer (starts with attach/copy) |
| `src/nse_momentum_lab/services/paper/db/replica_consumer.py` | Read-only dashboard reader |
| `src/nse_momentum_lab/services/paper/scripts/__init__.py` | Scripts package |
| `src/nse_momentum_lab/services/paper/scripts/paper_replay.py` | Replay via live pipeline |
| `src/nse_momentum_lab/services/paper/scripts/paper_live.py` | Live via Kite WebSocket |
| `src/nse_momentum_lab/services/paper/multi_variant.py` | Multi-variant planner |
| `src/nse_momentum_lab/cli/paper.py` | New paper CLI (from scratch) |
| `src/nse_momentum_lab/services/paper/__init__.py` | Package init |

---

## Risk Management Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `paper_max_daily_loss_pct` | 0.05 (5%) | Auto-flatten when breached |
| `paper_max_drawdown_pct` | 0.15 (15%) | Auto-flatten when breached |
| `paper_max_positions` | 10 | Maximum concurrent positions |
| `paper_max_position_pct` | 0.10 (10%) | Maximum per-position allocation |
| `paper_flatten_time` | 15:15 IST | Auto-flatten 15 min before close |
| `paper_stale_feed_timeout_sec` | 120 | Alert threshold for stale feed |
| `paper_stale_exit_sec` | 600 | Exit session after 10 min stale |

## Slippage Model (from existing engine)

| Category | Market Cap | Slippage |
|----------|-----------|----------|
| Large cap | > 100 Cr | 5 bps |
| Mid cap | 20-100 Cr | 10 bps |
| Small cap | < 20 Cr | 20 bps |

---

## Signal Lifecycle

```
NEW → QUALIFIED → ALERTED → ENTERED → MANAGED → EXITED → ARCHIVED
```

States tracked in `paper_signals` DuckDB table. `paper_session_signals` links signals to sessions with rank/score. The shared engine handles transitions identically across all three modes.

## Exit Reasons

STOP_INITIAL, STOP_BREAKEVEN, STOP_TRAIL, TIME_STOP, EXIT_EOD, RISK_BREACH,
TARGET_HIT, SL_HIT, TRAIL_STOP, DAILY_LOSS_LIMIT, DRAWDOWN_LIMIT, FLATTEN_EOD

---

## Multi-Day Replay Resume Design

### Bar-Group Watermark (Atomic Boundary)

The execution unit is a **bar group** (all symbols processed for one bar timestamp). Checkpointing happens at the bar-group level, not per-symbol:

```
paper_bar_checkpoints:
  (session_id, bar_end_ts) → {committed_symbol_count, fill_count, state_hash}
```

One checkpoint row is written **in the same DuckDB transaction** as all fills, position updates, and signal state changes for that bar group. On resume:
1. Query the latest committed `bar_end_ts` from checkpoints
2. Resume from the next bar timestamp
3. No partial-symbol state — either the whole bar group committed or none of it did

This matches the atomic boundary of `process_closed_bar_group()` which processes all symbols for a single bar in one pass.

### Trade-Date Watermark

The session metadata stores `last_processed_trade_date`. On multi-day replay resume:
1. Query `last_processed_trade_date` from session metadata
2. Start replay from the next trading day
3. For the resumed trade date, query `paper_bar_checkpoints` for the latest committed bar

### Pause/Resume Recovery

When a live session is paused and resumed:
1. On pause: the current bar-group checkpoint is written (same as normal bar completion)
2. On resume: query `v_5min` for bars between last checkpoint time and resume time
3. Feed missed bars through `LocalTickerAdapter` in fast-forward mode (no sleep between bars)
4. Then switch back to real-time Kite feed

---

## Testing Strategy

### Unit Tests
- **Shared engine**: deterministic inputs/outputs for each strategy type
- **Bar orchestrator**: position tracking, sizing, slot limits
- **Candle builder**: aggregation correctness, bar boundary handling
- **Paper DB**: session lifecycle, signal state transitions, checkpoint upserts

### Parity Tests (Golden Fixtures)
- Run canonical scenarios through backtest and replay, assert identical trade sequences
- Golden fixtures stored in `tests/fixtures/parity/` — known symbol/date/strategy combos with expected results
- Parity test CI gate: any divergence fails the build

### Feed Tests
- **LocalTickerAdapter**: produces correct ClosedCandle objects from DuckDB v_5min
- **KiteTickerAdapter mock**: tick → snapshot → candle pipeline
- **Quiet symbol synthesis**: symbols not receiving ticks get last-known-price
- **Gap-through-stop**: gap openings that skip past stop levels

### Recovery Tests
- **Pause/resume**: session pauses mid-bar, resumes, catches up correctly
- **Multi-day replay resume**: interrupt at day 3 of 5, resume completes days 3-5
- **Stale feed**: no ticks for 120s → alert, 600s → flatten + exit
- **Empty watchlist BLOCKED**: session with zero qualifying signals short-circuits cleanly

### Replica Tests
- Snapshot creation, dashboard read, TTL expiry
- Concurrent read/write (writer produces snapshot while dashboard reads)
- Crash recovery: dashboard detects corrupted snapshot, re-copies from source

### Notification Tests
- Alert dispatch queue, retry logic (3 attempts with backoff)
- Message formatting: HTML escaping of dynamic content, no raw URLs/tokens
- **Retry failure redaction**: verify that `httpx` exception strings logged to `alert_log` have bot tokens stripped
- Toggle controls: individual alert types can be enabled/disabled
- Audit trail: all dispatched alerts logged to `alert_log`

### End-to-End Windows Host Gate
- Run `nseml-paper daily-replay` → replica refresh → dashboard/API read, with a concurrent reader attached during writes
- Verify no DuckDB locking errors on Windows
- Verify dashboard shows updated data within 35 seconds of bar completion

### Observability
- **Counters**: replica age, queue depth, stale-feed events, alert dispatch success/failure
- **Dashboard stale alarm**: warning when replica age exceeds threshold
- **Feed audit**: per-session tick coverage stats (covered/stale/missing per symbol)

---

## Implementation Order

**Principle: build new → switch → delete old. The repo stays runnable throughout.**

### Phase 1 — Foundation ✅ Complete
- ✅ Create `services/paper/` package structure
- ✅ Implement `db/paper_db.py` (DuckDB schema with all tables, RLock, transaction() CM)
- ✅ Implement `db/replica.py` (snapshot writer using existing market_db pattern)
- ✅ Implement `db/replica_consumer.py` (returns `list[dict]`, TTL caching, crash recovery)
- ✅ Implement `engine/strategy_presets.py` (alias table + config resolution)

### Phase 2 — Shared Engine ✅ Complete
- ✅ Implement `engine/shared_engine.py` (routes through strategy_registry + intraday_execution)
- ✅ Implement `engine/paper_runtime.py` (session/symbol state, risk controls with open P&L, seed inserts signals)
- ✅ Implement `engine/paper_session_driver.py` (atomic 5-step loop via transaction(), mark_price written per HOLD bar)
- ✅ Implement `engine/bar_orchestrator.py` (position tracker, selection_score ranking)

### Phase 3 — Feeds ✅ Complete
- ✅ Implement `feeds/candle_builder.py` (FiveMinuteCandleBuilder)
- ✅ Implement `feeds/local_ticker.py` (LocalTickerAdapter from DuckDB)
- ✅ Implement `feeds/kite_ticker.py` (KiteTickerAdapter from WebSocket)

### Phase 4 — Notifications ✅ Complete
- ✅ Implement `notifiers/telegram.py` (html.escape on subject + body, token-redacted error logs)
- ✅ Implement `notifiers/alert_dispatcher.py` (async queue, retry dedup, _redact_url in alert_log)

### Phase 5 — Scripts + CLI ✅ Complete
- ✅ Implement `scripts/paper_replay.py` (multi-day resume, bar idempotency, strategy overrides)
- ✅ Implement `scripts/paper_live.py` (pause/resume recovery, flatten on all exit paths, mark_prices on crash flatten)
- ✅ Implement `multi_variant.py`
- ✅ Implement `cli/paper_v2.py` (14 subcommands: prepare, replay, live, plan, status, daily-sim, stop, pause, resume, flatten, archive, daily-prepare, daily-replay, daily-live)

### Phase 6 — Testing 🔲 Pending
- 🔲 Unit tests: shared engine (evaluate_candle LONG/SHORT), bar orchestrator, candle builder, paper DB
- 🔲 Parity tests: golden fixtures from canonical backtest runs
- 🔲 Feed tests: LocalTickerAdapter, quiet symbols, gap-through-stop
- 🔲 Recovery tests: pause/resume, multi-day replay resume, stale feed
- 🔲 Replica tests: concurrent read/write, crash recovery
- 🔲 Notification tests: HTML escaping, _redact_url regex, retry, audit trail
- Suggested location: `tests/unit/services/paper/test_paper_engine.py`

### Phase 7 — Dashboard Integration ✅ Complete
- ✅ Created `apps/nicegui/pages/paper_ledger_v2.py` (uses ReplicaConsumer, fill_ts/created_at schema-aligned)
- ✅ Mounted at `/paper_ledger` in NiceGUI
- ✅ Stale replica alarm on dashboard

### Phase 8 — Switch + Delete ✅ Complete
- ✅ Migrated `api/app.py` `/api/paper/*` endpoints to DuckDB paper_db (serializer methods, dict row access)
- ✅ Migrated `agents/agent.py` PaperPosition/Signal imports to DuckDB queries
- ✅ Deleted `services/paper/engine.py`, `runtime.py`, `session_planner.py`, `live_watchlist.py`
- ✅ Deleted `cli/paper.py` old version; `nseml-paper` now points to `paper_v2:main`
- ✅ Deleted `db/paper.py` PostgreSQL operations
- ✅ Deleted `db/init/006_walk_forward_fold.sql`
- ✅ Updated `pyproject.toml`: `nseml-paper = "nse_momentum_lab.cli.paper_v2:main"`
- ✅ Updated `docs/reference/COMMANDS.md` (v2 behaviors, /market_monitor page)
- ✅ Updated `docs/operations/pre-open-live-paper.md` (v2 callout, crash recovery, dashboard sections)
- ✅ Kept walk-forward research protocol files (walkforward.py, protocols.py, optimizer.py, validation.py)
- ✅ Kept candidate_builder.py as neutral shared module (backtest runner depends on it)

## Secrets Required (Doppler)

- `TELEGRAM_BOT_TOKEN` — Telegram bot token
- `TELEGRAM_CHAT_IDS` — Comma-separated chat IDs for alerts
- Existing: Kite credentials, DuckDB paths, MinIO keys (unchanged)
