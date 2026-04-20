# NSE Momentum Lab — Code Map

## Repository Layout

```
nse-momentum-lab/
├── src/nse_momentum_lab/        # Installable library (all importable code lives here)
│   ├── cli/                     # Click entrypoints (nseml-*)
│   ├── db/                      # Database access layer
│   ├── features/                # Feature registry + materializer
│   ├── services/
│   │   ├── backtest/            # Backtest engine
│   │   ├── kite/                # Kite Connect v4 integration
│   │   └── paper/               # Paper trading engine
│   ├── agents/                  # phidata research agents (read-only)
│   ├── api/                     # FastAPI REST endpoints (:8004)
│   └── utils/                   # Shared utilities
├── apps/nicegui/                # NiceGUI dashboard (:8501)
├── data/                        # Local data lake (gitignored)
├── db/init/                     # PostgreSQL init SQL (runs on first volume create)
├── scripts/                     # Operator and one-off scripts
├── tests/unit/                  # Fast unit tests (asyncio_mode = auto)
└── tests/integration/           # Longer tests requiring Docker services
```

---

## CLI Entrypoints (`src/nse_momentum_lab/cli/`)

| File | Command | Description |
|------|---------|-------------|
| `backtest.py` | `nseml-backtest` | Run backtests; `--preset` / `--list-presets` |
| `paper_v2.py` | `nseml-paper` | Paper trading: prepare / live / replay / status / flatten / archive |
| `kite_ingest.py` | `nseml-kite-ingest` | Ingest daily + 5-min OHLCV from Kite Connect |
| `build_features.py` | `nseml-build-features` | Rebuild `feat_daily` incrementally or fully |
| `market_monitor.py` | `nseml-market-monitor` | Refresh runtime monitor tables |
| `db_verify.py` | `nseml-db-verify` | Lightweight DB coverage check |
| `hygiene.py` | `nseml-hygiene` | Data quality scan (11 checks) |
| `kite_token.py` | `nseml-kite-token` | Manage Kite access token lifecycle |

---

## Database Layer (`src/nse_momentum_lab/db/`)

| File | Purpose |
|------|---------|
| `market_db.py` | DuckDB connection factory; view definitions; `FEAT_DAILY_QUERY_VERSION` |
| `models.py` | SQLAlchemy ORM models for PostgreSQL operational tables |
| `core.py` | PostgreSQL async engine + session factory |

---

## Features (`src/nse_momentum_lab/features/`)

| File | Purpose |
|------|---------|
| `registry.py` | Source of truth for feature sets, SQL/Python build logic, refresh policies |
| `materializer.py` | Orchestrates incremental `feat_daily` rebuilds; respects `FEAT_DAILY_QUERY_VERSION` |

**Key rule**: bump `FEAT_DAILY_QUERY_VERSION` in `market_db.py` whenever feature SQL changes — this triggers a rebuild on next `nseml-build-features` run.

---

## Backtest Engine (`src/nse_momentum_lab/services/backtest/`)

| File | Purpose |
|------|---------|
| `backtest_presets.py` | `StrategyPreset` frozen dataclass; 4 canonical presets |
| `filters.py` | 2LYNCH filter logic (N, Y, C, L, H) — Python + SQL clause builders |
| `signal_models.py` | `BacktestSignal` dataclass; `SignalMetadata` for gap, ATR, filter flags |
| `duckdb_backtest_runner.py` | Main DuckDB-backed runner; year-parallel execution |
| `vectorbt_engine.py` | vectorbt position engine (entry/exit arrays, R-ladder) |
| `intraday_execution.py` | 5-minute bar execution logic (entry trigger, stop fills) |
| `strategy_families.py` | Strategy family registry (breakout / breakdown families) |
| `strategy_registry.py` | Strategy name → family + preset resolution |
| `engine.py` | Backtest coordinator (dispatches to family + engine) |
| `progress.py` | `BufferedProgressWriter` — batched PostgreSQL progress writes |
| `walk_forward.py` | Walk-forward fold generation and gate validation |

**Experiment fingerprint** (`exp_id`) covers params + dataset + all files above except `progress.py`.

---

## Kite Integration (`src/nse_momentum_lab/services/kite/`)

| File | Purpose |
|------|---------|
| `auth.py` | `KiteAuth` singleton — token lifecycle, one token per process |
| `fetcher.py` | Historical OHLCV fetch (daily + 5-min); shared token-bucket rate limiter (~2.85 req/s) |
| `scheduler.py` | Orchestrates fetch → write → optional `feat_daily` update |
| `writer.py` | Parquet append with dedup |
| `streaming.py` | WebSocket tick stream for live intraday bars |
| `tradeable_master.py` | Kite instrument master → tradeable symbol list |

**Critical gotcha**: `KiteAuth` is a singleton per-process. If `KITE_ACCESS_TOKEN` changes in Doppler (new day), restart the process.

---

## Paper Trading Engine (`src/nse_momentum_lab/services/paper/`)

### Engine

| File | Purpose |
|------|---------|
| `engine/paper_runtime.py` | Core engine: 5-min candle processing, entry/exit, R-ladder, stop management |
| `engine/paper_session_driver.py` | Session lifecycle: start, bar loop, complete/error transitions |

### Database

| File | Purpose |
|------|---------|
| `db/paper_db.py` | DuckDB paper database: positions, signals, audit trail |
| `db/replica.py` | `ReplicaSync`: writes source → dashboard snapshot |
| `db/replica_consumer.py` | `ReplicaConsumer`: read-only dashboard access (avoids Windows DuckDB lock) |

### Notifiers

| File | Purpose |
|------|---------|
| `notifiers/alert_dispatcher.py` | Async queue, retry, best-effort delivery |
| `notifiers/telegram.py` | Telegram HTML formatter + TradingView chart button |
| `notifiers/email_notifier.py` | SMTP notifier (HTML + plaintext) |

### Scripts

| File | Purpose |
|------|---------|
| `scripts/paper_live.py` | Live market session orchestrator (calls Kite stream) |
| `scripts/paper_replay.py` | Historical replay orchestrator (reads stored 5-min bars) |

---

## DuckDB Catalog Split

Three DuckDB files with distinct roles:

| File | Role | Writers |
|------|------|---------|
| `data/market.duckdb` | Market views, `feat_daily`, dataset snapshots, materialization state | `nseml-build-features`, `nseml-market-monitor` |
| `data/backtest.duckdb` | Backtest engine writer catalog: `bt_experiment`, `bt_trade`, `bt_year_metrics` | Backtest engine only |
| `data/backtest_dashboard.duckdb` | Read-only snapshot for NiceGUI backtest pages | Refreshed from `backtest.duckdb` after each completed run |

**Why the split**: DuckDB is single-writer per file. The dashboard snapshot allows NiceGUI to remain open while backtests write results without lock contention.

---

## Key Runtime Tables (PostgreSQL)

| Table | Purpose |
|-------|---------|
| `signal` | Paper trading watchlist queue (loaded from DuckDB backtest signals) |
| `paper_position` | Open and closed paper positions |
| `paper_order` | Order records (entry/exit intents) |
| `paper_fill` | Fill records (confirmed executions) |
| `walk_forward_fold` | One row per walk-forward fold; used by promotion gate |
| `alert_log` | Dispatched alert history |

---

## Key DuckDB Views

| View | Source file | Description |
|------|-------------|-------------|
| `v_daily` | `market.duckdb` | Glob over `data/parquet/daily/*/kite.parquet` |
| `v_5min` | `market.duckdb` | Glob over `data/parquet/5min/*/YEAR.parquet` |
| `feat_daily` | `market.duckdb` | Materialised feature table (all 2LYNCH filter inputs) |
| `bt_experiment` | `backtest.duckdb` | One row per backtest run |
| `bt_trade` | `backtest.duckdb` | One row per trade in each experiment |
| `bt_year_metrics` | `backtest.duckdb` | Annual P&L summary per experiment |

---

## Utilities (`src/nse_momentum_lab/utils/`)

| File | Purpose |
|------|---------|
| `constants.py` | `FilterName`, `ExperimentStatus`, `EntryTimeframe` enums (all `StrEnum`) |
| `hash_utils.py` | `compute_short_hash()`, `compute_composite_hash()`, `compute_full_hash()` |
| `time_utils.py` | `normalize_candle_time()`, `minutes_from_nse_open()`, `nse_open_time()`, `nse_close_time()` |

---

## NiceGUI Dashboard (`apps/nicegui/`)

Routes:
- `/` — Overview / live session status
- `/paper_ledger` — Manual paper trading workflow (watchlist, positions, fills)
- `/backtest` — Backtest results browser
- `/walk_forward` — Walk-forward validation history and promotion gate
- `/data_quality` — DQ report viewer (reads `data/raw/kite/reports/dq_summary_latest.json`)

**NiceGUI note**: use `rows_per_page` (Quasar/JS convention), not `rowsPerPage`.

---

## Data Lake Layout (`data/`)

```
data/
├── market.duckdb
├── backtest.duckdb
├── backtest_dashboard.duckdb
├── parquet/
│   ├── daily/SYMBOL/kite.parquet       # Daily OHLCV per symbol
│   └── 5min/SYMBOL/YEAR.parquet        # 5-min OHLCV per symbol per year
├── raw/
│   ├── kite/daily/SYMBOL/*.csv         # Optional raw snapshots
│   ├── kite/5min/SYMBOL/*.csv
│   ├── kite/instruments/NSE.csv        # Instrument master cache
│   ├── kite/checkpoints/*.json         # Symbol-level ingest checkpoints
│   └── kite/reports/
│       └── dq_summary_latest.json      # Latest DQ report (+ timestamped snapshots)
└── NSE_EQUITY_SYMBOLS.csv              # EQ-series symbol filter for Kite instrument master
```
