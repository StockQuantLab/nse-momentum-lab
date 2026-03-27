# Table Load Matrix

This is the operator reference for common data and feature tables.
Use this instead of reading code for routine load behavior.

## DuckDB catalogs

### `data/market.duckdb`

| Table | Purpose | Load mode | Command |
|---|---|---|---|
| `feat_daily_core` | Strategy-agnostic daily feature base | Full or incremental | `nseml-build-features --feature-set daily_core` or `--since YYYY-MM-DD` |
| `feat_intraday_core` | 5-minute intraday feature base | Full or incremental | `nseml-build-features --feature-set intraday_core` or `--since YYYY-MM-DD` |
| `feat_event_core` | Event feature scaffold | Full or incremental | `nseml-build-features --feature-set event_core` or `--since YYYY-MM-DD` |
| `feat_2lynch_derived` | 2LYNCH strategy-derived features | Full or incremental | `nseml-build-features --feature-set 2lynch` or `--since YYYY-MM-DD` |
| `feat_daily` | Backward-compatible view over core features | Derived view | Recreated automatically after feature builds |
| `market_monitor_daily` | Regime / breadth snapshot table | Full or incremental | `nseml-market-monitor` or `nseml-market-monitor --incremental --since YYYY-MM-DD` |

### `data/backtest.duckdb`

| Table | Purpose | Load mode | Command |
|---|---|---|---|
| `bt_experiment` | Backtest run metadata and metrics | Append per run | `nseml-backtest`, `nseml-paper walk-forward` |
| `bt_trade` | Backtest trades | Append per run | Same as above |
| `bt_yearly_metric` | Per-year metrics | Append per run | Same as above |
| `bt_execution_diagnostic` | Execution audit rows | Append per run | Same as above |
| `bt_dataset_snapshot` | Source dataset lineage | Auto-managed | Same as above |
| `bt_materialization_state` | Feature build state | Auto-managed | Same as above |

### `data/backtest_dashboard.duckdb`

| Table | Purpose | Load mode | Command |
|---|---|---|---|
| `bt_experiment`, `bt_trade`, `bt_yearly_metric`, `bt_execution_diagnostic` | Read-only dashboard snapshot | Refreshed from `backtest.duckdb` | `MarketDataDB.refresh_backtest_read_snapshot()` is called after backtest completion |

## PostgreSQL operational tables

| Table | Purpose | Load mode | Command |
|---|---|---|---|
| `paper_session` | Paper / walk-forward session state | App-managed | `nseml-paper ...` |
| `walk_forward_fold` | Walk-forward fold results | App-managed | `nseml-paper walk-forward` |
| `signal`, `paper_order`, `paper_fill`, `paper_position` | Paper execution ledger | App-managed | `nseml-paper ...` |
| `job_run` | Background job tracking | App-managed | Pipeline workers |
| `dataset_manifest`, `partition_manifest`, `incremental_refresh_state` | Incremental lineage and partition tracking | App-managed | Ingest / refresh services |

## Load rules

1. Use `--since YYYY-MM-DD` for catch-up windows after a short data append.
2. Use `--force --allow-full-rebuild` only when you intentionally want a full rebuild.
3. Run `nseml-db-verify` after ingest and feature refresh to confirm the runtime tables are current.
4. For walk-forward, refresh in this order:
   `nseml-kite-ingest` -> `nseml-build-features --since YYYY-MM-DD` -> `nseml-market-monitor --incremental --since YYYY-MM-DD`.

## Where operators should look

1. `docs/operations/DATA_APPEND_GUIDE.md` for the append workflow.
2. `docs/reference/COMMANDS.md` for CLI command references.
3. `agents.md` and `docs/dev/AGENTS.md` for agent-facing operating rules.
