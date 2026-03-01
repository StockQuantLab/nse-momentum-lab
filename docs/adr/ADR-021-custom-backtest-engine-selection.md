# ADR-021: VectorBT as Backtest Engine + DuckDB Result Storage

Status: Accepted (Updated 2026-02-25)

## Context

ADR-003 designated vectorbt as the canonical backtesting engine. ADR-021 originally superseded it with a custom engine. However, the custom engine was never completed, and the VectorBT-based implementation proved reliable over the 2015‑2024 decade (840.67% total return, 9,261 trades).

## Decision

Use **VectorBT** as the backtest execution engine and **DuckDB** for result storage.

### Engine: VectorBT (`vectorbt_engine.py`)

- Handles portfolio simulation, position sizing, entry/exit mechanics
- Custom `_build_exit_signals()` implements the layered stop logic (initial -> breakeven -> trail -> time stop)
- Proven on 11-year backtest across 500+ symbols

### Storage: DuckDB (`market_db.py`)

Three tables in `data/market.duckdb`:

- `bt_experiment` â€” one row per run, SHA-256 hash PK for deduplication
- `bt_trade` â€” individual trades with symbol, prices, PnL, exit reason
- `bt_yearly_metric` â€” pre-computed yearly aggregates

### Orchestration: `DuckDBBacktestRunner`

- `BacktestParams` dataclass defines all parameters with `to_hash()` for dedup
- Year-by-year execution loop (proven memory-efficient approach)
- Signal generation via DuckDB SQL (same proven query as standalone script)

## Rationale

1. **VectorBT works**: 840.67% return over 10 years validated. No need to replace a working engine.
2. **DuckDB is already the data layer**: Market data lives in DuckDB/Parquet. Storing results there avoids cross-database dependencies and keeps PostgreSQL optional.
3. **Deduplication via hash**: Same params = same hash = skip re-run. Fast iteration.
4. **Dashboard reads DuckDB directly**: No API server needed for backtest visualization.

## When to Reconsider

- If VectorBT becomes unmaintained or incompatible with newer Python versions
- If we need intraday tick-by-tick simulation (VectorBT may be too slow)
- If we need distributed backtesting across multiple machines

## Consequences

- VectorBT is a required dependency
- Results are local to the machine (in DuckDB file)
- The standalone script (`backtest_10year_fixed.py`) is superseded by the packaged CLI `nseml-backtest` (`src/nse_momentum_lab/cli/backtest.py`)
