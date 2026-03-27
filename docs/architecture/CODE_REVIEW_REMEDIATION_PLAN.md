# Implementation Plan: NSE Momentum Lab — Guard Rails, Performance, CLI, Docs

**Status**: Codex-reviewed, approved after 3 rounds (2026-03-27)
**Session**: `019d2ec0-acb3-7713-a7fb-ad5245b8a20a`
**Scope**: 5,114 lines changed across 50 files
**Key constraint**: None of these fixes require a `--force --allow-full-rebuild` pack rebuild. All are code-only changes.

---

## Phase 1 — Critical (prevent damage / credential leak)

### 1.1 Fix SQL Injection in live_watchlist.py
**File**: `src/nse_momentum_lab/services/paper/live_watchlist.py`
**Lines**: 35, 113, 122, 186, 241, 372, 375, 390, 398

**Problem**: Symbol names and dates are f-string interpolated into SQL across 9 locations.

**Fix**: Use the repo's existing placeholder expansion pattern (already used in `strategy_registry.py:64`, `market_db.py:1892`, `paper.py:828`). Build `placeholders = ", ".join("?" for _ in symbols)` and pass `params = [*symbols, ...]` to `con.execute(query, params)`. For dates and floats, use `?` placeholders. Do NOT use `UNNEST(?)` — the repo convention is simple `?` expansion.

**Testing**: symbols containing quotes, empty symbol lists, result-order parity for `build_operational_universe()`.

### 1.2 Fix timezone inconsistency in paper DB layer
**Files**: `src/nse_momentum_lab/db/paper.py:28-29`, `src/nse_momentum_lab/cli/paper.py:90`

**Problem**: `_utc_now()` in `db/paper.py` returns `datetime.now(IST)` — the name lies. PostgreSQL ORM columns are `DateTime(timezone=True)` (see `models.py:316,571`) and store UTC via `func.now()`. Comparing IST datetimes against these columns corrupts stale-session cutoffs and timestamp sorting.

**Fix**: Standardize internal code to **UTC**. Rename `_utc_now()` to `_now()` and change its body to `datetime.now(UTC)`. This makes all comparisons correct against the timezone-aware ORM columns. Keep ORM columns as `DateTime(timezone=True)` — do NOT change schema. Also fix `cli/paper.py:90` which has the same IST usage.

**Scope**: Change `_utc_now()` in `db/paper.py:28` to return UTC. Audit and fix any IST usage in `cli/paper.py` that feeds into DB comparisons.

**Testing**: stale-session cutoff accuracy, created/updated timestamp round-trips, verify cutoff boundary is 48 hours from now (not 48.23 hours).

### 1.3 Fix credential leak in CLI output
**File**: `src/nse_momentum_lab/services/paper/runtime.py:1007,1038`
**Files**: `src/nse_momentum_lab/cli/paper.py:1546,1662`

**Problem**: `build_feed_plan()` includes the full Kite websocket URL, which embeds the access token. The CLI prints the `feed_plan` dict payload to stdout, leaking the access token to logs/terminal history.

**Fix**: Strip the websocket URL and any `access_token` fields from `feed_plan` before CLI output. Add a `_redact_credentials(plan: dict) -> dict` helper that removes keys matching `access_token`, `kite_access_token`, and redacts URLs containing `access_token=`. Apply in CLI print paths only — runtime.py can keep the full URL internally for websocket connection.

**Testing**: Assert CLI/session JSON output never contains `access_token`, `kite_access_token`, or a websocket URL with credentials.

### 1.4 Add scheduler concurrency guard
**File**: `src/nse_momentum_lab/services/kite/scheduler.py:162-292`

**Problem**: `_run_ingestion()` has no lock. Two concurrent calls corrupt checkpoint files and trigger DuckDB single-writer violations on Windows.

**Fix**: Add a file-based mutex using `msvcrt.locking()` (Windows) that spans the **entire** `_run_ingestion()` critical section — including checkpoint load, symbol loop, checkpoint persist/clear, and `_refresh_features()`. Not just the symbol loop. Import conditionally so module still works on non-Windows.

**Testing**: In `test_scheduler.py`: held-lock rejection, release-on-exception, no double refresh.

### 1.5 Fix silent exception swallowing in live_watchlist.py
**File**: `src/nse_momentum_lab/services/paper/live_watchlist.py:245-248, 261-263`

**Problem**: Two bare `except Exception: result = pl.DataFrame()` blocks hide SQL errors.

**Fix**: Replace with `logger.exception(...)` with enough context to distinguish primary-query failure vs fallback failure. Keep the empty-DataFrame fallback behavior (fail-closed is correct for watchlist).

**Testing**: `caplog` assertions for both primary and fallback exception paths.

---

## Phase 2 — High Priority (performance / correctness)

### 2.1 Add indexes on modular feature tables (NOT feat_daily view)
**Files**: `src/nse_momentum_lab/features/daily_core.py`, `src/nse_momentum_lab/features/strategy_derived.py`

**Problem**: Candidate queries in `strategy_families.py` reference `feat_daily` which is now a backward-compatibility VIEW over `feat_daily_core`. Indexes on the view don't work — they must go on the underlying materialized tables.

**Fix**: Add covering indexes on `feat_daily_core` and `feat_2lynch_derived` for columns that are actually stored on those tables and used in candidate queries. Specifically: `close_pos_in_range` and `vol_dryup_ratio` (present on both tables — see `daily_core.py:262,280` and `strategy_derived.py:49,52`). Do NOT index `value_traded_inr` — it does not exist on feature tables; candidate queries compute it inline as `close * volume` from `v_daily` (see `strategy_registry.py:88`). Also benchmark whether candidate queries should reference the materialized tables directly instead of the `feat_daily` view.

**Testing**: `EXPLAIN ANALYZE` before/after on candidate queries, output-equivalence regression.

### 2.2 Remove misleading identity aliases in candidate_builder.py
**File**: `src/nse_momentum_lab/services/paper/candidate_builder.py:191-196`

**Problem**: 3 of 5 column aliases are identity mappings (dead code). The 2 semantic remaps (`h_quality -> n_score`, `freshness -> y_score`) exist for downstream `selection_components_json` compatibility.

**Fix**: Keep the 2 semantic remaps, remove the 3 identity aliases, add a comment explaining why the remap exists.

**Testing**: Breakdown `selection_components_json` output stays stable.

### 2.3 Add CLI safeguards with isatty() awareness (backtest + walk-forward)
**Files**: `src/nse_momentum_lab/cli/backtest.py`, `src/nse_momentum_lab/cli/paper.py`

**Problem**: No confirmation for large runs. With backtest defaults (`--universe-size 500 --start-year 2015 --end-year 2025`), cost is `11 * 500 = 5500`. Walk-forward at `paper.py:1708` already computes exact fold counts but prints no summary or confirmation.

**Fix**:
- **Backtest** (`backtest.py`): Add `_estimate_cost()` printing the execution plan. If `sys.stdin.isatty()` and cost exceeds threshold, prompt `Continue? [y/N]`. Add `--yes` flag to skip prompt for CI/scripts.
- **Walk-forward** (`paper.py`): After fold computation at `paper.py:1708`, print fold count summary. If `sys.stdin.isatty()` and folds > 20, prompt confirmation. Add `--yes` flag.

**Testing**: CLI unit tests for TTY vs non-TTY, `--yes` flag behavior, fold count threshold for walk-forward.

### 2.4 Add Kite backfill cost estimation (correct model)
**File**: `src/nse_momentum_lab/cli/kite_ingest.py`

**Problem**: `--backfill` can trigger thousands of API calls with no time estimate. The cost model must account for daily vs 5-min ingestion modes.

**Fix**: Estimate AFTER symbol resolution. For daily mode: `symbols * 1 request * RATE_LIMIT_DELAY`. For 5-min mode: `symbols * ceil(days / 60) chunks * RATE_LIMIT_DELAY`. Add note that retries/backoff make actual time approximate. Warn if estimated time > 5 minutes.

**Testing**: CLI warnings for daily vs 5-minute modes with known symbol counts.

---

## Phase 3 — Medium Priority (efficiency / UX)

### 3.1 Batch DataFrame column operations in backtest runner and candidate_builder
**Files**: `duckdb_backtest_runner.py:1276-1298`, `candidate_builder.py:58-60`

**Problem**: Multiple separate `with_columns()` calls create unnecessary DataFrame copies.

**Fix**: Build all expressions in a dict, apply in single `with_columns(**expressions)`. Apply same batching in `runtime.py:345` so paper and backtest paths stay aligned.

**Testing**: Output-equivalence and schema-equivalence before/after refactor.

### 3.2 Add broker order execution guard
**Files**: `src/nse_momentum_lab/services/kite/client.py:175`, `src/nse_momentum_lab/config.py:30`

**Problem**: Current live path is paper-only (no real broker calls), but `place_order()` at `client.py:175` is fully functional. A future integration change could accidentally enable real orders.

**Fix**:
1. Add `broker_order_execution_enabled: bool = False` to `Settings` in `config.py:30`.
2. Add a guard at `client.py:175` that raises `RuntimeError("Broker order execution is disabled. Set BROKER_ORDER_EXECUTION_ENABLED=true to enable.")` when `broker_order_execution_enabled` is False.
3. This does NOT affect current paper-only flows since `place_order()` is not called in the paper runtime path.

**Testing**: Assert `place_order()` hard-fails by default. Assert it proceeds when `BROKER_ORDER_EXECUTION_ENABLED=true` is set.

---

## Phase 4 — Documentation

### 4.1 Update stale command references in docs (repo-wide)
**Files**: All `.md` files containing `python -m nse_momentum_lab.cli` or `services.*.worker`

**Problem**: Docs use `python -m nse_momentum_lab.cli.*` instead of installed `nseml-*` commands. Stale `services.*.worker` references. Note: `--allow-full-rebuild` is already documented in `agents.md:210` and `COMMANDS.md:265`.

**Fix**: `rg` repo-wide for all `python -m nse_momentum_lab` and `services.*.worker` patterns and replace with installed CLI names. Scope is repo-wide, not just the 4 listed files.

### 4.2 Add missing CLI documentation
**File**: `docs/reference/COMMANDS.md`

**Problem**: `nseml-backtest`, `nseml-backtest-batch`, `nseml-backtest-status` are not documented. Missing parameters for paper trading.

**Fix**: Add documentation referencing the installed CLI names defined in `pyproject.toml:49-55`.

---

## Constraints
- Windows 11 environment — no POSIX file locking (use `msvcrt.locking()`)
- DuckDB is single-writer on Windows — no parallel backtest runs
- Must not break existing experiment cache (version bumps invalidate correctly)
- The project uses `doppler run -- uv run` for all CLI commands
- Kite historical ingestion uses shared token-bucket pacing near the documented 3 req/sec cap
- Backward compatibility: IntradayEntry TypedDict shape must not change
- `feat_daily` is a backward-compatibility VIEW, not a materialized table — indexes must target `feat_daily_core` / `feat_2lynch_derived`
- PostgreSQL ORM columns are `DateTime(timezone=True)` — keep timezone-aware, standardize code to UTC
- None of these fixes require a `--force --allow-full-rebuild` pack rebuild

---

## Codex Review History

### Round 1 — VERDICT: REVISE
- feat_daily indexes targeted wrong table
- _utc_now() fix proposed wrong schema change
- Missed credential leak in CLI output
- Kite backfill cost model wrong
- pipeline.py already rejects future dates
- CLI guards need isatty() handling
- UNNEST(?) unnecessary — repo uses placeholder expansion

### Round 2 — VERDICT: REVISE
- value_traded_inr not on feature tables — removed from index plan
- Walk-forward safeguard missing from plan — added paper.py item
- Broker guard config field doesn't exist — added to config.py
- Timezone standardization unclear — explicitly chose UTC

### Round 3 — VERDICT: APPROVED
- All prior issues resolved
- 2 minor nits (wording only, no implementation impact)
