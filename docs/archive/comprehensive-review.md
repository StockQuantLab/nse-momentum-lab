# Comprehensive Project Review: NSE Momentum Lab

**Date:** 2026-02-28
**Reviewed by:** Code Reviewer, Security Auditor, Performance Optimizer, Test Engineer, DevOps Engineer

---

## Updated Notes

- **Environment:** the project now targets **Python 3.14 (only)** and **Postgres 18 (only)**; other runtimes have been removed from the repo and documentation.
- **DuckDB state:** stale experiments/NDJSON progress files were deleted and the dashboard selector now surfaces every completed run (single-year or batch), while the 2015‑2024 batch keeps clean history in DuckDB.
- **UI width policy:** Streamlit pages switched from `use_container_width` to `width="stretch"` so they remain compatible once the old parameter is dropped after 2025-12-31.
- **Latest run:** The 2015-2024 experiment (`21d35d9b903b7921`) completed with a 840.67% total return, 84.07% annualized, 9,261 trades, and all yearly metrics positive; NDJSON heartbeats now live under `data/progress/2015-2024_run_20260228_153930.ndjson`.
- **Quality gates:** `.pre-commit-config.yaml` installs hooks that run `scripts/quality_gate.py` (format + linters pre-commit, full suite pre-push), ensuring lint/test coverage before staging.


---

## Overall Verdict: NOT YET READY for clean initial commit

There are blocking issues that should be fixed first. Below is everything organized by priority.

---

## CRITICAL — Fix Before Commit

### 1. SQL Injection in DuckDB Queries

**Files:** `src/nse_momentum_lab/db/market_db.py:876,939,957`, `src/nse_momentum_lab/api/app.py:175-182`, `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py:505,600`

Symbol lists and column names are string-interpolated directly into SQL. The `columns` parameter in `query_5min`/`query_daily` has zero sanitization. The `analytics_coverage` API endpoint takes user input from `symbols_csv` and interpolates it.

```python
# BAD — current pattern everywhere
symbol_list = ",".join(f"'{s}'" for s in symbols)
f"WHERE symbol IN ({symbol_list})"

# FIX — use parameterized queries
placeholders = ",".join("?" for _ in symbols)
f"WHERE symbol IN ({placeholders})", [*symbols, ...]
```

Also vulnerable:
- `query_daily_multi` (line 876) — symbol list interpolated via f-string
- `get_features_range` (line 939) — same pattern
- `get_avg_dollar_vol_20_by_symbol` (line 957) — same pattern
- `_table_exists` (line 924) — table name interpolated
- `_ensure_column` (line 218) — table/column names interpolated
- `analytics_coverage` endpoint (api/app.py:175-182) — dates also injected as bare string literals

For column names, validate against an allowlist of known column names before interpolation.

---

### 2. Debug Scripts Leak Credentials

**Files:** `scripts/debug_db_url.py`, `scripts/debug_sqlalchemy_url.py`, `scripts/debug_url_create.py`, `scripts/debug_password.py`, `scripts/test_db_conn.py`

These print raw `postgres_password` to stdout and are tracked in git. If CI logs or terminal output are captured, credentials are exposed.

**Fix:** Delete all `debug_*` scripts from the repository before first commit. If debug tools are needed, use the existing `get_masked_database_url()` method and never print raw credentials.

---

### 3. CI Workflow Issues

**File:** `.github/workflows/ci.yml`

- MinIO `server /data` is in `options:` instead of container command — MinIO won't start in CI
- `uv sync` without `--group dev` won't install pytest, ruff, or mypy
- Missing `ruff format --check` step (pre-commit checks it but CI doesn't)
- `UV_SYSTEM_PYTHON: 1` disables uv's virtual environment isolation

---

## HIGH — Fix Before or Soon After Commit

### 4. API Authentication Disabled by Default

**File:** `src/nse_momentum_lab/api/security.py:201`

`API_KEY_REQUIRED` defaults to `"false"`. All API endpoints including `POST /api/pipeline/run` (which triggers data pipelines) are accessible without authentication.

**Fix:** Add a startup warning when auth is disabled:
```python
if not config["require_api_key"]:
    logger.warning("API key authentication is DISABLED. Set API_KEY_REQUIRED=true for production.")
```

---

### 5. Bare `except Exception:` Swallowing Errors

**File:** `src/nse_momentum_lab/db/market_db.py:150,523,589,926,989,1026,1040,1051`

Multiple critical paths catch `Exception` and return `False` or `0` with no log message. A corrupted DuckDB file, permissions error, or schema mismatch will silently produce wrong results.

```python
# BAD — current (silent failure)
def _table_exists(self, table: str) -> bool:
    try:
        self.con.execute(f"SELECT COUNT(*) FROM {table}")
        return True
    except Exception:
        return False

# FIX — catch specific exceptions, log the rest
def _table_exists(self, table: str) -> bool:
    try:
        self.con.execute(f"SELECT COUNT(*) FROM {table}")
        return True
    except duckdb.CatalogException:
        return False
    except Exception:
        logger.warning("Unexpected error checking table '%s'", table, exc_info=True)
        return False
```

Also affected: `vectorbt_engine.py:637` (sortino ratio), `duckdb_backtest_runner.py:1014` (5-min candle unregister).

---

### 6. Inconsistent API Error Responses

**File:** `src/nse_momentum_lab/api/app.py:511,572,606` vs `706,736`

Some endpoints return `{"error": "Not found"}` with HTTP 200, others properly raise `HTTPException(404)`. Clients cannot write a single error-handling branch.

```python
# BAD — returns 200 with error payload
return {"error": "Experiment not found"}

# FIX — raise proper HTTP error
raise HTTPException(status_code=404, detail="Experiment not found")
```

---

### 7. N+1 Query Patterns (Performance)

| Location | Issue | Fix |
|----------|-------|-----|
| `duckdb_features.py:100` | One `query_daily()` per symbol (500 queries for 500 symbols) | Use existing `query_daily_multi()` |
| `vectorbt_engine.py:129` | Same N+1 in `load_market_data_from_duckdb` | Use `query_daily_multi()` |
| `api/app.py:675` | N+1 metric queries in `compare_experiments` | Batch with `.in_()` |

These are the single largest performance bottleneck. Fixing 3 call sites eliminates ~500 redundant DuckDB queries per scan/backtest run.

---

### 8. Rate Limiter Vulnerabilities

**File:** `src/nse_momentum_lab/api/security.py:52-55`

- Trusts spoofable `X-Forwarded-For` header — attacker can bypass rate limiting by rotating the header value
- In-memory dict grows unbounded — every unique IP creates a new entry that is never evicted (memory DoS)

**Fix:** Only trust `X-Forwarded-For` behind a known proxy (add `TRUST_PROXY` config). Add LRU eviction or TTL cleanup to the rate limiter dict.

---

### 9. Missing `__init__.py` Files

These directories lack `__init__.py`, creating fragile namespace package behavior:

| Missing file | Impact |
|---|---|
| `src/nse_momentum_lab/agents/__init__.py` | `agents` sub-package not properly importable |
| `src/nse_momentum_lab/services/__init__.py` | `services` not a proper package |
| `tests/unit/backtest/__init__.py` | pytest collection may fail |
| `tests/unit/cli/__init__.py` | pytest collection may fail |
| `tests/unit/services/ingest/__init__.py` | pytest collection may fail |
| `tests/unit/services/dataset/__init__.py` | pytest collection may fail |
| `tests/integration/__init__.py` | pytest collection may fail |
| `apps/dashboard/__init__.py` | namespace package fragility |
| `apps/api/__init__.py` | namespace package fragility |

---

### 10. `.gitignore` Missing Patterns

Add the following:
```gitignore
# DuckDB WAL / lock files
*.wal
*.lock.db

# Node (playwright CLI tests)
node_modules/
package-lock.json

# Coverage base file
.coverage

# Claude Code local settings
.claude/settings.local.json

# uv build artifacts
*.egg-info/
```

Currently `.claude/settings.local.json` is staged for commit — it should be gitignored.

---

### 11. Look-Ahead Bias in Universe Selection

**File:** `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py:466-476`

Liquidity ranking hardcoded to `2018-01-01` to `2024-12-31` regardless of backtest date range. A 2015-2017 backtest ranks stocks by future (2018-2024) liquidity — this inflates backtest returns.

**Fix:** Derive the liquidity window from `params.start_year` / `params.end_year`.

---

### 12. No CSRF Protection on State-Changing Endpoints

**File:** `src/nse_momentum_lab/api/app.py`

`POST /api/pipeline/run` triggers full data pipeline. No CSRF token, no Origin header check, no CORS policy configured. A malicious page could trigger pipeline runs.

**Fix:** Add CORS middleware with explicit origin allowlist:
```python
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],  # Streamlit
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key"],
)
```

---

## MEDIUM — Should Fix

### 13. `_build_exit_signals` Performance

**File:** `src/nse_momentum_lab/services/backtest/vectorbt_engine.py:307-467`

Pure Python nested loop with `DataFrame.loc` per bar. For 500 symbols x 250 signals/year x 5 days = 625,000 `.loc` calls. Switching to pre-extracted NumPy arrays gives 5-10x speedup.

```python
# FIX — extract arrays before loop
close_arr = close_df.values
high_arr = high_df.values
col_index = {col: i for i, col in enumerate(close_df.columns)}
# Then use: high_arr[row_idx, col_i] instead of high_df.loc[date, symbol]
```

---

### 14. `prepare_signals` Linear Date Search

**File:** `src/nse_momentum_lab/services/backtest/vectorbt_engine.py:237-246`

O(n_dates) search for every signal. For 2,500 trading days x 500 signals = 1.25M date comparisons.

```python
# FIX — build index once
date_to_idx = {ts.date(): i for i, ts in enumerate(price_data.index)}
dt_idx = date_to_idx.get(signal_date)
```

---

### 15. Feature Materialization Row-by-Row

**File:** `src/nse_momentum_lab/services/scan/duckdb_features.py:41-83`

Inner `df.filter(...)` re-scans for each symbol (O(N symbols x total rows)), and `iter_rows(named=True)` forces row-by-row Python dispatch.

**Fix:** Use `group_by` + `to_dicts()` instead of per-symbol filter + iter_rows.

---

### 16. 191 `print()` Calls in Library Code

**Files:** `market_db.py` (14), `duckdb_backtest_runner.py` (22), and others

Library modules should use `logging` so operators can control verbosity. CLI scripts can keep `print()`.

---

### 17. Model Type Annotation Mismatches

**File:** `src/nse_momentum_lab/db/models.py:176-177`

`ScanRun.started_at` typed as `Mapped[date | None]` but column is `DateTime(timezone=True)`. Same mismatch on `ScanDefinition`, `CaEvent`, `Signal`, `PaperOrder`, `PaperFill`, `PaperPosition`, `JobRun`, `BtTrade` for `created_at` fields. Should be `Mapped[datetime | None]`.

---

### 18. Falsy Check on Float Fields

**File:** `src/nse_momentum_lab/services/backtest/vectorbt_engine.py:162-174`

```python
# BAD — 0.0 (valid return) becomes None
"ret_1d": float(row["ret_1d"]) if row["ret_1d"] else None,

# FIX
"ret_1d": float(row["ret_1d"]) if row["ret_1d"] is not None else None,
```

Affects: `ret_1d`, `ret_5d`, `atr_20`, `range_pct`, `close_pos_in_range`, `ma_20`, `ma_65`, `rs_252`, `vol_20`, `dollar_vol_20`.

---

### 19. Unused `series` API Parameter

**File:** `src/nse_momentum_lab/api/app.py:145,207,360`

Three endpoints declare `series: str = "EQ"`, validate it, but never use it in queries. Either wire it into the query or remove it.

---

### 20. `DataLakeConfig.from_env()` Duplicates `Settings` Logic

**File:** `src/nse_momentum_lab/db/market_db.py:49-87`

Re-implements the same env var parsing as `Settings.model_post_init()` in `config.py`. Any change must be made in two places.

**Fix:** Construct `DataLakeConfig` from `Settings` instead.

---

### 21. Mypy Configuration Suppresses Most Type Checking

**File:** `pyproject.toml:119-142`

Disables 10 error codes including `arg-type`, `attr-defined`, `call-arg`, `assignment`, `return-value`. For a financial computation engine, type errors have real monetary consequences. Enable incrementally starting with `arg-type` and `return-value`.

---

### 22. Pre-commit Depends on Doppler

**File:** `.pre-commit-config.yaml`

```yaml
entry: doppler run -- uv run python scripts/quality_gate.py --with-format-check
```

Breaks on any machine without Doppler installed. The quality gate doesn't need secrets to run lint/format/unit tests. Decouple from Doppler.

---

### 23. `analytics/returns` Endpoint Pure Python Computation

**File:** `src/nse_momentum_lab/api/app.py:238-266`

Loads all OHLCV rows, re-filters per symbol in Python, computes returns in a Python loop. Push the entire calculation into a single DuckDB SQL query with window functions.

---

### 24. `playwright` in Core Dependencies

**File:** `pyproject.toml`

Heavy browser automation package listed under core dependencies. Only used by `tests/playwright-cli/`. Move to `[dependency-groups] dev`.

---

### 25. `get_status()` Full Table Scan

**File:** `src/nse_momentum_lab/db/market_db.py:1003-1041`

Calls `get_dataset_snapshot()` which fires `COUNT(*)/COUNT(DISTINCT symbol)/MIN(date)/MAX(date)` full scans on both `v_daily` and `v_5min` on every call.

**Fix:** Cache the snapshot in the instance with invalidation on data changes.

---

### 26. Missing `Content-Security-Policy` Header

**File:** `src/nse_momentum_lab/api/security.py:144-148`

Security middleware sets `X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection`, `Strict-Transport-Security` but omits `Content-Security-Policy`.

---

### 27. `_emit_progress` Takes 11 Parameters

**File:** `src/nse_momentum_lab/services/backtest/duckdb_backtest_runner.py:408-456`

Called 8 times with 6 identical context args repeated verbatim. Extract static context into a frozen dataclass.

---

### 28. `asyncio.create_task` Without Result Tracking

**File:** `src/nse_momentum_lab/api/app.py:829`

Fire-and-forget task with race condition — the job ID lookup after `create_task` may run before the task has written a `JobRun` row.

---

### 29. Duplicate MIN/MAX Window Computations in feat_daily CTE

**File:** `src/nse_momentum_lab/db/market_db.py:764-766`

`range_percentile` computes `MIN(close)` and `MAX(close)` over the same 252-day window twice. Extract as named expressions.

---

## LOW — Nice to Have

| # | Issue | File |
|---|-------|------|
| 30 | `apps/api/app.py` is an empty 1-line stub | `apps/api/app.py` |
| 31 | `ExitReason.WEAK_FOLLOW_THROUGH` is dead enum member | `services/backtest/engine.py:12` |
| 32 | Global `_db` singleton has no thread safety lock | `market_db.py:1066-1081` |
| 33 | `minio/minio:latest` tag in docker-compose (should pin version) | `docker-compose.yml:32` |
| 34 | Agent model ID hardcoded | `agents/agent.py:60` |
| 35 | Dashboard calls `check_api_health` on every Streamlit rerender | `apps/dashboard/utils.py:27` |
| 36 | `import traceback` inside except block instead of top-level | `vectorbt_engine.py:771` |
| 37 | Duplicate comment in config.py | `config.py:70-72` |
| 38 | `check_volume_increase` compares 20d avg to 20d avg (near-identical values) | `services/scan/rules.py:161` |
| 39 | Chat input rendered as markdown without sanitization | `apps/dashboard/pages/01_Chat.py:66` |
| 40 | Agent tools pass dates to subprocess via `-c` code generation | `agents/tools/pipeline_tools.py:415` |
| 41 | Hardcoded `"2025-12-31"` as API date default | `api/app.py:172-173` |
| 42 | CI uses hardcoded MinIO default credentials `minioadmin/minioadmin` | `.github/workflows/ci.yml` |

---

## Test Suite Health

### Summary

| Metric | Status |
|--------|--------|
| Unit tests | **210 pass**, 0 fail |
| Integration tests | Auto-skip when services unavailable |
| Test structure | Well-organized AAA pattern |

### Critical Coverage Gaps

| Source Module | Risk Level | Notes |
|---|---|---|
| `api/validation.py` | HIGH | 14 pure functions, zero tests |
| `api/security.py` | HIGH | RateLimiter, SecurityMiddleware — zero tests |
| `db/market_db.py` | HIGH | Primary DuckDB layer — no dedicated test |
| `services/scan/duckdb_features.py` | HIGH | DuckDBFeatureEngine — no tests |
| `services/scan/duckdb_signal_generator.py` | HIGH | Core signal generation — no tests |
| `services/scan/legacy_2lynch_signal_generator.py` | HIGH | Legacy signal generator — no tests |
| `services/risk/position_sizing.py` | HIGH | Position sizing logic — no tests |
| `services/backtest/optimizer.py` | MEDIUM | ParameterGrid, ParameterOptimizer — no tests |
| `services/backtest/sensitivity.py` | MEDIUM | SensitivityResult — no tests |

### Test Quality Issues

- **Tautological assertions:** `assert len(result.trades) >= 0` (can never fail), `assert registry is not None`
- **Weak integration assertions:** `assert response.status_code in [404, 400, 200]` — accepts any response
- **Mock target wrong:** `test_app.py` patches `nse_momentum_lab.db.market_db.get_market_db` instead of `nse_momentum_lab.api.app.get_market_db`
- **Weak scheduler test:** `test_stop_scheduler` has no assertions
- **No shared fixtures:** Every unit test builds its own `DailyFeatures` objects inline
- **11 ad-hoc integration tests** run in default `pytest` invocation with no marker filtering; they pass with 0 signals if data is missing

### CI Test Issues

- No `pytest --ignore=tests/integration` or marker-based filtering separates unit from integration
- No coverage reporting (`--cov` flag absent)
- Ad-hoc integration tests will silently false-positive in CI (no Parquet data present)

---

## Positive Findings

1. **Docker ports bound to localhost** — all port mappings use `127.0.0.1:` binding
2. **Secrets via Doppler** — no `.env` files in production, reads from environment variables
3. **`.gitignore` covers sensitive files** — `.env`, `.env.*`, `.streamlit/secrets.toml`, `data/`
4. **No hardcoded API keys or tokens** — grep found zero hardcoded secrets
5. **SQLAlchemy ORM for PostgreSQL** — proper parameterization for Postgres layer
6. **Input validation module exists** — `validation.py` provides symbol sanitization, date validation
7. **Password masking in logs** — `config.py` provides `_mask_password()` and `get_masked_database_url()`
8. **Resource limits on Docker containers** — CPU and memory limits configured
9. **Integration test auto-skip** — properly gated behind service availability checks
10. **Well-structured backtest config** — 2LYNCH strategy parameters documented and regression-tested

---

## Scripts Directory Cleanup

The `scripts/` directory has 60+ files mixing production scripts with development throwaway files. Recommended cleanup before first commit:

**Delete (debug/throwaway):**
- `debug_db_url.py`, `debug_password.py`, `debug_sqlalchemy_url.py`, `debug_url_create.py`
- `test_db_conn.py`, `test_db_module.py`, `test_direct_conn.py`, `test_fresh_db.py`
- `test_ingest_sample.py`, `test_ingest_simple.py`, `test_sqlalchemy_conn.py`, `test_sqlalchemy_working.py`
- `test_vbt_simple.py`, `compare_urls.py`, `quick_check.py`
- `check_data.py`, `check_db_detailed.py`, `check_db_state.py`, `check_raw_db.py`

**Keep (production/utility):**
- `minio_init.sh`, `quality_gate.py`, `ingest_vendor_candles.py`
- `download_zerodha_daily.py`, `download_zerodha_data.py`
- `run_backtest.py`, `run_backtest_duckdb.py`, `run_adjustment.py`
- `backtest_*.py` (analysis scripts)
- `generate_html_report.py`, `trade_analysis.py`

---

## Recommended Action Plan

### Phase 1 ✅ COMPLETE

- [x] Delete debug scripts that leak credentials (removed the identified debug/test helpers).
- [x] Fix CI workflow (minio service command, --group dev, format check, UV_SYSTEM_PYTHON removed)
- [x] Parameterize all SQL queries (SQL injection fixes in market_db.py, app.py, duckdb_backtest_runner.py)
- [x] Add missing __init__.py files (created for tests/unit/api, tests/unit/services/risk)
- [x] Update .gitignore with missing patterns and node artifacts.
- [x] Remove .claude/settings.local.json from staging

### Phase 2 ✅ COMPLETE

- [x] Fix bare except clauses (added specific exceptions + logging)
- [x] Standardize API error responses (HTTPException with proper status codes)
- [x] Fix N+1 query patterns (batch queries in duckdb_features.py, vectorbt_engine.py, app.py)
- [x] Fix look-ahead bias (uses backtest date range for liquidity ranking)
- [x] Fix falsy float checks (is not None instead of falsy)
- [x] Add API auth startup warning (logs when disabled)
- [x] Add CORS middleware (CORS_ALLOWED_ORIGINS env var)

### Phase 3 ✅ COMPLETE

- [x] Replace `print()` with `logging` in library code (market_db.py, duckdb_backtest_runner.py)
- [x] Add tests for `validation.py` (35 test cases), `security.py` (22 test cases), `position_sizing.py` (25 test cases)
- [x] Performance: NumPy arrays in `_build_exit_signals` (5-10x speedup)
- [x] Performance: dict lookup in `prepare_signals` (O(n) → O(1) lookups)
- [x] Fix model type annotations (date → datetime for DateTime columns)
- [x] Decouple pre-commit from Doppler (removed doppler run wrapper)
- [x] Move `playwright` to dev dependencies (moved to [dependency-groups] dev)

### Phase 4 ✅ COMPLETE

- [x] Remove dead enum member `ExitReason.WEAK_FOLLOW_THROUGH` (from engine.py, validation.py, test_engine.py)
- [x] Remove duplicate comment in config.py
- [x] Fix import inside except block in vectorbt_engine.py (moved traceback to top)
- [x] Remove empty apps/api/app.py stub
- [x] Add Content-Security-Policy header to security middleware
- [x] Remove unused `defaultdict` import from security.py
- [x] Fix ALLOWED_COLUMNS to lowercase in market_db.py
