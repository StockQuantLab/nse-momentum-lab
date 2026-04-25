# Deployment & Rollback Guide — CPR Parity Improvements

Reference for deploying each phase of the CPR Parity Improvement Plan safely.

---

## Rollback Procedures

### General revert strategy

```bash
# 1. Identify commits for the phase
git log --oneline --grep="Phase N" | head -5

# 2. Revert in reverse order (newest first)
git revert --no-commit <commit-sha-3>
git revert --no-commit <commit-sha-2>
git revert --no-commit <commit-sha-1>
git commit -m "Revert Phase N: <reason>"

# 3. Validate
doppler run -- uv run pytest tests/unit/ -q
doppler run -- uv run nseml-db-verify
```

### Phase-specific rollback notes

| Phase | Revert concern | Recovery steps |
|-------|---------------|----------------|
| Phase 1 (Shared Eval) | Structural refactoring touching backtest + paper | Revert, then re-run `nseml-build-features --force --allow-full-rebuild`. Backtest `exp_id` values will change on revert (code_hash includes module paths) — this is expected. |
| Phase 2 (Command Lock) | Lock files in `.tmp_logs/` | Safe to revert. Delete stale `.lock` files manually: `rm .tmp_logs/*.lock .tmp_logs/*.lock.info` |
| Phase 3 (Live Hardening) | Pre-market connect, graceful shutdown | Safe to revert. No persistent state changes. |
| Phase 4 (Feed Audit Replay) | New `--pack-source` flag on replay | Safe to revert. Default `pack_source="market"` keeps existing behavior. |
| Phase 5 (Alert Retry) | Retry constants changed | Safe to revert. Alert dispatcher is stateless. |
| Phase 6 (Batch Sync) | `begin_batch()`/`end_batch()` on replica | Safe to revert. Methods are additive, no schema changes. |
| Phase 7 (Parity Trace) | Env-gated logging only | Safe to revert. Zero behavior change when `PARITY_TRACE` unset. |
| Phase 8 (This doc) | Documentation only | No code impact. |

### DuckDB state recovery

If a phase introduced state changes that need undoing:

1. **Paper DB** (`data/paper.duckdb`): Replica files in `data/paper_replica/` may contain post-refactor state. Check timestamps and use a pre-deployment replica if needed.
2. **Market DB** (`data/market.duckdb`): Phase 1 may change feature computation. After reverting, run `nseml-build-features --force --allow-full-rebuild` to regenerate.
3. **Backtest DB** (`data/backtest.duckdb`): `exp_id` values incorporate `code_hash` and will change on revert. Compare trade-level results, not IDs.

---

## Pre-Deployment Checklist

Run before deploying any phase:

```bash
# 1. Unit tests
doppler run -- uv run pytest tests/unit/ -q

# 2. Lint + type check
doppler run -- uv run ruff check .
doppler run -- uv run mypy src/ --ignore-missing-imports

# 3. DB health
doppler run -- uv run nseml-db-verify

# 4. Data quality
doppler run -- uv run nseml-hygiene --report
```

### Phase 1 additional checks

- [ ] Trade-level equivalence: entry/exit/price/qty/reason match against pre-deployment baseline
- [ ] `exp_id` changes are expected (code_hash shifts) — verify trade content, not IDs
- [ ] Backtest wall-clock within 5% of pre-deployment baseline (performance gate)
- [ ] Parity trace output (`PARITY_TRACE=1`) produces valid JSON at decision points

### Phase 2 additional checks

- [ ] Two concurrent `nseml-paper replay` processes fail with clear error message
- [ ] `nseml-paper stop` refuses when positions are open (without `--force`)
- [ ] `nseml-paper stop --force` flattens then stops

### Phase 3 additional checks

- [ ] Live session log shows "Waiting for market ready until" message before 09:15
- [ ] No trades fire before 09:20 (entry_start_minutes=5)
- [ ] SIGINT triggers graceful shutdown with position status report

### Phase 4 additional checks

- [ ] `nseml-paper replay --pack-source feed_audit` loads bars from paper_feed_audit
- [ ] `nseml-paper replay --pack-source market` matches existing behavior

---

## Deployment Timing Rules

| Phase | Timing | Reason |
|-------|--------|--------|
| Phase 1 (Shared Eval) | **Non-trading days only** (weekends, holidays) | Structural refactoring touching both backtest and paper engines |
| Phase 2 (Command Lock) | Any day | Additive feature, no behavior change for single-process usage |
| Phase 3 (Live Hardening) | Any day | Additive features (pre-market connect, graceful shutdown) |
| Phase 4 (Feed Audit Replay) | **Non-trading days** | Replay path changes; test thoroughly before market day |
| Phase 5 (Alert Retry) | Any day | Stateless change, backward-compatible |
| Phase 6 (Batch Sync) | Any day | Additive methods, no schema changes |
| Phase 7 (Parity Trace) | Any day | Env-gated, zero overhead when disabled |
| Phase 8 (This doc) | Any day | Documentation only |

### Hold rule

If canonical backtest trade-level results diverge from pre-deployment baseline after any phase, revert immediately. Investigate on a non-trading day.

---

## Monitoring Post-Deployment

After deploying any phase, watch for:

```bash
# Paper trading health
doppler run -- uv run nseml-paper status

# Feed audit comparison (post-session)
doppler run -- uv run python -c "
from nse_momentum_lab.services.paper.scripts.paper_feed_audit import compare_feed_audit
from nse_momentum_lab.services.paper.db.paper_db import PaperDB
db = PaperDB('data/paper.duckdb')
report = compare_feed_audit(trade_date='YYYY-MM-DD', paper_db=db)
print(f'Missing: {len(report[\"missing_bars\"])}')
print(f'Price diffs: {len(report[\"price_diffs\"])}')
print(f'Volume diffs: {len(report[\"volume_diffs\"])}')
db.close()
"

# Parity trace (Phase 7, env-gated)
PARITY_TRACE=1 doppler run -- uv run nseml-paper replay --session-id SID --trade-date YYYY-MM-DD
```
