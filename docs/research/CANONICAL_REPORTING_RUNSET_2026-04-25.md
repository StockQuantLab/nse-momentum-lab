# Canonical Reporting Runset (2026-04-25)

> ⚠️ **Superseded** by [`CANONICAL_REPORTING_RUNSET_2026-04-26.md`](CANONICAL_REPORTING_RUNSET_2026-04-26.md)
> (Post-Phase-1 + ISSUE-055 canonical set, 2026-04-26). Retained as historical reference.

Supersedes: `CANONICAL_REPORTING_RUNSET_2026-04-22.md`

This document freezes the canonical backtest IDs used as the **pre-Phase-1 reference baseline**
for the CPR Parity Improvement Plan. These IDs confirm zero regression from implementing
Phases 2-8 of that plan.

## What Changed (vs 2026-04-22 runset)

| Change | Detail |
|--------|--------|
| End date extended | 2026-04-22 → 2026-04-26 (parameter); effective data window ends 2026-04-23 (last ingested day) |
| Phases 2-8 code changes | `command_lock.py`, sentinel flatten, graceful shutdown, pre-market readiness, feed audit replay, alert retry, batch sync, parity trace logging |
| No behavioral change to backtest path | Phases 2-8 are all paper/alert/ops improvements — backtest engine untouched |

## Active Runset

Window: `2015-01-01 → 2026-04-26` (effective data through 2026-04-23), universe 2000.

| Leg | Strategy | Exp ID | Avg Annual | Max DD | Calmar | Profit Factor | Trades | Win% | Neg Years |
|-----|----------|--------|-----------|--------|--------|---------------|--------|------|-----------|
| Breakout 4% | `2LYNCHBreakout` | `bd22a5859c571c0d` | +54.5% | 3.16% | 17.3 | 20.80 | 2,217 | 40.6% | 0 |
| Breakout 2% | `2LYNCHBreakout` | `e5cbeed50a3c78e4` | +122.0% | 2.73% | 44.7 | 19.23 | 7,097 | 38.6% | 0 |
| Breakdown 4% | `2LYNCHBreakdown` | `d6b34cbfb49137de` | +3.1% | 0.74% | 4.2 | 5.50 | 258 | 36.0% | 2 |
| Breakdown 2% | `2LYNCHBreakdown` | `073e3a2225abb123` | +8.3% | 1.90% | 4.4 | 5.48 | 792 | 25.9% | 0 |

## Regression Comparison vs 2026-04-22

| Leg | Apr-22 Ann | Apr-25 Ann | Apr-22 Trades | Apr-25 Trades | Verdict |
|-----|-----------|-----------|---------------|---------------|---------|
| BREAKOUT_4% | +54.2% | +54.5% | 2,213 | 2,217 | ✅ no regression |
| BREAKOUT_2% | +122.0% | +122.0% | 7,086 | 7,097 | ✅ no regression |
| BREAKDOWN_4% | +3.1% | +3.1% | 258 | 258 | ✅ identical |
| BREAKDOWN_2% | +8.2% | +8.3% | 790 | 792 | ✅ no regression |

**Verdict: ✅ Zero regression from Phases 2-8.** All deltas are within expected noise from the
small end-date extension. No behavioral change in the backtest path.

## Purpose

These IDs serve as the **pre-Phase-1 reference** for the CPR Parity Improvement Plan:
- Phase 1 (Shared Evaluation Module) refactors `duckdb_backtest_runner.py` and `paper_runtime.py`
- After Phase 1, re-run canonical backtests and compare trade-level results against these IDs
- The new IDs become the post-Phase-1 canonical set; these IDs become historical

## Reproduction Command

```bash
doppler run -- uv run python scripts/run_full_operating_point.py \
  --start-date 2015-01-01 --end-date 2026-04-26 \
  --force
```

## Database State

All prior experiment IDs pruned from `data/backtest.duckdb` using `nseml-backtest-cleanup`.
Dashboard replica synced immediately after cleanup.

To prune future stale experiments (keeping current canonical IDs):
```bash
doppler run -- .venv\Scripts\python.exe -m nse_momentum_lab.cli.backtest_cleanup \
  --keep-only bd22a5859c571c0d e5cbeed50a3c78e4 d6b34cbfb49137de 073e3a2225abb123 \
  --dry-run
```
