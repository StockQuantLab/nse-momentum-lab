# Canonical Reporting Runset (2026-04-26)

Supersedes: `CANONICAL_REPORTING_RUNSET_2026-04-25.md`

This document freezes the canonical backtest IDs after:
- Phase 1 (Shared Evaluation Module): `evaluate_entry_trigger` and `evaluate_hold_quality_carry_rule` extracted to `shared_eval.py`
- ISSUE-055: `compute_h_filter_passed()` extracted; dead `_apply_hold_quality_carry_rule` removed; H-filter short threshold uses `round(1.0 - threshold, 6)` matching backtest SQL exactly

## What Changed (vs 2026-04-25 runset)

| Change | Detail |
|--------|--------|
| End date | 2026-04-26 → 2026-04-24 (last ingested day) |
| ISSUE-055 refactor | Shared H-filter helper extracted; rounding fix applied; no behavioral change |
| Phase 1 shared eval | entry/carry helpers unified; stop-distance semantics tightened (session low accumulation) |
| Short H-carry clamp | Short-side carry stop now correctly clamped (real bug fix) |

Trade count delta from pre-Phase-1: BREAKOUT_4% −2, BREAKOUT_2% −6, BREAKDOWN_4% −4, BREAKDOWN_2% ±0.
Caused by stricter session_low accumulation in `evaluate_entry_trigger` — trades that fail the
tighter stop-distance rule are excluded by design.

## Active Runset

Window: `2015-01-01 → 2026-04-24`, universe 2000.

| Leg | Strategy | Exp ID | Avg Annual | Max DD | Calmar | Profit Factor | Trades | Win% | Neg Years |
|-----|----------|--------|-----------|--------|--------|---------------|--------|------|-----------|
| Breakout 4% | `2LYNCHBreakout` | `6565aa5698186b01` | +54.5% | 3.16% | 17.3 | 20.78 | 2,215 | 40.6% | 0 |
| Breakout 2% | `2LYNCHBreakout` | `874515a0c02ba7ee` | +121.8% | 2.73% | 44.6 | 19.21 | 7,091 | 38.6% | 0 |
| Breakdown 4% | `2LYNCHBreakdown` | `a2f4063613d259b3` | +3.1% | 0.74% | 4.2 | 5.82 | 254 | 36.2% | 2 |
| Breakdown 2% | `2LYNCHBreakdown` | `9a5ed7575f68613a` | +8.3% | 1.90% | 4.4 | 5.48 | 792 | 25.9% | 0 |

## Regression Comparison vs Post-Phase-1 Set

| Leg | Post-Ph1 Ann | Apr-26 Ann | Post-Ph1 Trades | Apr-26 Trades | Verdict |
|-----|-------------|-----------|-----------------|---------------|---------|
| BREAKOUT_4% | +54.5% | +54.5% | 2,215 | 2,215 | ✅ identical |
| BREAKOUT_2% | +121.8% | +121.8% | 7,091 | 7,091 | ✅ identical |
| BREAKDOWN_4% | +3.1% | +3.1% | 254 | 254 | ✅ identical |
| BREAKDOWN_2% | +8.3% | +8.3% | 792 | 792 | ✅ identical |

**Verdict: ✅ Zero regression from ISSUE-055 refactor.** ISSUE-055 is a pure code organization
change with no behavioral impact on the backtest path.

## Purpose

These are the active canonical baselines for all future regression comparisons. Use these IDs in
`quick-baseline-regression` skill comparisons and Phase 7.x work.

## Reproduction Command

```bash
doppler run -- uv run python scripts/run_full_operating_point.py \
  --start-date 2015-01-01 --end-date 2026-04-24 \
  --force
```

## Database State

All 8 prior experiment IDs pruned from `data/backtest.duckdb` on 2026-04-26.
These 4 IDs are the only experiments remaining in the DB.

To prune future stale experiments (keeping current canonical IDs):
```bash
doppler run -- uv run python -c "
import duckdb; con = duckdb.connect('data/backtest.duckdb')
keep = ('6565aa5698186b01','874515a0c02ba7ee','a2f4063613d259b3','9a5ed7575f68613a')
for t in ['bt_execution_diagnostic','bt_trade','bt_yearly_metric']:
    con.execute(f'DELETE FROM {t} WHERE exp_id NOT IN {keep}')
con.execute(f'DELETE FROM bt_experiment WHERE exp_id NOT IN {keep}')
print('Done')
"
```
