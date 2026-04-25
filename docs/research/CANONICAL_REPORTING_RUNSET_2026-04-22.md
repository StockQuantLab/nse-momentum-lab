# Canonical Reporting Runset (2026-04-22)

> ⚠️ **SUPERSEDED** by `CANONICAL_REPORTING_RUNSET_2026-04-25.md`

Supersedes: `CANONICAL_REPORTING_RUNSET_2026-04-21.md`

This document freezes the current default backtest IDs used for dashboard comparisons and research reporting.

## What Changed (vs 2026-04-21 runset)

**Wave-1 strategy fixes applied** - these are not optional tuning changes, they fix fundamental correctness bugs:

| Fix | Impact |
|-----|--------|
| H-carry rule enabled (`h_carry_enabled=True`) | `WEAK_CLOSE_EXIT` now fires; losing trades exit same day instead of carrying overnight |
| Entry gate at 09:20 (`entry_start_minutes=5`) | No entries on the 09:15 bar (5-min candles only available from 09:20) |
| Filter direction parity (N, H for shorts) | `FilterChecker.check_n()` / `check_h()` now correct for breakdown strategies |
| pnl_r guard (`abs(initial_risk) < 0.01 → NULL`) | Eliminates corrupt 1e+12 aggregate R-multiples |

**Window extended** from 11 years (2015-2026) to the full current canonical slice ending on `2026-04-22`.

## Active Runset

Window: `2015-01-01 → 2026-04-22`, universe 2000, parallel workers 4.

| Leg | Strategy | Exp ID | Avg Annual | Max DD | Calmar | Profit Factor | Trades | Win% | Neg Years |
|-----|----------|--------|-----------|--------|--------|---------------|--------|------|-----------|
| Breakout 4% | `2LYNCHBreakout` | `d245816e1d89e196` | +54.2% | 3.16% | 17.2 | 20.75 | 2,213 | 40.6% | 0 |
| Breakout 2% | `2LYNCHBreakout` | `f5bf9a6836901550` | +122.0% | 2.73% | 44.7 | 16.50 | 7,086 | 38.7% | 0 |
| Breakdown 4% | `2LYNCHBreakdown` | `f4a125fce62ddb24` | +3.1% | 0.74% | 4.2 | 5.51 | 258 | 36.0% | 2 |
| Breakdown 2% | `2LYNCHBreakdown` | `be7958b0f79c3c1c` | +8.2% | 1.90% | 4.3 | 5.47 | 790 | 25.7% | 0 |

## Reproduction Command

```bash
doppler run -- uv run python scripts/run_full_operating_point.py \
  --start-year 2015 --end-year 2026 \
  --start-date 2015-01-01 --end-date 2026-04-22 \
  --universe-size 2000 --parallel-workers 4 --force
```

## Database State

All prior experiment IDs have been removed from `data/backtest.duckdb` using `nseml-backtest-cleanup`.
The versioned dashboard replica was synced immediately after cleanup.

To list current experiments:
```bash
doppler run -- .venv\Scripts\python.exe -m nse_momentum_lab.cli.backtest_cleanup --list
```

To prune future stale experiments (keeping new canonical IDs):
```bash
doppler run -- .venv\Scripts\python.exe -m nse_momentum_lab.cli.backtest_cleanup \
  --keep-only d245816e1d89e196 f5bf9a6836901550 f4a125fce62ddb24 be7958b0f79c3c1c \
  --dry-run
```

## Notes

- The 2026-04-22 rerun only nudged breakout legs by one extra held day. Breakdown legs stayed unchanged.
- 4% breakdown remains the lower-frequency leg; 2% breakdown is the more active short-side baseline.
