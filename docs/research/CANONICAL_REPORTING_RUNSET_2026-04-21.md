# Canonical Reporting Runset (2026-04-21)

> ⚠️ **SUPERSEDED** by `CANONICAL_REPORTING_RUNSET_2026-04-22.md`

Supersedes: `CANONICAL_REPORTING_RUNSET_2026-03-13.md`

This document freezes the current default backtest IDs used for dashboard comparisons and research reporting.

## What Changed (vs 2026-03-13 runset)

**Wave-1 strategy fixes applied** — these are not optional tuning changes, they fix fundamental correctness bugs:

| Fix | Impact |
|-----|--------|
| H-carry rule enabled (`h_carry_enabled=True`) | `WEAK_CLOSE_EXIT` now fires; losing trades exit same day instead of carrying overnight |
| Entry gate at 09:20 (`entry_start_minutes=5`) | No entries on the 09:15 bar (5-min candles only available from 09:20) |
| Filter direction parity (N, H for shorts) | `FilterChecker.check_n()` / `check_h()` now correct for breakdown strategies |
| pnl_r guard (`abs(initial_risk) < 0.01 → NULL`) | Eliminates corrupt 1e+12 aggregate R-multiples |

**Window extended** from 1-year (2025–2026) to **11 years** (2015–2026) for statistical robustness.

## Active Runset

Window: `2015-01-01 → 2026-04-21`, universe 2000, parallel workers 4.

| Leg | Strategy | Exp ID | Avg Annual | Max DD | Calmar | Profit Factor | Trades | Win% | Neg Years |
|-----|----------|--------|-----------|--------|--------|---------------|--------|------|-----------|
| Breakout 4% | `2LYNCHBreakout` | `f155489ee3422815` | +54.1% | 3.16% | 17.1 | 20.73 | 2,212 | 40.6% | 0 |
| Breakout 2% | `2LYNCHBreakout` | `8e219692ea67b157` | +121.9% | 2.73% | 44.7 | 16.49 | 7,082 | 38.7% | 0 |
| Breakdown 4% | `2LYNCHBreakdown` | `f0cd849cf08f4fdc` | +3.1% | 0.74% | 4.2 | 5.51 | 258 | 36.0% | 2 |
| Breakdown 2% | `2LYNCHBreakdown` | `1f910e9069a508d2` | +8.2% | 1.90% | 4.3 | 5.47 | 790 | 25.7% | 0 |

## Reproduction Command

```bash
doppler run -- uv run python scripts/run_full_operating_point.py \
  --start-year 2015 --end-year 2026 \
  --start-date 2015-01-01 --end-date 2026-04-21 \
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
  --keep-only f155489ee3422815 8e219692ea67b157 f0cd849cf08f4fdc 1f910e9069a508d2 \
  --dry-run
```

## Notes

- Win rate for breakdowns dropped (36.0% for 4%, 25.7% for 2%) vs breakout legs. This is **expected**: the old code unconditionally exited H=false shorts at EOD (capturing many small wins). New code carries profitable shorts overnight with a breakeven stop — some of these stop at breakeven (0R, not a win) but the survivors produce larger R-multiples. The result is higher return, lower drawdown, and better profit factor despite lower win rate.
- 4% breakdown has 2 negative years over the 2015–2026 window; this is the primary risk for that leg.
- 2% breakdown has 0 negative years over the 2015–2026 window with Calmar 4.3 — statistically robust.
