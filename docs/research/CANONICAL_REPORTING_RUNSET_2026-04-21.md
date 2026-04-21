# Canonical Reporting Runset (2026-04-21)

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

Window: `2015-01-01 → 2026-04-17`, universe 2000, parallel workers 4.

| Leg | Strategy | Exp ID | Avg Annual | Max DD | Calmar | Profit Factor | Trades | Win% | Neg Years |
|-----|----------|--------|-----------|--------|--------|---------------|--------|------|-----------|
| Breakout 4% | `2LYNCHBreakout` | `0cd353d536dd6f91` | +54.1% | 3.16% | 17.1 | 22.98 | 2,211 | 39.3% | 0 |
| Breakout 2% | `2LYNCHBreakout` | `f923e1a9517d9b2c` | +121.8% | 2.73% | 44.6 | 19.06 | 7,078 | 37.8% | 0 |
| Breakdown 4% | `2LYNCHBreakdown` | `f6e7646ac932697d` | +3.1% | 0.74% | 4.2 | 6.65 | 258 | 30.5% | 2 |
| Breakdown 2% | `2LYNCHBreakdown` | `b769984bf6d0c5c7` | +8.1% | 1.99% | 4.1 | 6.52 | 790 | 25.3% | 0 |

## Reproduction Command

```bash
doppler run -- uv run python scripts/run_full_operating_point.py \
  --start-year 2015 --end-year 2026 \
  --start-date 2015-01-01 --end-date 2026-04-17 \
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
  --keep-only 0cd353d536dd6f91 f923e1a9517d9b2c f6e7646ac932697d b769984bf6d0c5c7 \
  --dry-run
```

## Notes

- Win rate for breakdowns dropped (48%→30% for 4%, similar for 2%) vs old runs. This is **expected**: the old code unconditionally exited H=false shorts at EOD (capturing many small wins). New code carries profitable shorts overnight with a breakeven stop — some of these stop at breakeven (0R, not a win) but the survivors produce larger R-multiples. The result is higher return, lower drawdown, and better profit factor despite lower win rate.
- 4% breakdown has 2 negative years over 11 years; this is the primary risk for that leg.
- 2% breakdown has 0 negative years over 11 years with Calmar 4.1 — statistically robust.
