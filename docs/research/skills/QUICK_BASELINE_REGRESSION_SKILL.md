---
name: quick-baseline-regression
owner: breakdown-research
summary: Fast baseline comparison checklist for 4% and 2% breakdown experiments.
---

## Purpose
Run a quick, repeatable 2-minute baseline regression check after any new breakdown run.

## Inputs
- `base_4pct_exp`: your current 4% baseline experiment id
- `base_2pct_exp`: your current 2% baseline experiment id
- one or more new experiment ids with labels

## Mandatory first checks
```bash
doppler run -- uv run nseml-dashboard
```
Keep dashboard visible to inspect charts and audit tabs while validating.

## Compare command templates
```bash
doppler run -- uv run python scripts/compare_backtest_runs.py \
  --baseline <BASE_4PCT_EXP> \
  --exps <LABEL_A>:<NEW_4PCT_EXP>

doppler run -- uv run python scripts/compare_backtest_runs.py \
  --baseline <BASE_2PCT_EXP> \
  --exps <LABEL_B>:<NEW_2PCT_EXP>
```

## Standard runset pairs
Use these for immediate regression safety after step changes:
- 4%: `4pct-<step>` label
- 2%: `2pct-<step>-<profile>` label

## Baseline rule
- Compare against the current frozen baseline pair from the dropdown, not the historical hindsight rows.
- Keep the run envelope fixed across the 2% and 4% pair; only the threshold or intended strategy change should vary.

## Suggested sequence (workflow-driven)
```bash
doppler run -- uv run python scripts/run_breakdown_workflow.py --force --plan phase3a --plan phase3b --compare-baseline --baseline-4pct-exp <BASE_4PCT_EXP> --baseline-2pct-exp <BASE_2PCT_EXP>
```

## What to record (minimum)
- `total_ret`
- `total_trades`
- `max_drawdown_pct`
- `profit_factor`
- `win_rate_pct`
- `calmar` (annualised return / max dd)
- drawdown cluster pattern from `Execution Audit`

## Decision thresholds
- 4% and 2% should be compared against baseline on: return, DD, PF, and Calmar.
- If any new run increases DD materially with no return improvement, it is not a regression-safe upgrade.

## Anti-patterns
- Don’t compare only `trades` and `ret`; include `max_drawdown_pct`.
- Don’t finalize a change from one side only (evaluate both 4% and 2%).
