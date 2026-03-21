---
name: breakdown-research
owner: repeatable-research-workflow
summary: Standardizes repeated breakdown optimization runs and baseline comparisons.
---

## Skill Purpose
Use this skill as the default operator playbook for breakdown optimization experiments.

## Canonical Entry Script
`python scripts/run_breakdown_workflow.py`

## Preset Steps
- `baseline` — phase-1 canonical (budget=5, rs_min=-0.10, strict L, narrow-only N, skip gap-down)
- `ti65` — TI65 bearish trend gate
- `breadth` — market breadth gate (requires `--breakdown-breadth-threshold`)
- `atr-expansion` — ATR expansion hard gate
- `budget8` — candidate budget 8 with phase-1 + phase-3e
- `budget10` — candidate budget 10 with phase-1 + phase-3e

## Plan Aliases
- `phase3a` = `baseline`
- `phase3b` = `ti65`,`breadth`
- `phase3c` = `atr-expansion`
- `phase3d` = `budget8`
- `phase3f` = `budget10`

## Baseline-safe command set
```bash
doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step baseline

# Repeated step-by-step
# Example: TI65 then breadth
doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step ti65 --step breadth --compare-baseline --baseline-4pct-exp 909f04c033332b22 --baseline-2pct-exp a6e32e24a9c256f7

# Short profile variants (2% only)
doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step baseline --short-profile option-b
doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step baseline --short-profile aggressive
doppler run -- uv run python scripts/run_breakdown_workflow.py --force --step baseline --short-profile quick-scalp

# Run full pending sequence by phase alias
doppler run -- uv run python scripts/run_breakdown_workflow.py --force --plan phase3a --plan phase3b --plan phase3c --plan phase3d --plan phase3f
```

## Regression check
```bash
doppler run -- uv run python scripts/compare_backtest_runs.py --baseline <BASELINE_EXP> --exps <LABEL>:<NEW_EXP>
```

## Output behavior to expect
- prints `4%=<exp_id> 2%=<exp_id>` for each step
- `--compare-baseline` prints immediate compare command stubs

## Cleanup rule
- Avoid creating one-off temporary Python snippets for run comparisons.
- Prefer this workflow or `scripts/compare_backtest_runs.py` instead.
