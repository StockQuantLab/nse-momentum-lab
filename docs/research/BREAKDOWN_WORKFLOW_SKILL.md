# Breakdown Optimization Workflow Skill

Use this workflow skill instead of manual one-off scripts.

## Canonical command
```bash
doppler run -- uv run python scripts/run_breakdown_workflow.py --force
```

That defaults to `--step baseline`.

## Replacements for ad-hoc commands
- `scripts/run_breakdown_workflow.py` is now the repeatable entry-point.
- `scripts/compare_backtest_runs.py` is the canonical experiment comparison script.
- `tmp_deltas.py` was removed (one-off analysis snippet).

## Step presets
- `baseline`, `ti65`, `breadth`, `atr-expansion`, `atr-cap`, `day0-profit`, `budget8`, `budget10`
- Plan aliases: `phase3a`, `phase3b`, `phase3c`, `phase3d`, `phase3f`

## Comparison workflow
```bash
doppler run -- uv run python scripts/compare_backtest_runs.py --baseline <BASELINE_EXP> --exps <LABEL>:<NEW_EXP>
```

## Skill docs
- In-repo reference: [BREAKDOWN_RESEARCH_SKILL.md](/docs/research/skills/BREAKDOWN_RESEARCH_SKILL.md)

## Quick regression docs
- Fast check template: [QUICK_BASELINE_REGRESSION_SKILL.md](/docs/research/skills/QUICK_BASELINE_REGRESSION_SKILL.md)
