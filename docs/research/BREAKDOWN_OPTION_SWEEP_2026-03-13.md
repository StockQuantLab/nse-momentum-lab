# Breakdown Option Sweep (2026-03-13)

## Scope

- Strategy: `thresholdbreakdown` (2LYNCH short path)
- Date range: `2025-04-01` to `2026-03-10`
- Universe: `2000`
- Entry: `5min`, `abnormal_gap_mode=trail_after_gap`
- Baselines:
  - 4%: `2a075ce0a2362c49` (`37` trades, `+3.26%`, DD `1.50%`, PF `2.17`)
  - 2%: `f21a443f9a1eba84` (`292` trades, `+9.40%`, DD `3.46%`, PF `1.59`)

## Results

| Option | 4% Exp | 4% Outcome vs Baseline | 2% Exp | 2% Outcome vs Baseline | Decision |
|---|---|---|---|---|---|
| D (filter relaxation) | `588414cbe61a68d4` | Catastrophic degradation (`-375.32%`, DD `223.43%`) | `8cb63549d57e7d14` | Catastrophic degradation (`-545.51%`, DD `383.47%`) | Reject |
| F (`--short-entry-cutoff-minutes 30`) | `6944badfb1e1dae6` | Worse Calmar / lower return | `b19d3bd4325396ac` | Worse (`+3.44%`, DD `4.66%`, PF `1.37`) | Reject |
| C (`--short-post-day3-buffer-pct 0.005`) | `ad0b0aff60c836d7` | Slightly worse (`+1.62%`, DD `1.67%`, PF `1.63`) | `761b09ffb13d84fd` | Slightly worse (`+8.88%`, DD `3.59%`, PF `1.56`) | Reject |
| B (`--short-trail-activation-pct 0.04 --short-time-stop-days 3 --short-max-stop-dist-pct 0.05 --short-abnormal-profit-pct 0.05`) | `5f540ebfbce6cd20` | Better risk-adjusted (`+3.36%`, DD `1.24%`, PF `2.49`) | `8e826d9df7599852` | Worse (`+8.33%`, DD `3.78%`, PF `1.54`) | Split: keep for 4% only (optional) |

## Recommended Operating Point

- 2% breakdown: keep canonical baseline config (`f21a443f9a1eba84`).
- 4% breakdown:
  - conservative baseline: `2a075ce0a2362c49`
  - lower-DD alternative: Option-B tuned config (`5f540ebfbce6cd20`)

## Preset Automation (Implemented)

- Script: `scripts/run_breakdown_operating_point.py`
- One command:

```bash
doppler run -- uv run python scripts/run_breakdown_operating_point.py --force
```

- Latest verified preset run IDs:
  - 4% tuned: `84d9a58f3ad105be`
  - 2% canonical: `c52e19a02db552d1`

## Full 4-Leg Preset

- Script: `scripts/run_full_operating_point.py`
- Command:

```bash
doppler run -- uv run python scripts/run_full_operating_point.py
```

- Latest verified IDs:
  - breakout 4%: `1716b78c208a90f3`
  - breakout 2%: `87577645e9c99961`
  - breakdown 4%: `84d9a58f3ad105be`
  - breakdown 2%: `c52e19a02db552d1`

## Commands Used

```bash
doppler run -- uv run python -m nse_momentum_lab.cli.backtest --strategy thresholdbreakdown --breakout-threshold 0.04 --universe-size 2000 --start-year 2025 --end-year 2026 --start-date 2025-04-01 --end-date 2026-03-10 --abnormal-gap-mode trail_after_gap --short-post-day3-buffer-pct 0.005 --force
doppler run -- uv run python -m nse_momentum_lab.cli.backtest --strategy thresholdbreakdown --breakout-threshold 0.02 --universe-size 2000 --start-year 2025 --end-year 2026 --start-date 2025-04-01 --end-date 2026-03-10 --abnormal-gap-mode trail_after_gap --short-post-day3-buffer-pct 0.005 --force
doppler run -- uv run python -m nse_momentum_lab.cli.backtest --strategy thresholdbreakdown --breakout-threshold 0.04 --universe-size 2000 --start-year 2025 --end-year 2026 --start-date 2025-04-01 --end-date 2026-03-10 --abnormal-gap-mode trail_after_gap --short-trail-activation-pct 0.04 --short-time-stop-days 3 --short-max-stop-dist-pct 0.05 --short-abnormal-profit-pct 0.05 --force
doppler run -- uv run python -m nse_momentum_lab.cli.backtest --strategy thresholdbreakdown --breakout-threshold 0.02 --universe-size 2000 --start-year 2025 --end-year 2026 --start-date 2025-04-01 --end-date 2026-03-10 --abnormal-gap-mode trail_after_gap --short-trail-activation-pct 0.04 --short-time-stop-days 3 --short-max-stop-dist-pct 0.05 --short-abnormal-profit-pct 0.05 --force
```
