# Canonical Reporting Runset (2026-03-13)

This document freezes the current default backtest IDs used for dashboard/report comparisons.

## Active Runset

| Leg | Strategy | Exp ID | Trades | Ann Ret | Total Ret | Max DD | PF | Win% |
|---|---|---|---:|---:|---:|---:|---:|---:|
| Breakout 4% | `thresholdbreakout` | `1716b78c208a90f3` | 977 | 68.29% | 136.59% | 2.26% | 2.71 | 33.78% |
| Breakout 2% | `thresholdbreakout` | `87577645e9c99961` | 1845 | 79.19% | 158.37% | 3.53% | 2.51 | 32.41% |
| Breakdown 4% | `thresholdbreakdown` (Option-B short tuning) | `84d9a58f3ad105be` | 33 | 1.82% | 3.64% | 1.23% | 2.60 | 54.55% |
| Breakdown 2% | `thresholdbreakdown` (canonical short config) | `c52e19a02db552d1` | 292 | 5.16% | 10.31% | 3.24% | 1.63 | 37.67% |

## Reproduction Commands

```bash
doppler run -- uv run python scripts/run_full_operating_point.py
```

Breakdown-only preset:

```bash
doppler run -- uv run python scripts/run_breakdown_operating_point.py --force
```

## Notes

- This runset is for **reporting consistency**.
- Historical IDs (`2efe9e...`, `867f25...`, `2a075c...`, `f21a44...`) remain valid comparison baselines, but are no longer the default reporting anchors.
