# NSE Momentum Lab — Metrics Policy

## Metric Definitions

All headline KPIs use **daily-close equity only** — not intraday marks.

### Total Return (%)

Cumulative return of the equity curve over the backtest or paper window.

```
Total Return = (Final Equity - Initial Equity) / Initial Equity × 100
```

### Max Drawdown (%)

Maximum peak-to-trough decline in daily-close equity across the window.

```
Max DD = max over all t of (Peak_t - Equity_t) / Peak_t × 100
```

### Calmar Ratio

Return efficiency relative to drawdown risk.

```
Calmar = Total Return (%) / Max Drawdown (%)
```

Higher is better. **Target: > 5.0** for promotion consideration.

### Profit Factor

```
Profit Factor = Gross Profit / Gross Loss
```

Higher is better. **Target: > 1.4**. Profit factor below 1.0 means the strategy is losing money in aggregate.

### Win Rate (%)

```
Win Rate = Winning Trades / Total Closed Trades × 100
```

Informational only — **not a promotion gate**. A high-Calmar strategy may have a moderate win rate if winners are significantly larger than losers.

---

## Guardrails

A preset or experiment run **must not be promoted** to paper trading if any of the following are true:

| Guardrail | Threshold | Action if violated |
|-----------|-----------|-------------------|
| Max Drawdown | > 5% | Reject — do not promote |
| Calmar Ratio | < 3.0 | Reject — do not promote |
| Walk-Forward gate | Not passed | Reject — no live session |
| Parity check | Failed (>5% delta vs prior canonical) | Investigate before promoting |

---

## Promotion Criteria

A preset is eligible for live paper trading when **all** of the following hold:

1. **Backtest passes guardrails**: Max DD ≤ 5%, Calmar ≥ 3.0, Profit Factor ≥ 1.4
2. **Walk-forward coverage**: a completed `walk_forward` session for `thresholdbreakout` covers the intended trade date
3. **Parity check passes**: a rerun of the canonical experiment on the same window/universe produces results within tolerance of the frozen baseline
4. **Dataset/code hash lineage** (optional but recommended): `--experiment-id` supplied to the gate confirms the backtest was generated with current strategy logic

---

## How to Read a Backtest Report

Each experiment row in `bt_experiment` contains:

| Field | Description |
|-------|-------------|
| `exp_id` | 16-char SHA256 fingerprint of params + dataset + strategy code |
| `strategy_name` | `thresholdbreakout` or `2lynchbreakdown` |
| `start_date` / `end_date` | Backtest window |
| `universe_size` | Number of symbols evaluated |
| `breakout_threshold` | 0.02 (2%) or 0.04 (4%) |
| `filters_active` | Which of N, Y, C, L, H were applied |
| `total_return_pct` | Cumulative equity return |
| `max_drawdown_pct` | Peak-to-trough max DD |
| `calmar_ratio` | total_return / max_drawdown |
| `profit_factor` | gross profit / gross loss |
| `trade_count` | Total trades in window |

Use the **Execution Audit** tab in NiceGUI Backtest Results to inspect individual trade decisions before writing ad hoc SQL.

---

## Comparing Runs

Use `compare_backtest_runs.py` to surface drift between two experiments:
```bash
doppler run -- uv run python scripts/compare_backtest_runs.py \
  --old-exp <OLD_EXP_ID> --new-exp <NEW_EXP_ID>
```

Acceptable drift thresholds (informal guidelines):

| Metric | Acceptable delta |
|--------|-----------------|
| Total Return | ± 5 percentage points |
| Max Drawdown | ± 0.5 percentage points |
| Trade Count | ± 2% |

Larger deviations indicate a code change, filter logic change, or data difference and require investigation before promoting.

---

## Experiment Lineage

`exp_id` is a fingerprint of **params + dataset + strategy code files**. Changing any of the following invalidates prior `exp_id` values:

- `duckdb_backtest_runner.py`
- `strategy_families.py`
- `strategy_registry.py`
- `intraday_execution.py`
- `vectorbt_engine.py`
- `engine.py`
- `filters.py`
- `signal_models.py`

This prevents silent overwrites when strategy logic changes but CLI parameters stay the same.
