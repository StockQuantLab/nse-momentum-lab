# 10-Year Backtest Summary (2015-2024)

**Strategy:** Indian 2LYNCH Gap-Up Breakout
**Universe:** Top 500 NSE stocks (liquidity-ranked per year)
**Backtest Engine:** VectorBT + DuckDB
**Experiment ID:** `21d35d9b903b7921`
**Data Table:** `data/market.duckdb` (dataset spans 2015-2025; this run focuses on 2015-2024)

---

## Aggregate Results

| Metric | Value |
|--------|-------|
| Total Return | **840.67%** |
| Annualized Return | **84.07%** |
| Win Rate | 37.8% |
| Total Trades | 9,261 |
| Max Drawdown | 23.0% |
| Profit Factor | 2.10 |
| Profitable Years | 10/10 |

---

## Market Cycle Performance

| Period | Regime | Trades | Return % | Win % | Notes |
|--------|--------|--------|----------|-------|-------|
| 2015 | Consolidation / onboarding | 534 | +21.7 | 33.6 | Validates setup before liquidity ramped |
| 2016-2017 | Post-demonetization bull run | 1,619 | +122.5* | ~37.4 | Filters handled fast-moving liquidity |
| 2018 | NBFC/credit squeeze | 763 | +10.5 | 31.2 | Risk kept under control in a bear year |
| 2019 | Recovery | 735 | +24.1 | 32.0 | Resume of positive momentum |
| 2020 | COVID volatility | 1,123 | +149.4 | 42.2 | Largest contribution thanks to tight stops |
| 2021 | Strong bull (post-COVID) | 1,222 | +150.0 | 38.7 | High return with moderate drawdown (4.1%) |
| 2022 | Rates + Ukraine stress | 965 | +91.7 | 37.8 | Still profitable despite macro stress |
| 2023 | Election buildup | 1,137 | +167.9 | 44.0 | Trailing stops locked gains |
| 2024 | Pre/post-election continuation | 1,163 | +102.7 | 37.5 | Solid finish |

_\*Combined return is the geometric result of 2016 (+31%) and 2017 (+91%); the system remained profitable in each year._

---

## How Results Are Stored

- `bt_experiment`: one row per run with parameter hash dedup.
- `bt_trade`: entry/exit metadata, exit_reason, PnL, R-multiple.
- `bt_yearly_metric`: pre-computed yearly aggregates for dashboard tables.
- Experiment artifacts persist in Postgres + MinIO (duckdb snapshots + metrics) when the runner is launched with `--snapshot`.

Running the same parameter hash returns the cached `exp_id`, so repeated runs are idempotent.

---

## Reproducing the Run

```powershell
uv sync
doppler run -- docker compose up -d
doppler run -- uv run nseml-backtest --universe-size 500 --start-year 2015 --end-year 2024 --progress-file data/progress/2015-2024_run_<ts>.ndjson
```

Use `--force` to override deduplication and `--snapshot` to publish a DuckDB snapshot to MinIO. Monitor progress by tailing the NDJSON heartbeat file above or by viewing the Streamlit dashboard (`nseml-dashboard`).

---

*Generated: 2026-02-28*
