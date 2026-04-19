# Full Automation Layer — Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all manual backtest sweeps, EOD pipeline orchestration, and paper session lifecycle management with automated CLI commands and YAML-driven configs.

**Architecture:** Three independent subsystems — (1) Sweep Engine wraps existing `DuckDBBacktestRunner` with YAML sweep definitions and auto-comparison, (2) EOD Pipeline extends existing `pipeline.py` with a thin one-shot CLI, (3) Paper Lifecycle Manager auto-manages session start/flatten/archive using existing service boundaries. All leverage existing infrastructure; no reimplementation of core logic.

**Tech Stack:** Python 3.14, argparse CLI, PyYAML, DuckDB (existing), PostgreSQL (existing)

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `src/nse_momentum_lab/cli/sweep.py` | argparse CLI entrypoint for `nseml-sweep` |
| `src/nse_momentum_lab/services/backtest/sweep_runner.py` | Sweep orchestration engine |
| `src/nse_momentum_lab/services/backtest/sweep_schema.py` | YAML schema validation (SweepConfig, SweepAxis) |
| `src/nse_momentum_lab/services/backtest/comparison.py` | Extracted comparison logic (faithful to `compare_backtest_runs.py`) |
| `src/nse_momentum_lab/cli/paper_lifecycle.py` | argparse CLI entrypoint for `nseml-paper-lifecycle` |
| `src/nse_momentum_lab/services/paper/lifecycle_manager.py` | Auto session lifecycle management |
| `sweeps/example_threshold.yaml` | Example sweep definition |
| `sweeps/example_breakdown_profiles.yaml` | Example breakdown sweep |
| `tests/unit/services/backtest/test_sweep_runner.py` | Sweep engine tests |
| `tests/unit/services/backtest/test_sweep_schema.py` | Schema validation tests |
| `tests/unit/services/backtest/test_comparison.py` | Comparison logic tests |
| `tests/unit/services/paper/test_lifecycle_manager.py` | Lifecycle manager tests |

### Modified Files

| File | Change |
|------|--------|
| `pyproject.toml` | Add `pyyaml` dep, register `nseml-sweep`, `nseml-paper-lifecycle` scripts |
| `scripts/compare_backtest_runs.py` | Refactor to import `fetch_summary` from `comparison.py` (preserve as thin CLI wrapper) |
| `src/nse_momentum_lab/cli/pipeline.py` | Add `--one-shot` subcommand that chains ingest→features→scan→rollup with proper error handling (or keep existing pipeline.py as-is and add a new thin `nseml-eod` entrypoint that calls `run_daily_pipeline`) |

### Design Decisions (from Codex review — 5 rounds)

- **EOD**: Extend existing `pipeline.py` with `nseml-eod` thin wrapper, NOT a parallel entrypoint. Uses real `PipelineResult.overall_status` and `PipelineResult.stages` (dict, not list).
- **Comparison**: Uses `db.con.execute(...)` (real API), NOT `db.query()`. Faithful extraction of `fetch_summary()` from `compare_backtest_runs.py`. Uses `get_backtest_db(read_only=True)` instead of hardcoded path.
- **Paper lifecycle**: Calls `_build_daily_prepare_report` directly for readiness verdict (not `_cmd_daily_prepare` which only prints JSON). Captures stdout from `_cmd_daily_live` to parse session_id. Uses `date` objects for trade_date. Uses cartesian product for strategy×threshold variants. Uses `execute=False, run=False` to bootstrap sessions without blocking websocket loop.
- **No APScheduler**: Scoped to one-shot CLI commands. Cron scheduling is a future addition.
- **Sweep schema**: Validates base_params against BacktestParams fields, checks duplicate axes, empty values, and strategy resolvability.

---

## Task 1: Sweep Schema (YAML validation)

**Files:**
- Create: `src/nse_momentum_lab/services/backtest/sweep_schema.py`
- Test: `tests/unit/services/backtest/test_sweep_schema.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/services/backtest/test_sweep_schema.py
from __future__ import annotations

import textwrap

import pytest

from nse_momentum_lab.services.backtest.sweep_schema import (
    SweepAxis,
    SweepCompare,
    SweepConfig,
    load_sweep_config,
)


def test_sweep_axis_single():
    axis = SweepAxis(param="breakout_threshold", values=[0.02, 0.04])
    assert axis.param == "breakout_threshold"
    assert len(axis.combinations()) == 2


def test_sweep_axis_cartesian():
    axes = [
        SweepAxis(param="breakout_threshold", values=[0.02, 0.04]),
        SweepAxis(param="trail_activation_pct", values=[0.06, 0.08]),
    ]
    combos = SweepConfig._cartesian(axes)
    assert len(combos) == 4
    assert combos[0] == {"breakout_threshold": 0.02, "trail_activation_pct": 0.06}


def test_sweep_config_from_dict():
    raw = {
        "name": "test-sweep",
        "strategy": "thresholdbreakout",
        "base_params": {"universe_size": 2000, "min_price": 10},
        "sweep": [{"param": "breakout_threshold", "values": [0.02, 0.04]}],
        "compare": {"metric": "calmar_ratio", "sort": "desc", "top_n": 3},
    }
    cfg = SweepConfig.from_dict(raw)
    assert cfg.name == "test-sweep"
    assert cfg.strategy == "thresholdbreakout"
    assert len(cfg.combinations()) == 2


def test_load_sweep_config_from_yaml(tmp_path):
    yaml_file = tmp_path / "sweep.yaml"
    yaml_file.write_text(textwrap.dedent("""\
        name: test
        strategy: thresholdbreakout
        base_params:
          universe_size: 2000
        sweep:
          - param: breakout_threshold
            values: [0.02, 0.04]
    """))
    cfg = load_sweep_config(yaml_file)
    assert cfg.name == "test"
    assert len(cfg.combinations()) == 2


def test_invalid_param_rejected():
    raw = {
        "name": "bad",
        "strategy": "thresholdbreakout",
        "base_params": {"universe_size": 2000},
        "sweep": [{"param": "nonexistent_param", "values": [1, 2]}],
    }
    with pytest.raises(ValueError, match="not a valid BacktestParams field"):
        SweepConfig.from_dict(raw)


def test_base_params_validated():
    """base_params keys must also be valid BacktestParams fields."""
    raw = {
        "name": "bad-base",
        "strategy": "thresholdbreakout",
        "base_params": {"universe_size": 2000, "fake_param": 99},
        "sweep": [{"param": "breakout_threshold", "values": [0.02]}],
    }
    with pytest.raises(ValueError, match="not a valid BacktestParams field"):
        SweepConfig.from_dict(raw)


def test_duplicate_axis_rejected():
    """Duplicate param names in sweep axes are rejected."""
    raw = {
        "name": "dup",
        "strategy": "thresholdbreakout",
        "base_params": {},
        "sweep": [
            {"param": "breakout_threshold", "values": [0.02]},
            {"param": "breakout_threshold", "values": [0.04]},
        ],
    }
    with pytest.raises(ValueError, match="Duplicate sweep axis"):
        SweepConfig.from_dict(raw)


def test_empty_values_rejected():
    """Empty values list in a sweep axis is rejected."""
    raw = {
        "name": "empty",
        "strategy": "thresholdbreakout",
        "base_params": {},
        "sweep": [{"param": "breakout_threshold", "values": []}],
    }
    with pytest.raises(ValueError, match="empty values"):
        SweepConfig.from_dict(raw)


def test_invalid_strategy_rejected():
    """Strategy name must resolve via strategy_registry."""
    raw = {
        "name": "bad-strat",
        "strategy": "nonexistent_strategy",
        "base_params": {},
        "sweep": [{"param": "breakout_threshold", "values": [0.02]}],
    }
    with pytest.raises(ValueError, match="Cannot resolve strategy"):
        SweepConfig.from_dict(raw)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `doppler run -- uv run pytest tests/unit/services/backtest/test_sweep_schema.py -v`
Expected: FAIL — module does not exist

- [ ] **Step 3: Write sweep_schema.py implementation**

```python
# src/nse_momentum_lab/services/backtest/sweep_schema.py
"""YAML sweep configuration schema and loader."""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from nse_momentum_lab.services.backtest.duckdb_backtest_runner import BacktestParams

_VALID_FIELDS = frozenset(f.name for f in BacktestParams.__dataclass_fields__.values())

_VALID_COMPARE_METRICS = frozenset({
    "calmar_ratio", "win_rate", "annualised_return", "total_return",
    "max_drawdown", "profit_factor", "total_trades",
})


def _validate_param_names(params: dict[str, Any], context: str = "params") -> None:
    """Validate that all param names are valid BacktestParams fields."""
    for key in params:
        if key not in _VALID_FIELDS:
            raise ValueError(f"{key!r} in {context} is not a valid BacktestParams field")


@dataclass(frozen=True)
class SweepAxis:
    """Single parameter axis to sweep."""

    param: str
    values: list[Any]

    def __post_init__(self) -> None:
        if self.param not in _VALID_FIELDS:
            raise ValueError(f"{self.param!r} is not a valid BacktestParams field")
        if not self.values:
            raise ValueError(f"Sweep axis {self.param!r} has empty values")

    def combinations(self) -> list[dict[str, Any]]:
        return [{self.param: v} for v in self.values]


@dataclass(frozen=True)
class SweepCompare:
    """Comparison and ranking configuration."""

    metric: str = "calmar_ratio"
    sort: str = "desc"  # "asc" or "desc"
    top_n: int = 5
    include_yearly: bool = True

    def __post_init__(self) -> None:
        if self.metric not in _VALID_COMPARE_METRICS:
            raise ValueError(f"Unknown compare metric: {self.metric!r}")
        if self.sort not in ("asc", "desc"):
            raise ValueError(f"Sort must be 'asc' or 'desc', got {self.sort!r}")


@dataclass
class SweepConfig:
    """Full sweep configuration."""

    name: str
    strategy: str
    base_params: dict[str, Any] = field(default_factory=dict)
    sweep: list[SweepAxis] = field(default_factory=list)
    compare: SweepCompare = field(default_factory=SweepCompare)
    tags: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> SweepConfig:
        # Validate strategy resolvability
        from nse_momentum_lab.services.backtest.strategy_registry import resolve_strategy
        strategy_name = raw.get("strategy", "")
        try:
            resolve_strategy(strategy_name)
        except (KeyError, ValueError) as e:
            raise ValueError(f"Cannot resolve strategy {strategy_name!r}: {e}") from e

        # Validate base_params
        base_params = raw.get("base_params", {})
        _validate_param_names(base_params, context="base_params")

        # Validate sweep axes (check for duplicates)
        sweep_axes = []
        seen_params: set[str] = set()
        for a in raw.get("sweep", []):
            axis = SweepAxis(param=a["param"], values=a["values"])
            if axis.param in seen_params:
                raise ValueError(f"Duplicate sweep axis: {axis.param!r}")
            seen_params.add(axis.param)
            sweep_axes.append(axis)

        compare_raw = raw.get("compare", {})
        compare = SweepCompare(**compare_raw) if compare_raw else SweepCompare()

        return cls(
            name=raw["name"],
            strategy=strategy_name,
            base_params=base_params,
            sweep=sweep_axes,
            compare=compare,
            tags=raw.get("tags", []),
        )

    @classmethod
    def _cartesian(cls, axes: list[SweepAxis]) -> list[dict[str, Any]]:
        """Compute cartesian product of all axis combinations."""
        if not axes:
            return [{}]
        keys = [a.param for a in axes]
        value_lists = [a.values for a in axes]
        return [dict(zip(keys, combo)) for combo in itertools.product(*value_lists)]

    def combinations(self) -> list[dict[str, Any]]:
        return self._cartesian(self.sweep)

    def build_params_for(self, combo: dict[str, Any]) -> dict[str, Any]:
        """Merge base_params with a single sweep combination."""
        merged = {**self.base_params, "strategy": self.strategy, **combo}
        return merged


def load_sweep_config(path: Path) -> SweepConfig:
    """Load sweep configuration from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"YAML root must be a mapping, got {type(raw).__name__}")
    return SweepConfig.from_dict(raw)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `doppler run -- uv run pytest tests/unit/services/backtest/test_sweep_schema.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/nse_momentum_lab/services/backtest/sweep_schema.py tests/unit/services/backtest/test_sweep_schema.py
git commit -m "feat: add sweep YAML schema with full validation"
```

---

## Task 2: Extract Comparison Logic

**Files:**
- Create: `src/nse_momentum_lab/services/backtest/comparison.py`
- Test: `tests/unit/services/backtest/test_comparison.py`
- Modify: `scripts/compare_backtest_runs.py` (refactor to import from comparison.py)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/services/backtest/test_comparison.py
from __future__ import annotations

from nse_momentum_lab.services.backtest.comparison import (
    ExperimentSummary,
    rank_experiments,
    format_comparison_table,
)


def test_experiment_summary_creation():
    summary = ExperimentSummary(
        exp_id="abc123",
        label="2%",
        total_trades=100,
        win_rate=55.0,  # NOTE: win_rate is a percentage (0-100), not a fraction
        annualised_return=18.0,
        total_return=180.0,
        max_drawdown=15.0,
        profit_factor=1.4,
        calmar_ratio=1.2,
        yearly_returns={2023: 12.0, 2024: 20.0},
    )
    assert summary.calmar_ratio == 1.2
    assert summary.win_rate == 55.0


def test_rank_experiments_by_calmar():
    summaries = [
        ExperimentSummary("a", "A", 100, 50.0, 10.0, 100.0, 20.0, 1.2, 0.5, {}),
        ExperimentSummary("b", "B", 100, 60.0, 20.0, 200.0, 10.0, 1.5, 2.0, {}),
        ExperimentSummary("c", "C", 100, 40.0, 5.0, 50.0, 30.0, 1.1, 0.17, {}),
    ]
    ranked = rank_experiments(summaries, metric="calmar_ratio", sort="desc", top_n=2)
    assert ranked[0].exp_id == "b"
    assert ranked[1].exp_id == "a"
    assert len(ranked) == 2


def test_format_comparison_table():
    summaries = [
        ExperimentSummary("a", "4%-thresh", 100, 55.0, 18.0, 180.0, 15.0, 1.4, 1.2, {}),
        ExperimentSummary("b", "2%-thresh", 200, 52.0, 22.0, 220.0, 18.0, 1.3, 1.22, {}),
    ]
    table = format_comparison_table(summaries)
    assert "4%-thresh" in table
    assert "2%-thresh" in table
    assert "Calmar" in table
```

- [ ] **Step 2: Run test to verify it fails**

Run: `doppler run -- uv run pytest tests/unit/services/backtest/test_comparison.py -v`
Expected: FAIL — module does not exist

- [ ] **Step 3: Extract comparison logic faithful to `scripts/compare_backtest_runs.py`**

Read `scripts/compare_backtest_runs.py` fully, then create `comparison.py` using `db.con.execute(...)` (the real API). The `fetch_summary()` function MUST:
- Use `db.con.execute("SELECT * FROM bt_trade WHERE exp_id = ?", [exp_id]).pl()` (line 45 of compare_backtest_runs.py)
- Use `db.con.execute("SELECT * FROM bt_experiment WHERE exp_id = ?", [exp_id]).fetchdf()` (line 49)
- Compute win_rate as percentage (0-100), NOT fraction
- Compute calmar as `annualised / abs(max_dd)`
- Accept `MarketDataDB` instance (which exposes `.con` for DuckDB connection)
- Use `get_backtest_db(read_only=True)` for default DB, NOT hardcoded path

```python
# src/nse_momentum_lab/services/backtest/comparison.py
"""Backtest experiment comparison logic — extracted from scripts/compare_backtest_runs.py.

Uses the real MarketDataDB API: db.con.execute(...).pl() / .fetchdf().
Win rate is a percentage (0-100), matching the existing compare script.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from nse_momentum_lab.db.market_db import MarketDataDB

logger = logging.getLogger(__name__)


@dataclass
class ExperimentSummary:
    """Summarized metrics for a single backtest experiment."""

    exp_id: str
    label: str
    total_trades: int
    win_rate: float  # percentage 0-100, matching compare_backtest_runs.py
    annualised_return: float
    total_return: float
    max_drawdown: float
    profit_factor: float
    calmar_ratio: float
    avg_r: float = 0.0
    median_r: float = 0.0
    avg_hold: float = 0.0
    yearly_returns: dict[int, float] = field(default_factory=dict)


def fetch_experiment_summary(
    db: MarketDataDB, exp_id: str, label: str = ""
) -> ExperimentSummary:
    """Fetch and compute summary metrics for a single experiment.

    Faithful extraction from scripts/compare_backtest_runs.py:fetch_summary().
    Uses db.con.execute(...) — the real MarketDataDB API.
    """
    # Check experiment exists
    exp_df = db.con.execute(
        "SELECT * FROM bt_experiment WHERE exp_id = ?", [exp_id]
    ).fetchdf()
    if exp_df.empty:
        raise ValueError(f"Experiment {exp_id} not found")
    exp = exp_df.iloc[0]

    # Fetch trades
    trades = db.con.execute(
        "SELECT * FROM bt_trade WHERE exp_id = ?", [exp_id]
    ).pl()

    total_trades = len(trades)
    if total_trades == 0:
        return ExperimentSummary(
            exp_id=exp_id, label=label or exp_id,
            total_trades=0, win_rate=0.0, annualised_return=0.0,
            total_return=0.0, max_drawdown=0.0, profit_factor=0.0, calmar_ratio=0.0,
        )

    wins = trades.filter(pl.col("pnl_pct") > 0)
    losses = trades.filter(pl.col("pnl_pct") < 0)
    win_rate = len(wins) / total_trades * 100  # percentage, matching original

    gain = wins["pnl_pct"].sum() if len(wins) else 0.0
    loss = abs(losses["pnl_pct"].sum()) if len(losses) else 0.0
    profit_factor = gain / loss if loss else 0.0

    # Fetch yearly metrics
    yearly = db.con.execute(
        "SELECT * FROM bt_yearly_metric WHERE exp_id = ? ORDER BY year", [exp_id]
    ).pl()

    years_active = yearly.filter(pl.col("trades") > 0)
    total_ret = years_active["return_pct"].sum() if len(years_active) else 0.0
    n_years = len(years_active)
    annualised = total_ret / n_years if n_years else 0.0
    max_dd = yearly["max_dd_pct"].max() if len(yearly) else 0.0

    avg_r = trades["pnl_r"].mean() or 0.0
    median_r = trades["pnl_r"].median() or 0.0
    avg_hold = trades["holding_days"].mean() or 0.0

    calmar = annualised / abs(max_dd) if max_dd != 0 else float("inf")
    yearly_returns = {
        row["year"]: row.get("annualised_return", row.get("return_pct", 0.0))
        for row in yearly.to_dicts()
    }

    return ExperimentSummary(
        exp_id=exp_id,
        label=label or exp_id,
        total_trades=total_trades,
        win_rate=win_rate,
        annualised_return=annualised,
        total_return=total_ret,
        max_drawdown=max_dd,
        profit_factor=profit_factor,
        calmar_ratio=calmar,
        avg_r=avg_r,
        median_r=median_r,
        avg_hold=avg_hold,
        yearly_returns=yearly_returns,
    )


def compare_experiments(
    experiments: list[tuple[str, str]],
    db: MarketDataDB | None = None,
    metric: str = "calmar_ratio",
    sort: str = "desc",
    top_n: int = 5,
) -> list[ExperimentSummary]:
    """Fetch and compare multiple experiments, return ranked list."""
    if db is None:
        from nse_momentum_lab.db.market_db import get_backtest_db
        db = get_backtest_db(read_only=True)
    summaries: list[ExperimentSummary] = []
    for exp_id, label in experiments:
        try:
            summary = fetch_experiment_summary(db, exp_id, label)
            summaries.append(summary)
        except ValueError as e:
            logger.warning("Skipping %s: %s", exp_id, e)
    return rank_experiments(summaries, metric=metric, sort=sort, top_n=top_n)


def rank_experiments(
    summaries: list[ExperimentSummary],
    metric: str = "calmar_ratio",
    sort: str = "desc",
    top_n: int = 5,
) -> list[ExperimentSummary]:
    """Rank experiments by metric and return top_n."""
    reverse = sort == "desc"
    sorted_summaries = sorted(
        summaries,
        key=lambda s: getattr(s, metric, 0),
        reverse=reverse,
    )
    return sorted_summaries[:top_n]


def format_comparison_table(summaries: list[ExperimentSummary]) -> str:
    """Format ranked results as a readable table."""
    if not summaries:
        return "No results to display."
    header = (
        f"{'#':<4} {'Label':<30} {'Calmar':>8} {'Win%':>7} "
        f"{'AnnRet':>8} {'MaxDD':>8} {'Trades':>7} {'PF':>7}"
    )
    separator = "-" * len(header)
    lines = [header, separator]
    for i, s in enumerate(summaries, 1):
        lines.append(
            f"{i:<4} {s.label:<30} {s.calmar_ratio:>8.2f} {s.win_rate:>6.1f} "
            f"{s.annualised_return:>7.1f} {s.max_drawdown:>7.1f} "
            f"{s.total_trades:>7} {s.profit_factor:>7.2f}"
        )
    return "\n".join(lines)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `doppler run -- uv run pytest tests/unit/services/backtest/test_comparison.py -v`
Expected: ALL PASS

- [ ] **Step 5: Refactor `scripts/compare_backtest_runs.py`**

Replace the inline `fetch_summary` function (lines 44-134) with an import from `comparison.py`:
```python
from nse_momentum_lab.services.backtest.comparison import fetch_experiment_summary as fetch_summary
```
Keep `print_multi_comparison` and `main()` intact — they contain richer formatting that the CLI wrapper still needs.

- [ ] **Step 6: Commit**

```bash
git add src/nse_momentum_lab/services/backtest/comparison.py tests/unit/services/backtest/test_comparison.py scripts/compare_backtest_runs.py
git commit -m "feat: extract comparison logic using real db.con.execute API"
```

---

## Task 3: Sweep Runner Engine

**Files:**
- Create: `src/nse_momentum_lab/services/backtest/sweep_runner.py`
- Test: `tests/unit/services/backtest/test_sweep_runner.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/services/backtest/test_sweep_runner.py
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from nse_momentum_lab.services.backtest.sweep_runner import SweepResult, run_sweep


def test_sweep_result_records_all_combos():
    results = [
        SweepResult(exp_id="a", label="thresh=0.02", params={"breakout_threshold": 0.02}),
        SweepResult(exp_id="b", label="thresh=0.04", params={"breakout_threshold": 0.04}),
    ]
    assert len(results) == 2
    assert results[0].exp_id == "a"


@patch("nse_momentum_lab.services.backtest.sweep_runner.DuckDBBacktestRunner")
def test_run_sweep_generates_all_combos(mock_runner_cls):
    mock_runner = MagicMock()
    mock_runner_cls.return_value = mock_runner
    mock_runner.run.return_value = "test-exp-id"

    config = {
        "name": "test",
        "strategy": "thresholdbreakout",
        "base_params": {"universe_size": 500},
        "sweep": [{"param": "breakout_threshold", "values": [0.02, 0.04]}],
    }
    from nse_momentum_lab.services.backtest.sweep_schema import SweepConfig
    sweep_cfg = SweepConfig.from_dict(config)

    results = run_sweep(sweep_cfg, force=True, dry_run=False)
    assert len(results) == 2
    assert mock_runner.run.call_count == 2


@patch("nse_momentum_lab.services.backtest.sweep_runner.DuckDBBacktestRunner")
def test_run_sweep_dry_run_skips_runner(mock_runner_cls):
    config = {
        "name": "test",
        "strategy": "thresholdbreakout",
        "base_params": {},
        "sweep": [{"param": "breakout_threshold", "values": [0.02]}],
    }
    from nse_momentum_lab.services.backtest.sweep_schema import SweepConfig
    sweep_cfg = SweepConfig.from_dict(config)

    results = run_sweep(sweep_cfg, dry_run=True)
    assert len(results) == 1
    assert results[0].exp_id == "(dry-run)"
    mock_runner_cls().run.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `doppler run -- uv run pytest tests/unit/services/backtest/test_sweep_runner.py -v`
Expected: FAIL — module does not exist

- [ ] **Step 3: Write sweep_runner.py**

```python
# src/nse_momentum_lab/services/backtest/sweep_runner.py
"""Sweep orchestration engine — runs multiple backtests and auto-compares."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from nse_momentum_lab.services.backtest.comparison import (
    ExperimentSummary,
    compare_experiments,
    format_comparison_table,
)
from nse_momentum_lab.services.backtest.duckdb_backtest_runner import (
    BacktestParams,
    DuckDBBacktestRunner,
)
from nse_momentum_lab.services.backtest.sweep_schema import SweepConfig

logger = logging.getLogger(__name__)


@dataclass
class SweepResult:
    """Result of a single sweep combination."""

    exp_id: str
    label: str
    params: dict[str, Any]


def _build_label(combo: dict[str, Any]) -> str:
    """Build a human-readable label from a parameter combination."""
    parts = []
    for k, v in combo.items():
        short = k.replace("breakout_threshold", "thresh").replace("trail_activation_pct", "trail")
        parts.append(f"{short}={v}")
    return "-".join(parts)


def run_sweep(
    config: SweepConfig,
    force: bool = False,
    snapshot: bool = False,
    dry_run: bool = False,
) -> list[SweepResult]:
    """Execute all combinations in a sweep config.

    Args:
        config: SweepConfig with base params and sweep axes.
        force: Re-run even if cached.
        snapshot: Publish DuckDB snapshot per run.
        dry_run: Show combinations without running.

    Returns:
        List of SweepResult with exp_ids and labels.
    """
    combos = config.combinations()
    logger.info("Sweep '%s': %d combinations for strategy '%s'", config.name, len(combos), config.strategy)

    if dry_run:
        for combo in combos:
            label = _build_label(combo)
            logger.info("  [DRY-RUN] %s → %s", label, combo)
        return [SweepResult(exp_id="(dry-run)", label=_build_label(c), params=c) for c in combos]

    runner = DuckDBBacktestRunner()
    results: list[SweepResult] = []

    for combo in combos:
        label = _build_label(combo)
        params_dict = config.build_params_for(combo)
        params = BacktestParams(**params_dict)
        logger.info("Running %s …", label)
        exp_id = runner.run(params, force=force, snapshot=snapshot)
        results.append(SweepResult(exp_id=exp_id, label=label, params=combo))
        logger.info("  → %s", exp_id)

    return results


def run_sweep_with_comparison(
    config: SweepConfig, **kwargs: Any
) -> tuple[list[SweepResult], list[ExperimentSummary]]:
    """Run sweep and auto-compare results."""
    results = run_sweep(config, **kwargs)
    experiments = [(r.exp_id, r.label) for r in results if r.exp_id != "(dry-run)"]
    if not experiments:
        return results, []
    ranked = compare_experiments(
        experiments,
        metric=config.compare.metric,
        sort=config.compare.sort,
        top_n=config.compare.top_n,
    )
    return results, ranked
```

- [ ] **Step 4: Run test to verify it passes**

Run: `doppler run -- uv run pytest tests/unit/services/backtest/test_sweep_runner.py -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add src/nse_momentum_lab/services/backtest/sweep_runner.py tests/unit/services/backtest/test_sweep_runner.py
git commit -m "feat: add sweep runner engine with auto-comparison"
```

---

## Task 4: Sweep CLI Entrypoint

**Files:**
- Create: `src/nse_momentum_lab/cli/sweep.py`
- Create: `sweeps/example_threshold.yaml`
- Create: `sweeps/example_breakdown_profiles.yaml`
- Modify: `pyproject.toml` (register script)

- [ ] **Step 1: Create example YAML sweep configs**

```yaml
# sweeps/example_threshold.yaml
name: threshold-breakout-sweep
strategy: thresholdbreakout
base_params:
  universe_size: 2000
  min_price: 10
  min_filters: 5
  entry_timeframe: "5min"
  start_year: 2015
  end_year: 2025
sweep:
  - param: breakout_threshold
    values: [0.02, 0.03, 0.04, 0.05]
  - param: trail_activation_pct
    values: [0.06, 0.08, 0.10]
compare:
  metric: calmar_ratio
  sort: desc
  top_n: 5
tags: [threshold, breakout, research]
```

```yaml
# sweeps/example_breakdown_profiles.yaml
name: breakdown-profile-sweep
strategy: thresholdbreakdown
base_params:
  universe_size: 2000
  min_price: 10
  min_filters: 5
  entry_timeframe: "5min"
  start_year: 2020
  end_year: 2025
sweep:
  - param: breakout_threshold
    values: [0.02, 0.04]
  - param: short_trail_activation_pct
    values: [0.02, 0.03, 0.04]
  - param: short_time_stop_days
    values: [2, 3]
compare:
  metric: calmar_ratio
  sort: desc
  top_n: 10
tags: [breakdown, short, research]
```

- [ ] **Step 2: Create sweep CLI entrypoint**

```python
# src/nse_momentum_lab/cli/sweep.py
"""CLI entrypoint for nseml-sweep — YAML-driven backtest parameter sweeps."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from nse_momentum_lab.services.backtest.comparison import format_comparison_table
from nse_momentum_lab.services.backtest.sweep_runner import run_sweep_with_comparison
from nse_momentum_lab.services.backtest.sweep_schema import load_sweep_config

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nseml-sweep",
        description="Run parameter sweeps defined in YAML and auto-compare results.",
    )
    parser.add_argument("config", type=Path, help="Path to sweep YAML config")
    parser.add_argument("--dry-run", action="store_true", help="Show combinations without running")
    parser.add_argument("--force", action="store_true", help="Re-run even if cached")
    parser.add_argument("--snapshot", action="store_true", help="Publish DuckDB snapshot per run")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = build_parser().parse_args()

    config = load_sweep_config(args.config)
    logger.info("Sweep: %s (%d combinations)", config.name, len(config.combinations()))

    results, ranked = run_sweep_with_comparison(
        config,
        force=args.force,
        snapshot=args.snapshot,
        dry_run=args.dry_run,
    )

    if args.json:
        output = {
            "sweep": config.name,
            "total_combinations": len(config.combinations()),
            "results": [
                {"exp_id": r.exp_id, "label": r.label, "params": r.params}
                for r in results
            ],
            "ranked": [
                {
                    "exp_id": s.exp_id, "label": s.label,
                    "calmar_ratio": s.calmar_ratio, "win_rate": s.win_rate,
                    "annualised_return": s.annualised_return,
                    "max_drawdown": s.max_drawdown,
                }
                for s in ranked
            ],
        }
        print(json.dumps(output, indent=2))
    else:
        print(f"\n{'='*60}")
        print(f"Sweep: {config.name}")
        print(f"{'='*60}")
        print(f"Total combinations: {len(config.combinations())}")
        print(f"Completed: {len(results)}")
        print()
        for r in results:
            print(f"  {r.label:40s} → {r.exp_id}")
        if ranked:
            print(f"\n{'='*60}")
            print(f"Ranked Results (top {config.compare.top_n}):")
            print(f"{'='*60}")
            print(format_comparison_table(ranked))


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Register in pyproject.toml**

Add to `[project.scripts]`:
```toml
nseml-sweep = "nse_momentum_lab.cli.sweep:main"
```

- [ ] **Step 4: Verify sweep CLI works end-to-end**

Run: `doppler run -- uv run nseml-sweep sweeps/example_threshold.yaml --dry-run`
Expected: Shows all 12 combinations without running

- [ ] **Step 5: Commit**

```bash
git add src/nse_momentum_lab/cli/sweep.py sweeps/ pyproject.toml
git commit -m "feat: add nseml-sweep CLI with YAML-driven parameter sweeps"
```

---

## Task 5: Paper Session Lifecycle Manager

**Files:**
- Create: `src/nse_momentum_lab/services/paper/lifecycle_manager.py`
- Create: `src/nse_momentum_lab/cli/paper_lifecycle.py`
- Test: `tests/unit/services/paper/test_lifecycle_manager.py`
- Modify: `pyproject.toml` (register script)

**Design:** Calls `_build_daily_prepare_report` directly for readiness verdict (not `_cmd_daily_prepare` which only prints JSON). Captures stdout from `_cmd_daily_live` to parse session_id. Uses `date` objects for trade_date. Uses cartesian product for strategy×threshold variants. Uses `execute=False, run=False` to bootstrap sessions without blocking websocket loop.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/services/paper/test_lifecycle_manager.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_lifecycle_blocked_skips_live():
    """BLOCKED verdict skips live start."""
    with patch(
        "nse_momentum_lab.services.paper.lifecycle_manager._check_readiness",
        new_callable=AsyncMock,
        return_value={"verdict": "BLOCKED", "coverage_ready": False, "reasons": ["data_coverage_gap"]},
    ) as mock_ready, patch(
        "nse_momentum_lab.services.paper.lifecycle_manager._start_live_session",
        new_callable=AsyncMock,
    ) as mock_live:
        from nse_momentum_lab.services.paper.lifecycle_manager import (
            LifecycleConfig,
            run_daily_lifecycle,
        )
        config = LifecycleConfig(
            trade_date=date(2026, 4, 1),
            strategy_variants=[("thresholdbreakout", 0.04)],
        )
        result = await run_daily_lifecycle(config)
        assert result["prepare_verdict"] == "BLOCKED"
        mock_live.assert_not_called()


@pytest.mark.asyncio
async def test_lifecycle_ready_starts_live():
    """READY verdict starts live sessions for each variant."""
    with patch(
        "nse_momentum_lab.services.paper.lifecycle_manager._check_readiness",
        new_callable=AsyncMock,
        return_value={"verdict": "READY", "coverage_ready": True, "reasons": [], "remediation": []},
    ) as mock_ready, patch(
        "nse_momentum_lab.services.paper.lifecycle_manager._start_live_session",
        new_callable=AsyncMock,
        return_value={"session_id": "test-session", "strategy": "thresholdbreakout", "threshold": 0.04, "status": "LIVE"},
    ) as mock_live:
        from nse_momentum_lab.services.paper.lifecycle_manager import (
            LifecycleConfig,
            run_daily_lifecycle,
        )
        config = LifecycleConfig(
            trade_date=date(2026, 4, 1),
            strategy_variants=[
                ("thresholdbreakout", 0.04),
                ("thresholdbreakout", 0.02),
                ("thresholdbreakdown", 0.04),
            ],
            auto_flatten=False,
        )
        result = await run_daily_lifecycle(config)
        assert result["prepare_verdict"] == "READY"
        assert mock_live.call_count == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `doppler run -- uv run pytest tests/unit/services/paper/test_lifecycle_manager.py -v`
Expected: FAIL — module does not exist

- [ ] **Step 3: Write lifecycle_manager.py**

```python
# src/nse_momentum_lab/services/paper/lifecycle_manager.py
"""Paper session lifecycle manager — auto prepare, start, flatten, archive."""
from __future__ import annotations

import argparse
import io
import json as _json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class LifecycleConfig:
    """Configuration for paper session lifecycle."""

    trade_date: date
    strategy_variants: list[tuple[str, float]] = field(default_factory=list)
    auto_flatten: bool = False
    auto_archive: bool = False
    max_age_hours: int = 24

    def __post_init__(self) -> None:
        if not self.strategy_variants:
            # Default: 4 variants matching current daily workflow
            self.strategy_variants = [
                ("thresholdbreakout", 0.04),
                ("thresholdbreakout", 0.02),
                ("thresholdbreakdown", 0.04),
                ("thresholdbreakdown", 0.02),
            ]


async def _check_readiness(trade_date: date) -> dict[str, Any]:
    """Check daily readiness by calling _build_daily_prepare_report directly.

    Returns dict with 'verdict' key: READY, OBSERVE_ONLY, or BLOCKED.
    This avoids the problem of _cmd_daily_prepare printing JSON to stdout
    and not raising on blocked readiness.
    """
    from nse_momentum_lab.cli.paper import _build_daily_prepare_report, _resolve_daily_symbols

    # Build symbols list (all symbols, live mode — must pass live=True)
    args = argparse.Namespace(symbols=None, all_symbols=True)
    symbols = _resolve_daily_symbols(args, trade_date, live=True)

    report = _build_daily_prepare_report(
        trade_date=trade_date,
        symbols=symbols,
        mode="live",
    )
    verdict = report.get("verdict", "BLOCKED")
    return {
        "verdict": verdict,
        "coverage_ready": report.get("coverage_ready", False),
        "reasons": report.get("reasons", []),
        "remediation": report.get("remediation", []),
    }


async def _start_live_session(
    trade_date: date, strategy: str, threshold: float
) -> dict[str, Any]:
    """Start a live session for one strategy variant.

    Passes threshold via strategy_params so session IDs are distinct per variant.
    Uses date object (not string) for trade_date to match _resolve_trade_date_arg contract.
    Uses execute=False, run=False to bootstrap session without starting websocket loop
    (the websocket loop blocks, preventing remaining variants from starting).
    Captures stdout JSON to extract session_id.
    """
    from nse_momentum_lab.cli.paper import _cmd_daily_live

    strategy_params = f'{{"breakout_threshold": {threshold}}}'

    args = argparse.Namespace(
        trade_date=trade_date,  # Pass date object, NOT .isoformat()
        strategy=strategy,
        experiment_id=None,
        symbols=None,
        all_symbols=True,
        session_id=None,
        strategy_params=strategy_params,
        risk_config=None,
        notes=None,
        feed_mode=None,
        execute=False,  # Don't execute live entries
        run=False,      # Don't start websocket loop (it blocks!)
        observe=False,
        watchlist=False,
    )
    # Capture stdout to parse session_id from JSON output
    captured = io.StringIO()
    real_stdout = sys.stdout
    try:
        sys.stdout = captured
        await _cmd_daily_live(args)
    except SystemExit:
        pass  # _cmd_daily_live may call sys.exit on coverage gaps
    except Exception as e:
        return {"strategy": strategy, "threshold": threshold, "status": "ERROR", "reason": str(e)}
    finally:
        sys.stdout = real_stdout

    # Parse captured JSON — _cmd_daily_live prints non-JSON lines first
    # (e.g. "Operational universe: ...", "[WATCHLIST] ...") followed by
    # a pretty-printed JSON payload (indent=2) with session_id spanning
    # multiple lines. Extract the JSON blob by finding the first '{' at
    # the start of a line and parsing from there to EOF.
    output = captured.getvalue()
    json_start = None
    for i, line in enumerate(output.split("\n")):
        stripped = line.lstrip()
        if stripped.startswith("{"):
            json_start = i
            break
    if json_start is not None:
        json_blob = "\n".join(output.split("\n")[json_start:])
        try:
            parsed = _json.loads(json_blob)
            return {
                "strategy": strategy,
                "threshold": threshold,
                "session_id": parsed.get("session_id"),
                "status": parsed.get("status", "PLANNING"),
            }
        except (_json.JSONDecodeError, ValueError):
            pass  # Fall through

    return {
        "strategy": strategy,
        "threshold": threshold,
        "status": "UNKNOWN",
        "note": "Could not parse session_id from output",
    }


async def _flatten_session(session_id: str) -> dict[str, Any]:
    """Flatten open positions for a session."""
    from nse_momentum_lab.cli.paper import _cmd_flatten

    args = argparse.Namespace(
        session_id=session_id,
        notes="auto-flatten by lifecycle manager",
    )
    try:
        await _cmd_flatten(args)
        return {"session_id": session_id, "status": "FLATTENED"}
    except Exception as e:
        return {"session_id": session_id, "status": "ERROR", "reason": str(e)}


async def run_daily_lifecycle(config: LifecycleConfig) -> dict[str, Any]:
    """Run the full daily lifecycle: prepare → live-start → (optional flatten/archive)."""
    result: dict[str, Any] = {
        "trade_date": config.trade_date.isoformat(),
        "actions": [],
    }

    # Step 1: Prepare
    prepare_result = await _check_readiness(config.trade_date)
    verdict = prepare_result["verdict"]
    result["prepare_verdict"] = verdict
    result["actions"].append({"action": "prepare", **prepare_result})

    if verdict != "READY":
        logger.warning("Daily prepare verdict: %s — skipping live start", verdict)
        return result

    # Step 2: Start live sessions for each (strategy, threshold) variant
    started_sessions: list[str] = []
    for strategy_name, threshold in config.strategy_variants:
        live_result = await _start_live_session(config.trade_date, strategy_name, threshold)
        result["actions"].append(live_result)
        session_id = live_result.get("session_id", "")
        if session_id:
            started_sessions.append(session_id)
        logger.info("Live session for %s @ %.0f%%: %s", strategy_name, threshold * 100, live_result["status"])

    # Step 3: Auto-flatten (if configured)
    # NOTE: auto_flatten requires a scheduler/timer to trigger at EOD.
    # This is a placeholder for future cron integration.
    if config.auto_flatten and started_sessions:
        result["actions"].append({
            "action": "auto-flatten",
            "status": "SCHEDULED",
            "sessions": started_sessions,
            "note": "auto-flatten requires external scheduler (cron/Taskfile)",
        })

    return result
```

- [ ] **Step 4: Create paper_lifecycle.py CLI**

```python
# src/nse_momentum_lab/cli/paper_lifecycle.py
"""CLI entrypoint for nseml-paper-lifecycle — auto-manage paper sessions."""
from __future__ import annotations

import argparse
import asyncio
import itertools
import logging
import sys
from datetime import date

from nse_momentum_lab.services.paper.lifecycle_manager import (
    LifecycleConfig,
    run_daily_lifecycle,
)

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nseml-paper-lifecycle",
        description="Auto-manage paper trading session lifecycle.",
    )
    parser.add_argument("--date", "-d", type=str, help="Trading date (YYYY-MM-DD)")
    parser.add_argument(
        "--strategies", nargs="+", default=[],
        help="Strategy names (default: thresholdbreakout + thresholdbreakdown @ 2% and 4%)",
    )
    parser.add_argument(
        "--thresholds", nargs="+", type=float, default=[],
        help="Threshold values (default: 0.02, 0.04). Cartesian product with --strategies.",
    )
    parser.add_argument("--auto-flatten", action="store_true", help="Schedule auto-flatten at EOD")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen")
    return parser


def _resolve_variants(args: argparse.Namespace) -> list[tuple[str, float]]:
    """Build (strategy, threshold) variant list from CLI args.

    Uses cartesian product: --strategies thresholdbreakout --thresholds 0.02 0.04
    produces [(thresholdbreakout, 0.02), (thresholdbreakout, 0.04)].
    """
    strategies = args.strategies or []
    thresholds = args.thresholds or []
    if strategies and thresholds:
        return list(itertools.product(strategies, thresholds))
    if strategies:
        return [(s, t) for s in strategies for t in [0.02, 0.04]]
    # No args — use all 4 default variants
    return []  # LifecycleConfig.__post_init__ fills defaults


async def _run(args: argparse.Namespace) -> None:
    trade_date = date.fromisoformat(args.date) if args.date else date.today()
    variants = _resolve_variants(args)
    config = LifecycleConfig(
        trade_date=trade_date,
        strategy_variants=variants,
        auto_flatten=args.auto_flatten,
    )
    if args.dry_run:
        print(f"[DRY-RUN] Would run lifecycle for {trade_date}")
        print(f"  Variants: {config.strategy_variants}")
        print(f"  Auto-flatten: {config.auto_flatten}")
        return
    result = await run_daily_lifecycle(config)
    print(f"Lifecycle result for {result['trade_date']}: {result['prepare_verdict']}")
    for action in result["actions"]:
        print(f"  {action}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Register in pyproject.toml**

Add to `[project.scripts]`:
```toml
nseml-paper-lifecycle = "nse_momentum_lab.cli.paper_lifecycle:main"
```

- [ ] **Step 6: Run tests**

Run: `doppler run -- uv run pytest tests/unit/services/paper/test_lifecycle_manager.py -v`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add src/nse_momentum_lab/services/paper/lifecycle_manager.py src/nse_momentum_lab/cli/paper_lifecycle.py tests/unit/services/paper/test_lifecycle_manager.py pyproject.toml
git commit -m "feat: add paper session lifecycle manager with distinct variant tracking"
```

---

## Task 6: EOD One-Shot Runner

**Files:**
- Create: `src/nse_momentum_lab/cli/eod.py` (thin wrapper around existing `pipeline.py`)
- Modify: `pyproject.toml` (register script)

**Design:** Thin argparse wrapper that calls existing `run_daily_pipeline()`. No parallel entrypoint — just a convenience alias. Uses real `PipelineResult` API (`overall_status`, `stages` dict).

- [ ] **Step 1: Create eod.py**

```python
# src/nse_momentum_lab/cli/eod.py
"""CLI entrypoint for nseml-eod — one-shot EOD pipeline runner.

Thin wrapper around existing pipeline.run_daily_pipeline().
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, timedelta

from nse_momentum_lab.cli.pipeline import run_daily_pipeline

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nseml-eod",
        description="Run the EOD data pipeline for a given date (ingest → features → scan → rollup).",
    )
    parser.add_argument("--date", "-d", type=str, help="Trading date YYYY-MM-DD (default: today)")
    parser.add_argument("--yesterday", "-y", action="store_true", help="Use yesterday's date")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip ingestion stage")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run")
    return parser


def _resolve_date(args: argparse.Namespace) -> date:
    if args.yesterday:
        return date.today() - timedelta(days=1)
    if args.date:
        return date.fromisoformat(args.date)
    return date.today()


async def _run(args: argparse.Namespace) -> None:
    trading_date = _resolve_date(args)

    if args.dry_run:
        logger.info("[DRY-RUN] Would run EOD pipeline for %s (skip_ingest=%s)", trading_date, args.skip_ingest)
        return

    logger.info("Starting EOD pipeline for %s", trading_date)
    result = await run_daily_pipeline(
        trading_date=trading_date,
        skip_ingest=args.skip_ingest,
        track_job=True,
    )

    # Use real PipelineResult API: overall_status is a string
    if "FAILED" in result.overall_status:
        logger.error("EOD pipeline FAILED: %s", result.overall_status)
        sys.exit(1)
    elif result.overall_status == "SKIPPED":
        logger.info("EOD pipeline SKIPPED (already completed)")
    else:
        logger.info("EOD pipeline completed: %s", result.overall_status)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Register in pyproject.toml**

Add to `[project.scripts]`:
```toml
nseml-eod = "nse_momentum_lab.cli.eod:main"
```

- [ ] **Step 3: Verify CLI works**

Run: `doppler run -- uv run nseml-eod --dry-run`
Expected: `[DRY-RUN] Would run EOD pipeline for ...`

- [ ] **Step 4: Commit**

```bash
git add src/nse_momentum_lab/cli/eod.py pyproject.toml
git commit -m "feat: add nseml-eod one-shot EOD pipeline runner"
```

---

## Task 7: Run Full Test Suite and Lint (scoped to new files)

- [ ] **Step 1: Run all unit tests**

Run: `doppler run -- uv run pytest tests/unit/ -q`
Expected: ALL PASS

- [ ] **Step 2: Run linter on NEW files only**

Run: `doppler run -- uv run ruff check src/nse_momentum_lab/cli/sweep.py src/nse_momentum_lab/cli/eod.py src/nse_momentum_lab/cli/paper_lifecycle.py src/nse_momentum_lab/services/backtest/sweep_schema.py src/nse_momentum_lab/services/backtest/sweep_runner.py src/nse_momentum_lab/services/backtest/comparison.py src/nse_momentum_lab/services/paper/lifecycle_manager.py`

Expected: Clean (no errors)

- [ ] **Step 3: Fix any lint issues (scoped to new files only)**

Run: `doppler run -- uv run ruff check --fix src/nse_momentum_lab/cli/sweep.py src/nse_momentum_lab/cli/eod.py src/nse_momentum_lab/cli/paper_lifecycle.py src/nse_momentum_lab/services/backtest/sweep_schema.py src/nse_momentum_lab/services/backtest/sweep_runner.py src/nse_momentum_lab/services/backtest/comparison.py src/nse_momentum_lab/services/paper/lifecycle_manager.py`

- [ ] **Step 4: Final commit (if lint fixes needed)**

```bash
git add -u
git commit -m "chore: fix lint in automation layer"
```

---

## Summary of New Commands

| Command | Purpose | Replaces |
|---------|---------|----------|
| `nseml-sweep sweeps/config.yaml` | Run YAML-defined parameter sweep with auto-compare | Manual backtest + copy exp_id + compare |
| `nseml-sweep sweeps/config.yaml --dry-run` | Preview sweep combinations | Manual param counting |
| `nseml-sweep sweeps/config.yaml --json` | Machine-readable results | Manual result parsing |
| `nseml-eod` | Run full EOD pipeline for today | 6 manual commands per evening |
| `nseml-eod -d 2026-04-01 --skip-ingest` | Run with options | Manual pipeline.py invocation |
| `nseml-paper-lifecycle` | Auto daily paper session lifecycle (4 default variants) | Manual prepare + live x4 |
| `nseml-paper-lifecycle --strategies thresholdbreakout --thresholds 0.02 0.04` | Custom variants (cartesian product) | Manual per-variant commands |
