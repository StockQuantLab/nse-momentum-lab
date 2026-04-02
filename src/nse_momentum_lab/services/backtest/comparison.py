"""Backtest experiment comparison logic — extracted from scripts/compare_backtest_runs.py.

Uses the real MarketDataDB API: db.con.execute(...).pl() / .fetchdf().
Win rate is a percentage (0-100), matching the existing compare script.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

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


def fetch_experiment_summary(db: MarketDataDB, exp_id: str, label: str = "") -> ExperimentSummary:
    """Fetch and compute summary metrics for a single experiment.

    Faithful extraction from scripts/compare_backtest_runs.py:fetch_summary().
    Uses db.con.execute(...) — the real MarketDataDB API.
    """
    # Check experiment exists
    exp_df = db.con.execute("SELECT * FROM bt_experiment WHERE exp_id = ?", [exp_id]).fetchdf()
    if exp_df.empty:
        raise ValueError(f"Experiment {exp_id} not found")

    # Fetch trades
    trades = db.con.execute("SELECT * FROM bt_trade WHERE exp_id = ?", [exp_id]).pl()

    total_trades = len(trades)
    if total_trades == 0:
        return ExperimentSummary(
            exp_id=exp_id,
            label=label or exp_id,
            total_trades=0,
            win_rate=0.0,
            annualised_return=0.0,
            total_return=0.0,
            max_drawdown=0.0,
            profit_factor=0.0,
            calmar_ratio=0.0,
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
