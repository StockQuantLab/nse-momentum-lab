from __future__ import annotations

from nse_momentum_lab.services.backtest.comparison import (
    ExperimentSummary,
    format_comparison_table,
    rank_experiments,
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
