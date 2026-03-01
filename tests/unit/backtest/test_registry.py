from nse_momentum_lab.services.backtest.registry import ExperimentRegistry, ExperimentResult


class TestExperimentResult:
    def test_experiment_result_creation(self) -> None:
        result = ExperimentResult(
            exp_run_id=1,
            exp_hash="abc123",
            status="SUCCEEDED",
            metrics={"sharpe": 1.5, "returns": 0.2},
            trades=[{"symbol": "TEST", "pnl": 100}],
        )
        assert result.exp_run_id == 1
        assert result.status == "SUCCEEDED"
        assert result.metrics["sharpe"] == 1.5


class TestExperimentRegistry:
    def setup_method(self) -> None:
        self.registry = ExperimentRegistry()

    def test_init(self) -> None:
        assert self.registry is not None

    def test_compute_strategy_hash(self) -> None:
        strategy_hash = self.registry._compute_strategy_hash("test_strategy", {"param1": 1})
        assert isinstance(strategy_hash, str)
        assert len(strategy_hash) > 0

    def test_compute_exp_hash(self) -> None:
        exp_hash = self.registry._compute_exp_hash("strategy_hash", "dataset_hash")
        assert isinstance(exp_hash, str)
        assert len(exp_hash) > 0
