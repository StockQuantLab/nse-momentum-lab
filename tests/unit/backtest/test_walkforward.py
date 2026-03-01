from datetime import date

from nse_momentum_lab.services.backtest.walkforward import (
    WalkForwardFramework,
    WalkForwardResult,
    WalkForwardWindow,
    run_walk_forward,
)


class TestWalkForwardWindow:
    def test_window_creation(self) -> None:
        window = WalkForwardWindow(
            train_start=date(2020, 1, 1),
            train_end=date(2022, 12, 31),
            test_start=date(2023, 1, 1),
            test_end=date(2023, 6, 30),
        )
        assert window.train_start == date(2020, 1, 1)
        assert window.train_end == date(2022, 12, 31)
        assert window.test_start == date(2023, 1, 1)
        assert window.test_end == date(2023, 6, 30)


class TestWalkForwardFramework:
    def test_default_params(self) -> None:
        framework = WalkForwardFramework()
        assert framework.train_years == 3
        assert framework.test_months == 6

    def test_custom_params(self) -> None:
        framework = WalkForwardFramework(train_years=2, test_months=3)
        assert framework.train_years == 2
        assert framework.test_months == 3

    def test_generate_windows(self) -> None:
        framework = WalkForwardFramework(train_years=1, test_months=2)
        data_start = date(2020, 1, 1)
        data_end = date(2023, 12, 31)

        windows = list(framework.generate_windows(data_start, data_end))

        assert len(windows) > 0
        for window in windows:
            assert window.train_end < window.test_start
            assert window.train_start <= window.train_end
            assert window.test_start <= window.test_end
            assert window.train_end >= data_start
            assert window.test_end <= data_end

    def test_no_overlap(self) -> None:
        framework = WalkForwardFramework(train_years=1, test_months=6)
        data_start = date(2020, 1, 1)
        data_end = date(2024, 12, 31)

        windows = list(framework.generate_windows(data_start, data_end, roll_interval_days=180))

        for i, w1 in enumerate(windows):
            for j, w2 in enumerate(windows):
                if i != j:
                    assert not (w1.test_start <= w2.test_start <= w1.test_end)


class TestWalkForwardResult:
    def test_result_creation(self) -> None:
        window = WalkForwardWindow(
            train_start=date(2020, 1, 1),
            train_end=date(2022, 12, 31),
            test_start=date(2023, 1, 1),
            test_end=date(2023, 6, 30),
        )
        result = WalkForwardResult(
            window=window,
            train_result=None,
            test_result=None,
            params_used={"train_years": 3, "test_months": 6},
        )
        assert result.window == window
        assert result.train_result is None
        assert result.test_result is None
        assert result.params_used["train_years"] == 3


class TestRunWalkForwardConvenienceFunction:
    def test_run_walk_forward_function(self) -> None:
        result = run_walk_forward(
            strategy_name="test",
            signals=[],
            price_data={},
            value_traded_inr={},
            data_start=date(2020, 1, 1),
            data_end=date(2024, 12, 31),
        )
        assert isinstance(result, list)
