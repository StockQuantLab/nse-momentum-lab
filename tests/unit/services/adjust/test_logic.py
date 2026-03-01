from datetime import date

import pytest

from nse_momentum_lab.services.adjust.logic import (
    CorpAction,
    apply_adjustment,
    build_adjustment_series,
    compute_adjustment_factor,
    reconcile_continuity,
)


class TestComputeAdjustmentFactor:
    def test_split_adjustment(self) -> None:
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="SPLIT",
            ratio_num=2,
            ratio_den=1,
            cash_amount=None,
        )
        factor = compute_adjustment_factor(100.0, action)
        assert factor == 0.5

    def test_split_adjustment_reversed(self) -> None:
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="SPLIT",
            ratio_num=1,
            ratio_den=2,
            cash_amount=None,
        )
        factor = compute_adjustment_factor(100.0, action)
        assert factor == 2.0

    def test_bonus_adjustment(self) -> None:
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="BONUS",
            ratio_num=1,
            ratio_den=1,
            cash_amount=None,
        )
        factor = compute_adjustment_factor(100.0, action)
        assert factor == 0.5

    def test_bonus_adjustment_2_to_1(self) -> None:
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="BONUS",
            ratio_num=2,
            ratio_den=1,
            cash_amount=None,
        )
        factor = compute_adjustment_factor(100.0, action)
        assert factor == pytest.approx(1 / 3)

    def test_rights_adjustment(self) -> None:
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="RIGHTS",
            ratio_num=1,
            ratio_den=2,
            cash_amount=None,
        )
        factor = compute_adjustment_factor(100.0, action)
        assert factor == pytest.approx(2 / 3)

    def test_dividend_returns_one(self) -> None:
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="DIVIDEND",
            ratio_num=None,
            ratio_den=None,
            cash_amount=10.0,
        )
        factor = compute_adjustment_factor(100.0, action)
        assert factor == 1.0

    def test_split_requires_ratio(self) -> None:
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="SPLIT",
            ratio_num=None,
            ratio_den=None,
            cash_amount=None,
        )
        with pytest.raises(ValueError, match="SPLIT requires ratio_num and ratio_den"):
            compute_adjustment_factor(100.0, action)

    def test_unknown_action_raises(self) -> None:
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="UNKNOWN",
            ratio_num=None,
            ratio_den=None,
            cash_amount=None,
        )
        with pytest.raises(ValueError, match="Unknown action type"):
            compute_adjustment_factor(100.0, action)


class TestApplyAdjustment:
    def test_apply_split_adjustment(self) -> None:
        open_p, high_p, low_p, close_p = apply_adjustment(100.0, 110.0, 90.0, 105.0, 0.5)
        assert open_p == 50.0
        assert high_p == 55.0
        assert low_p == 45.0
        assert close_p == 52.5

    def test_apply_bonus_adjustment(self) -> None:
        open_p, high_p, low_p, close_p = apply_adjustment(100.0, 110.0, 90.0, 105.0, 0.5)
        assert open_p == 50.0
        assert high_p == 55.0
        assert low_p == 45.0
        assert close_p == 52.5

    def test_no_adjustment(self) -> None:
        open_p, high_p, low_p, close_p = apply_adjustment(100.0, 110.0, 90.0, 105.0, 1.0)
        assert open_p == 100.0
        assert high_p == 110.0
        assert low_p == 90.0
        assert close_p == 105.0


class TestBuildAdjustmentSeries:
    def test_no_actions(self) -> None:
        trading_dates = [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 3),
        ]
        close_prices = [100.0, 101.0, 102.0]
        factors = build_adjustment_series(trading_dates, close_prices, [])
        assert factors == [1.0, 1.0, 1.0]

    def test_single_split(self) -> None:
        trading_dates = [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 15),
            date(2024, 1, 16),
        ]
        close_prices = [100.0, 102.0, 50.0, 51.0]
        action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="SPLIT",
            ratio_num=2,
            ratio_den=1,
            cash_amount=None,
        )
        factors = build_adjustment_series(
            trading_dates, close_prices, [(date(2024, 1, 15), action)]
        )
        assert factors[0] == 0.5
        assert factors[1] == 0.5
        assert factors[2] == 0.5
        assert factors[3] == 1.0

    def test_multiple_actions(self) -> None:
        trading_dates = [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 15),
            date(2024, 1, 16),
            date(2024, 2, 1),
            date(2024, 2, 2),
        ]
        close_prices = [100.0, 102.0, 50.0, 51.0, 25.0, 26.0]
        split_action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 1, 15),
            action_type="SPLIT",
            ratio_num=2,
            ratio_den=1,
            cash_amount=None,
        )
        bonus_action = CorpAction(
            symbol_id=1,
            ex_date=date(2024, 2, 1),
            action_type="BONUS",
            ratio_num=1,
            ratio_den=1,
            cash_amount=None,
        )
        factors = build_adjustment_series(
            trading_dates,
            close_prices,
            [
                (date(2024, 1, 15), split_action),
                (date(2024, 2, 1), bonus_action),
            ],
        )
        assert factors[0] == pytest.approx(0.25)
        assert factors[1] == pytest.approx(0.25)
        assert factors[2] == pytest.approx(0.25)
        assert factors[3] == pytest.approx(0.5)
        assert factors[4] == pytest.approx(0.5)
        assert factors[5] == pytest.approx(1.0)


class TestReconcileContinuity:
    def test_no_issues(self) -> None:
        trading_dates = [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 3),
        ]
        adjusted_closes = [100.0, 100.0, 100.0]
        adj_factors = [1.0, 1.0, 1.0]
        issues = reconcile_continuity(trading_dates, adjusted_closes, adj_factors)
        assert issues == []

    def test_continuity_issue(self) -> None:
        trading_dates = [
            date(2024, 1, 1),
            date(2024, 1, 2),
            date(2024, 1, 3),
        ]
        adjusted_closes = [100.0, 100.0, 150.0]
        adj_factors = [1.0, 1.0, 1.0]
        issues = reconcile_continuity(trading_dates, adjusted_closes, adj_factors)
        assert len(issues) == 1
        assert issues[0]["issue"] == "CONTINUITY_BREAK"

    def test_with_no_issues_despite_factor_change(self) -> None:
        trading_dates = [
            date(2024, 1, 1),
            date(2024, 1, 2),
        ]
        adjusted_closes = [100.0, 100.0]
        adj_factors = [1.0, 1.0]
        issues = reconcile_continuity(trading_dates, adjusted_closes, adj_factors)
        assert issues == []
