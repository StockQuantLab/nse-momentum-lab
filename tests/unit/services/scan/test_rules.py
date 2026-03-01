from datetime import date, timedelta

from nse_momentum_lab.services.scan.features import DailyFeatures, FeatureEngine
from nse_momentum_lab.services.scan.rules import (
    ScanCandidate,
    ScanCheck,
    ScanConfig,
    ScanRuleEngine,
)


def add_days(start_date: date, days: int) -> date:
    return start_date + timedelta(days=days)


class TestScanConfig:
    def test_default_config(self) -> None:
        config = ScanConfig()
        assert config.breakout_threshold == 0.04
        assert config.close_pos_threshold == 0.70
        assert config.nr_percentile == 0.20
        assert config.min_r2_l == 0.70
        assert config.max_down_days_l == 7
        assert config.atr_compress_ratio == 0.80
        assert config.range_percentile == 0.20
        assert config.range_ref_window == 60
        assert config.vol_dryup_ratio == 0.80
        assert config.lookback_high == 20
        assert config.lookback_y == 90
        assert config.lookback_l == 20
        assert config.lookback_c == 15

    def test_custom_config(self) -> None:
        config = ScanConfig(
            breakout_threshold=0.05,
            close_pos_threshold=0.80,
            nr_percentile=0.25,
        )
        assert config.breakout_threshold == 0.05
        assert config.close_pos_threshold == 0.80
        assert config.nr_percentile == 0.25


class TestScanCheck:
    def test_scan_check_creation(self) -> None:
        check = ScanCheck(
            letter="4P",
            passed=True,
            reason="Test passed",
            value=0.05,
        )
        assert check.letter == "4P"
        assert check.passed is True
        assert check.reason == "Test passed"
        assert check.value == 0.05

    def test_scan_check_optional_value(self) -> None:
        check = ScanCheck(
            letter="N",
            passed=False,
            reason="No data",
        )
        assert check.value is None


class TestScanCandidate:
    def test_scan_candidate_creation(self) -> None:
        checks = [
            ScanCheck(letter="4P", passed=True, reason="OK", value=0.05),
            ScanCheck(letter="VOL", passed=True, reason="OK", value=1.2),
        ]
        candidate = ScanCandidate(
            symbol_id=1,
            symbol="TEST",
            trading_date=date(2024, 1, 1),
            score=0.75,
            checks=checks,
            passed=True,
            reason_json={"checks": [], "base_4p": True, "all_2lynch": True},
        )
        assert candidate.symbol_id == 1
        assert candidate.symbol == "TEST"
        assert candidate.passed is True
        assert candidate.score == 0.75


class TestScanRuleEngine:
    def setup_method(self) -> None:
        self.config = ScanConfig()
        self.engine = ScanRuleEngine(self.config)
        self.fe = FeatureEngine()

    def test_check_breakout_no_prev_data(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
            close=100.0,
            high=101.0,
            low=99.0,
            open_adj=102.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_breakout(features, None)
        assert result.letter == "4P"
        assert result.passed is False
        assert "No prior day data" in result.reason

    def test_check_breakout_gap_up_passed(self) -> None:
        prev_features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
            close=100.0,
            high=101.0,
            low=99.0,
            open_adj=99.5,
            ret_1d=0.01,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 3),
            close=105.0,
            high=106.0,
            low=104.0,
            open_adj=104.5,
            ret_1d=0.05,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_breakout(features, prev_features)
        assert result.letter == "4P"
        assert result.passed is True

    def test_check_breakout_gap_down_failed(self) -> None:
        prev_features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
            close=100.0,
            high=101.0,
            low=99.0,
            open_adj=99.5,
            ret_1d=0.01,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 3),
            close=102.0,
            high=103.0,
            low=101.0,
            open_adj=101.0,
            ret_1d=0.02,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_breakout(features, prev_features)
        assert result.passed is False

    def test_check_volume_increase_no_prev_data(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_volume_increase(features, None)
        assert result.letter == "VOL"
        assert result.passed is True

    def test_check_volume_increase(self) -> None:
        prev_features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=1000000.0,
            dollar_vol_20=None,
        )
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
            close=101.0,
            high=102.0,
            low=100.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=1100000.0,
            dollar_vol_20=None,
        )
        result = self.engine.check_volume_increase(features, prev_features)
        assert result.passed is True

    def test_check_h_no_data(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_h(features)
        assert result.letter == "H"
        assert result.passed is False

    def test_check_h_passed(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=0.80,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_h(features)
        assert result.passed is True

    def test_check_n_no_prev_data(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_n(features, None, [], 0)
        assert result.letter == "N"
        assert result.passed is False

    def test_check_n_negative_prior_return(self) -> None:
        prev = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=-0.02,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
            close=98.0,
            high=99.0,
            low=97.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_n(features, prev, [prev, features], 1)
        assert result.passed is True
        assert "Prior day negative return" in result.reason

    def test_check_2_insufficient_history(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_2(features, None, None)
        assert result.passed is False

    def test_check_2_both_up(self) -> None:
        prev1 = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=0.02,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        prev2 = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
            close=102.0,
            high=103.0,
            low=101.0,
            ret_1d=0.03,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 3),
            close=105.0,
            high=106.0,
            low=104.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_2(features, prev1, prev2)
        assert result.passed is False

    def test_check_y_no_prior_data(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        result = self.engine.check_y(features, [features], 0)
        assert result.passed is False

    def test_check_l_insufficient_history(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 5),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        all_features = []
        for i in range(5):
            all_features.append(
                DailyFeatures(
                    symbol_id=1,
                    trading_date=add_days(date(2024, 1, 1), i),
                    close=100.0,
                    high=101.0,
                    low=99.0,
                    ret_1d=0.01,
                    ret_5d=None,
                    atr_20=None,
                    range_pct=None,
                    close_pos_in_range=None,
                    ma_20=None,
                    ma_65=None,
                    rs_252=None,
                    vol_20=None,
                    dollar_vol_20=None,
                )
            )
        result = self.engine.check_l(features, all_features, 4)
        assert result.passed is False

    def test_check_c_insufficient_lookback(self) -> None:
        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 3),
            close=100.0,
            high=101.0,
            low=99.0,
            ret_1d=None,
            ret_5d=None,
            atr_20=None,
            range_pct=None,
            close_pos_in_range=None,
            ma_20=None,
            ma_65=None,
            rs_252=None,
            vol_20=None,
            dollar_vol_20=None,
        )
        all_features = []
        for i in range(3):
            all_features.append(
                DailyFeatures(
                    symbol_id=1,
                    trading_date=add_days(date(2024, 1, 1), i),
                    close=100.0,
                    high=101.0,
                    low=99.0,
                    ret_1d=0.01,
                    ret_5d=None,
                    atr_20=None,
                    range_pct=None,
                    close_pos_in_range=None,
                    ma_20=None,
                    ma_65=None,
                    rs_252=None,
                    vol_20=None,
                    dollar_vol_20=None,
                )
            )
        result = self.engine.check_c(features, all_features, 2)
        assert result.passed is False

    def test_run_scan_empty_features(self) -> None:
        result = self.engine.run_scan(1, "TEST", [], date(2024, 1, 1))
        assert result == []

    def test_run_scan_no_matching_date(self) -> None:
        features = [
            DailyFeatures(
                symbol_id=1,
                trading_date=date(2024, 1, 1),
                close=100.0,
                high=101.0,
                low=99.0,
                ret_1d=None,
                ret_5d=None,
                atr_20=None,
                range_pct=None,
                close_pos_in_range=None,
                ma_20=None,
                ma_65=None,
                rs_252=None,
                vol_20=None,
                dollar_vol_20=None,
            )
        ]
        result = self.engine.run_scan(1, "TEST", features, date(2024, 1, 2))
        assert result == []
