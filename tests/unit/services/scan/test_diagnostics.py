from datetime import date

from nse_momentum_lab.services.scan.rules import (
    ScanCandidate,
    ScanCheck,
    ScanConfig,
    ScanDiagnostics,
    ScanRuleEngine,
    aggregate_scan_diagnostics,
)


class TestScanDiagnostics:
    def test_diagnostics_defaults(self) -> None:
        diag = ScanDiagnostics()
        assert diag.total_scanned == 0
        assert diag.passed_breakout_4p == 0
        assert diag.passed_h == 0
        assert diag.passed_n == 0
        assert diag.passed_2 == 0
        assert diag.passed_y == 0
        assert diag.passed_l == 0
        assert diag.passed_c == 0
        assert diag.passed_liquidity == 0
        assert diag.passed_all == 0
        assert diag.fail_reason_counts == {}


class TestCheckLiquidity:
    def setup_method(self) -> None:
        self.engine = ScanRuleEngine()

    def test_liquidity_pass(self) -> None:
        from nse_momentum_lab.services.scan.features import DailyFeatures

        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=105.0,
            low=98.0,
            ret_1d=0.02,
            ret_5d=0.10,
            atr_20=5.0,
            range_pct=0.07,
            close_pos_in_range=0.8,
            ma_20=98.0,
            ma_65=95.0,
            rs_252=0.05,
            vol_20=1000000,
            dollar_vol_20=5000000.0,  # ₹50 lakh - above threshold
        )
        check = self.engine.check_liquidity(features)
        assert check.letter == "LIQ"
        assert check.passed is True
        assert "Rs" in check.reason

    def test_liquidity_fail_below_threshold(self) -> None:
        from nse_momentum_lab.services.scan.features import DailyFeatures

        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=105.0,
            low=98.0,
            ret_1d=0.02,
            ret_5d=0.10,
            atr_20=5.0,
            range_pct=0.07,
            close_pos_in_range=0.8,
            ma_20=98.0,
            ma_65=95.0,
            rs_252=0.05,
            vol_20=1000000,
            dollar_vol_20=100000.0,  # ₹1 lakh - below threshold
        )
        check = self.engine.check_liquidity(features)
        assert check.letter == "LIQ"
        assert check.passed is False
        assert "<" in check.reason

    def test_liquidity_no_data(self) -> None:
        from nse_momentum_lab.services.scan.features import DailyFeatures

        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=100.0,
            high=105.0,
            low=98.0,
            ret_1d=0.02,
            ret_5d=0.10,
            atr_20=5.0,
            range_pct=0.07,
            close_pos_in_range=0.8,
            ma_20=98.0,
            ma_65=95.0,
            rs_252=0.05,
            vol_20=1000000,
            dollar_vol_20=None,
        )
        check = self.engine.check_liquidity(features)
        assert check.letter == "LIQ"
        assert check.passed is False
        assert "No value traded data" in check.reason


class TestAggregateDiagnostics:
    def test_empty_candidates(self) -> None:
        diag = aggregate_scan_diagnostics([])
        assert diag.total_scanned == 0
        assert diag.passed_all == 0

    def test_single_candidate_all_passed(self) -> None:
        candidate = ScanCandidate(
            symbol_id=1,
            symbol="TEST",
            trading_date=date(2024, 1, 1),
            score=1.0,
            checks=[
                ScanCheck(letter="4P", passed=True, reason="Breakout"),
                ScanCheck(letter="H", passed=True, reason="Close position"),
                ScanCheck(letter="LIQ", passed=True, reason="Liquidity"),
            ],
            passed=True,
            reason_json={},
        )
        diag = aggregate_scan_diagnostics([candidate])
        assert diag.total_scanned == 1
        assert diag.passed_breakout_4p == 1
        assert diag.passed_h == 1
        assert diag.passed_liquidity == 1
        assert diag.passed_all == 1

    def test_single_candidate_all_failed(self) -> None:
        candidate = ScanCandidate(
            symbol_id=1,
            symbol="TEST",
            trading_date=date(2024, 1, 1),
            score=0.0,
            checks=[
                ScanCheck(letter="4P", passed=False, reason="Not a breakout"),
                ScanCheck(letter="H", passed=False, reason="Close not near high"),
                ScanCheck(letter="LIQ", passed=False, reason="Below liquidity threshold"),
            ],
            passed=False,
            reason_json={},
        )
        diag = aggregate_scan_diagnostics([candidate])
        assert diag.total_scanned == 1
        assert diag.passed_breakout_4p == 0
        assert diag.passed_h == 0
        assert diag.passed_liquidity == 0
        assert diag.passed_all == 0
        assert len(diag.fail_reason_counts) > 0


class TestScanConfigWithLiquidity:
    def test_default_liquidity_threshold(self) -> None:
        config = ScanConfig()
        assert config.min_value_traded_inr == 3000000.0

    def test_custom_liquidity_threshold(self) -> None:
        config = ScanConfig(min_value_traded_inr=5000000.0)
        assert config.min_value_traded_inr == 5000000.0


class TestCheckMethods:
    def setup_method(self) -> None:
        self.engine = ScanRuleEngine()

    def test_check_2_both_up(self) -> None:
        from nse_momentum_lab.services.scan.features import DailyFeatures

        prev1 = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
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
            trading_date=date(2024, 1, 1),
            close=98.0,
            high=99.0,
            low=97.0,
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
        check = self.engine.check_2(features, prev1, prev2)
        assert check.letter == "2"
        assert check.passed is False

    def test_check_2_one_down(self) -> None:
        from nse_momentum_lab.services.scan.features import DailyFeatures

        prev1 = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 2),
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
        prev2 = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 1),
            close=98.0,
            high=99.0,
            low=97.0,
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
        check = self.engine.check_2(features, prev1, prev2)
        assert check.letter == "2"
        assert check.passed is True

    def test_check_2_no_history(self) -> None:
        from nse_momentum_lab.services.scan.features import DailyFeatures

        features = DailyFeatures(
            symbol_id=1,
            trading_date=date(2024, 1, 3),
            close=102.0,
            high=103.0,
            low=101.0,
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
        check = self.engine.check_2(features, None, None)
        assert check.letter == "2"
        assert check.passed is False
        assert "Insufficient return history" in check.reason
