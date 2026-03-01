from datetime import date, timedelta

from nse_momentum_lab.services.scan.features import (
    DailyFeatures,
    FeatureEngine,
    PriceData,
    compute_features,
)


def add_days(start_date: date, days: int) -> date:
    return start_date + timedelta(days=days)


class TestFeatureEngine:
    def setup_method(self) -> None:
        self.engine = FeatureEngine()

    def test_compute_all_empty_prices(self) -> None:
        result = self.engine.compute_all(1, [])
        assert result == []

    def test_compute_all_single_price(self) -> None:
        prices = [
            PriceData(
                trading_date=date(2024, 1, 1),
                open=100.0,
                high=105.0,
                low=98.0,
                close=102.0,
                volume=1000000,
                value_traded=100_000_000.0,
            ),
            PriceData(
                trading_date=date(2024, 1, 2),
                open=102.0,
                high=108.0,
                low=101.0,
                close=106.0,
                volume=1100000,
                value_traded=110_000_000.0,
            ),
        ]
        result = self.engine.compute_all(1, prices)
        assert len(result) == 2

    def test_ret_1d_first_day_none(self) -> None:
        prices = [
            PriceData(
                trading_date=date(2024, 1, 1),
                open=100.0,
                high=105.0,
                low=98.0,
                close=102.0,
                volume=1000000,
                value_traded=100_000_000.0,
            ),
            PriceData(
                trading_date=date(2024, 1, 2),
                open=102.0,
                high=108.0,
                low=101.0,
                close=106.0,
                volume=1100000,
                value_traded=110_000_000.0,
            ),
        ]
        result = self.engine.compute_all(1, prices)
        assert result[0].ret_1d is None
        assert result[1].ret_1d is not None

    def test_atr_20_insufficient_data(self) -> None:
        prices = []
        for i in range(10):
            prices.append(
                PriceData(
                    trading_date=add_days(date(2024, 1, 1), i),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=98.0 + i,
                    close=102.0 + i,
                    volume=1000000,
                    value_traded=100_000_000.0,
                )
            )
        result = self.engine.compute_all(1, prices)
        for i in range(10):
            assert result[i].atr_20 is None

    def test_atr_20_calculated_after_20_days(self) -> None:
        prices = []
        for i in range(30):
            prices.append(
                PriceData(
                    trading_date=add_days(date(2024, 1, 1), i),
                    open=100.0 + i * 0.1,
                    high=105.0 + i * 0.1,
                    low=98.0 + i * 0.1,
                    close=102.0 + i * 0.1,
                    volume=1000000,
                    value_traded=100_000_000.0,
                )
            )
        result = self.engine.compute_all(1, prices)
        for i in range(20):
            assert result[i].atr_20 is None
        assert result[20].atr_20 is not None

    def test_range_pct(self) -> None:
        prices = [
            PriceData(
                trading_date=date(2024, 1, 1),
                open=100.0,
                high=105.0,
                low=98.0,
                close=102.0,
                volume=1000000,
                value_traded=100_000_000.0,
            ),
            PriceData(
                trading_date=date(2024, 1, 2),
                open=102.0,
                high=108.0,
                low=101.0,
                close=106.0,
                volume=1100000,
                value_traded=110_000_000.0,
            ),
        ]
        result = self.engine.compute_all(1, prices)
        assert result[1].range_pct is not None

    def test_close_pos_in_range(self) -> None:
        prices = [
            PriceData(
                trading_date=add_days(date(2024, 1, 1), 1),
                open=100.0,
                high=105.0,
                low=98.0,
                close=102.0,
                volume=1000000,
                value_traded=100_000_000.0,
            ),
            PriceData(
                trading_date=add_days(date(2024, 1, 1), 2),
                open=102.0,
                high=108.0,
                low=101.0,
                close=106.0,
                volume=1100000,
                value_traded=110_000_000.0,
            ),
        ]
        result = self.engine.compute_all(1, prices)
        assert result[1].close_pos_in_range is not None

    def test_close_pos_in_range_zero_range(self) -> None:
        prices = [
            PriceData(
                trading_date=date(2024, 1, 1),
                open=100.0,
                high=100.0,
                low=100.0,
                close=100.0,
                volume=1000000,
                value_traded=100_000_000.0,
            ),
            PriceData(
                trading_date=date(2024, 1, 2),
                open=100.0,
                high=100.0,
                low=100.0,
                close=100.0,
                volume=1000000,
                value_traded=100_000_000.0,
            ),
        ]
        result = self.engine.compute_all(1, prices)
        assert result[0].close_pos_in_range is None

    def test_ma_20(self) -> None:
        prices = []
        for i in range(25):
            prices.append(
                PriceData(
                    trading_date=add_days(date(2024, 1, 1), i),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=98.0 + i,
                    close=102.0 + i,
                    volume=1000000,
                    value_traded=100_000_000.0,
                )
            )
        result = self.engine.compute_all(1, prices)
        for i in range(19):
            assert result[i].ma_20 is None
        assert result[19].ma_20 is not None

    def test_ma_65(self) -> None:
        prices = []
        for i in range(70):
            prices.append(
                PriceData(
                    trading_date=add_days(date(2024, 1, 1), i),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=98.0 + i,
                    close=102.0 + i,
                    volume=1000000,
                    value_traded=100_000_000.0,
                )
            )
        result = self.engine.compute_all(1, prices)
        for i in range(64):
            assert result[i].ma_65 is None
        assert result[64].ma_65 is not None

    def test_rs_252(self) -> None:
        prices = []
        for i in range(260):
            prices.append(
                PriceData(
                    trading_date=add_days(date(2024, 1, 1), i),
                    open=100.0 + i * 0.05,
                    high=105.0 + i * 0.05,
                    low=98.0 + i * 0.05,
                    close=102.0 + i * 0.05,
                    volume=1000000,
                    value_traded=100_000_000.0,
                )
            )
        result = self.engine.compute_all(1, prices)
        assert result[251].rs_252 is not None

    def test_avg_vol(self) -> None:
        prices = []
        for i in range(25):
            prices.append(
                PriceData(
                    trading_date=add_days(date(2024, 1, 1), i),
                    open=100.0,
                    high=105.0,
                    low=98.0,
                    close=102.0,
                    volume=1000000 + i * 10000,
                    value_traded=100_000_000.0,
                )
            )
        result = self.engine.compute_all(1, prices)
        for i in range(19):
            assert result[i].vol_20 is None
        assert result[19].vol_20 is not None

    def test_avg_dollar_vol(self) -> None:
        prices = []
        for i in range(25):
            prices.append(
                PriceData(
                    trading_date=add_days(date(2024, 1, 1), i),
                    open=100.0,
                    high=105.0,
                    low=98.0,
                    close=102.0,
                    volume=1000000,
                    value_traded=100_000_000.0 + i * 1_000_000,
                )
            )
        result = self.engine.compute_all(1, prices)
        for i in range(19):
            assert result[i].dollar_vol_20 is None
        assert result[19].dollar_vol_20 is not None

    def test_compute_features_convenience_function(self) -> None:
        prices = [
            PriceData(
                trading_date=date(2024, 1, 1),
                open=100.0,
                high=105.0,
                low=98.0,
                close=102.0,
                volume=1000000,
                value_traded=100_000_000.0,
            ),
            PriceData(
                trading_date=date(2024, 1, 2),
                open=102.0,
                high=108.0,
                low=101.0,
                close=106.0,
                volume=1100000,
                value_traded=110_000_000.0,
            ),
        ]
        result = compute_features(1, prices)
        assert len(result) == 2
        assert isinstance(result[0], DailyFeatures)


class TestPriceData:
    def test_price_data_creation(self) -> None:
        pd = PriceData(
            trading_date=date(2024, 1, 1),
            open=100.0,
            high=105.0,
            low=98.0,
            close=102.0,
            volume=1000000,
            value_traded=100_000_000.0,
        )
        assert pd.trading_date == date(2024, 1, 1)
        assert pd.close == 102.0

    def test_price_data_optional_value_traded(self) -> None:
        pd = PriceData(
            trading_date=date(2024, 1, 1),
            open=100.0,
            high=105.0,
            low=98.0,
            close=102.0,
            volume=1000000,
            value_traded=None,
        )
        assert pd.value_traded is None


class TestDailyFeatures:
    def test_daily_features_defaults(self) -> None:
        df = DailyFeatures(
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
        assert df.symbol_id == 1
        assert df.ret_1d is None
